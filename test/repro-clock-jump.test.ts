/**
 * CLOCK-JUMP — wall-clock jump resilience at the component level (unit-ish).
 *
 * The "restart with a clock jump" scenario: an operator restarts the server on a
 * machine whose wall clock jumped (NTP step, VM resume, DST-broken TZ data, a
 * plain misconfigured RTC). We cannot move the OS clock in CI, but bun:test's
 * `setSystemTime()` fakes `Date.now()` in-process, and QueueManager runs fully
 * in-process — so every time comparison the server makes (delayed-job `runAt`,
 * lock `expiresAt`, cron `nextRun`, recovery) can be exercised under real jumps.
 *
 * IMPORTANT setSystemTime caveat driving the test design: only `Date.now()` is
 * faked — `setTimeout`/`setInterval` still run on REAL time. So:
 *   - lock-expiry sweeps are driven by configuring `stallCheckMs` very low (the
 *     real interval fires often, each run reads the FAKED Date.now()),
 *   - cron ticks are driven by calling the scheduler's internal `tick()`
 *     directly (the precise setTimeout it arms is real-time and would not
 *     re-target after a jump until the 60s safety interval).
 *
 * Time-flow map (verified while writing these tests):
 *   - delayed promotion/demotion is LAZY: pull asks GroupScheduler to classify
 *     secondary ready/delayed indexes against `Date.now()`. There is no
 *     promotion timer, so jumps are reflected instantly.
 *   - lock expiry: `checkExpiredLocks` (src/application/lockManager.ts:40) runs on
 *     a real setInterval of `stallCheckMs` and compares `lock.expiresAt` with
 *     `Date.now()`; expired -> requeue with attempts++ and lock deleted.
 *   - late-ACK semantics (#101/#33): QueueManager.ack (queueManager.ts:350) —
 *     grace window via isExpiredButOwned, or completion of the stall-retried
 *     queued job via completeStallRetriedJob. Never a double-completion.
 *   - cron: nextRun is an absolute ms timestamp persisted in SQLite; on restart
 *     `CronScheduler.load()` (cronScheduler.ts:230) recomputes past nextRun into
 *     the future when skipMissedOnRestart (DEFAULT true, domain/types/cron.ts:102);
 *     with the flag off, one tick fires ONE catch-up job and then recomputes
 *     nextRun from `Date.now()` — never one job per missed slot.
 *   - recovery: `recover()` (backgroundTasks.ts:227) reloads pending jobs with
 *     their persisted ABSOLUTE run_at; delayed-vs-waiting is re-derived against
 *     the current (possibly jumped) clock.
 */

import { afterEach, describe, expect, it, setSystemTime } from 'bun:test';
import { existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import type { QueueManagerConfig } from '../src/application/types';
import type { Job } from '../src/domain/types/job';

const HOUR = 3_600_000;

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

const openManagers: QueueManager[] = [];
const dbPaths: string[] = [];

function freshDb(tag: string): string {
  const p = join(
    tmpdir(),
    `bunq-clockjump-${tag}-${process.pid}-${Math.random().toString(36).slice(2)}.db`
  );
  dbPaths.push(p);
  return p;
}

function cleanDb(db: string): void {
  for (const suffix of ['', '-wal', '-shm']) {
    const f = `${db}${suffix}`;
    if (existsSync(f)) rmSync(f, { force: true });
  }
}

function open(config: QueueManagerConfig = {}): QueueManager {
  const m = new QueueManager(config);
  openManagers.push(m);
  return m;
}

/** Shut a manager down mid-test (restart simulation). Safe to re-run in afterEach. */
function close(m: QueueManager): void {
  try {
    m.shutdown();
  } catch {
    /* double-shutdown tolerated in teardown */
  }
  const i = openManagers.indexOf(m);
  if (i !== -1) openManagers.splice(i, 1);
}

/** Force the buffered SQLite write-buffer to disk (cron-fired pushes are buffered). */
function flushStorage(m: QueueManager): void {
  const storage = (m as unknown as { storage: { flushWriteBuffer(): number } | null }).storage;
  storage?.flushWriteBuffer();
}

/**
 * Drive one cron scheduler tick against the CURRENT (possibly faked) Date.now().
 * The scheduler's precise setTimeout is real-time, so after a fake jump the only
 * in-process paths that would notice are the 60s safety interval or a restart —
 * far too slow for a unit test. tick() is the exact function both of those call.
 */
async function cronTick(m: QueueManager): Promise<void> {
  const sched = (m as unknown as { cronScheduler: { tick(): Promise<void> } }).cronScheduler;
  await sched.tick();
}

/** Poll a condition on REAL time (performance.now is monotonic, unaffected by setSystemTime). */
async function waitFor(
  cond: () => boolean | Promise<boolean>,
  timeoutMs: number
): Promise<boolean> {
  const t0 = performance.now();
  while (performance.now() - t0 < timeoutMs) {
    if (await cond()) return true;
    await Bun.sleep(20);
  }
  return await cond();
}

/** Pull everything currently ready from a queue, acking each job. Returns ids. */
async function drainReady(m: QueueManager, queue: string, max = 100): Promise<string[]> {
  const ids: string[] = [];
  for (let i = 0; i < max; i++) {
    const job = await m.pull(queue, 0);
    if (!job) break;
    ids.push(String(job.id));
    await m.ack(job.id, { drained: true });
  }
  return ids;
}

afterEach(() => {
  // ALWAYS restore the real clock first — a leaked fake clock would poison
  // every later test file in the run.
  setSystemTime();
  for (const m of openManagers.splice(0)) {
    try {
      m.shutdown();
    } catch {
      /* ignore */
    }
  }
  for (const db of dbPaths.splice(0)) cleanDb(db);
});

// ---------------------------------------------------------------------------
// CJ1 — forward jump, delayed jobs
// ---------------------------------------------------------------------------

describe('CJ1 — forward clock jump promotes delayed jobs (live and across restart)', () => {
  it('CJ1a: live +2h jump — 60s-delayed jobs promote and are each pullable exactly once', async () => {
    const db = freshDb('cj1a');
    const m = open({ dataPath: db });
    const Q = 'cj1a';

    const pushed: string[] = [];
    for (let i = 0; i < 5; i++) {
      const job = await m.push(Q, { data: { i }, delay: 60_000, durable: true });
      pushed.push(String(job.id));
    }

    // Before the jump: delayed, not pullable.
    for (const id of pushed) {
      expect(await m.getJobState(id as Job['id'])).toBe('delayed');
    }
    expect(await m.pull(Q, 0)).toBeNull();

    // Wall clock jumps forward 2 hours (NTP step / VM resume).
    setSystemTime(new Date(Date.now() + 2 * HOUR));

    // Promotion is lazy (isReady(job, now) at pull), so the jump is visible
    // immediately: every job promotes, none is lost, none is duplicated.
    const pulled = await drainReady(m, Q);
    expect(new Set(pulled)).toEqual(new Set(pushed));
    expect(pulled.length).toBe(5);
    expect(await m.pull(Q, 0)).toBeNull();
  }, 15_000);

  it('CJ1b: restart while the clock is jumped +2h — persisted absolute run_at promotes; no loss, no dupes', async () => {
    const db = freshDb('cj1b');
    const Q = 'cj1b';

    // Phase 1: normal clock — push durable delayed jobs, then shut down cleanly.
    const m1 = open({ dataPath: db });
    const pushed: string[] = [];
    for (let i = 0; i < 5; i++) {
      const job = await m1.push(Q, { data: { i }, delay: 60_000, durable: true });
      pushed.push(String(job.id));
    }
    for (const id of pushed) {
      expect(await m1.getJobState(id as Job['id'])).toBe('delayed');
    }
    close(m1);

    // Phase 2: the machine's clock jumped 2h forward while the server was down.
    setSystemTime(new Date(Date.now() + 2 * HOUR));
    const m2 = open({ dataPath: db });

    // run_at is stored as an ABSOLUTE timestamp; recovery re-derives
    // delayed-vs-waiting against the (jumped) clock, so the jobs are now ready.
    for (const id of pushed) {
      const state = await m2.getJobState(id as Job['id']);
      expect(['waiting', 'prioritized']).toContain(state);
    }

    // All 5 promote correctly: pullable exactly once each, none lost/duplicated.
    const pulled = await drainReady(m2, Q);
    expect(new Set(pulled)).toEqual(new Set(pushed));
    expect(pulled.length).toBe(5);
    expect(await m2.pull(Q, 0)).toBeNull();
  }, 20_000);
});

// ---------------------------------------------------------------------------
// CJ2 — backward jump, delayed jobs
// ---------------------------------------------------------------------------

describe('CJ2 — backward clock jump: no early runs, no permanent wedge', () => {
  it('CJ2a: -1h jump — a 60s-delayed job does not run early, and still promotes once the clock passes its original run_at', async () => {
    const db = freshDb('cj2a');
    const m = open({ dataPath: db });
    const Q = 'cj2a';
    const t0 = Date.now();

    const job = await m.push(Q, { data: { n: 1 }, delay: 60_000, durable: true });

    // Clock jumps BACK one hour.
    setSystemTime(new Date(t0 - HOUR));

    // Not early: runAt (t0+60s) is far in the "future" of the jumped clock.
    expect(await m.pull(Q, 0)).toBeNull();
    expect(await m.getJobState(job.id)).toBe('delayed');

    // A mid-way recovery of the clock (still before run_at) must not run it either.
    setSystemTime(new Date(t0 + 30_000));
    expect(await m.pull(Q, 0)).toBeNull();
    expect(await m.getJobState(job.id)).toBe('delayed');

    // Once the clock passes the ORIGINAL absolute run_at, the job promotes —
    // it is not stuck forever. (runAt comparisons are pure `runAt <= now`, no
    // cached "already checked" flag exists that a backward jump could poison.)
    setSystemTime(new Date(t0 + 61_000));
    const pulled = await m.pull(Q, 0);
    expect(pulled?.id).toBe(job.id);
    await m.ack(job.id, { done: true });
    expect(await m.getJobState(job.id)).toBe('completed');
  }, 15_000);

  it('CJ2b: -1h jump retro-delays an already-READY job (documented actual behavior — self-healing, not a wedge)', async () => {
    const db = freshDb('cj2b');
    const m = open({ dataPath: db });
    const Q = 'cj2b';
    const t0 = Date.now();

    // No delay: runAt = t0, the job is immediately ready.
    const job = await m.push(Q, { data: { n: 1 }, durable: true });
    expect(await m.getJobState(job.id)).toBe('waiting');

    // Clock jumps BACK one hour.
    setSystemTime(new Date(t0 - HOUR));

    // ACTUAL BEHAVIOR (documented, by design of absolute-timestamp comparison):
    // a job that was ready flips back to 'delayed' and becomes unpullable until
    // the wall clock re-reaches its runAt (t0). Under a real NTP step-back this
    // is up to a 1h availability gap for already-enqueued work — but it is
    // self-healing and loses nothing, so it is recorded here as behavior, not
    // failed as a defect. (A monotonic-clock scheduler would avoid the gap.)
    expect(await m.getJobState(job.id)).toBe('delayed');
    expect(await m.pull(Q, 0)).toBeNull();

    // Self-heals: as soon as the clock reaches runAt again, the job runs.
    setSystemTime(new Date(t0 + 1));
    const pulled = await m.pull(Q, 0);
    expect(pulled?.id).toBe(job.id);
    await m.ack(job.id);
    expect(await m.getJobState(job.id)).toBe('completed');
  }, 15_000);

  it('CJ2c: a ready grouped job is also demoted after a backward jump', async () => {
    const m = open();
    const Q = 'cj2c-grouped';
    const t0 = Date.now();
    const job = await m.push(Q, { data: { n: 1 }, groupId: 'tenant-a' });

    setSystemTime(new Date(t0 - HOUR));
    expect(await m.getJobState(job.id)).toBe('delayed');
    expect(await m.pull(Q, 0, undefined, { concurrency: 1 })).toBeNull();

    setSystemTime(new Date(t0 + 1));
    expect((await m.pull(Q, 0, undefined, { concurrency: 1 }))?.id).toBe(job.id);
    await m.ack(job.id);
  });
});

// ---------------------------------------------------------------------------
// CJ3 — lock TTL under a forward jump
// ---------------------------------------------------------------------------

describe('CJ3 — lock TTL under forward jump: exactly-once reclaim, no double-completion', () => {
  it('CJ3a: +10s jump past a 5s lock TTL — sweep reclaims exactly once (attempts+1), late ACK from the old token completes without a duplicate run', async () => {
    const db = freshDb('cj3a');
    // stallCheckMs drives the checkExpiredLocks setInterval — REAL timers keep
    // running under setSystemTime, so a 25ms interval sweeps constantly and each
    // sweep evaluates the FAKED Date.now() against lock.expiresAt.
    const m = open({ dataPath: db, stallCheckMs: 25 });
    const Q = 'cj3a';

    const job = await m.push(Q, { data: { n: 1 }, durable: true });
    const { job: leased, token } = await m.pullWithLock(Q, 'worker-1', 0, 5_000);
    expect(leased?.id).toBe(job.id);
    expect(token).not.toBeNull();
    expect(leased!.attempts).toBe(0);
    expect(await m.getJobState(job.id)).toBe('active');

    // Wall clock jumps +10s: past the 5s lock TTL, but safely below the 30s
    // stall interval + 5s grace, so ONLY the lock-expiry path is in play.
    setSystemTime(new Date(Date.now() + 10_000));

    // The sweep requeues the job (state: waiting — runAt is unchanged and in
    // the past of the jumped clock).
    expect(await waitFor(async () => (await m.getJobState(job.id)) === 'waiting', 4_000)).toBe(
      true
    );

    const requeued = await m.getJob(job.id);
    expect(requeued).not.toBeNull();
    expect(requeued!.attempts).toBe(1); // incremented exactly once
    expect(m.getLockInfo(job.id)).toBeNull(); // expired lock removed

    // Exactly-once: ~12 more sweep cycles must not reclaim it again.
    await Bun.sleep(300);
    const after = await m.getJob(job.id);
    expect(after!.attempts).toBe(1);
    expect(await m.getJobState(job.id)).toBe('waiting');

    // Late ACK from the worker whose lock expired (the #33/#101 family): the
    // job already finished on that worker, so the server completes the
    // requeued copy instead of letting it run a second time. It must complete
    // EXACTLY once and never be pullable again.
    await m.ack(job.id, { late: true }, token!);
    expect(await m.getJobState(job.id)).toBe('completed');
    expect(m.getResult(job.id)).toEqual({ late: true });
    expect(await m.pull(Q, 0)).toBeNull(); // no duplicate execution

    // A second ACK targets an already terminal job and is rejected, while the
    // first committed result remains authoritative.
    await expect(m.ack(job.id, { again: true }, token!)).rejects.toThrow(/not found/i);
    expect(await m.getJobState(job.id)).toBe('completed');
    expect(m.getResult(job.id)).toEqual({ late: true });
  }, 20_000);

  it('CJ3b: ACK arriving after a +10s jump past the TTL but BEFORE any sweep is graced per #101 — completed, not lost, attempts untouched', async () => {
    const db = freshDb('cj3b');
    // Default stallCheckMs (5s REAL) — no expiry sweep can fire during this
    // test's few-ms window, isolating the pure grace path in ack().
    const m = open({ dataPath: db });
    const Q = 'cj3b';

    const job = await m.push(Q, { data: { n: 1 }, durable: true });
    const { job: leased, token } = await m.pullWithLock(Q, 'worker-1', 0, 5_000);
    expect(leased?.id).toBe(job.id);

    // Jump past the TTL: verifyLock now fails (expiresAt < faked now)...
    setSystemTime(new Date(Date.now() + 10_000));

    // ...but the job is still in processing, the token still matches, and the
    // lock belongs to the current lease (createdAt >= startedAt), so
    // isExpiredButOwned grants the #101 grace and the completion is honored.
    await m.ack(job.id, { graced: true }, token!);
    expect(await m.getJobState(job.id)).toBe('completed');
    expect(m.getResult(job.id)).toEqual({ graced: true });

    // Never requeued, never re-runnable: exactly one execution.
    expect(await m.pull(Q, 0)).toBeNull();
    const completed = await m.getJob(job.id);
    expect(completed?.attempts ?? 0).toBe(0);
  }, 15_000);
});

// ---------------------------------------------------------------------------
// CJ4 — cron across restart with a forward jump
// ---------------------------------------------------------------------------

describe('CJ4 — cron restart with +3h jump: sane next_run, no backlog storm', () => {
  it('CJ4a: default skipMissedOnRestart — after a +3h jump restart, next_run is recomputed to the future and ZERO missed slots fire', async () => {
    const db = freshDb('cj4a');
    const Q = 'qcj4a';
    const m1 = open({ dataPath: db });

    const cron = m1.addCron({
      name: 'cj4a-cron',
      queue: Q,
      data: { task: 'tick' },
      schedule: '* * * * *', // every minute
      timezone: 'UTC',
      // skipMissedOnRestart defaults to TRUE (domain/types/cron.ts)
    });
    const firstRun = cron.nextRun;
    expect(firstRun).toBeGreaterThan(Date.now());

    // Let exactly one run fire: move the clock to its slot and drive tick()
    // (the scheduler's precise setTimeout is real-time and won't re-target).
    setSystemTime(new Date(firstRun + 10));
    await cronTick(m1);
    expect(m1.getCron('cj4a-cron')!.executions).toBe(1);
    expect(m1.count(Q)).toBe(1); // the one fired job sits in the queue
    flushStorage(m1); // cron-fired pushes are buffered — force them to disk
    close(m1);

    // Restart 3h (+5s to sit mid-minute) later on the same DB — the persisted
    // next_run is now ~3h in the past (180 missed minutes).
    setSystemTime(new Date(firstRun + 3 * HOUR + 5_000));
    const m2 = open({ dataPath: db });

    // Give any zero-delay startup timer a real-time chance to fire wrongly.
    await Bun.sleep(400);

    // Catch-up policy (default): missed slots are SKIPPED. next_run was
    // recomputed into the future and persisted; executions preserved; the only
    // job in the queue is the one fired before the jump. No backlog storm.
    const reloaded = m2.getCron('cj4a-cron');
    expect(reloaded).toBeDefined();
    expect(reloaded!.nextRun).toBeGreaterThan(Date.now()); // monotonic, future
    expect(reloaded!.executions).toBe(1);
    expect(m2.count(Q)).toBe(1); // 1 pre-jump job, 0 backlogged fires
  }, 20_000);

  it('CJ4b: skipMissedOnRestart:false — the +3h backlog collapses into ONE catch-up fire, not one per missed minute', async () => {
    const db = freshDb('cj4b');
    const Q = 'qcj4b';
    const m1 = open({ dataPath: db });

    const cron = m1.addCron({
      name: 'cj4b-cron',
      queue: Q,
      data: { task: 'tick' },
      schedule: '* * * * *',
      timezone: 'UTC',
      skipMissedOnRestart: false, // opt IN to catch-up
    });
    const firstRun = cron.nextRun;

    setSystemTime(new Date(firstRun + 10));
    await cronTick(m1);
    expect(m1.count(Q)).toBe(1);

    // A worker consumes the fired job before shutdown (production-faithful, and
    // it releases the cron's preventOverlap uniqueKey `cron:<name>` — crons
    // default to preventOverlap:true, so an unconsumed pre-jump job would
    // dedup-swallow the catch-up push and hide what this test measures).
    const preJump = await m1.pull(Q, 0);
    expect(preJump).not.toBeNull();
    await m1.ack(preJump!.id, { done: true });
    expect(m1.count(Q)).toBe(0);
    flushStorage(m1);
    close(m1);

    // Restart with the clock +3h — next_run is ~180 slots in the past.
    setSystemTime(new Date(firstRun + 3 * HOUR + 5_000));
    const m2 = open({ dataPath: db });

    // The startup scheduleNext arms a 0ms timer for the overdue cron; its tick
    // fires ONE catch-up job and recomputes next_run from Date.now().
    expect(await waitFor(() => m2.count(Q) >= 1, 4_000)).toBe(true);

    // ...and it stays at exactly ONE catch-up — tick() computes the new
    // next_run from the CURRENT time, so the 180 missed slots never replay.
    await Bun.sleep(500);
    expect(m2.count(Q)).toBe(1); // exactly 1 catch-up fire, not ~180
    const reloaded = m2.getCron('cj4b-cron');
    expect(reloaded!.executions).toBe(2);
    expect(reloaded!.nextRun).toBeGreaterThan(Date.now()); // future, monotonic
  }, 20_000);
});

// ---------------------------------------------------------------------------
// CJ5 — backward jump + live cron
// ---------------------------------------------------------------------------

describe('CJ5 — backward jump on a live cron: no early fire, next_run monotonic', () => {
  it('CJ5: -1h jump — the live scheduler does not fire early, next_run never regresses, and the slot still fires exactly once when reached', async () => {
    const m = open(); // in-memory: this targets the live scheduler component
    const Q = 'qcj5';

    const cron = m.addCron({
      name: 'cj5-cron',
      queue: Q,
      data: { task: 'tick' },
      schedule: '* * * * *',
      timezone: 'UTC',
    });
    const nextRun0 = cron.nextRun;
    expect(nextRun0).toBeGreaterThan(Date.now());

    // Wall clock jumps BACK one hour.
    setSystemTime(new Date(Date.now() - HOUR));

    // Drive the live tick() repeatedly (what the safety interval would do):
    // nextRun is now far in the "future", so nothing may fire and nextRun must
    // not be recomputed/regressed by a tick that does no work.
    for (let i = 0; i < 3; i++) {
      await cronTick(m);
    }
    expect(m.count(Q)).toBe(0); // no early fire
    const during = m.getCron('cj5-cron')!;
    expect(during.executions).toBe(0);
    expect(during.nextRun).toBe(nextRun0); // monotonic: unchanged, not regressed

    // The clock recovers and reaches the ORIGINAL slot: fires exactly once.
    setSystemTime(new Date(nextRun0 + 10));
    await cronTick(m);
    expect(m.count(Q)).toBe(1);
    const after = m.getCron('cj5-cron')!;
    expect(after.executions).toBe(1);
    expect(after.nextRun).toBeGreaterThan(nextRun0); // strictly advanced

    // An immediate re-tick at the same instant must not double-fire the slot.
    await cronTick(m);
    expect(m.count(Q)).toBe(1);
    expect(m.getCron('cj5-cron')!.executions).toBe(1);
  }, 15_000);
});
