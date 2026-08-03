/**
 * CHAOS (crash-recovery) — hard-kill durability under SIGKILL.
 *
 * The single most important property for a 24/7 deployment: a DURABLE job, once
 * acknowledged by the server, is NEVER lost when the process dies violently
 * (`kill -9` / SIGKILL — no graceful shutdown, no flush). These tests spawn the
 * REAL server as a subprocess with on-disk SQLite (WAL), push durable jobs, then
 * SIGKILL the process at various points, restart against the SAME database, and
 * assert:
 *
 *   - NO LOSS: every durable job that got an ok:true PUSH is still present after
 *     restart (GetState !== 'unknown'). Recovery routes an interrupted 'active'
 *     job back to waiting/delayed (attempts++, stallCount++) — never drops it
 *     (src/application/backgroundTasks.ts recover()).
 *   - COMPLETION PERSISTS: a job ACKed and given time to flush stays 'completed'
 *     across a crash — it does not silently re-run.
 *   - NO WEDGE ON RESTART: the restarted server accepts connections and serves
 *     queries (recovery does not deadlock).
 *
 * Buffered (non-durable) jobs are intentionally excluded: the write buffer's
 * ~10ms window is a documented throughput/durability trade-off, not a bug.
 */

import { afterAll, afterEach, describe, expect, it } from 'bun:test';
import { existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { TcpClient } from '../src/client/tcp/client';

type Subprocess = ReturnType<typeof Bun.spawn>;

interface Server {
  proc: Subprocess;
  port: number;
  db: string;
}

let active: Server | null = null;
let activeClient: TcpClient | null = null;

function randomPort(): number {
  return 20000 + Math.floor(Math.random() * 20000);
}

/** A port confirmed free right now (bind+close), to avoid cross-test collisions. */
function getFreePort(): number {
  for (let i = 0; i < 50; i++) {
    const port = randomPort();
    try {
      const l = Bun.listen({ hostname: '127.0.0.1', port, socket: { data() {} } });
      l.stop();
      return port;
    } catch {
      /* taken — try another */
    }
  }
  throw new Error('no free port found');
}

// 60s: CI runners cold-start the real server binary much slower than a dev machine
async function waitPort(port: number, timeoutMs = 60000): Promise<void> {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    try {
      const s = await Bun.connect({ hostname: '127.0.0.1', port, socket: { data() {} } });
      s.end();
      return;
    } catch {
      await Bun.sleep(100);
    }
  }
  throw new Error(`server not ready on :${port}`);
}

/** Spawn the real server against a given db + port. Waits until it accepts TCP. */
async function spawnServer(db: string, port: number): Promise<Server> {
  const proc = Bun.spawn([process.execPath, 'run', 'src/main.ts'], {
    cwd: process.cwd(),
    env: {
      ...process.env,
      BUNQUEUE_EMBEDDED: '',
      TCP_PORT: String(port),
      // The HTTP endpoint is not part of this test. Let the kernel allocate it
      // so a parallel suite cannot make the broker exit on an adjacent-port
      // collision before the TCP recovery endpoint becomes ready.
      HTTP_PORT: '0',
      BUNQUEUE_DATA_PATH: db,
      LOG_LEVEL: 'error',
    },
    stdout: 'ignore',
    stderr: 'ignore',
  });
  await waitPort(port);
  const s: Server = { proc, port, db };
  active = s;
  return s;
}

/** Fresh connected client to the current server. */
async function connect(port: number): Promise<TcpClient> {
  if (activeClient) {
    try {
      activeClient.close();
    } catch {
      /* ignore */
    }
  }
  const c = new TcpClient({
    host: '127.0.0.1',
    port,
    autoReconnect: false,
    pingInterval: 0,
    commandTimeout: 5000,
    connectTimeout: 5000,
  });
  await c.connect();
  activeClient = c;
  return c;
}

/** SIGKILL the server and wait for the OS to reap it (no graceful shutdown). */
async function hardKill(s: Server): Promise<void> {
  if (activeClient) {
    try {
      activeClient.close();
    } catch {
      /* ignore */
    }
    activeClient = null;
  }
  s.proc.kill(9); // SIGKILL — violent, no flush, no cleanup
  await s.proc.exited;
  // Small settle so the OS releases the listening port for the restart.
  await Bun.sleep(200);
}

function cleanDb(db: string): void {
  for (const suffix of ['', '-wal', '-shm']) {
    const f = `${db}${suffix}`;
    if (existsSync(f)) rmSync(f, { force: true });
  }
}

afterEach(async () => {
  if (activeClient) {
    try {
      activeClient.close();
    } catch {
      /* ignore */
    }
    activeClient = null;
  }
  if (active) {
    try {
      active.proc.kill(9);
      await active.proc.exited;
    } catch {
      /* ignore */
    }
    cleanDb(active.db);
    active = null;
  }
});

// Safety net: if any test times out before its afterEach settles, make sure no
// spawned server child is left holding a port / tmpdir DB into the next file.
afterAll(async () => {
  if (active) {
    try {
      active.proc.kill(9);
      await active.proc.exited;
    } catch {
      /* ignore */
    }
    cleanDb(active.db);
    active = null;
  }
});

describe('CRASH-RECOVERY — durable jobs survive SIGKILL', () => {
  it('CR1: durable jobs are never lost across an immediate SIGKILL (3 crash cycles)', async () => {
    const db = join(tmpdir(), `bunq-cr1-${process.pid}-${randomPort()}.db`);
    cleanDb(db);
    const port = getFreePort();
    const N = 30;
    const QUEUE = 'cr1';
    const allIds: string[] = [];

    let s = await spawnServer(db, port);
    let c = await connect(port);

    // 3 cycles: push a batch of durable jobs, SIGKILL, restart, assert none lost.
    for (let cycle = 0; cycle < 3; cycle++) {
      const cycleIds: string[] = [];
      for (let i = 0; i < N; i++) {
        const res = await c.send({
          cmd: 'PUSH',
          queue: QUEUE,
          data: { cycle, i },
          durable: true,
        });
        expect(res.ok).toBe(true); // ok:true means the durable write reached disk
        cycleIds.push(String(res.id));
      }
      allIds.push(...cycleIds);

      // Violent crash — no graceful shutdown.
      await hardKill(s);

      // Restart against the same DB and verify EVERY durable job ever pushed is
      // still present (recoverable), not just this cycle's.
      s = await spawnServer(db, port);
      c = await connect(port);

      for (const id of allIds) {
        const st = await c.send({ cmd: 'GetState', queue: QUEUE, id });
        expect(st.state).not.toBe('unknown'); // 'unknown' == lost
      }
    }

    // Final tally: all 90 durable jobs accounted for across queue states.
    const counts = await c.send({ cmd: 'GetJobCounts', queue: QUEUE });
    const cnt = counts.counts as Record<string, number>;
    const total =
      (cnt.waiting ?? 0) +
      (cnt.prioritized ?? 0) +
      (cnt.delayed ?? 0) +
      (cnt.active ?? 0) +
      (cnt.paused ?? 0);
    expect(total).toBe(allIds.length);
  }, 150000); // 4 server spawns, budget ~15s cold-start each plus churn

  it('CR2: an ACKed durable job stays completed across SIGKILL (no silent re-run)', async () => {
    const db = join(tmpdir(), `bunq-cr2-${process.pid}-${randomPort()}.db`);
    cleanDb(db);
    const port = getFreePort();
    const QUEUE = 'cr2';

    let s = await spawnServer(db, port);
    let c = await connect(port);

    const push = await c.send({ cmd: 'PUSH', queue: QUEUE, data: { n: 1 }, durable: true });
    const id = String(push.id);

    const pull = await c.send({ cmd: 'PULL', queue: QUEUE, owner: 'w', lockTtl: 30000 });
    expect((pull.job as { id: string } | null)?.id).toBe(id);
    const ack = await c.send({
      cmd: 'ACK',
      id,
      token: pull.token as string,
      result: { done: true },
    });
    expect(ack.ok).toBe(true);
    expect(await stateOf(c, QUEUE, id)).toBe('completed');

    // Give the completion write time to flush past the ~10ms buffer window.
    await Bun.sleep(400);

    await hardKill(s);
    s = await spawnServer(db, port);
    c = await connect(port);

    // The job must NOT be re-queued as waiting — it stays completed.
    const st = await stateOf(c, QUEUE, id);
    expect(st).toBe('completed');
  }, 150000);

  it('CR3: crash amid a produce/consume churn loses no durable job; flushed completions persist', async () => {
    const db = join(tmpdir(), `bunq-cr3-${process.pid}-${randomPort()}.db`);
    cleanDb(db);
    const port = getFreePort();
    const QUEUE = 'cr3';
    const M = 50;

    let s = await spawnServer(db, port);
    let c = await connect(port);

    // Push 50 durable jobs.
    const ids: string[] = [];
    for (let i = 0; i < M; i++) {
      const p = await c.send({ cmd: 'PUSH', queue: QUEUE, data: { i }, durable: true });
      expect(p.ok).toBe(true);
      ids.push(String(p.id));
    }

    // Process and ACK ~half, then let them flush.
    const completedBeforeCrash: string[] = [];
    for (let i = 0; i < M / 2; i++) {
      const pull = await c.send({ cmd: 'PULL', queue: QUEUE, owner: 'w', lockTtl: 30000 });
      const job = pull.job as { id: string } | null;
      if (!job?.id) break;
      const ack = await c.send({ cmd: 'ACK', id: job.id, token: pull.token as string });
      if (ack.ok) completedBeforeCrash.push(job.id);
    }
    await Bun.sleep(400); // flush completions to disk

    // Violent crash mid-churn.
    await hardKill(s);
    s = await spawnServer(db, port);
    c = await connect(port);

    // NO LOSS: every one of the 50 durable ids is still present after recovery.
    for (const id of ids) {
      const st = await stateOf(c, QUEUE, id);
      expect(st).not.toBe('unknown');
    }

    // COMPLETION PERSISTS: the jobs acked-and-flushed before the crash are still
    // completed (not re-queued for a duplicate run).
    for (const id of completedBeforeCrash) {
      const st = await stateOf(c, QUEUE, id);
      expect(st).toBe('completed');
    }

    // The remaining (never-processed) durable jobs are recoverable and drainable
    // — the queue is not wedged after recovery.
    const drained = new Set<string>();
    const deadline = Date.now() + 8000;
    while (drained.size < M - completedBeforeCrash.length && Date.now() < deadline) {
      const pull = await c.send({
        cmd: 'PULL',
        queue: QUEUE,
        owner: 'w2',
        lockTtl: 30000,
        timeout: 0,
      });
      const job = pull.job as { id: string } | null;
      if (!job?.id) {
        await Bun.sleep(50);
        continue;
      }
      await c.send({ cmd: 'ACK', id: job.id, token: pull.token as string });
      drained.add(job.id);
    }
    // Everything that wasn't already completed drains cleanly post-recovery.
    expect(drained.size + completedBeforeCrash.length).toBeGreaterThanOrEqual(M);
  }, 150000); // 2 server spawns, aligned with the other 2-spawn tests in this file
});

describe('CRASH-RECOVERY — control-state & DLQ persist across SIGKILL', () => {
  it('CR4: a paused queue is still paused after a crash, and its jobs are not processed on restart', async () => {
    const db = join(tmpdir(), `bunq-cr4-${process.pid}-${randomPort()}.db`);
    cleanDb(db);
    const port = getFreePort();
    const QUEUE = 'cr4';

    let s = await spawnServer(db, port);
    let c = await connect(port);

    // Push durable jobs, then pause the queue and let the queue_state row persist.
    const ids: string[] = [];
    for (let i = 0; i < 10; i++) {
      const p = await c.send({ cmd: 'PUSH', queue: QUEUE, data: { i }, durable: true });
      ids.push(String(p.id));
    }
    const pause = await c.send({ cmd: 'Pause', queue: QUEUE });
    expect(pause.ok).toBe(true);
    await Bun.sleep(300); // let queue_state write-through hit disk

    await hardKill(s);
    s = await spawnServer(db, port);
    c = await connect(port);

    // Paused state survived the crash (#100 write-through).
    const isPaused = await c.send({ cmd: 'IsPaused', queue: QUEUE });
    expect(isPaused.paused).toBe(true);

    // A pull returns nothing while paused — the jobs are held, not processed.
    const pull = await c.send({ cmd: 'PULL', queue: QUEUE, owner: 'w', timeout: 0 });
    expect((pull.job as unknown) ?? null).toBeNull();

    // After resume, the durable jobs are all still there and drainable.
    await c.send({ cmd: 'Resume', queue: QUEUE });
    const drained = new Set<string>();
    const deadline = Date.now() + 8000;
    while (drained.size < ids.length && Date.now() < deadline) {
      const p = await c.send({ cmd: 'PULL', queue: QUEUE, owner: 'w', timeout: 0 });
      const job = p.job as { id: string } | null;
      if (!job?.id) {
        await Bun.sleep(30);
        continue;
      }
      drained.add(job.id);
      await c.send({ cmd: 'ACK', id: job.id, token: p.token as string });
    }
    expect(drained.size).toBe(ids.length);
  }, 150000);

  it('CR5: a job failed into the DLQ survives a crash (still in DLQ after restart)', async () => {
    const db = join(tmpdir(), `bunq-cr5-${process.pid}-${randomPort()}.db`);
    cleanDb(db);
    const port = getFreePort();
    const QUEUE = 'cr5';

    let s = await spawnServer(db, port);
    let c = await connect(port);

    const push = await c.send({ cmd: 'PUSH', queue: QUEUE, data: { n: 1 }, durable: true });
    const id = String(push.id);
    const pull = await c.send({ cmd: 'PULL', queue: QUEUE, owner: 'w', lockTtl: 30000 });
    // Fail terminally → straight to DLQ.
    const fail = await c.send({
      cmd: 'FAIL',
      id,
      token: pull.token as string,
      error: 'boom',
      unrecoverable: true,
    });
    expect(fail.ok).toBe(true);
    // Confirm it's in the DLQ before the crash.
    const dlqBefore = await c.send({ cmd: 'Dlq', queue: QUEUE, count: 100 });
    expect((dlqBefore.jobs as unknown[]).some((j) => String((j as { id: string }).id) === id)).toBe(
      true
    );
    await Bun.sleep(300); // flush DLQ persistence

    await hardKill(s);
    s = await spawnServer(db, port);
    c = await connect(port);

    // DLQ entry survived the crash.
    const dlqAfter = await c.send({ cmd: 'Dlq', queue: QUEUE, count: 100 });
    const found = (dlqAfter.jobs as unknown[]).some((j) => String((j as { id: string }).id) === id);
    expect(found).toBe(true);
    // And the job is NOT resurrected into a runnable state (that would be a
    // duplicate execution). It stays in its terminal DLQ/failed state — and it
    // is certainly not 'unknown' (lost) since we just confirmed it in the DLQ.
    const st = await stateOf(c, QUEUE, id);
    expect(['waiting', 'prioritized', 'delayed', 'active']).not.toContain(st);
    expect(st).not.toBe('unknown');
  }, 150000);

  it('CR6: a durable job ACTIVE at crash time is recovered (attempts incremented) and re-runnable — no orphan, no loss', async () => {
    const db = join(tmpdir(), `bunq-cr6-${process.pid}-${randomPort()}.db`);
    cleanDb(db);
    const port = getFreePort();
    const QUEUE = 'cr6';

    let s = await spawnServer(db, port);
    let c = await connect(port);

    const push = await c.send({ cmd: 'PUSH', queue: QUEUE, data: { n: 1 }, durable: true });
    const id = String(push.id);
    // Lease it → the durable row is now 'active' on disk. Crash without ACK.
    const pull = await c.send({ cmd: 'PULL', queue: QUEUE, owner: 'w', lockTtl: 30000 });
    expect((pull.job as { id: string; attempts?: number } | null)?.id).toBe(id);
    const attemptsBefore = (pull.job as { attempts?: number }).attempts ?? 0;
    await Bun.sleep(200); // ensure the active row is on disk

    await hardKill(s);
    s = await spawnServer(db, port);
    c = await connect(port);

    // Not lost: recovery routed the interrupted active job back to a runnable
    // state (waiting/prioritized/delayed), NOT stuck 'active', NOT 'unknown'.
    const st = await stateOf(c, QUEUE, id);
    expect(st).not.toBe('unknown');
    expect(st).not.toBe('active');

    // Re-runnable: it can be pulled again, and its attempts were incremented by
    // recovery (it counts the interrupted run as a stall).
    const repull = await pullUntil(c, QUEUE, id, 10000);
    expect(repull).not.toBeNull();
    const attemptsAfter = (repull as { attempts?: number }).attempts ?? 0;
    expect(attemptsAfter).toBeGreaterThan(attemptsBefore);

    // It completes cleanly on the second run.
    const ack = await c.send({ cmd: 'ACK', id, token: (repull as { token?: string }).token });
    expect(ack.ok).toBe(true);
    expect(await stateOf(c, QUEUE, id)).toBe('completed');
  }, 150000);

  it('CR7: crash fuzzing — CRASH_CYCLES kill/restart cycles amid durable pushes lose nothing cumulatively', async () => {
    const CYCLES = Number(process.env.CRASH_CYCLES ?? 12);
    const db = join(tmpdir(), `bunq-cr7-${process.pid}-${randomPort()}.db`);
    cleanDb(db);
    const port = getFreePort();
    const QUEUE = 'cr7';
    const allIds: string[] = [];

    let s = await spawnServer(db, port);
    let c = await connect(port);

    for (let cycle = 0; cycle < CYCLES; cycle++) {
      // Push a small batch of durable jobs.
      const batch = 5 + (cycle % 4);
      for (let i = 0; i < batch; i++) {
        const p = await c.send({ cmd: 'PUSH', queue: QUEUE, data: { cycle, i }, durable: true });
        if (p.ok) allIds.push(String(p.id));
      }
      // Randomly lease a few (leaving some active) before the kill, to vary the
      // crash point across waiting/active mid-write states.
      const lease = cycle % 3;
      for (let i = 0; i < lease; i++) {
        await c.send({ cmd: 'PULL', queue: QUEUE, owner: 'w', lockTtl: 30000, timeout: 0 });
      }
      await Bun.sleep(50);

      // Violent crash + restart against the same DB.
      await hardKill(s);
      s = await spawnServer(db, port);
      c = await connect(port);

      // Cumulative invariant: EVERY durable job pushed so far is still present.
      for (const id of allIds) {
        const st = await stateOf(c, QUEUE, id);
        expect(st).not.toBe('unknown'); // 'unknown' == lost
      }
    }

    // After all cycles, everything is drainable — no permanent orphan.
    const drained = new Set<string>();
    const deadline = Date.now() + 15000;
    while (drained.size < allIds.length && Date.now() < deadline) {
      const p = await c.send({ cmd: 'PULL', queue: QUEUE, owner: 'w', lockTtl: 30000, timeout: 0 });
      const job = p.job as { id: string } | null;
      if (!job?.id) {
        await Bun.sleep(40);
        continue;
      }
      await c.send({ cmd: 'ACK', id: job.id, token: p.token as string });
      drained.add(job.id);
    }
    expect(drained.size).toBe(allIds.length);
  }, 360000); // 13 server spawns (1 + CRASH_CYCLES=12 restarts), ~25s budget each
});

async function stateOf(c: TcpClient, queue: string, id: string): Promise<string> {
  const st = await c.send({ cmd: 'GetState', queue, id });
  return String(st.state);
}

/** Pull repeatedly until the given job id is leased, or timeout. */
async function pullUntil(
  c: TcpClient,
  queue: string,
  id: string,
  timeoutMs: number
): Promise<{ id: string; token?: string; attempts?: number } | null> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const pull = await c.send({ cmd: 'PULL', queue, owner: 'rw', lockTtl: 30000, timeout: 0 });
    const job = pull.job as { id: string; attempts?: number } | null;
    if (job?.id === id) return { ...job, token: pull.token as string };
    if (job?.id) {
      // A different job — release it back by NOT acking; re-queue via short lock
      // is unnecessary here since we pull with a fresh owner. Just continue.
    }
    await Bun.sleep(40);
  }
  return null;
}
