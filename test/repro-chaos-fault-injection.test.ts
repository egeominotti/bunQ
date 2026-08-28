/**
 * CHAOS (category 3) — fault injection & at-least-once semantics.
 *
 * These tests inject faults that a 24/7 deployment WILL hit — workers that die
 * mid-job (hard socket drop), locks that expire under contention, and clock skew
 * for cron — and assert the queue's delivery guarantees hold:
 *
 *   - AT-LEAST-ONCE: a job whose worker dies before ACK is redelivered (never
 *     silently lost).
 *   - NO SPURIOUS DOUBLE-DISPATCH: a job that a *live* worker heartbeated is NOT
 *     instantly re-dispatched when a different pooled socket drops; only
 *     lock-expiry / stall detection reclaims it (bounded), so it is neither lost
 *     nor run twice concurrently.
 *   - CRON MONOTONICITY UNDER SKEW: next-run computation always moves strictly
 *     forward regardless of wall-clock jumps / DST.
 *
 * Faults are injected through a REAL TCP server. The "dying worker" is a raw
 * framed socket we can `terminate()` (a true drop, not a graceful close) so the
 * server's connection-close recovery path is exercised exactly as in prod.
 *
 * Internals map (from source): server close handler releases a client's jobs;
 * `releaseClientJobs` requeues a job ONLY if its lock has renewalCount === 0
 * (never heartbeated) — heartbeated jobs are left for lock-expiry
 * (src/application/clientTracking.ts, lockManager.ts). Lock TTL comes from the
 * PULL `lockTtl` field; both background loops tick at `stallCheckMs`.
 */

import { describe, it, expect, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';
import { TcpClient } from '../src/client/tcp/client';
import { FrameParser } from '../src/infrastructure/server/protocol';
import {
  getNextCronRun,
  getNextIntervalRun,
  isDue,
} from '../src/infrastructure/scheduler/cronParser';
import { pack, unpack } from 'msgpackr';

function randomPort(): number {
  return 30000 + Math.floor(Math.random() * 25000);
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

async function waitFor<T>(
  fn: () => T | Promise<T>,
  timeoutMs: number,
  intervalMs = 20
): Promise<T | false> {
  const start = Date.now();
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() - start >= timeoutMs) return false;
    await Bun.sleep(intervalMs);
  }
}

/**
 * Minimal framed TCP client that can be HARD-dropped (`terminate`). Responses
 * are matched to requests by reqId. This is our "worker that dies mid-job".
 */
interface FramedClient {
  send(cmd: Record<string, unknown>, timeoutMs?: number): Promise<Record<string, unknown>>;
  terminate(): void; // hard drop (RST-ish) — no graceful FIN
  end(): void; // graceful close
}

async function openFramed(port: number): Promise<FramedClient> {
  const parser = new FrameParser();
  const pending = new Map<string, (r: Record<string, unknown>) => void>();
  let counter = 0;

  const socket = await Bun.connect({
    hostname: '127.0.0.1',
    port,
    socket: {
      data(_s, data: Buffer) {
        let frames: Uint8Array[];
        try {
          frames = parser.addData(data);
        } catch {
          return;
        }
        for (const f of frames) {
          let msg: Record<string, unknown>;
          try {
            msg = unpack(f) as Record<string, unknown>;
          } catch {
            continue;
          }
          const rid = msg.reqId as string | undefined;
          if (rid && pending.has(rid)) {
            pending.get(rid)?.(msg);
            pending.delete(rid);
          }
        }
      },
      close() {
        for (const resolve of pending.values()) resolve({ ok: false, error: 'socket closed' });
        pending.clear();
      },
      error() {
        for (const resolve of pending.values()) resolve({ ok: false, error: 'socket error' });
        pending.clear();
      },
    },
  });

  return {
    send(cmd, timeoutMs = 3000) {
      const reqId = `f${counter++}`;
      const frame = FrameParser.frame(pack({ ...cmd, reqId }));
      return new Promise((resolve) => {
        const timer = setTimeout(() => {
          pending.delete(reqId);
          resolve({ ok: false, error: 'timeout' });
        }, timeoutMs);
        pending.set(reqId, (r) => {
          clearTimeout(timer);
          resolve(r);
        });
        socket.write(frame);
        (socket as unknown as { flush?: () => void }).flush?.();
      });
    },
    terminate() {
      // A true hard drop when the runtime supports it (RST-ish, exercises the
      // dead-client release path); fall back to a graceful FIN otherwise. Never
      // both — `?? ` would call end() even after terminate() returns undefined.
      try {
        const s = socket as unknown as { terminate?: () => void; end: () => void };
        if (typeof s.terminate === 'function') s.terminate();
        else s.end();
      } catch {
        /* ignore */
      }
    },
    end() {
      try {
        socket.end();
      } catch {
        /* ignore */
      }
    },
  };
}

interface Harness {
  qm: QueueManager;
  server: TcpServer;
  port: number;
  clients: TcpClient[];
  framed: FramedClient[];
}
let harness: Harness | null = null;

function startServer(opts?: { stallCheckMs?: number }): Harness {
  const qm = new QueueManager(opts?.stallCheckMs ? { stallCheckMs: opts.stallCheckMs } : undefined);
  const port = getFreePort();
  const server = createTcpServer(qm, { hostname: '127.0.0.1', port });
  const h: Harness = { qm, server, port, clients: [], framed: [] };
  harness = h;
  return h;
}

async function openClient(h: Harness): Promise<TcpClient> {
  const c = new TcpClient({
    host: '127.0.0.1',
    port: h.port,
    autoReconnect: false,
    pingInterval: 0,
    commandTimeout: 3000,
    connectTimeout: 2000,
  });
  await c.connect();
  h.clients.push(c);
  return c;
}

async function openWorkerSocket(h: Harness): Promise<FramedClient> {
  const f = await openFramed(h.port);
  h.framed.push(f);
  return f;
}

afterEach(async () => {
  if (harness) {
    for (const c of harness.clients) {
      try {
        c.close();
      } catch {
        /* ignore */
      }
    }
    for (const f of harness.framed) {
      try {
        f.end();
      } catch {
        /* ignore */
      }
    }
    try {
      harness.server.stop();
    } catch {
      /* ignore */
    }
    try {
      harness.qm.shutdown();
    } catch {
      /* ignore */
    }
    harness = null;
  }
  await Bun.sleep(120);
});

describe('CHAOS — worker death & at-least-once redelivery', () => {
  it('C1: hard socket drop of a non-heartbeated worker → job is REDELIVERED to another worker (at-least-once)', async () => {
    const h = startServer();
    const ctrl = await openClient(h);
    const QUEUE = 'chaos-c1';

    const push = await ctrl.send({ cmd: 'PUSH', queue: QUEUE, data: { n: 1 } });
    expect(push.ok).toBe(true);
    const jobId = String(push.id);

    // Worker A pulls with a lock (owner set) but NEVER heartbeats.
    const workerA = await openWorkerSocket(h);
    const pullA = await workerA.send({ cmd: 'PULL', queue: QUEUE, owner: 'wA', lockTtl: 30000 });
    expect(pullA.ok).toBe(true);
    expect((pullA.job as { id: string } | null)?.id).toBe(jobId);

    // A dies mid-job — hard drop before ACK.
    workerA.terminate();

    // The server's close handler must requeue the un-heartbeated job. A second
    // worker then pulls the SAME job id → redelivery proven.
    const workerB = await openWorkerSocket(h);
    const redelivered = await waitFor(async () => {
      const pull = await workerB.send({ cmd: 'PULL', queue: QUEUE, owner: 'wB', lockTtl: 30000 });
      const job = pull.job as { id: string; attempts?: number } | null;
      return job?.id === jobId ? job : null;
    }, 5000);

    expect(redelivered).not.toBe(false);
    expect((redelivered as { id: string }).id).toBe(jobId);
  });

  it('C2: a worker that HEARTBEATED then dropped is NOT instantly re-dispatched (no double-exec), and lock-expiry reclaims it (no loss)', async () => {
    // Small stall tick + lock TTL so expiry fires fast and deterministically.
    const h = startServer({ stallCheckMs: 60 });
    const ctrl = await openClient(h);
    const QUEUE = 'chaos-c2';

    const push = await ctrl.send({ cmd: 'PUSH', queue: QUEUE, data: { n: 2 } });
    const jobId = String(push.id);

    // Worker A pulls with a lock and sends one heartbeat (renewalCount→1). The
    // TTL is deliberately generous (2s) so the "not immediately re-dispatched"
    // check below has a ~10x margin over the close→pull window (~150ms) — no
    // race against lock expiry — while still expiring well inside the recovery
    // deadline to prove eventual reclaim.
    const workerA = await openWorkerSocket(h);
    const pullA = await workerA.send({ cmd: 'PULL', queue: QUEUE, owner: 'wA', lockTtl: 2000 });
    const token = String(pullA.token);
    const hb = await workerA.send({ cmd: 'JobHeartbeat', id: jobId, token });
    expect((hb.data as { ok: boolean } | undefined)?.ok ?? hb.ok).toBeTruthy();

    // A dies. Because it heartbeated, the drop must NOT immediately requeue the
    // job (that would risk concurrent double-execution with a pooled sibling).
    workerA.terminate();
    await Bun.sleep(50); // let the server close handler run

    const workerB = await openWorkerSocket(h);
    const immediatePull = await workerB.send({
      cmd: 'PULL',
      queue: QUEUE,
      owner: 'wB',
      lockTtl: 30000,
      timeout: 0,
    });
    // No job yet (lock still valid ~1.8s out): the heartbeated-then-dropped job
    // is left in place for expiry, not instantly re-dispatched.
    expect((immediatePull.job as unknown) ?? null).toBeNull();

    // But it is NOT lost: once the short lock TTL expires, lock-expiry requeues
    // it and worker B can pick it up. Bounded recovery.
    const recovered = await waitFor(async () => {
      const pull = await workerB.send({
        cmd: 'PULL',
        queue: QUEUE,
        owner: 'wB',
        lockTtl: 30000,
        timeout: 0,
      });
      const job = pull.job as { id: string } | null;
      return job?.id === jobId ? job : null;
    }, 5000);

    expect(recovered).not.toBe(false);
  });

  it('C3: lock expiry under contention — 20 jobs pulled and abandoned are ALL recoverable (none permanently lost)', async () => {
    const h = startServer({ stallCheckMs: 60 });
    const ctrl = await openClient(h);
    const QUEUE = 'chaos-c3';
    const N = 20;

    const ids = new Set<string>();
    for (let i = 0; i < N; i++) {
      const push = await ctrl.send({ cmd: 'PUSH', queue: QUEUE, data: { i } });
      ids.add(String(push.id));
    }

    // Worker A leases all 20 with a short lock, then abandons them (no ack, no
    // heartbeat, socket stays open so this is pure lock-expiry, not close-release).
    const workerA = await openWorkerSocket(h);
    let leased = 0;
    for (let i = 0; i < N; i++) {
      const pull = await workerA.send({
        cmd: 'PULL',
        queue: QUEUE,
        owner: 'wA',
        lockTtl: 250,
        timeout: 0,
      });
      if ((pull.job as { id: string } | null)?.id) leased++;
    }
    expect(leased).toBe(N);

    // After the locks expire, every abandoned job must become recoverable.
    // Worker B drains the queue post-expiry and we assert we recover all ids.
    const workerB = await openWorkerSocket(h);
    const recovered = new Set<string>();
    const ok = await waitFor(async () => {
      // Drain whatever is currently available this round.
      for (;;) {
        const pull = await workerB.send({
          cmd: 'PULL',
          queue: QUEUE,
          owner: 'wB',
          lockTtl: 30000,
          timeout: 0,
        });
        const job = pull.job as { id: string } | null;
        if (!job?.id) break;
        recovered.add(job.id);
        // Ack so it does not re-expire and re-appear (keeps the accounting clean).
        await workerB.send({ cmd: 'ACK', id: job.id });
      }
      return recovered.size >= N ? true : null;
    }, 8000);

    expect(ok).toBe(true);
    // Every original job id was redelivered — nothing lost.
    for (const id of ids) expect(recovered.has(id)).toBe(true);
  });

  it('C4: after redelivery the job still completes cleanly (ACK from the second worker succeeds, state=completed)', async () => {
    const h = startServer();
    const ctrl = await openClient(h);
    const QUEUE = 'chaos-c4';

    const push = await ctrl.send({ cmd: 'PUSH', queue: QUEUE, data: { n: 4 } });
    const jobId = String(push.id);

    const workerA = await openWorkerSocket(h);
    await workerA.send({ cmd: 'PULL', queue: QUEUE, owner: 'wA', lockTtl: 30000 });
    workerA.terminate();

    const workerB = await openWorkerSocket(h);
    const job = await waitFor(async () => {
      const pull = await workerB.send({ cmd: 'PULL', queue: QUEUE, owner: 'wB', lockTtl: 30000 });
      const j = pull.job as { id: string; token?: string } | null;
      return j?.id === jobId ? { j, token: pull.token as string } : null;
    }, 5000);
    expect(job).not.toBe(false);

    const ackRes = await workerB.send({
      cmd: 'ACK',
      id: jobId,
      token: (job as { token: string }).token,
      result: { done: true },
    });
    expect(ackRes.ok).toBe(true);

    const state = await ctrl.send({ cmd: 'GetState', queue: QUEUE, id: jobId });
    expect(state.state).toBe('completed');
  });
});

describe('CHAOS — cron clock skew (parser monotonicity)', () => {
  it('C5a: next cron run is always strictly AFTER fromTime, even when the clock jumps backward', async () => {
    const expr = '*/5 * * * *'; // every 5 minutes
    const now = 1_700_000_000_000; // fixed epoch (Nov 2023), no Date.now()
    const next = getNextCronRun(expr, now);
    expect(next).toBeGreaterThan(now);

    // Simulate a backward clock jump: compute from a time 1h in the past.
    const past = now - 3_600_000;
    const nextFromPast = getNextCronRun(expr, past);
    expect(nextFromPast).toBeGreaterThan(past);
    // And it is a real, finite timestamp (the parser returned a date, not 0).
    expect(Number.isFinite(nextFromPast)).toBe(true);
    expect(nextFromPast).toBeGreaterThan(0);
  });

  it('C5b: DST boundary — next run across a spring-forward transition is a valid future instant', async () => {
    // Europe/Rome springs forward 2024-03-31 02:00→03:00. A daily 02:30 job on
    // that date must still resolve to a real future instant (the parser handles the
    // skipped wall-clock hour); the parser must not return 0 or a past time.
    const expr = '30 2 * * *';
    const tz = 'Europe/Rome';
    // 2024-03-31 00:00:00 UTC, before the transition.
    const beforeDst = Date.UTC(2024, 2, 31, 0, 0, 0);
    const next = getNextCronRun(expr, beforeDst, tz);
    expect(next).toBeGreaterThan(beforeDst);
    expect(Number.isFinite(next)).toBe(true);
    expect(next).toBeGreaterThan(0);
  });

  it('C5c: fixed-interval next run advances monotonically regardless of skew; isDue is consistent', async () => {
    const interval = 30_000;
    const base = 1_700_000_000_000;
    const next = getNextIntervalRun(interval, base);
    expect(next).toBe(base + interval);

    // Not due before, due at/after.
    expect(isDue(next, base)).toBe(false);
    expect(isDue(next, next)).toBe(true);
    expect(isDue(next, next + 1)).toBe(true);

    // A backward clock (now < scheduled) keeps the job not-due (no premature fire).
    expect(isDue(next, base - 10_000)).toBe(false);
  });
});
