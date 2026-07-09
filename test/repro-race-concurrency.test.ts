/**
 * RACE (category 6) — deterministic concurrency & race conditions.
 *
 * Every test here forces a specific interleaving that a 24/7 multi-worker
 * deployment hits constantly, and asserts the queue's core invariants:
 *
 *   - SINGLE DELIVERY: N concurrent PULLs on one job → exactly one worker gets it.
 *   - IDEMPOTENT ADD: N concurrent PUSHes with the same jobId → exactly one job.
 *   - ACTIVE RE-ADD SAFE: re-adding a jobId while it is ACTIVE is an idempotent
 *     skip, never a UNIQUE-constraint crash (#69 class).
 *   - CANCEL-DURING-ACTIVE: cancel of an active job is a no-op (cannot cancel a
 *     running job) and leaves the job able to complete normally — no corruption.
 *   - NO DOUBLE-COMPLETE: a stale ACK racing lock-expiry + re-lease does not
 *     complete a job twice.
 *   - NO LOSS / NO DUP under a full K-workers × M-jobs drain: every job is
 *     processed exactly once; counts reconcile.
 *
 * Driven through a real TCP server. Concurrency is real (Promise.all of raw
 * framed sends), not simulated.
 */

import { describe, it, expect, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';
import { TcpClient } from '../src/client/tcp/client';
import { FrameParser } from '../src/infrastructure/server/protocol';
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

async function waitFor<T>(fn: () => T | Promise<T>, timeoutMs: number, intervalMs = 20) {
  const start = Date.now();
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() - start >= timeoutMs) return false;
    await Bun.sleep(intervalMs);
  }
}

/** Framed request/response client keyed by reqId (concurrent-send safe). */
interface FramedClient {
  send(cmd: Record<string, unknown>, timeoutMs?: number): Promise<Record<string, unknown>>;
  end(): void;
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
        for (const r of pending.values()) r({ ok: false, error: 'closed' });
        pending.clear();
      },
      error() {
        for (const r of pending.values()) r({ ok: false, error: 'error' });
        pending.clear();
      },
    },
  });
  return {
    send(cmd, timeoutMs = 5000) {
      const reqId = `r${counter++}`;
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
    commandTimeout: 5000,
    connectTimeout: 2000,
  });
  await c.connect();
  h.clients.push(c);
  return c;
}

async function openWorker(h: Harness): Promise<FramedClient> {
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

describe('RACE — single delivery under concurrent pulls', () => {
  it('R1: 25 concurrent PULLs on ONE job → exactly one worker receives it', async () => {
    const h = startServer();
    const ctrl = await openClient(h);
    const QUEUE = 'race-r1';
    const push = await ctrl.send({ cmd: 'PUSH', queue: QUEUE, data: { n: 1 } });
    const jobId = String(push.id);

    // 25 independent sockets all PULL at once.
    const workers = await Promise.all(Array.from({ length: 25 }, () => openWorker(h)));
    const results = await Promise.all(
      workers.map((w, i) =>
        w.send({ cmd: 'PULL', queue: QUEUE, owner: `w${i}`, lockTtl: 30000, timeout: 0 })
      )
    );

    const winners = results.filter((r) => (r.job as { id: string } | null)?.id === jobId);
    expect(winners.length).toBe(1); // exactly one delivery, no double-dispatch
  });
});

describe('RACE — idempotent add under concurrency', () => {
  it('R2: 25 concurrent PUSHes with the SAME jobId → exactly one job exists', async () => {
    const h = startServer();
    const ctrl = await openClient(h);
    const QUEUE = 'race-r2';
    const CUSTOM = 'dedupe-me';

    const senders = await Promise.all(Array.from({ length: 25 }, () => openWorker(h)));
    const results = await Promise.all(
      senders.map((s) => s.send({ cmd: 'PUSH', queue: QUEUE, data: { x: 1 }, jobId: CUSTOM }))
    );
    // All calls succeed (idempotent), none error out.
    expect(results.every((r) => r.ok === true)).toBe(true);

    // Exactly one job landed in the queue.
    const counts = await ctrl.send({ cmd: 'GetJobCounts', queue: QUEUE });
    const c = counts.counts as Record<string, number>;
    expect((c.waiting ?? 0) + (c.active ?? 0) + (c.delayed ?? 0)).toBe(1);
  });

  it('R3: re-adding a jobId while the job is ACTIVE is an idempotent skip (no UNIQUE crash, no duplicate)', async () => {
    const h = startServer();
    const ctrl = await openClient(h);
    const QUEUE = 'race-r3';
    const CUSTOM = 'active-readd';

    const push = await ctrl.send({ cmd: 'PUSH', queue: QUEUE, data: { x: 1 }, jobId: CUSTOM });
    expect(push.ok).toBe(true);

    // Pull it → now ACTIVE.
    const worker = await openWorker(h);
    const pull = await worker.send({ cmd: 'PULL', queue: QUEUE, owner: 'w', lockTtl: 30000 });
    const token = String(pull.token);
    expect((pull.job as { id: string } | null)?.id).toBeTruthy();

    // Concurrently re-add the same jobId 10× while it is active — must all be
    // safe (ok, not an error), and must NOT create a second job.
    const readds = await Promise.all(
      Array.from({ length: 10 }, () =>
        ctrl.send({ cmd: 'PUSH', queue: QUEUE, data: { x: 2 }, jobId: CUSTOM })
      )
    );
    expect(readds.every((r) => r.ok === true)).toBe(true);

    const counts = await ctrl.send({ cmd: 'GetJobCounts', queue: QUEUE });
    const c = counts.counts as Record<string, number>;
    expect(c.active ?? 0).toBe(1);
    expect(c.waiting ?? 0).toBe(0);

    // The active job still completes cleanly.
    const jobId = String((pull.job as { id: string }).id);
    const ack = await worker.send({ cmd: 'ACK', id: jobId, token, result: { ok: true } });
    expect(ack.ok).toBe(true);
    const state = await ctrl.send({ cmd: 'GetState', queue: QUEUE, id: jobId });
    expect(state.state).toBe('completed');
  });
});

describe('RACE — cancel during active', () => {
  it('R4: cancelling an ACTIVE job is a no-op (rejected) and the job still completes normally', async () => {
    const h = startServer();
    const ctrl = await openClient(h);
    const QUEUE = 'race-r4';
    const push = await ctrl.send({ cmd: 'PUSH', queue: QUEUE, data: { n: 1 } });
    const jobId = String(push.id);

    const worker = await openWorker(h);
    const pull = await worker.send({ cmd: 'PULL', queue: QUEUE, owner: 'w', lockTtl: 30000 });
    const token = String(pull.token);

    // Cancel while active — cannot cancel a running job.
    const cancel = await ctrl.send({ cmd: 'Cancel', queue: QUEUE, id: jobId });
    expect(cancel.ok).toBe(false);
    expect(String(cancel.error)).toContain('cannot be cancelled');

    // The job is untouched and still ackable → completes.
    const ack = await worker.send({ cmd: 'ACK', id: jobId, token, result: { done: true } });
    expect(ack.ok).toBe(true);
    const state = await ctrl.send({ cmd: 'GetState', queue: QUEUE, id: jobId });
    expect(state.state).toBe('completed');
  });
});

describe('RACE — stale ACK vs lock-expiry (no double-complete)', () => {
  it('R5: a stale ACK after lock-expiry + re-lease does not complete the job twice', async () => {
    const h = startServer({ stallCheckMs: 60 });
    const ctrl = await openClient(h);
    const QUEUE = 'race-r5';
    const push = await ctrl.send({ cmd: 'PUSH', queue: QUEUE, data: { n: 1 } });
    const jobId = String(push.id);

    // Worker A leases with a short lock and stalls (no heartbeat, no ack).
    const workerA = await openWorker(h);
    const pullA = await workerA.send({ cmd: 'PULL', queue: QUEUE, owner: 'wA', lockTtl: 250 });
    const staleToken = String(pullA.token);
    expect((pullA.job as { id: string } | null)?.id).toBe(jobId);

    // Wait for the lock to expire and the job to be requeued, then worker B
    // leases it under a FRESH lock.
    const workerB = await openWorker(h);
    const leaseB = await waitFor(async () => {
      const pull = await workerB.send({
        cmd: 'PULL',
        queue: QUEUE,
        owner: 'wB',
        lockTtl: 30000,
        timeout: 0,
      });
      const j = pull.job as { id: string } | null;
      return j?.id === jobId ? { token: pull.token as string } : null;
    }, 5000);
    expect(leaseB).not.toBe(false);
    const freshToken = (leaseB as { token: string }).token;

    // Now A's stale ACK arrives late. Its token is expired / re-leased, so the
    // server MUST reject it — it must not complete the job under A's identity.
    const ackA = await workerA.send({
      cmd: 'ACK',
      id: jobId,
      token: staleToken,
      result: { from: 'A' },
    });
    expect(ackA.ok).toBe(false); // stale-token ACK rejected → no double-complete via A

    // B completes the job.
    const ackB = await workerB.send({
      cmd: 'ACK',
      id: jobId,
      token: freshToken,
      result: { from: 'B' },
    });
    expect(ackB.ok).toBe(true);

    // Exactly one completion, owned by B: state completed, no lingering active,
    // and the STORED RESULT is B's — proving A's stale ack neither completed nor
    // overwrote the job (the crux of the no-double-complete guarantee).
    const state = await ctrl.send({ cmd: 'GetState', queue: QUEUE, id: jobId });
    expect(state.state).toBe('completed');
    const result = await ctrl.send({ cmd: 'GetResult', queue: QUEUE, id: jobId });
    expect((result.result as { from: string })?.from).toBe('B');
    const counts = await ctrl.send({ cmd: 'GetJobCounts', queue: QUEUE });
    const c = counts.counts as Record<string, number>;
    expect(c.active ?? 0).toBe(0);
  });
});

describe('RACE — full drain: K workers × M jobs, exactly-once processing', () => {
  it('R6: 8 workers draining 100 jobs → every job ACKed exactly once, none lost, none duplicated', async () => {
    const h = startServer();
    const ctrl = await openClient(h);
    const QUEUE = 'race-r6';
    const M = 100;
    const K = 8;

    const pushed = new Set<string>();
    // addBulk-style: push all jobs concurrently in small batches.
    for (let i = 0; i < M; i++) {
      const p = await ctrl.send({ cmd: 'PUSH', queue: QUEUE, data: { i } });
      pushed.add(String(p.id));
    }
    expect(pushed.size).toBe(M);

    const acked: string[] = [];
    const workers = await Promise.all(Array.from({ length: K }, () => openWorker(h)));

    // Each worker loops: PULL (with lock) → ACK, until the queue yields nothing
    // for a few consecutive tries.
    const drain = async (w: FramedClient, id: number) => {
      let emptyStreak = 0;
      while (emptyStreak < 5) {
        const pull = await w.send({
          cmd: 'PULL',
          queue: QUEUE,
          owner: `w${id}`,
          lockTtl: 30000,
          timeout: 0,
        });
        const job = pull.job as { id: string } | null;
        if (!job?.id) {
          emptyStreak++;
          await Bun.sleep(10);
          continue;
        }
        emptyStreak = 0;
        const ack = await w.send({ cmd: 'ACK', id: job.id, token: pull.token as string });
        if (ack.ok) acked.push(job.id);
      }
    };

    await Promise.all(workers.map((w, i) => drain(w, i)));

    // No loss: every pushed job was acked.
    const ackedSet = new Set(acked);
    expect(ackedSet.size).toBe(M);
    // No duplicate completion: acked list has no repeats.
    expect(acked.length).toBe(M);
    for (const id of pushed) expect(ackedSet.has(id)).toBe(true);

    // Queue fully drained.
    const counts = await ctrl.send({ cmd: 'GetJobCounts', queue: QUEUE });
    const c = counts.counts as Record<string, number>;
    expect((c.waiting ?? 0) + (c.active ?? 0)).toBe(0);
  });
});
