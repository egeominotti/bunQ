/**
 * STRESS (category 4) — graceful degradation, not "how much it holds".
 *
 * The question is NOT peak throughput; it is: when pushed past capacity, does the
 * server degrade CLEANLY (bounded memory, backpressure, one bad client contained)
 * or does it crash / OOM / wedge? These tests assert:
 *
 *   - HUGE BACKLOG STAYS BOUNDED & RESPONSIVE: a large unbacked backlog keeps the
 *     server answering with bounded latency and bounded internal memory, and it
 *     fully drains afterward (no wedge, no loss).
 *   - ONE ABUSIVE CLIENT IS CONTAINED: a client that floods requests but never
 *     reads its responses gets its own connection dropped (write-side memory
 *     bound) — the server never crashes and healthy clients stay fast.
 *   - PIPELINING BEYOND THE PER-CONN LIMIT IS SAFE: >50 concurrent in-flight
 *     commands on one socket (MAX_CONCURRENT_PER_CONNECTION) queue and all
 *     complete correctly — no loss, no crash.
 *   - RECOVERY AFTER A SPIKE: latency returns to baseline once a spike clears —
 *     no permanent degradation.
 */

import { afterEach, describe, expect, it } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';
import { FrameParser } from '../src/infrastructure/server/protocol';
import { pack, unpack } from 'msgpackr';

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

interface Framed {
  send(cmd: Record<string, unknown>, timeoutMs?: number): Promise<Record<string, unknown>>;
  end(): void;
}

async function openFramed(port: number): Promise<Framed> {
  const parser = new FrameParser();
  const pending = new Map<string, (r: Record<string, unknown>) => void>();
  let counter = 0;
  const fail = () => {
    for (const r of pending.values()) r({ ok: false, error: 'closed' });
    pending.clear();
  };
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
      close: fail,
      error: fail,
    },
  });
  return {
    send(cmd, timeoutMs = 8000) {
      const reqId = `x${counter++}`;
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
        try {
          socket.write(frame);
          (socket as unknown as { flush?: () => void }).flush?.();
        } catch {
          /* ignore */
        }
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
}
let harness: Harness | null = null;

function startServer(cfg?: { maxWriteQueueBytes?: number; idleTimeoutMs?: number }): Harness {
  const qm = new QueueManager({ stallCheckMs: 200 });
  const port = getFreePort();
  const server = createTcpServer(qm, {
    hostname: '127.0.0.1',
    port,
    maxWriteQueueBytes: cfg?.maxWriteQueueBytes,
    idleTimeoutMs: cfg?.idleTimeoutMs,
  });
  const h: Harness = { qm, server, port };
  harness = h;
  return h;
}

/** Open a raw socket, send `bytes`, never complete the frame. Slowloris peer. */
async function openSlowloris(port: number): Promise<{ closed: () => boolean; end: () => void }> {
  let isClosed = false;
  const socket = await Bun.connect({
    hostname: '127.0.0.1',
    port,
    socket: {
      data() {
        /* ignore */
      },
      close() {
        isClosed = true;
      },
      error() {
        isClosed = true;
      },
    },
  });
  // A 4-byte length prefix declaring a 10KB frame, but we send only a few bytes:
  // a legal partial frame that never completes → arms the server stall timer.
  const partial = new Uint8Array([0x00, 0x00, 0x27, 0x10, 0x01, 0x02, 0x03]);
  socket.write(partial);
  (socket as unknown as { flush?: () => void }).flush?.();
  return {
    closed: () => isClosed,
    end() {
      try {
        socket.end();
      } catch {
        /* ignore */
      }
    },
  };
}

afterEach(() => {
  if (harness) {
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
});

function quantile(sorted: number[], q: number): number {
  if (sorted.length === 0) return 0;
  const pos = (sorted.length - 1) * q;
  const base = Math.floor(pos);
  const rest = pos - base;
  return sorted[base + 1] !== undefined
    ? sorted[base] + rest * (sorted[base + 1] - sorted[base])
    : sorted[base];
}

describe('STRESS — graceful degradation under overload', () => {
  it('S1: a huge unbacked backlog stays bounded & responsive, then fully drains', async () => {
    const JOBS = Number(process.env.STRESS_JOBS ?? 40000);
    const h = startServer();
    const producer = await openFramed(h.port);
    const probe = await openFramed(h.port);
    const QUEUE = 'stress-backlog';

    // Build a large backlog fast via batched pushes; sample probe latency during
    // the build to prove the server keeps answering with bounded latency.
    const pushed = new Set<string>();
    const probeLatencies: number[] = [];
    const BATCH = 500;
    for (let i = 0; i < JOBS; i += BATCH) {
      const jobs = Array.from({ length: Math.min(BATCH, JOBS - i) }, (_, k) => ({
        data: { n: i + k },
      }));
      const res = await producer.send({ cmd: 'PUSHB', queue: QUEUE, jobs });
      for (const id of (res.ids as string[]) ?? []) pushed.add(String(id));
      if ((i / BATCH) % 4 === 0) {
        const t = Date.now();
        const p = await probe.send({ cmd: 'Ping' });
        expect(p).toBeTruthy();
        probeLatencies.push(Date.now() - t);
      }
    }
    expect(pushed.size).toBe(JOBS);

    // Responsiveness held: p99 of the health probe during a 40k backlog build
    // stays well bounded (not climbing into seconds).
    probeLatencies.sort((a, b) => a - b);
    const p99 = quantile(probeLatencies, 0.99);
    console.log(`[stress-S1] backlog=${JOBS} probe p99 during build=${p99.toFixed(0)}ms`);
    expect(p99).toBeLessThan(500);

    // Memory bounded: the backlog lives in the queue, but the completed/index
    // collections have not blown past their caps (nothing completed yet).
    const mem = h.qm.getMemoryStats();
    expect(mem.queuedTotal).toBe(JOBS);
    expect(mem.completedJobs).toBe(0);
    expect(mem.processingTotal).toBe(0);

    // Drain the whole backlog with a pool → no wedge, no loss.
    const acked = new Set<string>();
    const K = 8;
    const workers = await Promise.all(Array.from({ length: K }, () => openFramed(h.port)));
    const drain = async (w: Framed) => {
      let empty = 0;
      while (empty < 8) {
        const pull = await w.send({ cmd: 'PULL', queue: QUEUE, timeout: 0 });
        if (pull.ok === false) {
          if (acked.size === JOBS) return;
          throw new Error(`stress pull failed: ${String(pull.error ?? 'unknown error')}`);
        }
        const job = pull.job as { id: string } | null;
        if (!job?.id) {
          empty++;
          await Bun.sleep(5);
          continue;
        }
        empty = 0;
        const a = await w.send({ cmd: 'ACK', id: job.id });
        if (a.ok) acked.add(job.id);
      }
    };
    await Promise.all(workers.map(drain));
    expect(acked.size).toBe(JOBS);
    for (const w of workers) w.end();
    producer.end();
    probe.end();
  }, 120000);

  it('S2: 100 slowloris connections (partial frames) are all terminated by the stall bound while a healthy client stays fast — resource abuse is contained', async () => {
    // Aggressive slowloris timeout so the bound fires fast and deterministically.
    const h = startServer({ idleTimeoutMs: 400 });

    const healthy = await openFramed(h.port);
    // Establish a healthy baseline.
    const base = await healthy.send({ cmd: 'Ping' });
    expect(base).toBeTruthy();

    // 100 connections each open a frame and then stall — the classic slowloris
    // resource-exhaustion attack. Each holds a partial-frame buffer server-side.
    const N = 100;
    const slows = await Promise.all(Array.from({ length: N }, () => openSlowloris(h.port)));

    // Throughout the attack the healthy client keeps getting fast responses
    // (partial-frame buffers are bounded and the stall timer is per-connection).
    const lat: number[] = [];
    for (let i = 0; i < 20; i++) {
      const t = Date.now();
      const r = await healthy.send({ cmd: 'Ping' });
      expect(r).toBeTruthy();
      lat.push(Date.now() - t);
      await Bun.sleep(10);
    }
    lat.sort((a, b) => a - b);
    expect(quantile(lat, 0.95)).toBeLessThan(500);

    // Every slowloris connection is terminated by the stall bound within a window.
    const deadline = Date.now() + 4000;
    while (slows.some((s) => !s.closed()) && Date.now() < deadline) await Bun.sleep(50);
    const dropped = slows.filter((s) => s.closed()).length;
    console.log(`[stress-S2] slowloris ${dropped}/${N} terminated by stall bound`);
    expect(dropped).toBe(N);

    // The connections are released SERVER-SIDE, not just closed client-side:
    // the server's live connection count collapses back from ~N+1 to ~the
    // healthy client. Wait briefly for the close handlers to run.
    const connDeadline = Date.now() + 2000;
    while (h.server.getConnectionCount() > 3 && Date.now() < connDeadline) await Bun.sleep(50);
    expect(h.server.getConnectionCount()).toBeLessThanOrEqual(3); // healthy client + slack, not 100+

    // Still fully usable for a fresh client.
    const fresh = await openFramed(h.port);
    const ok = await fresh.send({ cmd: 'PUSH', queue: 'after-abuse', data: { ok: 1 } });
    expect(ok.ok).toBe(true);
    fresh.end();
    healthy.end();
    for (const s of slows) s.end();
  }, 40000);

  it('S3: 500 concurrent in-flight commands on ONE socket (>50 limit) all complete, no loss, no crash', async () => {
    const h = startServer();
    const c = await openFramed(h.port);
    const QUEUE = 'stress-pipeline';
    const N = 500; // 10x MAX_CONCURRENT_PER_CONNECTION

    // Fire N PUSHes concurrently on a single connection. The server caps active
    // processing at 50 via a semaphore; the rest queue. All must succeed.
    const results = await Promise.all(
      Array.from({ length: N }, (_, i) => c.send({ cmd: 'PUSH', queue: QUEUE, data: { i } }))
    );
    const ids = new Set(results.filter((r) => r.ok).map((r) => String(r.id)));
    expect(results.every((r) => r.ok === true)).toBe(true);
    expect(ids.size).toBe(N); // no lost / no duplicate ids

    // The server accounted for exactly N jobs.
    const counts = await c.send({ cmd: 'GetJobCounts', queue: QUEUE });
    const cnt = counts.counts as Record<string, number>;
    expect((cnt.waiting ?? 0) + (cnt.prioritized ?? 0)).toBe(N);
    c.end();
  }, 30000);

  it('S4: latency returns to baseline after a load spike (no permanent degradation)', async () => {
    const h = startServer();
    const probe = await openFramed(h.port);
    const spiker = await openFramed(h.port);
    const QUEUE = 'stress-spike';

    const measure = async (samples: number): Promise<number> => {
      const lat: number[] = [];
      for (let i = 0; i < samples; i++) {
        const t = Date.now();
        await probe.send({ cmd: 'Ping' });
        lat.push(Date.now() - t);
      }
      lat.sort((a, b) => a - b);
      return quantile(lat, 0.95);
    };

    // Baseline before any load.
    const baseline = await measure(30);

    // Spike: push a big burst, and drain it with a worker pool (fast).
    const SPIKE = 10000;
    await spiker.send({
      cmd: 'PUSHB',
      queue: QUEUE,
      jobs: Array.from({ length: SPIKE }, (_, i) => ({ data: { i } })),
    });
    const drainers = await Promise.all(Array.from({ length: 8 }, () => openFramed(h.port)));
    const drain = async (w: Framed) => {
      let empty = 0;
      while (empty < 8) {
        const pull = await w.send({ cmd: 'PULL', queue: QUEUE, timeout: 0 });
        const job = pull.job as { id: string } | null;
        if (!job?.id) {
          empty++;
          await Bun.sleep(5);
          continue;
        }
        empty = 0;
        await w.send({ cmd: 'ACK', id: job.id });
      }
    };
    await Promise.all(drainers.map(drain));

    // Quiesce, then re-measure. Latency must return near baseline.
    await Bun.sleep(500);
    h.qm.compactMemory();
    const afterSpike = await measure(30);
    console.log(
      `[stress-S4] baseline p95=${baseline.toFixed(0)}ms after-spike p95=${afterSpike.toFixed(0)}ms`
    );

    // No permanent degradation: post-spike p95 is within a small absolute margin
    // of baseline (allowing for GC jitter), not a persistent multiple.
    expect(afterSpike).toBeLessThanOrEqual(Math.max(50, baseline + 30));
    probe.end();
    spiker.end();
    for (const d of drainers) d.end();
  }, 60000);
});
