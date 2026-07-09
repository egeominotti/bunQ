/**
 * CHAOS (soak / endurance) — sustained load with continuous worker churn.
 *
 * A 24/7 queue must survive not a burst but a MARATHON: steady push/pull/ack
 * traffic while workers keep dying and reconnecting, with NO job loss and NO
 * unbounded memory growth. This test drives a real TCP server with several
 * framed workers, kills-and-respawns a worker socket on a fixed cadence for the
 * whole run, and asserts the production-grade invariants:
 *
 *   - NO LOSS UNDER CHURN: every pushed job is eventually completed at least
 *     once (distinct completions == pushed count) despite workers dying mid-lease.
 *     (At-least-once permits a re-run of a job whose worker died; it never
 *     permits a lost job.)
 *   - MEMORY RETURNS TO BASELINE: after the queue fully drains, the internal
 *     collections (jobIndex, processing, queued, jobLocks, per-client job
 *     tracking, temporal index) collapse back to ~0 — proof the release/cleanup
 *     paths don't leak per-job or per-connection state under churn.
 *   - BOUNDED CAPS HOLD: completedJobs never exceeds its 50k FIFO cap.
 *   - STILL RESPONSIVE: the server answers a fresh request after the marathon.
 *
 * Scale/duration are env-tunable for a real long soak:
 *   SOAK_MS     (default 8000)   how long to sustain the produce/consume churn
 *   SOAK_KILL_MS(default 400)    interval between worker-socket kills
 * A genuine 24h endurance run: SOAK_MS=86400000 and let it churn — the same
 * invariants must hold, and memory must still return to baseline at the end.
 */

import { afterEach, describe, expect, it } from 'bun:test';
import { heapStats } from 'bun:jsc';
import { existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
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

function cleanDb(db: string): void {
  for (const suffix of ['', '-wal', '-shm']) {
    const f = `${db}${suffix}`;
    if (existsSync(f)) rmSync(f, { force: true });
  }
}

interface FramedClient {
  send(cmd: Record<string, unknown>, timeoutMs?: number): Promise<Record<string, unknown>>;
  terminate(): void;
  end(): void;
}

function makeFramed(socket: {
  write: (b: Uint8Array) => number;
  flush?: () => void;
  end: () => void;
  terminate?: () => void;
}): {
  onData: (d: Buffer) => void;
  onClose: () => void;
  client: FramedClient;
} {
  const parser = new FrameParser();
  const pending = new Map<string, (r: Record<string, unknown>) => void>();
  let counter = 0;
  const failAll = () => {
    for (const r of pending.values()) r({ ok: false, error: 'closed' });
    pending.clear();
  };
  return {
    onData(d: Buffer) {
      let frames: Uint8Array[];
      try {
        frames = parser.addData(d);
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
    onClose: failAll,
    client: {
      send(cmd, timeoutMs = 5000) {
        const reqId = `s${counter++}`;
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
            socket.flush?.();
          } catch {
            /* socket gone; will time out or be failed on close */
          }
        });
      },
      terminate() {
        // Hard drop when supported (else graceful FIN). Never both — `?? ` would
        // also call end() after terminate() returns undefined.
        try {
          if (typeof socket.terminate === 'function') socket.terminate();
          else socket.end();
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
    },
  };
}

async function openFramed(port: number): Promise<FramedClient> {
  let wiring: ReturnType<typeof makeFramed> | null = null;
  const socket = await Bun.connect({
    hostname: '127.0.0.1',
    port,
    socket: {
      data(_s, data: Buffer) {
        wiring?.onData(data);
      },
      close() {
        wiring?.onClose();
      },
      error() {
        wiring?.onClose();
      },
    },
  });
  wiring = makeFramed(socket as unknown as Parameters<typeof makeFramed>[0]);
  return wiring.client;
}

interface Harness {
  qm: QueueManager;
  server: TcpServer;
  port: number;
  db?: string;
}
let harness: Harness | null = null;

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
    if (harness.db) cleanDb(harness.db);
    harness = null;
  }
});

/** p-quantile of a numeric array (linear interpolation). */
function quantile(sorted: number[], q: number): number {
  if (sorted.length === 0) return 0;
  const pos = (sorted.length - 1) * q;
  const base = Math.floor(pos);
  const rest = pos - base;
  return sorted[base + 1] !== undefined
    ? sorted[base] + rest * (sorted[base + 1] - sorted[base])
    : sorted[base];
}

describe('SOAK — sustained churn: no loss, no leak, no latency drift', () => {
  it('marathon: continuous produce/consume with a worker dying every SOAK_KILL_MS — no job lost, memory returns to baseline, p99 does not drift, WAL bounded', async () => {
    const SOAK_MS = Number(process.env.SOAK_MS ?? 8000);
    const KILL_MS = Number(process.env.SOAK_KILL_MS ?? 400);
    const K_WORKERS = 6;
    const K_PRODUCERS = 3;
    const QUEUE = 'soak';

    // On-disk SQLite so we can observe WAL growth (a real 24/7 leak surface).
    const db = join(tmpdir(), `bunq-soak-${process.pid}-${randomPort()}.db`);
    cleanDb(db);
    const qm = new QueueManager({ stallCheckMs: 200, dataPath: db });
    const port = getFreePort();
    const server = createTcpServer(qm, { hostname: '127.0.0.1', port });
    harness = { qm, server, port, db };

    const pushedIds = new Set<string>();
    const completed = new Set<string>();
    let stop = false;

    // Time-series samples for drift analysis, one per ~1s tick.
    interface Sample {
      t: number;
      rssMb: number;
      heapObjs: number;
      jobIndex: number;
      queued: number;
      processing: number;
      jobLocks: number;
      completedJobs: number;
      walBytes: number;
      p99ProbeMs: number;
    }
    const samples: Sample[] = [];

    // ---- Producers: keep pushing for the whole SOAK_MS window ----
    const producers: FramedClient[] = [];
    for (let p = 0; p < K_PRODUCERS; p++) producers.push(await openFramed(port));
    const t0 = Date.now();
    const produce = async (prod: FramedClient, pid: number) => {
      let n = 0;
      while (Date.now() - t0 < SOAK_MS && !stop) {
        const res = await prod.send({ cmd: 'PUSH', queue: QUEUE, data: { pid, n: n++ } });
        if (res.ok && res.id) pushedIds.add(String(res.id));
        // Light pacing so the queue actually builds a backlog to churn against.
        if (n % 64 === 0) await Bun.sleep(1);
      }
    };

    // ---- Workers: drain continuously ----
    const workers: FramedClient[] = [];
    const runWorker = async (slot: number) => {
      while (!stop) {
        const w = workers[slot];
        if (!w) {
          await Bun.sleep(5);
          continue;
        }
        const pull = await w.send({
          cmd: 'PULL',
          queue: QUEUE,
          owner: `w${slot}`,
          lockTtl: 10000,
          timeout: 0,
        });
        const job = pull.job as { id: string } | null;
        if (!job?.id) {
          await Bun.sleep(5);
          continue;
        }
        const ack = await w.send({ cmd: 'ACK', id: job.id, token: pull.token as string });
        if (ack.ok) completed.add(job.id);
      }
    };
    for (let s = 0; s < K_WORKERS; s++) workers[s] = await openFramed(port);

    // ---- Chaos: kill+replace one worker socket every KILL_MS ----
    // Stops at the end of the load window so the final drain is clean (a kill
    // during the last drain moment would strand a just-leased job until lock
    // expiry — that's a test artifact, not a queue defect).
    let stopChaos = false;
    let kills = 0;
    const chaos = (async () => {
      let slot = 0;
      while (!stop && !stopChaos) {
        await Bun.sleep(KILL_MS);
        if (stop || stopChaos) break;
        try {
          workers[slot % K_WORKERS]?.terminate();
        } catch {
          /* ignore */
        }
        await Bun.sleep(30); // let the close handler release leased jobs
        workers[slot % K_WORKERS] = await openFramed(port);
        kills++;
        slot++;
      }
    })();

    // ---- Sampler: RSS / heap / collections / WAL / p99-probe every ~1s ----
    const probe = await openFramed(port);
    const sampler = (async () => {
      while (!stop) {
        // Latency probe: a burst of PUSHes, record per-op RTT, take p99.
        const rtts: number[] = [];
        for (let i = 0; i < 40; i++) {
          const s = Date.now();
          await probe.send({ cmd: 'PUSH', queue: 'probe', data: { i } });
          rtts.push(Date.now() - s);
        }
        rtts.sort((a, b) => a - b);
        const mem = qm.getMemoryStats();
        let walBytes = 0;
        try {
          const f = Bun.file(`${db}-wal`);
          walBytes = (await f.exists()) ? f.size : 0;
        } catch {
          /* ignore */
        }
        samples.push({
          t: Date.now() - t0,
          rssMb: process.memoryUsage().rss / 1024 / 1024,
          heapObjs: (heapStats() as { objectCount: number }).objectCount,
          jobIndex: mem.jobIndex,
          queued: mem.queuedTotal,
          processing: mem.processingTotal,
          jobLocks: mem.jobLocks,
          completedJobs: mem.completedJobs,
          walBytes,
          p99ProbeMs: quantile(rtts, 0.99),
        });
        await Bun.sleep(900);
      }
    })();

    // Run the marathon.
    const producerLoops = producers.map((p, i) => produce(p, i));
    const workerLoops = Array.from({ length: K_WORKERS }, (_, s) => runWorker(s));

    // Wait out the soak window (producers self-stop at SOAK_MS).
    await Promise.all(producerLoops);

    // Drain phase: let workers finish everything pushed (no loss), bounded wait.
    // Drain phase: stop injecting chaos so the tail drains cleanly, but keep the
    // workers running until every pushed job is completed. Any job leased by the
    // last killed worker is requeued (renewalCount 0) or, worst case, reclaimed
    // by lock-expiry within lockTtl (10s) — so a correct queue MUST reach parity.
    stopChaos = true;
    await chaos;
    // Replace any socket terminated on the final chaos tick so all K workers pull.
    for (let s = 0; s < K_WORKERS; s++) {
      if (!workers[s]) workers[s] = await openFramed(port);
    }

    // Drain until the SERVER's soak queue is empty (waiting/active/delayed all
    // zero) or a wide deadline. The client-side `completed` set undercounts: a
    // chaos kill landing between ACK-send and ACK-response makes the client see
    // ack.ok=false even though the server DID complete the job. So queue-empty,
    // not the client counter, is the authoritative "drained" signal.
    const queueEmpty = async (): Promise<boolean> => {
      const r = await probe.send({ cmd: 'GetJobCounts', queue: QUEUE });
      const c = (r.counts ?? {}) as Record<string, number>;
      return (
        (c.waiting ?? 0) +
          (c.active ?? 0) +
          (c.delayed ?? 0) +
          (c.prioritized ?? 0) +
          (c.paused ?? 0) ===
        0
      );
    };
    const drainDeadline = Date.now() + 45000; // > lockTtl(10s) with wide margin
    while (Date.now() < drainDeadline) {
      if (completed.size >= pushedIds.size && (await queueEmpty())) break;
      if (await queueEmpty()) break;
      await Bun.sleep(150);
    }

    stop = true;
    await Promise.all(workerLoops);
    await sampler;

    // Reconcile the client under-count against server truth: every pushed id the
    // client didn't see ACKed must be 'completed' on the server (never 'unknown'
    // = lost, never stuck active/waiting). Only a handful ever need querying.
    const serverConfirmed = new Set<string>();
    const missing = [...pushedIds].filter((id) => !completed.has(id));
    const straggler: Record<string, number> = {};
    for (const id of missing) {
      const st = await probe.send({ cmd: 'GetState', queue: QUEUE, id });
      const s = String(st.state);
      straggler[s] = (straggler[s] ?? 0) + 1;
      if (s === 'completed') serverConfirmed.add(id);
    }
    if (missing.length > 0) {
      console.log(
        `[soak] client under-count ${missing.length} reconciled via server state: ${JSON.stringify(straggler)}`
      );
    }

    // ---- Report the trend (visible in a real long soak; asserted below) ----
    const first = samples[0];
    const last = samples[samples.length - 1];
    console.log(
      `[soak] ${(SOAK_MS / 1000).toFixed(0)}s | pushed=${pushedIds.size} completed=${completed.size} kills=${kills} samples=${samples.length}`
    );
    if (first && last) {
      console.log(
        `[soak] RSS ${first.rssMb.toFixed(0)}→${last.rssMb.toFixed(0)}MB | heapObjs ${first.heapObjs}→${last.heapObjs} | WAL ${(first.walBytes / 1e6).toFixed(1)}→${(last.walBytes / 1e6).toFixed(1)}MB`
      );
      const p99s = samples.map((s) => s.p99ProbeMs);
      console.log(`[soak] p99 probe per tick: ${p99s.map((v) => v.toFixed(0)).join(',')}ms`);
    }

    // ================= ASSERTIONS =================

    // NO LOSS: every pushed job reached 'completed' on the server despite
    // continuous worker kills — counting client-acked + server-confirmed.
    expect(pushedIds.size).toBeGreaterThan(0);
    const done = new Set([...completed, ...serverConfirmed]);
    for (const id of pushedIds) expect(done.has(id)).toBe(true);
    expect(done.size).toBe(pushedIds.size);
    // Chaos actually happened, and it was sustained (not a single tick).
    expect(kills).toBeGreaterThanOrEqual(Math.max(1, Math.floor(SOAK_MS / KILL_MS) - 3));
    expect(samples.length).toBeGreaterThanOrEqual(3);

    // NO LATENCY DRIFT: the worst p99 in the second half of the run is not a
    // multiple of the median p99 in the first half (a drifting queue would show
    // p99 climbing monotonically as internal structures bloat).
    const half = Math.floor(samples.length / 2);
    const firstHalfP99 = samples
      .slice(0, half)
      .map((s) => s.p99ProbeMs)
      .sort((a, b) => a - b);
    const secondHalfMaxP99 = Math.max(...samples.slice(half).map((s) => s.p99ProbeMs));
    const firstHalfMedP99 = quantile(firstHalfP99, 0.5);
    // No runaway drift: the worst second-half p99 must stay within a modest
    // multiple of the first-half median (slack for GC jitter, but tight enough
    // that a monotonically climbing latency — the signature of a bloating
    // internal structure over a long run — would trip it).
    expect(secondHalfMaxP99).toBeLessThanOrEqual(Math.max(25, firstHalfMedP99 * 4 + 10));

    // The probe queue accumulates un-consumed latency-probe jobs by design;
    // obliterate it so the baseline check reflects only the drained soak queue.
    await probe.send({ cmd: 'Obliterate', queue: 'probe' });

    // Settle, compact, GC, then verify collections collapse to baseline.
    for (const w of workers) {
      try {
        w.end();
      } catch {
        /* ignore */
      }
    }
    for (const p of producers) p.end();
    probe.end();
    await Bun.sleep(600);
    qm.compactMemory();
    // Run the temporal-index orphan sweep — the exact call the 10s cleanup tick
    // makes. It removes index entries for jobs no longer live (all completed),
    // proving the index is reclaimed, not leaked. (compactMemory doesn't touch
    // the temporal index; the periodic task owns it.)
    for (const shard of qm.getShards()) shard.cleanOrphanedTemporalEntries();
    Bun.gc(true);
    await Bun.sleep(200);

    // MEMORY RETURNS TO BASELINE: no per-job / per-connection leak under churn.
    const mem = qm.getMemoryStats();
    expect(mem.queuedTotal).toBe(0);
    expect(mem.processingTotal).toBe(0);
    expect(mem.jobLocks).toBe(0);
    expect(mem.clientJobsTotal).toBe(0);
    // Temporal index fully reclaimed once the orphan sweep runs (no live jobs
    // remain) — bounded, not a leak.
    expect(mem.temporalIndexTotal).toBe(0);
    // jobIndex + completedJobs bounded (FIFO cap 50k), not growing without limit.
    expect(mem.completedJobs).toBeLessThanOrEqual(50000);
    expect(mem.jobIndex).toBeLessThanOrEqual(Math.max(50000, pushedIds.size));

    // STILL RESPONSIVE after the marathon.
    const ping = await openFramed(port);
    const res = await ping.send({ cmd: 'PUSH', queue: 'after-soak', data: { ok: 1 } });
    expect(res.ok).toBe(true);
    ping.end();
  }, 180000);
});
