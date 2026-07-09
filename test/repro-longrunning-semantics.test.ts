/**
 * LONG-RUNNING SEMANTICS (category 8) — behaviors that only bite after days.
 *
 * Bugs that never show in a 5-minute test but wreck a 24/7 service: cron drift
 * accumulating over thousands of ticks, DST transitions, and — the classic —
 * unbounded memory growth as per-job bookkeeping (results, custom-id dedup, DLQ)
 * accretes forever. These tests assert:
 *
 *   - CRON DOES NOT DRIFT: next-run stays exactly on the schedule across many
 *     ticks; fixed intervals anchor to the slot; DST transitions resolve.
 *   - LRU COLLECTIONS ARE BOUNDED UNDER PRESSURE: jobResults and the custom-id
 *     dedup map never exceed their caps no matter how many jobs flow through —
 *     the oldest are evicted, the newest are retained, nothing crashes.
 *   - DLQ IS BOUNDED AND RETRYABLE: the DLQ evicts oldest past maxEntries and
 *     stays retryable, so a queue that fails forever can't OOM the server.
 */

import { afterEach, describe, expect, it } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';
import { FrameParser } from '../src/infrastructure/server/protocol';
import { getNextCronRun, getNextIntervalRun } from '../src/infrastructure/scheduler/cronParser';
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
      const reqId = `l${counter++}`;
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

function startServer(config?: ConstructorParameters<typeof QueueManager>[0]): Harness {
  const qm = new QueueManager({ stallCheckMs: 200, ...config });
  const port = getFreePort();
  const server = createTcpServer(qm, { hostname: '127.0.0.1', port });
  const h: Harness = { qm, server, port };
  harness = h;
  return h;
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

describe('LONG-RUNNING — cron does not drift over time', () => {
  it('L1a: "every minute" next-run stays exactly 60s apart across 1000 ticks (no accumulating drift)', () => {
    const expr = '* * * * *'; // every minute
    const t = 1_700_000_000_000; // fixed epoch, aligned computation is croner's job
    let prev = getNextCronRun(expr, t);
    const firstSlot = prev;
    for (let i = 0; i < 1000; i++) {
      const next = getNextCronRun(expr, prev);
      expect(next - prev).toBe(60_000); // exactly one minute, every time
      prev = next;
    }
    // After 1000 minutes the slot is exactly 1000 minutes past the first — the
    // schedule has not slipped by even a millisecond.
    expect(prev - firstSlot).toBe(1000 * 60_000);
  });

  it('L1b: hourly cron stays exactly 3600s apart across 500 ticks', () => {
    const expr = '0 * * * *'; // top of every hour
    let prev = getNextCronRun(expr, 1_700_000_000_000);
    for (let i = 0; i < 500; i++) {
      const next = getNextCronRun(expr, prev);
      expect(next - prev).toBe(3_600_000);
      prev = next;
    }
  });

  it('L1c: fixed-interval next-run anchors to lastRun (no drift from processing time)', () => {
    const interval = 30_000;
    let last = 1_700_000_000_000;
    for (let i = 0; i < 1000; i++) {
      const next = getNextIntervalRun(interval, last);
      expect(next - last).toBe(interval); // anchored to the slot, not wall-clock
      last = next;
    }
  });

  it('L1d: DST spring-forward and fall-back both resolve to valid forward instants', () => {
    const expr = '30 2 * * *'; // 02:30 daily
    const tz = 'Europe/Rome';
    // Spring forward 2024-03-31 (02:00→03:00) and fall back 2024-10-27 (03:00→02:00).
    for (const from of [Date.UTC(2024, 2, 30, 12, 0, 0), Date.UTC(2024, 9, 26, 12, 0, 0)]) {
      const next = getNextCronRun(expr, from, tz);
      expect(next).toBeGreaterThan(from);
      expect(Number.isFinite(next)).toBe(true);
      expect(next).toBeGreaterThan(0);
    }
  });
});

describe('LONG-RUNNING — LRU collections stay bounded under pressure', () => {
  it('L2: jobResults never exceeds its cap; oldest results evict, newest are retained', async () => {
    const CAP = 100;
    const h = startServer({ maxJobResults: CAP, maxCompletedJobs: 1000 });
    const c = await openFramed(h.port);
    const QUEUE = 'lr-results';
    const TOTAL = 400; // 4x the cap

    const ids: string[] = [];
    for (let i = 0; i < TOTAL; i++) {
      const p = await c.send({ cmd: 'PUSH', queue: QUEUE, data: { i } });
      const id = String(p.id);
      ids.push(id);
      const pull = await c.send({ cmd: 'PULL', queue: QUEUE, owner: 'w', lockTtl: 30000 });
      await c.send({
        cmd: 'ACK',
        id,
        token: pull.token as string,
        result: { i, value: `r${i}` },
      });
    }

    // Bounded: jobResults never grew past its cap despite 400 completions.
    const mem = h.qm.getMemoryStats();
    expect(mem.jobResults).toBeLessThanOrEqual(CAP);

    // Newest result retained; oldest evicted (result no longer retrievable).
    const newest = await c.send({ cmd: 'GetResult', queue: QUEUE, id: ids[TOTAL - 1] });
    expect((newest.result as { i: number })?.i).toBe(TOTAL - 1);
    const oldest = await c.send({ cmd: 'GetResult', queue: QUEUE, id: ids[0] });
    expect(oldest.result ?? null).toBeNull(); // evicted → null, not a crash

    c.end();
  }, 60000);

  it('L3: custom-id dedup map stays bounded under pressure; dedup holds within the retention window', async () => {
    const CAP = 100;
    const h = startServer({ maxCustomIds: CAP, maxCompletedJobs: 1000 });
    const c = await openFramed(h.port);
    const QUEUE = 'lr-customid';
    const TOTAL = 400;

    // Push TOTAL jobs each with a distinct custom jobId (never completing them,
    // so they stay live and exercise the custom-id map under pressure).
    for (let i = 0; i < TOTAL; i++) {
      const p = await c.send({ cmd: 'PUSH', queue: QUEUE, data: { i }, jobId: `cid-${i}` });
      expect(p.ok).toBe(true);
    }

    // Bounded: the custom-id map never exceeded its cap.
    const mem = h.qm.getMemoryStats();
    expect(mem.customIdMap).toBeLessThanOrEqual(CAP);

    // Dedup still works for a RECENT id (within the retention window): re-adding
    // it does not create a second job.
    const recent = `cid-${TOTAL - 1}`;
    const before = await c.send({ cmd: 'GetJobCounts', queue: QUEUE });
    const beforeWaiting = (before.counts as Record<string, number>).waiting ?? 0;
    const re = await c.send({ cmd: 'PUSH', queue: QUEUE, data: { x: 1 }, jobId: recent });
    expect(re.ok).toBe(true);
    const after = await c.send({ cmd: 'GetJobCounts', queue: QUEUE });
    const afterWaiting = (after.counts as Record<string, number>).waiting ?? 0;
    // No net-new job for the deduped recent id.
    expect(afterWaiting).toBe(beforeWaiting);

    // The server did not crash and is still serving.
    const ping = await c.send({ cmd: 'Ping' });
    expect(ping).toBeTruthy();
    c.end();
  }, 60000);
});

describe('LONG-RUNNING — DLQ bounded and retryable', () => {
  it('L4: a queue that fails forever keeps its DLQ bounded to maxEntries (oldest evicted), and entries remain retryable', async () => {
    const MAX_ENTRIES = 50;
    const h = startServer();
    const QUEUE = 'lr-dlq';
    // Set the per-queue DLQ cap on every shard (only the owning shard matters).
    for (const shard of h.qm.getShards()) {
      shard.setDlqConfig(QUEUE, { maxEntries: MAX_ENTRIES, autoRetry: false });
    }

    const c = await openFramed(h.port);
    const TOTAL = 250; // 5x the DLQ cap

    for (let i = 0; i < TOTAL; i++) {
      const p = await c.send({ cmd: 'PUSH', queue: QUEUE, data: { i } });
      const id = String(p.id);
      const pull = await c.send({ cmd: 'PULL', queue: QUEUE, owner: 'w', lockTtl: 30000 });
      // Fail terminally → straight to DLQ.
      await c.send({
        cmd: 'FAIL',
        id,
        token: pull.token as string,
        error: 'always',
        unrecoverable: true,
      });
    }

    // DLQ bounded: never exceeded maxEntries despite 250 failures.
    const dlq = await c.send({ cmd: 'Dlq', queue: QUEUE, count: 10000 });
    const entries = dlq.jobs as unknown[];
    expect(entries.length).toBeLessThanOrEqual(MAX_ENTRIES);
    expect(entries.length).toBeGreaterThan(0);

    // Still retryable: RetryDlq moves surviving entries back to the queue.
    const retry = await c.send({ cmd: 'RetryDlq', queue: QUEUE });
    expect(retry.ok).toBe(true);
    const counts = await c.send({ cmd: 'GetJobCounts', queue: QUEUE });
    const waiting = (counts.counts as Record<string, number>).waiting ?? 0;
    expect(waiting).toBeGreaterThan(0);
    expect(waiting).toBeLessThanOrEqual(MAX_ENTRIES);

    c.end();
  }, 60000);
});
