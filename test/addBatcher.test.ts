import { describe, test, expect, afterEach } from 'bun:test';
import { AddBatcher, type FlushCallback } from '../src/client/queue/addBatcher';
import type { Job } from '../src/client/types';

// ─── Helpers ────────────────────────────────────────────────────

type D = { value: number };

function mockJob(id: string, name: string, data: D): Job<D> {
  return { id, name, data, queueName: 'test', attemptsMade: 0, timestamp: Date.now(), progress: 0, delay: 0, stacktrace: null, stalledCounter: 0, priority: 0, opts: {}, attemptsStarted: 0 } as Job<D>;
}

/** Auto-resolving flush: resolves immediately with generated Job objects */
function autoFlush() {
  const batches: number[] = []; // records batch sizes
  const cb: FlushCallback<D> = (jobs) => {
    batches.push(jobs.length);
    return Promise.resolve(jobs.map((j, i) => mockJob(String(i + 1), j.name, j.data)));
  };
  return { cb, batches };
}

/**
 * Controlled flush: each invocation pushes a handle to `calls`.
 * Tests resolve/reject each flush manually.
 * Eliminates timing dependencies.
 */
function controlledFlush() {
  const calls: Array<{
    jobs: Array<{ name: string; data: D; opts?: unknown }>;
    resolve: (jobs: Job<D>[]) => void;
    reject: (err: Error) => void;
  }> = [];

  const cb: FlushCallback<D> = (jobs) =>
    new Promise<Job<D>[]>((resolve, reject) => {
      calls.push({ jobs: jobs as typeof calls[0]['jobs'], resolve, reject });
    });

  return { cb, calls };
}

/** Resolve a controlled flush handle with auto-generated Job IDs */
function resolveHandle(
  handle: { jobs: Array<{ name: string; data: D }>; resolve: (j: Job<D>[]) => void },
  prefix = '',
) {
  handle.resolve(handle.jobs.map((j, i) => mockJob(`${prefix}${i + 1}`, j.name, j.data)));
}

/** Yield microtasks until doFlush loop fully completes */
async function yieldMicrotasks(n = 5) {
  for (let i = 0; i < n; i++) await Promise.resolve();
}

// ─── Tests ──────────────────────────────────────────────────────

describe('AddBatcher', () => {
  let batcher: AddBatcher<D> | null = null;

  afterEach(() => {
    // stop() may reject pending promises — no-op if already stopped/null
    try { batcher?.stop(); } catch { /* ignore */ }
    batcher = null;
  });

  // ─── Immediate flush (no in-flight) ───────────────────────────

  describe('immediate flush (no in-flight)', () => {
    test('single enqueue flushes immediately', async () => {
      const { cb, calls } = controlledFlush();
      batcher = new AddBatcher({ maxSize: 50, maxDelayMs: 1000 }, cb);

      const p = batcher.enqueue('job1', { value: 1 });

      // Flush callback was invoked synchronously inside enqueue
      expect(calls).toHaveLength(1);
      expect(calls[0].jobs).toHaveLength(1);
      expect(calls[0].jobs[0].name).toBe('job1');

      resolveHandle(calls[0]);
      const job = await p;
      expect(job.id).toBe('1');
    });

    test('sequential awaits work without timer penalty', async () => {
      const { cb, batches } = autoFlush();
      batcher = new AddBatcher({ maxSize: 50, maxDelayMs: 1000 }, cb);

      const j1 = await batcher.enqueue('a', { value: 1 });
      const j2 = await batcher.enqueue('b', { value: 2 });
      const j3 = await batcher.enqueue('c', { value: 3 });

      expect(j1.name).toBe('a');
      expect(j2.name).toBe('b');
      expect(j3.name).toBe('c');
      // Each add resolved — no timer delay
      expect(batches.length).toBeGreaterThanOrEqual(1);
    });
  });

  // ─── Batching during in-flight flush ──────────────────────────

  describe('batching during in-flight flush', () => {
    test('items enqueued during flush are batched together', async () => {
      const { cb, calls } = controlledFlush();
      batcher = new AddBatcher({ maxSize: 50, maxDelayMs: 5 }, cb);

      // First enqueue triggers immediate flush (held open)
      const p1 = batcher.enqueue('a', { value: 1 });
      expect(calls).toHaveLength(1);
      expect(calls[0].jobs).toHaveLength(1);

      // These arrive while first flush is in-flight — buffered
      const p2 = batcher.enqueue('b', { value: 2 });
      const p3 = batcher.enqueue('c', { value: 3 });

      // Resolve first flush → doFlush loop drains buffered items
      resolveHandle(calls[0], 'f1-');
      await p1;
      await yieldMicrotasks();

      // Second flush picked up the 2 batched items
      expect(calls).toHaveLength(2);
      expect(calls[1].jobs).toHaveLength(2);
      expect(calls[1].jobs[0].name).toBe('b');
      expect(calls[1].jobs[1].name).toBe('c');

      resolveHandle(calls[1], 'f2-');
      const [j2, j3] = await Promise.all([p2, p3]);
      expect(j2.id).toBe('f2-1');
      expect(j3.id).toBe('f2-2');
    });

    test('large concurrent burst batches correctly', async () => {
      const { cb, calls } = controlledFlush();
      batcher = new AddBatcher({ maxSize: 200, maxDelayMs: 5 }, cb);

      // Fire 100 adds concurrently
      const promises = Array.from({ length: 100 }, (_, i) =>
        batcher!.enqueue(`job-${i}`, { value: i })
      );

      // First enqueue triggered immediate flush with 1 item
      expect(calls).toHaveLength(1);
      expect(calls[0].jobs).toHaveLength(1);

      // Resolve first → doFlush drains remaining 99
      resolveHandle(calls[0], 'f1-');
      await yieldMicrotasks();

      expect(calls).toHaveLength(2);
      expect(calls[1].jobs).toHaveLength(99);

      resolveHandle(calls[1], 'f2-');
      const results = await Promise.all(promises);
      expect(results).toHaveLength(100);
      expect(results[0].id).toBe('f1-1');
    });
  });

  // ─── maxSize threshold ────────────────────────────────────────

  describe('maxSize threshold', () => {
    test('triggers flush when buffer reaches maxSize during in-flight', async () => {
      const { cb, calls } = controlledFlush();
      batcher = new AddBatcher({ maxSize: 3, maxDelayMs: 100000 }, cb);

      // First triggers immediate flush (held open)
      const p1 = batcher.enqueue('a', { value: 1 });
      expect(calls).toHaveLength(1);

      // Buffer accumulates: 2 items (below maxSize=3)
      const p2 = batcher.enqueue('b', { value: 2 });
      const p3 = batcher.enqueue('c', { value: 3 });
      expect(calls).toHaveLength(1); // still buffered

      // 4th item → buffer has 3 items → hits maxSize → triggers new flush
      const p4 = batcher.enqueue('d', { value: 4 });
      expect(calls).toHaveLength(2);
      expect(calls[1].jobs).toHaveLength(3);

      resolveHandle(calls[0], 'f1-');
      resolveHandle(calls[1], 'f2-');
      const [j1, j2, j3, j4] = await Promise.all([p1, p2, p3, p4]);

      expect(j1.id).toBe('f1-1');
      expect(j2.id).toBe('f2-1');
      expect(j3.id).toBe('f2-2');
      expect(j4.id).toBe('f2-3');
    });
  });

  // ─── Error propagation ────────────────────────────────────────

  describe('error propagation', () => {
    test('rejects single promise on flush error', async () => {
      const { cb, calls } = controlledFlush();
      batcher = new AddBatcher({ maxSize: 50, maxDelayMs: 5 }, cb);

      const p = batcher.enqueue('a', { value: 1 });
      calls[0].reject(new Error('PUSHB failed'));

      await expect(p).rejects.toThrow('PUSHB failed');
    });

    test('rejects ALL promises in a batched flush on error', async () => {
      const { cb, calls } = controlledFlush();
      batcher = new AddBatcher({ maxSize: 50, maxDelayMs: 5 }, cb);

      // Hold first flush open so p2/p3 accumulate in buffer
      const p1 = batcher.enqueue('a', { value: 1 });
      const p2 = batcher.enqueue('b', { value: 2 });
      const p3 = batcher.enqueue('c', { value: 3 });

      // Resolve first flush (1 item) → doFlush drains buffered p2/p3
      resolveHandle(calls[0]);
      await p1;
      await yieldMicrotasks();

      // Second flush has 2 items — reject it
      expect(calls[1].jobs).toHaveLength(2);

      // Attach handlers BEFORE rejecting to avoid Bun's unhandled rejection detection
      // (p2 and p3 are rejected in the same microtask, so serial await would leave p3 unhandled)
      const settled = Promise.allSettled([p2, p3]);
      calls[1].reject(new Error('network error'));

      const results = await settled;
      expect(results[0].status).toBe('rejected');
      expect(results[1].status).toBe('rejected');
      if (results[0].status === 'rejected') expect(results[0].reason.message).toBe('network error');
      if (results[1].status === 'rejected') expect(results[1].reason.message).toBe('network error');
    });

    test('error in one flush does not affect next flush', async () => {
      const { cb, calls } = controlledFlush();
      batcher = new AddBatcher({ maxSize: 50, maxDelayMs: 5 }, cb);

      const p1 = batcher.enqueue('a', { value: 1 });
      calls[0].reject(new Error('transient'));
      await expect(p1).rejects.toThrow('transient');
      await yieldMicrotasks();

      const p2 = batcher.enqueue('b', { value: 2 });
      resolveHandle(calls[1]);
      const j2 = await p2;
      expect(j2.name).toBe('b');
    });
  });

  // ─── Overflow protection ──────────────────────────────────────

  describe('overflow protection', () => {
    test('drops oldest 10% when buffer exceeds maxPending', async () => {
      const { cb, calls } = controlledFlush();
      // Use small maxPending=20 to keep test fast
      batcher = new AddBatcher({ maxSize: 100, maxDelayMs: 100000, maxPending: 20 }, cb);

      // Hold first flush open so items buffer
      batcher.enqueue('first', { value: 0 }).catch(() => {});
      expect(calls).toHaveLength(1);

      // Fill buffer to maxPending (20 items buffered while flush in-flight)
      const buffered: Promise<Job<D>>[] = [];
      for (let i = 0; i < 20; i++) {
        buffered.push(batcher.enqueue(`job-${i}`, { value: i }));
      }

      // Attach catch handlers to all buffered promises to prevent
      // unhandled rejections when afterEach calls stop()
      for (const p of buffered) p.catch(() => {});

      // Next enqueue triggers overflow — oldest 10% (2 items) rejected
      batcher.enqueue('overflow', { value: 999 }).catch(() => {});

      const [r0, r1] = await Promise.allSettled([buffered[0], buffered[1]]);
      expect(r0.status).toBe('rejected');
      expect(r1.status).toBe('rejected');
      if (r0.status === 'rejected') expect(r0.reason.message).toContain('overflow');
      if (r1.status === 'rejected') expect(r1.reason.message).toContain('overflow');

      // Non-dropped item is still pending (not rejected yet)
      const race = await Promise.race([
        buffered[2].then(() => 'resolved' as const),
        Bun.sleep(20).then(() => 'timeout' as const),
      ]);
      expect(race).toBe('timeout');
    });
  });

  // ─── Stop ─────────────────────────────────────────────────────

  describe('stop', () => {
    test('rejects buffered entries on stop', async () => {
      const { cb } = controlledFlush();
      batcher = new AddBatcher({ maxSize: 100, maxDelayMs: 100000 }, cb);

      // Hold first flush open
      batcher.enqueue('a', { value: 1 }).catch(() => {});

      // These buffer while flush is in-flight
      const p2 = batcher.enqueue('b', { value: 2 });
      const p3 = batcher.enqueue('c', { value: 3 });

      batcher.stop();
      batcher = null;

      await expect(p2).rejects.toThrow('AddBatcher stopped');
      await expect(p3).rejects.toThrow('AddBatcher stopped');
    });

    test('rejects new enqueues after stop', async () => {
      const { cb } = autoFlush();
      batcher = new AddBatcher({ maxSize: 100, maxDelayMs: 100000 }, cb);
      batcher.stop();

      const p = batcher.enqueue('a', { value: 1 });
      batcher = null;
      await expect(p).rejects.toThrow('AddBatcher stopped');
    });
  });

  // ─── flush() / waitForInFlight() ──────────────────────────────

  describe('flush and waitForInFlight', () => {
    test('flush is no-op when buffer is empty', async () => {
      const { cb, calls } = controlledFlush();
      batcher = new AddBatcher({ maxSize: 100, maxDelayMs: 100000 }, cb);

      await batcher.flush();
      expect(calls).toHaveLength(0);
    });

    test('waitForInFlight resolves immediately with no flushes', async () => {
      const { cb } = autoFlush();
      batcher = new AddBatcher({ maxSize: 100, maxDelayMs: 100000 }, cb);
      await batcher.waitForInFlight(); // must not hang
    });

    test('waitForInFlight blocks until in-flight flush completes', async () => {
      const { cb, calls } = controlledFlush();
      batcher = new AddBatcher({ maxSize: 100, maxDelayMs: 100000 }, cb);

      batcher.enqueue('a', { value: 1 }).catch(() => {});

      let waited = false;
      const waitP = batcher.waitForInFlight().then(() => { waited = true; });

      await Promise.resolve();
      expect(waited).toBe(false);

      resolveHandle(calls[0]);
      await waitP;
      expect(waited).toBe(true);
    });
  });

  // ─── Options passthrough ──────────────────────────────────────

  describe('opts passthrough', () => {
    test('forwards JobOptions to flush callback', async () => {
      const { cb, calls } = controlledFlush();
      batcher = new AddBatcher({ maxSize: 50, maxDelayMs: 1000 }, cb);

      const p1 = batcher.enqueue('a', { value: 1 }, { priority: 10, delay: 5000 });
      expect(calls[0].jobs[0]).toMatchObject({ name: 'a', opts: { priority: 10, delay: 5000 } });
      resolveHandle(calls[0]);
      await p1;
      await yieldMicrotasks();

      const p2 = batcher.enqueue('b', { value: 2 }, { attempts: 5 });
      expect(calls[1].jobs[0]).toMatchObject({ name: 'b', opts: { attempts: 5 } });
      resolveHandle(calls[1]);
      await p2;
    });

    test('undefined opts passed through as undefined', async () => {
      const { cb, calls } = controlledFlush();
      batcher = new AddBatcher({ maxSize: 50, maxDelayMs: 1000 }, cb);

      const p = batcher.enqueue('a', { value: 1 });
      expect(calls[0].jobs[0].opts).toBeUndefined();
      resolveHandle(calls[0]);
      await p;
    });
  });

  // ─── Durable bypass (Queue-level) ─────────────────────────────

  describe('durable bypass', () => {
    test('durable option is passed through — Queue decides to bypass', async () => {
      const { cb, calls } = controlledFlush();
      batcher = new AddBatcher({ maxSize: 50, maxDelayMs: 1000 }, cb);

      // AddBatcher itself doesn't check durable — it just passes opts.
      // The Queue.add() method checks durable and bypasses the batcher.
      // Verify opts are preserved so Queue can inspect them.
      const p = batcher.enqueue('a', { value: 1 }, { durable: true });
      expect(calls[0].jobs[0].opts).toMatchObject({ durable: true });
      resolveHandle(calls[0]);
      await p;
    });
  });
});
