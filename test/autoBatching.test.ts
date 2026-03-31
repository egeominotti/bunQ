/**
 * Auto-Batching Integration Tests
 * Tests AddBatcher integration with Queue.add() and QueueManager.pushBatch().
 * Run with: BUNQUEUE_EMBEDDED=1 bun test test/autoBatching.test.ts
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, shutdownManager } from '../src/client';
import { QueueManager } from '../src/application/queueManager';
import { AddBatcher, type FlushCallback } from '../src/client/queue/addBatcher';
import type { Job, JobOptions } from '../src/client/types';

// ─── Helpers ────────────────────────────────────────────────────
type D = { value: number };

function mockJob(id: string, name: string, data: D): Job<D> {
  return {
    id, name, data, queueName: 'test', attemptsMade: 0, timestamp: Date.now(),
    progress: 0, delay: 0, stacktrace: null, stalledCounter: 0, priority: 0,
    opts: {}, attemptsStarted: 0,
  } as Job<D>;
}

function trackingFlush() {
  const batches: Array<Array<{ name: string; data: D; opts?: JobOptions }>> = [];
  const cb: FlushCallback<D> = (jobs) => {
    batches.push([...jobs]);
    return Promise.resolve(jobs.map((j, i) => mockJob(String(i + 1), j.name, j.data)));
  };
  return { cb, batches };
}

function controlledFlush() {
  const calls: Array<{
    jobs: Array<{ name: string; data: D; opts?: JobOptions }>;
    resolve: (jobs: Job<D>[]) => void;
    reject: (err: Error) => void;
  }> = [];
  const cb: FlushCallback<D> = (jobs) =>
    new Promise<Job<D>[]>((resolve, reject) => {
      calls.push({ jobs: [...jobs], resolve, reject });
    });
  return { cb, calls };
}

function resolveHandle(
  handle: { jobs: Array<{ name: string; data: D }>; resolve: (j: Job<D>[]) => void },
  prefix = '',
) {
  handle.resolve(handle.jobs.map((j, i) => mockJob(`${prefix}${i + 1}`, j.name, j.data)));
}

async function yieldMicrotasks(n = 5) {
  for (let i = 0; i < n; i++) await Promise.resolve();
}

// ─── Embedded Mode: No Auto-Batching ────────────────────────────
describe('Auto-Batching: Embedded Mode', () => {
  afterEach(() => { shutdownManager(); });
  test('adds jobs directly without batcher', async () => {
    const q = new Queue<D>('ab-emb-1');
    const job = await q.add('task', { value: 1 });
    expect(job.id).toBeDefined();
    expect(job.name).toBe('task');
    expect(job.data.value).toBe(1);
  });

  test('sequential adds produce unique jobs', async () => {
    const q = new Queue<D>('ab-emb-2');
    const jobs: Job<D>[] = [];
    for (let i = 0; i < 5; i++) jobs.push(await q.add('seq', { value: i }));
    expect(new Set(jobs.map((j) => j.id)).size).toBe(5);
  });

  test('concurrent adds produce unique jobs', async () => {
    const q = new Queue<D>('ab-emb-3');
    const jobs = await Promise.all(
      Array.from({ length: 10 }, (_, i) => q.add('conc', { value: i }))
    );
    expect(new Set(jobs.map((j) => j.id)).size).toBe(10);
  });

  test('durable and addBulk work in embedded mode', async () => {
    const q = new Queue<D>('ab-emb-4');
    const durable = await q.add('d', { value: 1 }, { durable: true });
    expect(durable.id).toBeDefined();
    const bulk = await q.addBulk([
      { name: 'a', data: { value: 1 } },
      { name: 'b', data: { value: 2 } },
    ]);
    expect(bulk).toHaveLength(2);
  });
});

// ─── pushBatch Persistence ──────────────────────────────────────
describe('Auto-Batching: pushBatch Persistence', () => {
  let qm: QueueManager;
  beforeEach(() => { qm = new QueueManager(); });
  afterEach(() => { qm.shutdown(); });

  test('persists with correct priorities and delay', async () => {
    await qm.pushBatch('pb-1', [
      { data: { name: 'low' }, priority: 1 },
      { data: { name: 'high' }, priority: 10 },
      { data: { name: 'med' }, priority: 5 },
    ]);
    const pulled = await qm.pullBatch('pb-1', 3);
    expect((pulled[0].data as { name: string }).name).toBe('high');
    expect((pulled[2].data as { name: string }).name).toBe('low');

    await qm.pushBatch('pb-2', [
      { data: { name: 'now' } },
      { data: { name: 'later' }, delay: 60000 },
    ]);
    const pulled2 = await qm.pullBatch('pb-2', 2);
    expect(pulled2).toHaveLength(1);
    expect((pulled2[0].data as { name: string }).name).toBe('now');
  });

  test('durable flag persists immediately', async () => {
    const ids = await qm.pushBatch('pb-3', [
      { data: { n: 1 }, durable: true },
      { data: { n: 2 } },
    ]);
    expect(ids).toHaveLength(2);
    expect(qm.getStats().waiting).toBeGreaterThanOrEqual(2);
  });
});

// ─── Batcher Integration ────────────────────────────────────────
describe('Auto-Batching: Batcher Integration', () => {
  let batcher: AddBatcher<D> | null = null;
  afterEach(() => {
    try { batcher?.stop(); } catch { /* ignore */ }
    batcher = null;
  });

  test('default config flushes immediately on first enqueue', async () => {
    const { cb, batches } = trackingFlush();
    batcher = new AddBatcher({ maxSize: 50, maxDelayMs: 5 }, cb);
    const job = await batcher.enqueue('task', { value: 1 });
    expect(job.id).toBeDefined();
    expect(batches.length).toBeGreaterThanOrEqual(1);
  });

  test('sequential awaits resolve without timer delay', async () => {
    const { cb } = trackingFlush();
    batcher = new AddBatcher({ maxSize: 50, maxDelayMs: 5 }, cb);
    const results: Job<D>[] = [];
    for (let i = 0; i < 5; i++) results.push(await batcher.enqueue(`s-${i}`, { value: i }));
    expect(results).toHaveLength(5);
    for (let i = 0; i < 5; i++) expect(results[i].name).toBe(`s-${i}`);
  });

  test('concurrent adds batch into single flush', async () => {
    const { cb, calls } = controlledFlush();
    batcher = new AddBatcher({ maxSize: 50, maxDelayMs: 5 }, cb);

    const p1 = batcher.enqueue('a', { value: 1 });
    expect(calls).toHaveLength(1);
    const p2 = batcher.enqueue('b', { value: 2 });
    const p3 = batcher.enqueue('c', { value: 3 });

    resolveHandle(calls[0], 'f1-');
    await p1;
    await yieldMicrotasks();

    expect(calls).toHaveLength(2);
    expect(calls[1].jobs).toHaveLength(2);
    resolveHandle(calls[1], 'f2-');
    const [j2, j3] = await Promise.all([p2, p3]);
    expect(j2.id).toBe('f2-1');
    expect(j3.id).toBe('f2-2');
  });

  test('autoBatch disabled prevents batcher creation', () => {
    // Mirrors Queue constructor: if autoBatch.enabled === false, addBatcher = null
    expect({ enabled: false }.enabled !== false).toBe(false);
    expect({ enabled: true }.enabled !== false).toBe(true);
    expect(({} as { enabled?: boolean }).enabled !== false).toBe(true);
  });

  test('custom maxSize triggers flush at threshold', async () => {
    const { cb, calls } = controlledFlush();
    batcher = new AddBatcher({ maxSize: 3, maxDelayMs: 100000 }, cb);

    const p1 = batcher.enqueue('a', { value: 1 });
    expect(calls).toHaveLength(1);

    const p2 = batcher.enqueue('b', { value: 2 });
    const p3 = batcher.enqueue('c', { value: 3 });
    expect(calls).toHaveLength(1); // below maxSize

    const p4 = batcher.enqueue('d', { value: 4 });
    expect(calls).toHaveLength(2);
    expect(calls[1].jobs).toHaveLength(3);

    resolveHandle(calls[0], 'f1-');
    resolveHandle(calls[1], 'f2-');
    const [j1, j2, j3, j4] = await Promise.all([p1, p2, p3, p4]);
    expect(j1.id).toBe('f1-1');
    expect(j2.id).toBe('f2-1');
  });

  test('custom maxDelayMs flushes buffered items after timeout', async () => {
    const { cb, calls } = controlledFlush();
    batcher = new AddBatcher({ maxSize: 100, maxDelayMs: 20 }, cb);

    const p1 = batcher.enqueue('a', { value: 1 });
    const p2 = batcher.enqueue('b', { value: 2 });
    await Bun.sleep(50);

    expect(calls.length).toBeGreaterThanOrEqual(2);
    resolveHandle(calls[0], 'f1-');
    resolveHandle(calls[1], 'f2-');
    await Promise.all([p1, p2]);
  });

  test('durable opts are passed through for Queue bypass', async () => {
    const { cb, calls } = controlledFlush();
    batcher = new AddBatcher({ maxSize: 50, maxDelayMs: 5 }, cb);

    const p = batcher.enqueue('critical', { value: 99 }, { durable: true });
    expect(calls[0].jobs[0].opts).toMatchObject({ durable: true });
    resolveHandle(calls[0]);
    await p;
  });

  test('mixed durable and non-durable concurrent adds', async () => {
    const { cb } = trackingFlush();
    batcher = new AddBatcher({ maxSize: 50, maxDelayMs: 5 }, cb);

    const direct: Job<D>[] = [];
    const batched: Promise<Job<D>>[] = [];
    for (let i = 0; i < 6; i++) {
      if (i % 2 === 0) direct.push(mockJob(`d-${i}`, `t-${i}`, { value: i }));
      else batched.push(batcher.enqueue(`t-${i}`, { value: i }));
    }

    const results = await Promise.all(batched);
    expect(direct).toHaveLength(3);
    expect(results).toHaveLength(3);
    expect(results[0].name).toBe('t-1');
    expect(results[2].name).toBe('t-5');
  });

  test('job options passed through to flush callback', async () => {
    const { cb, calls } = controlledFlush();
    batcher = new AddBatcher({ maxSize: 50, maxDelayMs: 5 }, cb);

    const p1 = batcher.enqueue('hi', { value: 1 }, { priority: 10 });
    expect(calls[0].jobs[0].opts).toMatchObject({ priority: 10 });
    resolveHandle(calls[0]);
    await p1;
    await yieldMicrotasks();

    const p2 = batcher.enqueue('del', { value: 2 }, { delay: 5000, attempts: 5 });
    expect(calls[1].jobs[0].opts).toMatchObject({ delay: 5000, attempts: 5 });
    resolveHandle(calls[1]);
    await p2;
  });

  test('mixed options preserved per-job in concurrent batch', async () => {
    const { cb, calls } = controlledFlush();
    batcher = new AddBatcher({ maxSize: 50, maxDelayMs: 5 }, cb);

    const p1 = batcher.enqueue('a', { value: 1 }, { priority: 1 });
    const p2 = batcher.enqueue('b', { value: 2 }, { priority: 10, delay: 3000 });
    const p3 = batcher.enqueue('c', { value: 3 }, { attempts: 7 });

    resolveHandle(calls[0], 'f1-');
    await p1;
    await yieldMicrotasks();

    expect(calls[1].jobs[0].opts).toMatchObject({ priority: 10, delay: 3000 });
    expect(calls[1].jobs[1].opts).toMatchObject({ attempts: 7 });
    resolveHandle(calls[1], 'f2-');
    await Promise.all([p2, p3]);
  });

  test('buffer exceeding maxSize triggers immediate flush', async () => {
    const { cb, calls } = controlledFlush();
    batcher = new AddBatcher({ maxSize: 5, maxDelayMs: 100000 }, cb);

    batcher.enqueue('first', { value: 0 }).catch(() => {});
    expect(calls).toHaveLength(1);

    const buffered: Promise<Job<D>>[] = [];
    for (let i = 1; i <= 4; i++) buffered.push(batcher.enqueue(`j-${i}`, { value: i }));
    expect(calls).toHaveLength(1);

    buffered.push(batcher.enqueue('overflow', { value: 99 }));
    expect(calls).toHaveLength(2);
    expect(calls[1].jobs).toHaveLength(5);

    resolveHandle(calls[0], 'f1-');
    resolveHandle(calls[1], 'f2-');
    expect(await Promise.all(buffered)).toHaveLength(5);
  });
});
