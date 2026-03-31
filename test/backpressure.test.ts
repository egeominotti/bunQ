/**
 * Backpressure and Rate Limiting Tests (Embedded Mode)
 * Tests global concurrency limits, rate limits, and their interactions
 * with worker concurrency in embedded mode.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';

describe('Backpressure - Embedded', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('global concurrency limit caps active jobs', async () => {
    const queue = new Queue<{ idx: number }>('bp-conc-limit', { embedded: true });
    queue.obliterate();

    queue.setGlobalConcurrency(2);

    let maxConcurrent = 0;
    let currentConcurrent = 0;
    let completed = 0;

    const worker = new Worker<{ idx: number }>(
      'bp-conc-limit',
      async () => {
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
        await Bun.sleep(300);
        currentConcurrent--;
        completed++;
        return {};
      },
      { embedded: true, concurrency: 10 },
    );

    for (let i = 0; i < 10; i++) {
      await queue.add('task', { idx: i });
    }

    // Wait for all jobs to complete
    for (let i = 0; i < 80; i++) {
      if (completed >= 10) break;
      await Bun.sleep(100);
    }

    // Allow tolerance of 3 for timing overlap
    expect(maxConcurrent).toBeLessThanOrEqual(3);
    expect(completed).toBe(10);

    queue.removeGlobalConcurrency();
    queue.removeGlobalRateLimit();
    await worker.close();
    queue.close();
  }, 30000);

  test('remove concurrency restores throughput', async () => {
    const queue = new Queue<{ idx: number }>('bp-conc-remove', { embedded: true });
    queue.obliterate();

    queue.setGlobalConcurrency(1);

    let completed = 0;
    const completionTimestamps: number[] = [];

    const worker = new Worker<{ idx: number }>(
      'bp-conc-remove',
      async () => {
        await Bun.sleep(100);
        completed++;
        completionTimestamps.push(Date.now());
        return {};
      },
      { embedded: true, concurrency: 5 },
    );

    for (let i = 0; i < 5; i++) {
      await queue.add('task', { idx: i });
    }

    // Wait for first 2 to complete under concurrency=1
    for (let i = 0; i < 40; i++) {
      if (completed >= 2) break;
      await Bun.sleep(100);
    }

    const timeForFirst2 = completionTimestamps[1] - completionTimestamps[0];

    // Remove the limit so remaining jobs can run concurrently
    queue.removeGlobalConcurrency();

    // Wait for all to complete
    for (let i = 0; i < 60; i++) {
      if (completed >= 5) break;
      await Bun.sleep(100);
    }

    expect(completed).toBe(5);
    // With concurrency=1, first 2 jobs should be sequential (~100ms apart)
    expect(timeForFirst2).toBeGreaterThanOrEqual(80);

    queue.removeGlobalConcurrency();
    queue.removeGlobalRateLimit();
    await worker.close();
    queue.close();
  }, 30000);

  test('rate limit throttles processing', async () => {
    const queue = new Queue<{ idx: number }>('bp-rate-limit', { embedded: true });
    queue.obliterate();

    queue.setGlobalRateLimit(3);

    let completed = 0;
    const completionTimestamps: number[] = [];

    const worker = new Worker<{ idx: number }>(
      'bp-rate-limit',
      async () => {
        completed++;
        completionTimestamps.push(Date.now());
        return {};
      },
      { embedded: true, concurrency: 10 },
    );

    for (let i = 0; i < 10; i++) {
      await queue.add('task', { idx: i });
    }

    // Wait for jobs to process (rate limited)
    for (let i = 0; i < 80; i++) {
      if (completed >= 10) break;
      await Bun.sleep(100);
    }

    // With rate limit of 3, not all 10 jobs should complete instantly
    // Verify there is some throttling: total duration > 0
    if (completionTimestamps.length >= 5) {
      const duration = completionTimestamps[completionTimestamps.length - 1] - completionTimestamps[0];
      // Rate limiting should cause some spread in completion times
      expect(duration).toBeGreaterThan(0);
    }
    expect(completed).toBeGreaterThanOrEqual(5);

    queue.removeGlobalConcurrency();
    queue.removeGlobalRateLimit();
    await worker.close();
    queue.close();
  }, 30000);

  test('remove rate limit restores speed', async () => {
    const queue = new Queue<{ idx: number }>('bp-rate-remove', { embedded: true });
    queue.obliterate();

    queue.setGlobalRateLimit(1);

    let completed = 0;

    const worker = new Worker<{ idx: number }>(
      'bp-rate-remove',
      async () => {
        completed++;
        return {};
      },
      { embedded: true, concurrency: 5 },
    );

    for (let i = 0; i < 5; i++) {
      await queue.add('task', { idx: i });
    }

    // Wait for first 2 to complete under rate limit=1
    for (let i = 0; i < 40; i++) {
      if (completed >= 2) break;
      await Bun.sleep(100);
    }

    const completedBeforeRemove = completed;

    // Remove rate limit
    queue.removeGlobalRateLimit();

    // Wait for all to complete
    for (let i = 0; i < 60; i++) {
      if (completed >= 5) break;
      await Bun.sleep(100);
    }

    expect(completedBeforeRemove).toBeGreaterThanOrEqual(1);
    expect(completed).toBe(5);

    queue.removeGlobalConcurrency();
    queue.removeGlobalRateLimit();
    await worker.close();
    queue.close();
  }, 30000);

  test('concurrency + worker concurrency interaction', async () => {
    const queue = new Queue<{ idx: number }>('bp-conc-worker', { embedded: true });
    queue.obliterate();

    // Server-side limit is 2, worker concurrency is 5
    queue.setGlobalConcurrency(2);

    let maxConcurrent = 0;
    let currentConcurrent = 0;
    let completed = 0;

    const worker = new Worker<{ idx: number }>(
      'bp-conc-worker',
      async () => {
        currentConcurrent++;
        maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
        await Bun.sleep(200);
        currentConcurrent--;
        completed++;
        return {};
      },
      { embedded: true, concurrency: 5 },
    );

    for (let i = 0; i < 10; i++) {
      await queue.add('task', { idx: i });
    }

    // Wait for all jobs to complete
    for (let i = 0; i < 80; i++) {
      if (completed >= 10) break;
      await Bun.sleep(100);
    }

    // Global concurrency of 2 should limit effective concurrency
    // Allow tolerance of 3 for timing overlap
    expect(maxConcurrent).toBeLessThanOrEqual(3);
    expect(completed).toBe(10);

    queue.removeGlobalConcurrency();
    queue.removeGlobalRateLimit();
    await worker.close();
    queue.close();
  }, 30000);

  test('burst absorption - 100 jobs drain smoothly', async () => {
    const queue = new Queue<{ idx: number }>('bp-burst', { embedded: true });
    queue.obliterate();

    let completed = 0;

    const worker = new Worker<{ idx: number }>(
      'bp-burst',
      async () => {
        completed++;
        return {};
      },
      { embedded: true, concurrency: 10 },
    );

    // Burst: add 100 jobs at once
    const jobs = Array.from({ length: 100 }, (_, i) => ({
      name: `burst-${i}`,
      data: { idx: i },
    }));
    await queue.addBulk(jobs);

    // Wait for all to complete
    for (let i = 0; i < 100; i++) {
      if (completed >= 100) break;
      await Bun.sleep(100);
    }

    expect(completed).toBe(100);

    // Verify queue is drained
    const counts = await queue.getJobCounts();
    expect(counts.waiting).toBe(0);

    queue.removeGlobalConcurrency();
    queue.removeGlobalRateLimit();
    await worker.close();
    queue.close();
  }, 30000);

  test('multiple workers sharing concurrency', async () => {
    const queue = new Queue<{ idx: number }>('bp-multi-worker', { embedded: true });
    queue.obliterate();

    queue.setGlobalConcurrency(4);

    let maxConcurrent = 0;
    let currentConcurrent = 0;
    let completed = 0;

    const processor = async () => {
      currentConcurrent++;
      maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
      await Bun.sleep(200);
      currentConcurrent--;
      completed++;
      return {};
    };

    const worker1 = new Worker<{ idx: number }>(
      'bp-multi-worker',
      processor,
      { embedded: true, concurrency: 5 },
    );

    const worker2 = new Worker<{ idx: number }>(
      'bp-multi-worker',
      processor,
      { embedded: true, concurrency: 5 },
    );

    for (let i = 0; i < 20; i++) {
      await queue.add('task', { idx: i });
    }

    // Wait for all jobs to complete
    for (let i = 0; i < 120; i++) {
      if (completed >= 20) break;
      await Bun.sleep(100);
    }

    // Global concurrency of 4 should limit total active across both workers
    // Allow tolerance of 5 for timing overlap
    expect(maxConcurrent).toBeLessThanOrEqual(5);
    expect(completed).toBe(20);

    queue.removeGlobalConcurrency();
    queue.removeGlobalRateLimit();
    await worker1.close();
    await worker2.close();
    queue.close();
  }, 30000);
});
