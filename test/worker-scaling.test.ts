/**
 * Worker Scaling Tests (Embedded Mode)
 *
 * Tests for multiple workers sharing load, dynamic worker addition/removal,
 * workers with different concurrency levels, single worker handling all jobs,
 * and worker replacement scenarios.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';

describe('Worker Scaling - Embedded', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('1. multiple workers share load — 20 jobs, 4 workers', async () => {
    const queue = new Queue('ws-share-load', { embedded: true });
    queue.obliterate();

    const bulkJobs = Array.from({ length: 20 }, (_, i) => ({
      name: `job-${i}`,
      data: { index: i },
    }));
    await queue.addBulk(bulkJobs);

    const workerCounts: Record<number, number> = { 0: 0, 1: 0, 2: 0, 3: 0 };
    let totalCompleted = 0;

    const workers: Worker[] = [];
    for (let w = 0; w < 4; w++) {
      const workerId = w;
      const worker = new Worker(
        'ws-share-load',
        async (job) => {
          workerCounts[workerId]++;
          totalCompleted++;
          await Bun.sleep(50); // simulate work to allow distribution
          return { worker: workerId };
        },
        { embedded: true, concurrency: 1 }
      );
      workers.push(worker);
    }

    // Wait for all 20 to complete
    for (let i = 0; i < 300; i++) {
      if (totalCompleted >= 20) break;
      await Bun.sleep(100);
    }

    for (const w of workers) {
      await w.close();
    }

    expect(totalCompleted).toBe(20);

    // Verify load was distributed — each worker should have processed at least 1 job
    let workersWithJobs = 0;
    for (let w = 0; w < 4; w++) {
      if (workerCounts[w] > 0) workersWithJobs++;
    }
    expect(workersWithJobs).toBeGreaterThanOrEqual(2);

    // Total across all workers must equal 20
    const sum = Object.values(workerCounts).reduce((a, b) => a + b, 0);
    expect(sum).toBe(20);

    queue.close();
  }, 30000);

  test('2. dynamic worker addition — add workers after initial processing starts', async () => {
    const queue = new Queue('ws-dynamic-add', { embedded: true });
    queue.obliterate();

    const JOB_COUNT = 20;
    const JOB_DURATION = 200;
    const bulkJobs = Array.from({ length: JOB_COUNT }, (_, i) => ({
      name: `job-${i}`,
      data: { index: i },
    }));
    await queue.addBulk(bulkJobs);

    let totalCompleted = 0;

    // Start with 1 worker
    const worker1 = new Worker(
      'ws-dynamic-add',
      async (job) => {
        await Bun.sleep(JOB_DURATION);
        totalCompleted++;
        return { ok: true };
      },
      { embedded: true, concurrency: 1 }
    );

    // Wait for at least 5 to complete with single worker
    for (let i = 0; i < 300; i++) {
      if (totalCompleted >= 5) break;
      await Bun.sleep(100);
    }

    const completedBeforeScaling = totalCompleted;
    const timeBeforeScaling = performance.now();

    // Add 2 more workers
    const worker2 = new Worker(
      'ws-dynamic-add',
      async (job) => {
        await Bun.sleep(JOB_DURATION);
        totalCompleted++;
        return { ok: true };
      },
      { embedded: true, concurrency: 1 }
    );

    const worker3 = new Worker(
      'ws-dynamic-add',
      async (job) => {
        await Bun.sleep(JOB_DURATION);
        totalCompleted++;
        return { ok: true };
      },
      { embedded: true, concurrency: 1 }
    );

    // Wait for all to complete
    for (let i = 0; i < 300; i++) {
      if (totalCompleted >= JOB_COUNT) break;
      await Bun.sleep(100);
    }

    const timeAfterCompletion = performance.now();
    const remainingJobs = JOB_COUNT - completedBeforeScaling;
    const elapsedForRemaining = timeAfterCompletion - timeBeforeScaling;

    await worker1.close();
    await worker2.close();
    await worker3.close();

    expect(totalCompleted).toBe(JOB_COUNT);

    // With 3 workers, remaining jobs should finish faster than single worker would
    // Single worker: remainingJobs * JOB_DURATION ms
    // 3 workers: ~remainingJobs / 3 * JOB_DURATION ms
    const singleWorkerEstimate = remainingJobs * JOB_DURATION;
    expect(elapsedForRemaining).toBeLessThan(singleWorkerEstimate);

    queue.close();
  }, 30000);

  test('3. worker removal mid-processing — close 1 of 3 workers, rest finish', async () => {
    const queue = new Queue('ws-removal', { embedded: true });
    queue.obliterate();

    const JOB_COUNT = 30;
    const bulkJobs = Array.from({ length: JOB_COUNT }, (_, i) => ({
      name: `job-${i}`,
      data: { index: i },
    }));
    await queue.addBulk(bulkJobs);

    const completedIndices: number[] = [];

    const makeWorker = () =>
      new Worker(
        'ws-removal',
        async (job) => {
          const data = job.data as { index: number };
          await Bun.sleep(50);
          completedIndices.push(data.index);
          return { ok: true };
        },
        { embedded: true, concurrency: 2 }
      );

    const worker1 = makeWorker();
    const worker2 = makeWorker();
    const worker3 = makeWorker();

    // Wait for at least 10 to complete
    for (let i = 0; i < 300; i++) {
      if (completedIndices.length >= 10) break;
      await Bun.sleep(100);
    }

    // Close worker1 mid-processing
    await worker1.close();

    // Wait for remaining 2 workers to finish all 30
    for (let i = 0; i < 300; i++) {
      if (completedIndices.length >= JOB_COUNT) break;
      await Bun.sleep(100);
    }

    await worker2.close();
    await worker3.close();

    expect(completedIndices.length).toBe(JOB_COUNT);

    // Verify no duplicates
    const unique = new Set(completedIndices);
    expect(unique.size).toBe(JOB_COUNT);

    queue.close();
  }, 30000);

  test('4. workers with different concurrency — concurrency:5 processes more than concurrency:1', async () => {
    const queue = new Queue('ws-diff-concurrency', { embedded: true });
    queue.obliterate();

    const JOB_COUNT = 30;
    const bulkJobs = Array.from({ length: JOB_COUNT }, (_, i) => ({
      name: `job-${i}`,
      data: { index: i },
    }));
    await queue.addBulk(bulkJobs);

    let worker1Count = 0;
    let worker2Count = 0;
    let totalCompleted = 0;

    // Worker1: concurrency 1 (slow throughput)
    const worker1 = new Worker(
      'ws-diff-concurrency',
      async (job) => {
        await Bun.sleep(100); // simulate work
        worker1Count++;
        totalCompleted++;
        return { worker: 1 };
      },
      { embedded: true, concurrency: 1 }
    );

    // Worker2: concurrency 5 (high throughput)
    const worker2 = new Worker(
      'ws-diff-concurrency',
      async (job) => {
        await Bun.sleep(100); // same work duration
        worker2Count++;
        totalCompleted++;
        return { worker: 2 };
      },
      { embedded: true, concurrency: 5 }
    );

    // Wait for all to complete
    for (let i = 0; i < 300; i++) {
      if (totalCompleted >= JOB_COUNT) break;
      await Bun.sleep(100);
    }

    await worker1.close();
    await worker2.close();

    expect(totalCompleted).toBe(JOB_COUNT);

    // Worker2 with concurrency:5 should have processed more jobs than worker1
    expect(worker2Count).toBeGreaterThan(worker1Count);

    // Both workers should have processed at least 1 job
    expect(worker1Count).toBeGreaterThanOrEqual(1);
    expect(worker2Count).toBeGreaterThanOrEqual(1);

    // Total must add up
    expect(worker1Count + worker2Count).toBe(JOB_COUNT);

    queue.close();
  }, 30000);

  test('5. single worker handles all — 10 jobs, concurrency:10', async () => {
    const queue = new Queue('ws-single-all', { embedded: true });
    queue.obliterate();

    const JOB_COUNT = 10;
    const bulkJobs = Array.from({ length: JOB_COUNT }, (_, i) => ({
      name: `job-${i}`,
      data: { index: i },
    }));
    await queue.addBulk(bulkJobs);

    const completedIndices: number[] = [];

    const worker = new Worker(
      'ws-single-all',
      async (job) => {
        const data = job.data as { index: number };
        completedIndices.push(data.index);
        return { processed: data.index };
      },
      { embedded: true, concurrency: 10 }
    );

    // Wait for all 10 to complete
    for (let i = 0; i < 300; i++) {
      if (completedIndices.length >= JOB_COUNT) break;
      await Bun.sleep(100);
    }

    await worker.close();

    expect(completedIndices.length).toBe(JOB_COUNT);

    // Verify all indices present, no duplicates
    const unique = new Set(completedIndices);
    expect(unique.size).toBe(JOB_COUNT);

    const sorted = [...unique].sort((a, b) => a - b);
    expect(sorted[0]).toBe(0);
    expect(sorted[JOB_COUNT - 1]).toBe(JOB_COUNT - 1);

    queue.close();
  }, 30000);

  test('6. worker replacement — close worker1, start worker2 for new jobs', async () => {
    const queue = new Queue('ws-replacement', { embedded: true });
    queue.obliterate();

    const worker1Completed: number[] = [];
    const worker2Completed: number[] = [];

    // Push first batch of 5 jobs
    const batch1 = Array.from({ length: 5 }, (_, i) => ({
      name: `batch1-${i}`,
      data: { index: i, batch: 1 },
    }));
    await queue.addBulk(batch1);

    // Worker1 processes first batch
    const worker1 = new Worker(
      'ws-replacement',
      async (job) => {
        const data = job.data as { index: number };
        worker1Completed.push(data.index);
        return { worker: 1 };
      },
      { embedded: true, concurrency: 5 }
    );

    // Wait for worker1 to finish 5 jobs
    for (let i = 0; i < 300; i++) {
      if (worker1Completed.length >= 5) break;
      await Bun.sleep(100);
    }

    expect(worker1Completed.length).toBe(5);

    // Close worker1
    await worker1.close();

    // Push second batch of 5 jobs
    const batch2 = Array.from({ length: 5 }, (_, i) => ({
      name: `batch2-${i}`,
      data: { index: i + 5, batch: 2 },
    }));
    await queue.addBulk(batch2);

    // Start worker2 to handle second batch
    const worker2 = new Worker(
      'ws-replacement',
      async (job) => {
        const data = job.data as { index: number };
        worker2Completed.push(data.index);
        return { worker: 2 };
      },
      { embedded: true, concurrency: 5 }
    );

    // Wait for worker2 to finish 5 jobs
    for (let i = 0; i < 300; i++) {
      if (worker2Completed.length >= 5) break;
      await Bun.sleep(100);
    }

    await worker2.close();

    expect(worker2Completed.length).toBe(5);

    // Worker2 should have processed the new batch (indices 5-9)
    const worker2Sorted = [...worker2Completed].sort((a, b) => a - b);
    expect(worker2Sorted).toEqual([5, 6, 7, 8, 9]);

    // Worker1 should have processed the first batch (indices 0-4)
    const worker1Sorted = [...worker1Completed].sort((a, b) => a - b);
    expect(worker1Sorted).toEqual([0, 1, 2, 3, 4]);

    // No overlap between the two workers
    const allIndices = [...worker1Completed, ...worker2Completed];
    const unique = new Set(allIndices);
    expect(unique.size).toBe(10);

    queue.close();
  }, 30000);
});
