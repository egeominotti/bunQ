/**
 * Advanced Stress Tests (Embedded Mode)
 *
 * Tests for high-volume concurrent push, mixed success/failure under load,
 * multiple queues under load, burst-then-steady patterns, worker churn,
 * and flow chains under load.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, FlowProducer, shutdownManager } from '../src/client';

describe('Stress Advanced - Embedded', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('1. high-volume concurrent push — 500 jobs, 5 workers, no losses', async () => {
    const queue = new Queue('stress-highvol', { embedded: true });
    queue.obliterate();

    // Push 500 jobs via addBulk
    const bulkJobs = Array.from({ length: 500 }, (_, i) => ({
      name: `job-${i}`,
      data: { index: i },
    }));
    await queue.addBulk(bulkJobs);

    const completedIndices: number[] = [];

    // 5 workers, each with concurrency 10
    const workers: Worker[] = [];
    for (let w = 0; w < 5; w++) {
      const worker = new Worker(
        'stress-highvol',
        async (job) => {
          const data = job.data as { index: number };
          completedIndices.push(data.index);
          return { processed: data.index };
        },
        { embedded: true, concurrency: 10 }
      );
      workers.push(worker);
    }

    // Wait for all 500 to complete
    for (let i = 0; i < 300; i++) {
      if (completedIndices.length >= 500) break;
      await Bun.sleep(100);
    }

    // Close all workers
    for (const w of workers) {
      await w.close();
    }

    expect(completedIndices.length).toBe(500);

    // Verify no duplicates
    const uniqueIndices = new Set(completedIndices);
    expect(uniqueIndices.size).toBe(500);

    // Verify all indices present
    const sorted = [...uniqueIndices].sort((a, b) => a - b);
    expect(sorted[0]).toBe(0);
    expect(sorted[499]).toBe(499);

    queue.close();
  }, 30000);

  test('2. mixed success/failure under load — 200 jobs, ~30% fail', async () => {
    const queue = new Queue('stress-mixed', { embedded: true });
    queue.obliterate();

    const bulkJobs = Array.from({ length: 200 }, (_, i) => ({
      name: `job-${i}`,
      data: { index: i },
      opts: { attempts: 1 },
    }));
    await queue.addBulk(bulkJobs);

    let completedCount = 0;
    let failedCount = 0;

    const worker = new Worker(
      'stress-mixed',
      async (job) => {
        const data = job.data as { index: number };
        // ~30% fail rate using deterministic check
        if (data.index % 3 === 0) {
          throw new Error(`Deliberate failure for job ${data.index}`);
        }
        return { ok: true };
      },
      { embedded: true, concurrency: 10 }
    );

    worker.on('completed', () => { completedCount++; });
    worker.on('failed', () => { failedCount++; });

    // Wait for all jobs to settle
    for (let i = 0; i < 300; i++) {
      if (completedCount + failedCount >= 200) break;
      await Bun.sleep(100);
    }

    await worker.close();

    // Every third job fails: 0,3,6,...,198 => 67 failures, 133 completions
    expect(completedCount + failedCount).toBe(200);
    expect(failedCount).toBeGreaterThan(0);
    expect(completedCount).toBeGreaterThan(0);

    queue.close();
  }, 30000);

  test('3. multiple queues under load — 3 queues, 100 jobs each', async () => {
    const queueNames = ['stress-mq-a', 'stress-mq-b', 'stress-mq-c'];
    const queues: Queue[] = [];
    const workers: Worker[] = [];
    const counts: Record<string, number> = {};

    for (const name of queueNames) {
      const q = new Queue(name, { embedded: true });
      q.obliterate();
      queues.push(q);
      counts[name] = 0;

      // Push 100 jobs per queue
      const jobs = Array.from({ length: 100 }, (_, i) => ({
        name: `job-${i}`,
        data: { index: i, queue: name },
      }));
      await q.addBulk(jobs);

      // One worker per queue
      const w = new Worker(
        name,
        async (job) => {
          const data = job.data as { queue: string };
          counts[data.queue]++;
          return { ok: true };
        },
        { embedded: true, concurrency: 5 }
      );
      workers.push(w);
    }

    // Wait for all 300 jobs
    const totalTarget = 300;
    for (let i = 0; i < 300; i++) {
      const total = Object.values(counts).reduce((a, b) => a + b, 0);
      if (total >= totalTarget) break;
      await Bun.sleep(100);
    }

    for (const w of workers) {
      await w.close();
    }

    // Each queue should have processed exactly 100
    for (const name of queueNames) {
      expect(counts[name]).toBe(100);
    }

    for (const q of queues) {
      q.close();
    }
  }, 30000);

  test('4. burst followed by steady — 100 immediate + 50 trickled', async () => {
    const queue = new Queue('stress-burst', { embedded: true });
    queue.obliterate();

    const completedIndices: number[] = [];

    const worker = new Worker(
      'stress-burst',
      async (job) => {
        const data = job.data as { index: number };
        completedIndices.push(data.index);
        return { ok: true };
      },
      { embedded: true, concurrency: 10 }
    );

    // Burst: push 100 jobs immediately
    const burstJobs = Array.from({ length: 100 }, (_, i) => ({
      name: `burst-${i}`,
      data: { index: i },
    }));
    await queue.addBulk(burstJobs);

    // Steady: push 1 job every 50ms for 50 more
    for (let i = 100; i < 150; i++) {
      await queue.add(`steady-${i}`, { index: i });
      await Bun.sleep(50);
    }

    // Wait for all 150 to complete
    for (let i = 0; i < 300; i++) {
      if (completedIndices.length >= 150) break;
      await Bun.sleep(100);
    }

    await worker.close();

    expect(completedIndices.length).toBe(150);

    // Verify all indices present
    const sorted = [...new Set(completedIndices)].sort((a, b) => a - b);
    expect(sorted.length).toBe(150);
    expect(sorted[0]).toBe(0);
    expect(sorted[149]).toBe(149);

    queue.close();
  }, 30000);

  test('5. worker churn — close after 25, new worker finishes rest', async () => {
    const queue = new Queue('stress-churn', { embedded: true });
    queue.obliterate();

    // Push 50 jobs
    const bulkJobs = Array.from({ length: 50 }, (_, i) => ({
      name: `job-${i}`,
      data: { index: i },
    }));
    await queue.addBulk(bulkJobs);

    const completedByWorker1: number[] = [];
    const completedByWorker2: number[] = [];

    // Start first worker — slow processing so we can close mid-way
    const worker1 = new Worker(
      'stress-churn',
      async (job) => {
        const data = job.data as { index: number };
        completedByWorker1.push(data.index);
        await Bun.sleep(200);
        return { worker: 1 };
      },
      { embedded: true, concurrency: 2 }
    );

    // Wait for worker1 to complete at least 10 jobs
    for (let i = 0; i < 300; i++) {
      if (completedByWorker1.length >= 10) break;
      await Bun.sleep(100);
    }

    // Close worker1
    await worker1.close();
    const w1Count = completedByWorker1.length;

    // Start second worker to finish remaining
    const worker2 = new Worker(
      'stress-churn',
      async (job) => {
        const data = job.data as { index: number };
        completedByWorker2.push(data.index);
        return { worker: 2 };
      },
      { embedded: true, concurrency: 10 }
    );

    // Wait for all 50 to be done
    for (let i = 0; i < 300; i++) {
      const total = completedByWorker1.length + completedByWorker2.length;
      if (total >= 50) break;
      await Bun.sleep(100);
    }

    await worker2.close();

    const allCompleted = [...completedByWorker1, ...completedByWorker2];
    expect(allCompleted.length).toBe(50);

    // Verify no duplicates
    const unique = new Set(allCompleted);
    expect(unique.size).toBe(50);

    // Worker1 should have done at least 10
    expect(w1Count).toBeGreaterThanOrEqual(10);
    // Worker2 should have picked up the rest
    expect(completedByWorker2.length).toBeGreaterThan(0);

    queue.close();
  }, 30000);

  test('6. flow chains under load — 20 concurrent 3-step chains', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('stress-flow', { embedded: true });
    queue.obliterate();

    const flowResults = new Map<number, number[]>();
    let completedChains = 0;

    const worker = new Worker(
      'stress-flow',
      async (job) => {
        const data = job.data as { chainId: number; step: number };
        if (!flowResults.has(data.chainId)) {
          flowResults.set(data.chainId, []);
        }
        flowResults.get(data.chainId)!.push(data.step);

        if (data.step === 2) {
          completedChains++;
        }

        await Bun.sleep(10); // simulate work
        return { chainId: data.chainId, step: data.step };
      },
      { embedded: true, concurrency: 20 }
    );

    // Launch 20 chains concurrently
    const CHAIN_COUNT = 20;
    await Promise.all(
      Array.from({ length: CHAIN_COUNT }, (_, i) =>
        flow.addChain([
          { name: `s0`, queueName: 'stress-flow', data: { chainId: i, step: 0 } },
          { name: `s1`, queueName: 'stress-flow', data: { chainId: i, step: 1 } },
          { name: `s2`, queueName: 'stress-flow', data: { chainId: i, step: 2 } },
        ])
      )
    );

    // Wait for all chains to complete
    for (let i = 0; i < 300; i++) {
      if (completedChains >= CHAIN_COUNT) break;
      await Bun.sleep(100);
    }

    await worker.close();

    // All 20 chains should be complete
    expect(completedChains).toBe(CHAIN_COUNT);

    // Each chain should have all 3 steps in order
    for (let i = 0; i < CHAIN_COUNT; i++) {
      const steps = flowResults.get(i);
      expect(steps).toBeDefined();
      expect(steps!.length).toBe(3);
      // Verify order within each chain
      expect(steps!).toEqual([0, 1, 2]);
    }

    // Total jobs should be 60
    let totalJobs = 0;
    for (const [, steps] of flowResults) {
      totalJobs += steps.length;
    }
    expect(totalJobs).toBe(60);

    flow.close();
    queue.close();
  }, 30000);
});
