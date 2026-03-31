/**
 * Bulk Operations Advanced Tests (Embedded Mode)
 *
 * Tests for large bulk adds, mixed priorities, delays, custom jobIds,
 * performance, sequential batches, and concurrent processing.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';

describe('Bulk Operations Advanced - Embedded', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('1. large bulk add — 1000 jobs all added with correct counts', async () => {
    const queue = new Queue('bulk-adv-large', { embedded: true });
    queue.obliterate();

    const jobs = Array.from({ length: 1000 }, (_, i) => ({
      name: `job-${i}`,
      data: { index: i },
    }));

    const result = await queue.addBulk(jobs);
    expect(result.length).toBe(1000);

    // Verify each returned job has a unique id
    const ids = new Set(result.map((j) => j.id));
    expect(ids.size).toBe(1000);

    // Verify counts
    const counts = queue.getJobCounts();
    expect(counts.waiting).toBe(1000);

    queue.close();
  }, 30000);

  test('2. bulk add with mixed priorities — processed in priority order', async () => {
    const queue = new Queue('bulk-adv-prio', { embedded: true });
    queue.obliterate();

    // Add jobs with varied priorities: higher number = processed sooner
    const jobs = [
      { name: 'low-1', data: { label: 'low-1' }, opts: { priority: 1 } },
      { name: 'low-2', data: { label: 'low-2' }, opts: { priority: 1 } },
      { name: 'mid-1', data: { label: 'mid-1' }, opts: { priority: 5 } },
      { name: 'mid-2', data: { label: 'mid-2' }, opts: { priority: 5 } },
      { name: 'high-1', data: { label: 'high-1' }, opts: { priority: 10 } },
      { name: 'high-2', data: { label: 'high-2' }, opts: { priority: 10 } },
    ];
    await queue.addBulk(jobs);

    const processedOrder: string[] = [];

    // Use concurrency 1 to guarantee serial processing order
    const worker = new Worker(
      'bulk-adv-prio',
      async (job) => {
        const data = job.data as { label: string };
        processedOrder.push(data.label);
        return { ok: true };
      },
      { embedded: true, concurrency: 1 }
    );

    // Wait for all 6 jobs to complete
    for (let i = 0; i < 300; i++) {
      if (processedOrder.length >= 6) break;
      await Bun.sleep(50);
    }

    await worker.close();

    expect(processedOrder.length).toBe(6);

    // High priority jobs (10) should come before mid (5), which come before low (1)
    const highIndices = processedOrder
      .map((l, i) => (l.startsWith('high') ? i : -1))
      .filter((i) => i >= 0);
    const midIndices = processedOrder
      .map((l, i) => (l.startsWith('mid') ? i : -1))
      .filter((i) => i >= 0);
    const lowIndices = processedOrder
      .map((l, i) => (l.startsWith('low') ? i : -1))
      .filter((i) => i >= 0);

    // All high-priority jobs should be processed before any mid-priority
    expect(Math.max(...highIndices)).toBeLessThan(Math.min(...midIndices));
    // All mid-priority jobs should be processed before any low-priority
    expect(Math.max(...midIndices)).toBeLessThan(Math.min(...lowIndices));

    queue.close();
  }, 30000);

  test('3. bulk add with delays — delayed jobs process after non-delayed', async () => {
    const queue = new Queue('bulk-adv-delay', { embedded: true });
    queue.obliterate();

    const jobs = [
      { name: 'immediate-1', data: { label: 'immediate-1' } },
      { name: 'immediate-2', data: { label: 'immediate-2' } },
      { name: 'immediate-3', data: { label: 'immediate-3' } },
      { name: 'delayed-1', data: { label: 'delayed-1' }, opts: { delay: 2000 } },
      { name: 'delayed-2', data: { label: 'delayed-2' }, opts: { delay: 2000 } },
    ];
    await queue.addBulk(jobs);

    const processedOrder: string[] = [];

    const worker = new Worker(
      'bulk-adv-delay',
      async (job) => {
        const data = job.data as { label: string };
        processedOrder.push(data.label);
        return { ok: true };
      },
      { embedded: true, concurrency: 1 }
    );

    // Wait for all 5 jobs to complete (delayed ones need extra time)
    for (let i = 0; i < 300; i++) {
      if (processedOrder.length >= 5) break;
      await Bun.sleep(100);
    }

    await worker.close();

    expect(processedOrder.length).toBe(5);

    // All immediate jobs should appear before any delayed jobs
    const immediateIndices = processedOrder
      .map((l, i) => (l.startsWith('immediate') ? i : -1))
      .filter((i) => i >= 0);
    const delayedIndices = processedOrder
      .map((l, i) => (l.startsWith('delayed') ? i : -1))
      .filter((i) => i >= 0);

    expect(immediateIndices.length).toBe(3);
    expect(delayedIndices.length).toBe(2);
    expect(Math.max(...immediateIndices)).toBeLessThan(Math.min(...delayedIndices));

    queue.close();
  }, 30000);

  test('4. bulk add with custom jobIds — created with correct IDs', async () => {
    const queue = new Queue('bulk-adv-customid', { embedded: true });
    queue.obliterate();

    const customId1 = `custom-bulk-${Date.now()}-a`;
    const customId2 = `custom-bulk-${Date.now()}-b`;

    const jobs = [
      { name: 'task-1', data: { val: 1 }, opts: { jobId: customId1 } },
      { name: 'task-2', data: { val: 2 }, opts: { jobId: customId2 } },
      { name: 'task-3', data: { val: 3 } }, // no custom ID
    ];
    const result = await queue.addBulk(jobs);

    expect(result.length).toBe(3);

    // Verify custom IDs are assigned correctly
    expect(result[0].id).toBe(customId1);
    expect(result[1].id).toBe(customId2);
    // Third job should have a system-generated ID (not empty)
    expect(result[2].id).toBeTruthy();
    expect(result[2].id).not.toBe(customId1);
    expect(result[2].id).not.toBe(customId2);

    // Verify we can retrieve jobs by their custom IDs
    const fetched1 = await queue.getJob(customId1);
    expect(fetched1).not.toBeNull();
    expect(fetched1!.id).toBe(customId1);
    expect((fetched1!.data as { val: number }).val).toBe(1);

    const fetched2 = await queue.getJob(customId2);
    expect(fetched2).not.toBeNull();
    expect(fetched2!.id).toBe(customId2);
    expect((fetched2!.data as { val: number }).val).toBe(2);

    queue.close();
  }, 30000);

  test('5. bulk add performance — 500 jobs added in under 2 seconds', async () => {
    const queue = new Queue('bulk-adv-perf', { embedded: true });
    queue.obliterate();

    const jobs = Array.from({ length: 500 }, (_, i) => ({
      name: `perf-job-${i}`,
      data: { index: i, payload: `data-${i}` },
    }));

    const start = performance.now();
    const result = await queue.addBulk(jobs);
    const elapsed = performance.now() - start;

    expect(result.length).toBe(500);
    expect(elapsed).toBeLessThan(2000);

    // Verify all jobs are processable
    const completedIndices: number[] = [];

    const worker = new Worker(
      'bulk-adv-perf',
      async (job) => {
        const data = job.data as { index: number };
        completedIndices.push(data.index);
        return { ok: true };
      },
      { embedded: true, concurrency: 20 }
    );

    for (let i = 0; i < 300; i++) {
      if (completedIndices.length >= 500) break;
      await Bun.sleep(100);
    }

    await worker.close();

    expect(completedIndices.length).toBe(500);
    const unique = new Set(completedIndices);
    expect(unique.size).toBe(500);

    queue.close();
  }, 30000);

  test('6. sequential bulk batches — 5 batches of 100 all processed', async () => {
    const queue = new Queue('bulk-adv-seq', { embedded: true });
    queue.obliterate();

    // Add 5 separate batches of 100 jobs each
    for (let batch = 0; batch < 5; batch++) {
      const jobs = Array.from({ length: 100 }, (_, i) => ({
        name: `batch-${batch}-job-${i}`,
        data: { batch, index: i, globalIndex: batch * 100 + i },
      }));
      await queue.addBulk(jobs);
    }

    // Verify total count
    const counts = queue.getJobCounts();
    expect(counts.waiting).toBe(500);

    const completedGlobalIndices: number[] = [];

    const worker = new Worker(
      'bulk-adv-seq',
      async (job) => {
        const data = job.data as { globalIndex: number };
        completedGlobalIndices.push(data.globalIndex);
        return { ok: true };
      },
      { embedded: true, concurrency: 20 }
    );

    // Wait for all 500 to complete
    for (let i = 0; i < 300; i++) {
      if (completedGlobalIndices.length >= 500) break;
      await Bun.sleep(100);
    }

    await worker.close();

    expect(completedGlobalIndices.length).toBe(500);

    // Verify no duplicates and all indices present
    const unique = new Set(completedGlobalIndices);
    expect(unique.size).toBe(500);

    const sorted = [...unique].sort((a, b) => a - b);
    expect(sorted[0]).toBe(0);
    expect(sorted[499]).toBe(499);

    queue.close();
  }, 30000);

  test('7. bulk add with concurrent processing — 200 jobs, 4 workers, no duplicates', async () => {
    const queue = new Queue('bulk-adv-concurrent', { embedded: true });
    queue.obliterate();

    const jobs = Array.from({ length: 200 }, (_, i) => ({
      name: `conc-job-${i}`,
      data: { index: i },
    }));
    await queue.addBulk(jobs);

    const completedIndices: number[] = [];

    // 4 workers with concurrency 5 each
    const workers: Worker[] = [];
    for (let w = 0; w < 4; w++) {
      const worker = new Worker(
        'bulk-adv-concurrent',
        async (job) => {
          const data = job.data as { index: number };
          completedIndices.push(data.index);
          return { processed: data.index };
        },
        { embedded: true, concurrency: 5 }
      );
      workers.push(worker);
    }

    // Wait for all 200 to complete
    for (let i = 0; i < 300; i++) {
      if (completedIndices.length >= 200) break;
      await Bun.sleep(100);
    }

    // Close all workers
    for (const w of workers) {
      await w.close();
    }

    expect(completedIndices.length).toBe(200);

    // Verify no duplicates
    const unique = new Set(completedIndices);
    expect(unique.size).toBe(200);

    // Verify all indices present
    const sorted = [...unique].sort((a, b) => a - b);
    expect(sorted[0]).toBe(0);
    expect(sorted[199]).toBe(199);

    queue.close();
  }, 30000);
});
