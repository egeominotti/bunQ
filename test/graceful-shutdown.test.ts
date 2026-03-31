/**
 * Graceful Shutdown Tests (Embedded Mode)
 * Tests that worker.close() properly waits for active jobs to finish,
 * handles idle workers, concurrent jobs, and burst load scenarios.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';

describe('Graceful Shutdown - Embedded', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('graceful close waits for active job', async () => {
    const queue = new Queue('shutdown-wait-active', { embedded: true });
    queue.obliterate();

    let jobCompleted = false;
    let completedViaEvent = false;

    const worker = new Worker(
      'shutdown-wait-active',
      async () => {
        await Bun.sleep(1000);
        jobCompleted = true;
        return { done: true };
      },
      { embedded: true, concurrency: 1 }
    );

    worker.on('completed', () => {
      completedViaEvent = true;
    });

    await queue.add('slow-job', { value: 1 });

    // Wait for the worker to pick up the job
    for (let i = 0; i < 20; i++) {
      await Bun.sleep(100);
      const counts = queue.getJobCounts();
      if (counts.active > 0) break;
    }

    // Close while job is still processing - should wait for it
    await worker.close();

    expect(jobCompleted).toBe(true);
    expect(completedViaEvent).toBe(true);

    queue.close();
  }, 30000);

  test('close with no active jobs resolves quickly', async () => {
    const queue = new Queue('shutdown-idle', { embedded: true });
    queue.obliterate();

    const worker = new Worker(
      'shutdown-idle',
      async () => {
        return { done: true };
      },
      { embedded: true, concurrency: 1 }
    );

    // Give worker time to start polling
    await Bun.sleep(200);

    const start = Date.now();
    await worker.close();
    const elapsed = Date.now() - start;

    expect(elapsed).toBeLessThan(500);

    queue.close();
  }, 30000);

  test('close with multiple active jobs waits for all', async () => {
    const queue = new Queue('shutdown-multi-active', { embedded: true });
    queue.obliterate();

    const completedJobs: string[] = [];

    const worker = new Worker(
      'shutdown-multi-active',
      async (job) => {
        await Bun.sleep(500);
        completedJobs.push(job.id);
        return { done: true };
      },
      { embedded: true, concurrency: 3 }
    );

    await queue.add('job-a', { idx: 0 });
    await queue.add('job-b', { idx: 1 });
    await queue.add('job-c', { idx: 2 });

    // Wait for all jobs to become active
    for (let i = 0; i < 30; i++) {
      await Bun.sleep(100);
      const counts = queue.getJobCounts();
      if (counts.active >= 3) break;
    }

    // Close while all 3 are processing
    await worker.close();

    expect(completedJobs.length).toBe(3);

    queue.close();
  }, 30000);

  test('jobs pushed after close are not processed by closed worker', async () => {
    const queue = new Queue('shutdown-no-new', { embedded: true });
    queue.obliterate();

    const processedByWorker1: string[] = [];
    let firstCompleted = false;

    const worker1 = new Worker(
      'shutdown-no-new',
      async (job) => {
        processedByWorker1.push(job.id);
        await Bun.sleep(100);
        return { done: true };
      },
      { embedded: true, concurrency: 1 }
    );

    worker1.on('completed', () => {
      firstCompleted = true;
    });

    // Add 5 jobs
    for (let i = 0; i < 5; i++) {
      await queue.add(`batch1-${i}`, { batch: 1, idx: i });
    }

    // Wait for the first job to complete
    for (let i = 0; i < 50; i++) {
      await Bun.sleep(100);
      if (firstCompleted) break;
    }

    // Close worker1
    await worker1.close();
    const worker1Count = processedByWorker1.length;

    // Add 5 more jobs after worker1 is closed
    for (let i = 0; i < 5; i++) {
      await queue.add(`batch2-${i}`, { batch: 2, idx: i });
    }

    // Start a new worker to pick up remaining
    const processedByWorker2: string[] = [];
    const worker2 = new Worker(
      'shutdown-no-new',
      async (job) => {
        processedByWorker2.push(job.id);
        return { done: true };
      },
      { embedded: true, concurrency: 5 }
    );

    // Wait for worker2 to process remaining jobs
    for (let i = 0; i < 50; i++) {
      await Bun.sleep(100);
      const counts = queue.getJobCounts();
      if (counts.waiting === 0 && counts.active === 0) break;
    }

    await worker2.close();

    // Worker2 should have processed the remaining jobs
    expect(processedByWorker2.length).toBeGreaterThan(0);
    // Total processed should account for all 10 jobs
    expect(worker1Count + processedByWorker2.length).toBe(10);

    queue.close();
  }, 30000);

  test('sequential close and restart picks up remaining jobs', async () => {
    const queue = new Queue('shutdown-restart', { embedded: true });
    queue.obliterate();

    // Add 10 jobs
    for (let i = 0; i < 10; i++) {
      await queue.add(`task-${i}`, { idx: i });
    }

    const processedPhase1: string[] = [];

    // First worker processes some jobs
    const worker1 = new Worker(
      'shutdown-restart',
      async (job) => {
        processedPhase1.push(job.id);
        await Bun.sleep(50);
        return { done: true };
      },
      { embedded: true, concurrency: 1 }
    );

    // Let it process a few jobs
    for (let i = 0; i < 30; i++) {
      await Bun.sleep(100);
      if (processedPhase1.length >= 3) break;
    }

    await worker1.close();
    const phase1Count = processedPhase1.length;
    expect(phase1Count).toBeGreaterThanOrEqual(1);

    // Check remaining
    const countsAfterClose = queue.getJobCounts();
    const remaining = countsAfterClose.waiting;

    // Start new worker for remaining jobs
    const processedPhase2: string[] = [];
    const worker2 = new Worker(
      'shutdown-restart',
      async (job) => {
        processedPhase2.push(job.id);
        return { done: true };
      },
      { embedded: true, concurrency: 5 }
    );

    // Wait for all remaining to be processed
    for (let i = 0; i < 50; i++) {
      await Bun.sleep(100);
      const counts = queue.getJobCounts();
      if (counts.waiting === 0 && counts.active === 0) break;
    }

    await worker2.close();

    // All jobs should be processed across both phases
    expect(phase1Count + processedPhase2.length).toBe(10);

    queue.close();
  }, 30000);

  test('close under burst load preserves all jobs', async () => {
    const queue = new Queue('shutdown-burst', { embedded: true });
    queue.obliterate();

    const totalJobs = 50;

    // Add 50 jobs
    for (let i = 0; i < totalJobs; i++) {
      await queue.add(`burst-${i}`, { idx: i });
    }

    let completedCount = 0;

    const worker = new Worker(
      'shutdown-burst',
      async () => {
        await Bun.sleep(50);
        completedCount++;
        return { done: true };
      },
      { embedded: true, concurrency: 5 }
    );

    // Wait until roughly 20 jobs are completed
    for (let i = 0; i < 100; i++) {
      await Bun.sleep(100);
      if (completedCount >= 20) break;
    }

    // Close the worker
    await worker.close();
    const finalCompleted = completedCount;

    // Check remaining waiting jobs
    const counts = queue.getJobCounts();
    const remainingWaiting = counts.waiting;

    // No jobs should be lost: completed + waiting should equal total
    // (active should be 0 after close since close waits for active jobs)
    expect(finalCompleted + remainingWaiting).toBe(totalJobs);
    expect(finalCompleted).toBeGreaterThanOrEqual(20);
    expect(counts.active).toBe(0);

    queue.close();
  }, 30000);
});
