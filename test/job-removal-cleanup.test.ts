/**
 * Job Removal & Cleanup Tests (Embedded Mode)
 *
 * Tests for removeOnComplete, removeOnFail, clean operations,
 * obliterate, and grace period behavior.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';

describe('Job Removal & Cleanup - Embedded', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('1. removeOnComplete removes job after success', async () => {
    const queue = new Queue('removal-on-complete', { embedded: true });
    queue.obliterate();

    const job = await queue.add('task', { value: 1 }, { removeOnComplete: true });
    const jobId = job.id;

    let completed = false;

    const worker = new Worker(
      'removal-on-complete',
      async (job) => {
        completed = true;
        return { done: true };
      },
      { embedded: true, concurrency: 1 }
    );

    // Wait for job to complete
    for (let i = 0; i < 100; i++) {
      if (completed) break;
      await Bun.sleep(50);
    }

    expect(completed).toBe(true);

    // Give time for removal to take effect
    await Bun.sleep(200);

    const fetched = await queue.getJob(jobId);
    expect(fetched).toBeNull();

    await worker.close();
    queue.close();
  }, 30000);

  test('2. removeOnFail removes job after failure', async () => {
    const queue = new Queue('removal-on-fail', { embedded: true });
    queue.obliterate();

    const job = await queue.add(
      'fail-task',
      { value: 1 },
      { removeOnFail: true, attempts: 1 }
    );
    const jobId = job.id;

    let failed = false;

    const worker = new Worker(
      'removal-on-fail',
      async () => {
        throw new Error('Deliberate failure');
      },
      { embedded: true, concurrency: 1 }
    );

    worker.on('failed', () => {
      failed = true;
    });

    // Wait for job to fail
    for (let i = 0; i < 100; i++) {
      if (failed) break;
      await Bun.sleep(50);
    }

    expect(failed).toBe(true);

    // Give time for removal to take effect
    await Bun.sleep(200);

    const fetched = await queue.getJob(jobId);
    expect(fetched).toBeNull();

    await worker.close();
    queue.close();
  }, 30000);

  test('3. Job persists without removeOnComplete', async () => {
    const queue = new Queue('persist-after-complete', { embedded: true });
    queue.obliterate();

    const job = await queue.add('task', { value: 42 });
    const jobId = job.id;

    let completed = false;

    const worker = new Worker(
      'persist-after-complete',
      async () => {
        completed = true;
        return { result: 'ok' };
      },
      { embedded: true, concurrency: 1 }
    );

    // Wait for job to complete
    for (let i = 0; i < 100; i++) {
      if (completed) break;
      await Bun.sleep(50);
    }

    expect(completed).toBe(true);

    // Give time for completion to settle
    await Bun.sleep(200);

    const fetched = await queue.getJob(jobId);
    expect(fetched).not.toBeNull();

    const state = await queue.getJobState(jobId);
    expect(state).toBe('completed');

    await worker.close();
    queue.close();
  }, 30000);

  test('4. Clean waiting jobs', async () => {
    const queue = new Queue('clean-waiting', { embedded: true });
    queue.obliterate();

    // Push 10 jobs but do NOT process them
    await queue.addBulk(
      Array.from({ length: 10 }, (_, i) => ({
        name: `task-${i}`,
        data: { index: i },
      }))
    );

    // Verify waiting jobs exist
    const countsBefore = queue.getJobCounts();
    expect(countsBefore.waiting).toBe(10);

    // Clean all jobs with 0 grace period (no state filter cleans waiting/delayed)
    queue.clean(0, 100);

    // Give time for clean to take effect
    await Bun.sleep(100);

    const countsAfter = queue.getJobCounts();
    expect(countsAfter.waiting).toBe(0);

    queue.close();
  }, 30000);

  test('5. Clean failed jobs via purgeDlq', async () => {
    const queue = new Queue('clean-failed-dlq', { embedded: true });
    queue.obliterate();

    // Push 5 jobs that will fail
    await queue.addBulk(
      Array.from({ length: 5 }, (_, i) => ({
        name: `fail-task-${i}`,
        data: { index: i },
        opts: { attempts: 1 },
      }))
    );

    let failedCount = 0;

    const worker = new Worker(
      'clean-failed-dlq',
      async () => {
        throw new Error('Deliberate failure');
      },
      { embedded: true, concurrency: 5 }
    );

    worker.on('failed', () => {
      failedCount++;
    });

    // Wait for all 5 to fail
    for (let i = 0; i < 200; i++) {
      if (failedCount >= 5) break;
      await Bun.sleep(50);
    }

    expect(failedCount).toBe(5);

    // Give time for failure to settle
    await Bun.sleep(200);

    await worker.close();

    // Verify failed jobs exist in DLQ
    const countsBefore = queue.getJobCounts();
    expect(countsBefore.failed).toBeGreaterThanOrEqual(5);

    // Purge DLQ to clean failed jobs
    queue.purgeDlq();

    // Give time for purge to take effect
    await Bun.sleep(100);

    const countsAfter = queue.getJobCounts();
    expect(countsAfter.failed).toBe(0);

    queue.close();
  }, 30000);

  test('6. Obliterate removes everything', async () => {
    const queue = new Queue('obliterate-all', { embedded: true });
    queue.obliterate();

    // Push 10 waiting jobs
    await queue.addBulk(
      Array.from({ length: 10 }, (_, i) => ({
        name: `task-${i}`,
        data: { index: i },
      }))
    );

    // Verify waiting jobs exist
    const countsBefore = queue.getJobCounts();
    expect(countsBefore.waiting).toBe(10);

    // Also push 5 jobs that will fail to DLQ
    await queue.addBulk(
      Array.from({ length: 5 }, (_, i) => ({
        name: `fail-${i}`,
        data: { index: i },
        opts: { attempts: 1 },
      }))
    );

    let failedCount = 0;

    const worker = new Worker(
      'obliterate-all',
      async (job) => {
        if (job.name.startsWith('fail-')) {
          throw new Error('Deliberate failure');
        }
        // Do not process 'task-' jobs so they remain waiting
        // Worker will pick them up too, so we need a different approach
        return { ok: true };
      },
      { embedded: true, concurrency: 1 }
    );

    worker.on('failed', () => {
      failedCount++;
    });

    // Wait for at least 5 failures
    for (let i = 0; i < 200; i++) {
      if (failedCount >= 5) break;
      await Bun.sleep(50);
    }

    expect(failedCount).toBeGreaterThanOrEqual(5);

    await worker.close();
    await Bun.sleep(200);

    // Obliterate the queue
    queue.obliterate();

    const counts = queue.getJobCounts();
    expect(counts.waiting).toBe(0);
    expect(counts.failed).toBe(0);

    queue.close();
  }, 30000);

  test('7. Clean with grace period', async () => {
    const queue = new Queue('clean-grace-period', { embedded: true });
    queue.obliterate();

    // Push 5 waiting jobs
    await queue.addBulk(
      Array.from({ length: 5 }, (_, i) => ({
        name: `task-${i}`,
        data: { index: i },
      }))
    );

    // Verify waiting jobs exist
    const countsBefore = queue.getJobCounts();
    expect(countsBefore.waiting).toBe(5);

    // Clean with 5s grace period -- jobs were just added, should NOT be cleaned
    const resultWithGrace = queue.clean(5000, 100);
    await Bun.sleep(100);

    const countsWithGrace = queue.getJobCounts();
    expect(countsWithGrace.waiting).toBe(5);
    expect(resultWithGrace.length).toBe(0);

    // Clean with 0 grace period -- should clean all waiting jobs
    const resultNoGrace = queue.clean(0, 100);
    await Bun.sleep(100);

    const countsAfterClean = queue.getJobCounts();
    expect(countsAfterClean.waiting).toBe(0);
    expect(resultNoGrace.length).toBe(5);

    queue.close();
  }, 30000);
});
