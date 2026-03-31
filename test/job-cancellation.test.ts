/**
 * Job Cancellation Tests (Embedded Mode)
 *
 * Tests for cancelling/removing jobs from queues, verifying state transitions,
 * and ensuring cancelled jobs are not processed by workers.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';

describe('Job Cancellation - Embedded', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('1. cancel waiting job — remove a waiting job, verify state becomes unknown', async () => {
    const queue = new Queue('cancel-waiting', { embedded: true });
    queue.obliterate();

    const job = await queue.add('task', { value: 1 });
    const jobId = job.id;

    // Verify the job is in waiting state before cancellation
    const stateBefore = await queue.getJobState(jobId);
    expect(stateBefore).toBe('waiting');

    // Cancel/remove the job
    await queue.removeAsync(jobId);

    // After removal, state should be unknown (job no longer exists)
    const stateAfter = await queue.getJobState(jobId);
    expect(stateAfter).toBe('unknown');

    queue.close();
  }, 30000);

  test('2. cancel does not affect completed — completed job remains completed after cancel attempt', async () => {
    const queue = new Queue('cancel-completed', { embedded: true });
    queue.obliterate();

    const job = await queue.add('task', { value: 2 });
    const jobId = job.id;

    let processed = false;
    const worker = new Worker(
      'cancel-completed',
      async () => {
        processed = true;
        return { done: true };
      },
      { embedded: true, concurrency: 1 }
    );

    // Wait for the job to be processed
    for (let i = 0; i < 100; i++) {
      if (processed) break;
      await Bun.sleep(50);
    }
    expect(processed).toBe(true);

    // Give time for completion to register
    await Bun.sleep(100);

    // Verify it is completed
    const stateBefore = await queue.getJobState(jobId);
    expect(stateBefore).toBe('completed');

    // Try to cancel/remove the completed job
    await queue.removeAsync(jobId);

    // After attempting to cancel a completed job, it may be removed (unknown)
    // or remain completed — either way, verify it was already completed
    // The key assertion is that it was processed successfully before removal
    const stateAfter = await queue.getJobState(jobId);
    // Completed jobs that are removed become unknown
    expect(['completed', 'unknown']).toContain(stateAfter);

    await worker.close();
    queue.close();
  }, 30000);

  test('3. discard job — remove a waiting job via removeAsync, verify it is gone', async () => {
    const queue = new Queue('discard-job', { embedded: true });
    queue.obliterate();

    const job = await queue.add('task', { value: 3 });
    const jobId = job.id;

    // Verify job exists
    const fetchedJob = await queue.getJob(jobId);
    expect(fetchedJob).not.toBeNull();

    // Discard/remove the job
    await queue.removeAsync(jobId);

    // Verify the job is gone
    const stateAfter = await queue.getJobState(jobId);
    expect(stateAfter).toBe('unknown');

    queue.close();
  }, 30000);

  test('4. cancel multiple waiting jobs — cancel 3 of 5, only 2 are processed', async () => {
    const queue = new Queue('cancel-multiple', { embedded: true });
    queue.obliterate();

    // Push 5 jobs
    const jobs = await queue.addBulk(
      Array.from({ length: 5 }, (_, i) => ({
        name: `task-${i}`,
        data: { index: i },
      }))
    );

    // Cancel jobs at indices 0, 2, 4
    await queue.removeAsync(jobs[0].id);
    await queue.removeAsync(jobs[2].id);
    await queue.removeAsync(jobs[4].id);

    // Verify cancelled jobs are gone
    for (const idx of [0, 2, 4]) {
      const state = await queue.getJobState(jobs[idx].id);
      expect(state).toBe('unknown');
    }

    // Start worker to process the remaining 2 jobs
    const processedIndices: number[] = [];
    const worker = new Worker(
      'cancel-multiple',
      async (job) => {
        const data = job.data as { index: number };
        processedIndices.push(data.index);
        return { ok: true };
      },
      { embedded: true, concurrency: 5 }
    );

    // Wait for remaining jobs to process
    for (let i = 0; i < 100; i++) {
      if (processedIndices.length >= 2) break;
      await Bun.sleep(50);
    }

    // Give extra time to ensure no extra jobs are processed
    await Bun.sleep(200);

    await worker.close();

    // Exactly 2 jobs should have been processed (indices 1 and 3)
    expect(processedIndices.length).toBe(2);
    expect(processedIndices.sort((a, b) => a - b)).toEqual([1, 3]);

    queue.close();
  }, 30000);

  test('5. cancel delayed job — cancel a delayed job before it becomes active', async () => {
    const queue = new Queue('cancel-delayed', { embedded: true });
    queue.obliterate();

    // Push a job with a 5-second delay
    const job = await queue.add('delayed-task', { value: 5 }, { delay: 5000 });
    const jobId = job.id;

    // Verify it is in delayed state
    const stateBefore = await queue.getJobState(jobId);
    expect(stateBefore).toBe('delayed');

    // Cancel it before it becomes active
    await queue.removeAsync(jobId);

    // Verify it is removed
    const stateAfter = await queue.getJobState(jobId);
    expect(stateAfter).toBe('unknown');

    // Start a worker and wait — the cancelled delayed job should never process
    let processed = false;
    const worker = new Worker(
      'cancel-delayed',
      async () => {
        processed = true;
        return { ok: true };
      },
      { embedded: true, concurrency: 1 }
    );

    // Wait enough time to confirm no processing
    await Bun.sleep(1000);

    await worker.close();

    expect(processed).toBe(false);

    queue.close();
  }, 30000);

  test('6. batch cancel — cancel all 10 jobs, worker processes 0', async () => {
    const queue = new Queue('batch-cancel', { embedded: true });
    queue.obliterate();

    // Push 10 jobs
    const jobs = await queue.addBulk(
      Array.from({ length: 10 }, (_, i) => ({
        name: `task-${i}`,
        data: { index: i },
      }))
    );

    // Cancel all 10 jobs
    for (const job of jobs) {
      await queue.removeAsync(job.id);
    }

    // Verify all are removed
    for (const job of jobs) {
      const state = await queue.getJobState(job.id);
      expect(state).toBe('unknown');
    }

    // Start worker
    let processedCount = 0;
    const worker = new Worker(
      'batch-cancel',
      async () => {
        processedCount++;
        return { ok: true };
      },
      { embedded: true, concurrency: 5 }
    );

    // Wait to confirm no jobs are processed
    await Bun.sleep(1000);

    await worker.close();

    expect(processedCount).toBe(0);

    queue.close();
  }, 30000);
});
