/**
 * Bug reproduction: getJobs({ state: 'completed' }) returns empty array
 * even when completed jobs exist and getJobCounts() reports completed > 0.
 *
 * Issue: queryOperations.getJobs() has no branch for 'completed' state.
 *
 * Run with: BUNQUEUE_EMBEDDED=1 bun test test/get-completed-jobs.test.ts
 */

import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';

describe('Bug: completed jobs listing returns empty', () => {
  let queue: Queue<{ value: number }>;
  let worker: Worker<{ value: number }, { ok: boolean }>;

  beforeEach(async () => {
    queue = new Queue<{ value: number }>('completed-listing-test');
    queue.obliterate();
    await Bun.sleep(50);
  });

  afterEach(async () => {
    await worker?.close();
    shutdownManager();
  });

  it('getJobsAsync({ state: "completed" }) should return completed jobs', async () => {
    // Add a job
    const job = await queue.add('task', { value: 42 });

    // Process it with a worker
    worker = new Worker<{ value: number }, { ok: boolean }>(
      'completed-listing-test',
      async (_j) => ({ ok: true }),
      { concurrency: 1 }
    );

    // Wait for job to complete
    await Bun.sleep(500);

    // Verify the job is completed
    const state = await queue.getJobState(job.id);
    expect(state).toBe('completed');

    // Verify counts show completed
    const counts = await queue.getJobCountsAsync();
    expect(counts.completed).toBeGreaterThanOrEqual(1);

    // BUG: This returns empty array despite completed count > 0
    const completedJobs = await queue.getJobsAsync({ state: 'completed', start: 0, end: 50 });
    expect(completedJobs.length).toBeGreaterThanOrEqual(1);
  });

  it('getCompletedAsync() should return completed jobs', async () => {
    // Add a job
    const job = await queue.add('task', { value: 99 });

    // Process it
    worker = new Worker<{ value: number }, { ok: boolean }>(
      'completed-listing-test',
      async (_j) => ({ ok: true }),
      { concurrency: 1 }
    );

    await Bun.sleep(500);

    const state = await queue.getJobState(job.id);
    expect(state).toBe('completed');

    // BUG: This returns empty array
    const completed = await queue.getCompletedAsync(0, 50);
    expect(completed.length).toBeGreaterThanOrEqual(1);
  });

  it('getJobs({ state: "completed" }) should return completed jobs (sync)', async () => {
    const job = await queue.add('task', { value: 7 });

    worker = new Worker<{ value: number }, { ok: boolean }>(
      'completed-listing-test',
      async (_j) => ({ ok: true }),
      { concurrency: 1 }
    );

    await Bun.sleep(500);

    const state = await queue.getJobState(job.id);
    expect(state).toBe('completed');

    // Sync version also affected
    const completedJobs = queue.getJobs({ state: 'completed' });
    expect(completedJobs.length).toBeGreaterThanOrEqual(1);
  });

  it('completed jobs listing should include correct job data', async () => {
    const job = await queue.add('my-task', { value: 123 });

    worker = new Worker<{ value: number }, { ok: boolean }>(
      'completed-listing-test',
      async (_j) => ({ ok: true }),
      { concurrency: 1 }
    );

    await Bun.sleep(500);

    const completedJobs = await queue.getJobsAsync({ state: 'completed', start: 0, end: 50 });
    expect(completedJobs.length).toBeGreaterThanOrEqual(1);

    const found = completedJobs.find((j) => j.id === job.id);
    expect(found).toBeDefined();
    expect(found!.data.value).toBe(123);
  });

  it('getJobs with no state filter should include completed jobs', async () => {
    const job = await queue.add('task', { value: 1 });

    worker = new Worker<{ value: number }, { ok: boolean }>(
      'completed-listing-test',
      async (_j) => ({ ok: true }),
      { concurrency: 1 }
    );

    await Bun.sleep(500);

    // With no state filter, all jobs should be returned including completed
    const allJobs = await queue.getJobsAsync({ start: 0, end: 50 });
    const found = allJobs.find((j) => j.id === job.id);
    expect(found).toBeDefined();
  });
});
