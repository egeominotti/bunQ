/**
 * Bug #50: getJob returns null for jobs in a `failed` state.
 * When a job exhausts retry attempts and moves to DLQ, getJob() cannot
 * find it because the 'dlq' case only checks ctx.storage (null in embedded)
 * and ctx.completedJobsData (doesn't contain DLQ jobs).
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';

describe('getJob for failed/DLQ jobs without storage', () => {
  let qm: QueueManager;

  beforeEach(() => {
    // No dataPath = no SQLite storage (embedded mode)
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('should return job after it enters DLQ (failed state)', async () => {
    // Push a job with only 1 attempt so it goes to DLQ on first fail
    const job = await qm.push('test-queue', {
      data: { value: 42 },
      maxAttempts: 1,
    });

    // Pull and fail the job
    const pulled = await qm.pull('test-queue');
    expect(pulled).not.toBeNull();
    expect(pulled!.id).toBe(job.id);

    await qm.fail(job.id, 'Intentional error');

    // Verify the job state is failed
    const state = await qm.getJobState(job.id);
    expect(state).toBe('failed');

    // BUG: getJob returns null for failed/DLQ jobs
    const failedJob = await qm.getJob(job.id);
    expect(failedJob).not.toBeNull();
    expect(failedJob!.id).toBe(job.id);
    expect((failedJob!.data as Record<string, unknown>).value).toBe(42);
  });

  test('should return job after exhausting all retry attempts', async () => {
    const job = await qm.push('test-queue', {
      data: { important: true },
      maxAttempts: 3,
      backoff: 0,
    });

    // Fail 3 times to exhaust retries
    for (let i = 0; i < 3; i++) {
      const pulled = await qm.pull('test-queue');
      expect(pulled).not.toBeNull();
      await qm.fail(pulled!.id, `Attempt ${i + 1} failed`);
    }

    // Job should now be in DLQ/failed state
    const state = await qm.getJobState(job.id);
    expect(state).toBe('failed');

    // BUG: getJob returns null
    const failedJob = await qm.getJob(job.id);
    expect(failedJob).not.toBeNull();
    expect(failedJob!.id).toBe(job.id);
    expect((failedJob!.data as Record<string, unknown>).important).toBe(true);
  });
});
