/**
 * Bug: getJob returns null for completed jobs when storage is null.
 * It falls through to ctx.storage?.getJob() which returns null,
 * but should also check ctx.completedJobsData.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';

describe('getJob for completed jobs without storage', () => {
  let qm: QueueManager;

  beforeEach(() => {
    // No dataPath = no SQLite storage
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('should return completed job even without storage', async () => {
    const job = await qm.push('test-queue', { data: { value: 42 } });
    const pulled = await qm.pull('test-queue');
    expect(pulled).not.toBeNull();
    expect(pulled!.id).toBe(job.id);

    // Complete the job
    await qm.ack(job.id, { success: true });

    // Verify the job state is completed
    const state = await qm.getJobState(job.id);
    expect(state).toBe('completed');

    // This is the bug: getJob returns null for completed jobs without storage
    const completedJob = await qm.getJob(job.id);
    expect(completedJob).not.toBeNull();
    expect(completedJob!.id).toBe(job.id);
    expect((completedJob!.data as Record<string, unknown>).value).toBe(42);
  });

  test('should return result for completed job without storage', async () => {
    const job = await qm.push('test-queue', { data: { x: 1 } });
    await qm.pull('test-queue');
    await qm.ack(job.id, { result: 'done' });

    // Result should be available
    const result = qm.getResult(job.id);
    expect(result).toEqual({ result: 'done' });
  });
});
