/** Retry completed jobs in the embedded in-memory runtime. */

import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { Queue, shutdownManager } from '../src/client';
import { getSharedManager } from '../src/client/manager';

describe('retryCompleted', () => {
  const QUEUE_NAME = 'test-retry-completed';

  beforeEach(() => {
    shutdownManager();
  });

  afterEach(() => {
    shutdownManager();
  });

  test('retrying non-existent job returns 0', () => {
    const queue = new Queue(QUEUE_NAME, { embedded: true });
    const count = queue.retryCompleted('non-existent-job-id');
    expect(count).toBe(0);
  });

  test('retryCompleted requeues a retained job without SQLite', async () => {
    const queue = new Queue<{ msg: string }>(QUEUE_NAME, { embedded: true });
    const job = await queue.add('test-job', { msg: 'hello' }, { removeOnComplete: false });
    const manager = getSharedManager();
    const pulled = await manager.pull(QUEUE_NAME);
    expect(pulled?.id).toBe(job.id);
    await manager.ack(job.id, { ok: true });
    expect(await manager.getJobState(job.id)).toBe('completed');

    const retryCount = queue.retryCompleted(job.id);
    expect(retryCount).toBe(1);
    expect(await manager.getJobState(job.id)).toBe('waiting');
    expect((await manager.pull(QUEUE_NAME))?.id).toBe(job.id);
    await queue.close();
  });

  test('removeOnComplete jobs cannot be retried', async () => {
    const queue = new Queue<{ msg: string }>(QUEUE_NAME, { embedded: true });
    const job = await queue.add('disposable', { msg: 'bye' }, { removeOnComplete: true });
    const manager = getSharedManager();
    expect((await manager.pull(QUEUE_NAME))?.id).toBe(job.id);
    await manager.ack(job.id, { ok: true });
    expect(await manager.getJobState(job.id)).toBe('unknown');

    expect(queue.retryCompleted(job.id)).toBe(0);
    await queue.close();
  });

  test('completedJobs set is properly tracked', async () => {
    const queue = new Queue<{ msg: string }>(QUEUE_NAME, { embedded: true });
    const job = await queue.add('job1', { msg: 'test' }, { removeOnComplete: false });
    const manager = getSharedManager();
    expect((await manager.pull(QUEUE_NAME))?.id).toBe(job.id);
    await manager.ack(job.id, { ok: true });

    expect(await manager.getJobState(job.id)).toBe('completed');
    expect(queue.getJobCounts().completed).toBe(1);
    await queue.close();
  });
});
