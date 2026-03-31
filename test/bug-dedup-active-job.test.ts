/**
 * Bug: deduplication does not block pushes when job is active (being processed)
 *
 * When a job with a uniqueKey is active (pulled by a worker), pushing another
 * job with the same uniqueKey should be deduplicated. Currently it is NOT —
 * because handleDeduplication only checks if the job is in the *waiting* queue.
 * Active jobs are not in the priority queue, so q.find() returns null and the
 * code falls through to "allow new insert".
 *
 * This causes cron jobs with long runtimes to accumulate duplicate jobs in the
 * queue while the first instance is still running.
 */

import { describe, test, expect, beforeEach } from 'bun:test';
import { getSharedManager } from '../src/client/manager';

describe('Bug: dedup with active job', () => {
  const QUEUE = 'test-dedup-active';

  beforeEach(() => {
    getSharedManager().drain(QUEUE);
  });

  test('pushing with same uniqueKey while job is active should be deduplicated', async () => {
    const manager = getSharedManager();

    // Push first job with a uniqueKey
    const job1 = await manager.push(QUEUE, {
      data: { n: 1 },
      uniqueKey: 'singleton-key',
    });
    expect(job1).toBeDefined();

    // Pull the job — it becomes active; uniqueKey is still registered
    const pulled = await manager.pull(QUEUE, {});
    expect(pulled).not.toBeNull();
    expect(pulled!.id).toBe(job1.id);

    // Push a duplicate with the same uniqueKey while job1 is active
    const job2 = await manager.push(QUEUE, {
      data: { n: 2 },
      uniqueKey: 'singleton-key',
    });

    // Should return the existing active job's ID (deduplicated), NOT insert a new job
    expect(job2.id).toBe(job1.id);

    // Verify: pulling again returns nothing (no second job was queued)
    const job3 = await manager.pull(QUEUE, 0);
    expect(job3).toBeNull();
  });

  test('pushing with same uniqueKey after job completes should be allowed', async () => {
    const manager = getSharedManager();

    const job1 = await manager.push(QUEUE, {
      data: { n: 1 },
      uniqueKey: 'singleton-key-2',
    });

    // Pull and complete the job
    const pulled = await manager.pull(QUEUE, {});
    expect(pulled!.id).toBe(job1.id);
    await manager.ack(pulled!.id, { result: 'done' });

    // Now a new job with the same key should be allowed
    const job2 = await manager.push(QUEUE, {
      data: { n: 2 },
      uniqueKey: 'singleton-key-2',
    });

    expect(job2.id).not.toBe(job1.id);
  });
});
