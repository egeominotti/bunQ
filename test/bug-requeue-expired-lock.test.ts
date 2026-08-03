/**
 * Bug: requeueExpiredJob in lockManager.ts doesn't call shard.incrementQueued() or shard.notify()
 *
 * When a job's lock expires and it's requeued, the shard counters are not updated
 * and waiting workers are not notified, meaning the requeued job may never be consumed.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import type { JobId } from '../src/domain/types/job';

describe('requeueExpiredJob should update shard counters and notify', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('requeued job after lock expiry should be pullable by a new worker', async () => {
    // Push a job and pull it with a very short lock TTL
    await qm.push('test-queue', { data: { value: 'important' } });
    const { job, token } = await qm.pullWithLock('test-queue', 'worker-1', 0, 50); // 50ms TTL
    expect(job).not.toBeNull();
    expect(token).not.toBeNull();

    // Wait for lock to expire
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Run expired locks check (this triggers requeueExpiredJob)
    // We import and call it through the internal background task mechanism
    const { checkExpiredLocks } = await import('../src/application/lockManager');

    // Before lock check, job should still be in processing state
    const stateBefore = await qm.getJobState(job!.id);
    expect(stateBefore).toBe('active');

    // Trigger the background lock expiration check
    // This is normally done by backgroundTasks.ts every 5s
    // We need to access the lock context - use the internal method
    await checkExpiredLocks(getInternalLockContext(qm));

    // After lock check, job should be back in queue (waiting state)
    const stateAfter = await qm.getJobState(job!.id);
    expect(stateAfter).toBe('waiting');

    // CRITICAL: The job should now be pullable by another worker
    // If incrementQueued/notify were missing, this pull would hang or return null
    const result = await qm.pull('test-queue', 0);
    expect(result).not.toBeNull();
    expect(result!.id).toBe(job!.id);
    expect((result!.data as Record<string, unknown>).value).toBe('important');
  });

  test('shard stats should be correct after requeue from expired lock', async () => {
    await qm.push('test-queue', { data: { x: 1 } });
    const { job } = await qm.pullWithLock('test-queue', 'worker-1', 0, 50);
    expect(job).not.toBeNull();

    // Check initial stats - job is active, 0 waiting
    const stats1 = qm.getStats();
    expect(stats1.active).toBe(1);
    expect(stats1.waiting).toBe(0);

    // Wait for lock expiry
    await new Promise((resolve) => setTimeout(resolve, 100));

    // Run expired lock check
    const { checkExpiredLocks } = await import('../src/application/lockManager');
    await checkExpiredLocks(getInternalLockContext(qm));

    // Stats should show job back in waiting
    const stats2 = qm.getStats();
    expect(stats2.active).toBe(0);
    expect(stats2.waiting).toBe(1);
  });
});

/**
 * Helper to get internal lock context from QueueManager.
 * This accesses private fields via runtime for testing purposes.
 */
function getInternalLockContext(
  qm: QueueManager
): Parameters<typeof import('../src/application/lockManager').checkExpiredLocks>[0] {
  return (
    qm as unknown as {
      contextFactory: {
        getLockContext(): Parameters<
          typeof import('../src/application/lockManager').checkExpiredLocks
        >[0];
      };
    }
  ).contextFactory.getLockContext();
}
