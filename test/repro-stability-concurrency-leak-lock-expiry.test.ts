/**
 * STABILITY BUG: Lock expiration leaks the per-queue concurrency slot.
 *
 * A queue configured with setConcurrency('q', N) permanently wedges once N
 * lock expirations occur, even though all those jobs were requeued and are
 * waiting again.
 *
 * Root cause: src/application/lockManager.ts
 *   - At pull time, tryPullFromShard (src/application/operations/pull.ts:259)
 *     calls shard.tryAcquireConcurrency(queue), which increments the
 *     ConcurrencyLimiter's `active` counter (src/domain/types/queue.ts:72).
 *   - When the lock expires, checkExpiredLocks -> processExpiredLockInner
 *     (lockManager.ts:105) routes to either:
 *       * requeueExpiredJob (lockManager.ts:193-208) — pushes the job back to
 *         the waiting queue but NEVER calls shard.releaseConcurrency(queue)
 *         / shard.releaseJobResources(...), OR
 *       * handleMaxStallsExceeded (lockManager.ts:167-179) — moves to DLQ but
 *         also never releases the slot.
 *     Only the cron branch (lockManager.ts:125) calls
 *     shard.releaseJobResources(...) which releases the slot.
 *   - The slot acquired at pull is therefore leaked. After N expiries the
 *     limiter reports active >= N forever, and tryAcquireConcurrency() always
 *     returns false. The queue cannot dispatch any of its still-waiting jobs.
 *
 * Reproduction (deterministic, no background timers):
 *   setConcurrency('q', 2); push 3 jobs; pull 2 with a short lock TTL; let the
 *   locks expire; run checkExpiredLocks() so both jobs requeue. Now there are 3
 *   jobs waiting and ZERO jobs in processing, yet pull() returns null because
 *   both concurrency slots were leaked.
 *
 * CORRECT (post-fix) behavior: after the expiries the limiter's active count
 * returns to 0 and all 3 waiting jobs can be pulled — the queue is not wedged.
 * Control: clearConcurrency('q') removes the leaked limiter and pull succeeds,
 * isolating the cause to the leaked slot (not job state corruption).
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { checkExpiredLocks } from '../src/application/lockManager';
import { shardIndex } from '../src/shared/hash';

function getInternalLockContext(qm: QueueManager): any {
  const qmAny = qm as any;
  return {
    shards: qmAny.shards,
    shardLocks: qmAny.shardLocks,
    processingShards: qmAny.processingShards,
    processingLocks: qmAny.processingLocks,
    jobIndex: qmAny.jobIndex,
    jobLocks: qmAny.jobLocks,
    clientJobs: qmAny.clientJobs,
    eventsManager: qmAny.eventsManager,
    stalledCandidates: qmAny.stalledCandidates,
    storage: qmAny.storage,
    dashboardEmit: null,
  };
}

/** Read the ConcurrencyLimiter's active count via internal poking. */
function getActiveConcurrency(qm: QueueManager, queue: string): number | null {
  const shard: any = qm.getShards()[shardIndex(queue)];
  const limiter = shard.limiterManager?.concurrencyLimiters?.get(queue);
  return limiter ? limiter.getActive() : null;
}

describe('Stability: lock expiration leaks the per-queue concurrency slot', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('queue with setConcurrency() is NOT wedged after lock expiries requeue its jobs', async () => {
    const QUEUE = 'limited';

    // 1. Limit concurrency to 2.
    qm.setConcurrency(QUEUE, 2);

    // 2. Push 3 jobs.
    for (let i = 0; i < 3; i++) {
      await qm.push(QUEUE, { data: { i } });
    }

    // 3. Pull 2 jobs with a short lock TTL — this acquires 2 concurrency slots.
    const p1 = await qm.pullWithLock(QUEUE, 'worker-1', 0, 50);
    const p2 = await qm.pullWithLock(QUEUE, 'worker-2', 0, 50);
    expect(p1.job).not.toBeNull();
    expect(p2.job).not.toBeNull();

    // Both slots are now in use; the 3rd job is blocked by the concurrency gate.
    expect(getActiveConcurrency(qm, QUEUE)).toBe(2);
    expect(await qm.pull(QUEUE, 0)).toBeNull(); // gate full, expected

    // 4. Let the locks expire (no heartbeat renewal).
    await Bun.sleep(80);

    // 5. Run the lock-expiry sweep. Both jobs are requeued back to 'waiting'.
    await checkExpiredLocks(getInternalLockContext(qm));

    // Sanity: both pulled jobs went back to the queue, nothing left in processing.
    expect(await qm.getJobState(p1.job!.id)).toBe('waiting');
    expect(await qm.getJobState(p2.job!.id)).toBe('waiting');

    // ===== CORRECT post-fix behavior =====
    // The 2 slots acquired at pull must have been released on requeue.
    expect(getActiveConcurrency(qm, QUEUE)).toBe(0);

    // With 3 jobs waiting, zero in processing, and a limit of 2, the queue must
    // be able to dispatch jobs again. Pulling twice should succeed.
    const after1 = await qm.pull(QUEUE, 0);
    const after2 = await qm.pull(QUEUE, 0);
    expect(after1).not.toBeNull(); // FAILS NOW: leaked slot keeps the gate full
    expect(after2).not.toBeNull();
  });

  test('control: lock-expiry releases the slot so the limited queue stays usable; clearConcurrency() also works', async () => {
    const QUEUE = 'limited-ctrl';

    qm.setConcurrency(QUEUE, 2);
    for (let i = 0; i < 3; i++) {
      await qm.push(QUEUE, { data: { i } });
    }

    const p1 = await qm.pullWithLock(QUEUE, 'worker-1', 0, 50);
    const p2 = await qm.pullWithLock(QUEUE, 'worker-2', 0, 50);
    expect(p1.job).not.toBeNull();
    expect(p2.job).not.toBeNull();

    await Bun.sleep(80);
    await checkExpiredLocks(getInternalLockContext(qm));

    // After the fix, lock-expiry releases the concurrency slots, so the requeued
    // jobs are immediately pullable again WITHOUT clearing the limiter (pre-fix
    // this returned null — the queue was wedged by the leaked slots).
    const reacquired = await qm.pull(QUEUE, 0);
    expect(reacquired).not.toBeNull();

    // clearConcurrency() also works as an operator remediation and never blocks.
    qm.clearConcurrency(QUEUE);
    const job = await qm.pull(QUEUE, 0);
    expect(job).not.toBeNull();
  });
});
