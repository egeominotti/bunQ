/**
 * Bug verification: Silent duplicate job discard & stall ack handling
 *
 * Traces the exact server-side behavior when stall detection re-queues a job.
 *
 * Scenario 1 (NO locks): ack after stall re-queue → BUG: no recovery path
 * Scenario 2 (WITH locks, no re-pull): ack with T1 → Issue #33 handles it
 * Scenario 3 (WITH locks + re-pull): T1 overwritten by T2 → token mismatch
 * Scenario 4: stall detection confirms re-queue works
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { shardIndex, processingShardIndex } from '../src/shared/hash';

interface QueueManagerProbe {
  jobIndex: ReadonlyMap<string, { type: string }>;
}

function getLocationType(manager: QueueManager, id: string): string | undefined {
  return (manager as unknown as QueueManagerProbe).jobIndex.get(id)?.type;
}

async function waitForLocationType(
  manager: QueueManager,
  id: string,
  expected: string,
  timeoutMs = 5_000
): Promise<string | undefined> {
  const deadline = Date.now() + timeoutMs;
  let current = getLocationType(manager, id);

  while (current !== expected && Date.now() < deadline) {
    await Bun.sleep(25);
    current = getLocationType(manager, id);
  }

  return current;
}

describe('Stall re-queue ack handling', () => {
  let manager: QueueManager;

  afterEach(async () => {
    if (manager) await manager.shutdown();
  });

  /**
   * Scenario 1: No locks — ack after stall re-queue
   * The worker completes processing and acks without a token.
   * The job was stall-retried to queue, so ack should complete it from queue
   * (same as Issue #33 handling does for locked workers).
   */
  test('no-lock ack after stall re-queue should complete from queue', async () => {
    manager = new QueueManager({ stallCheckMs: 150 });
    const queueName = `stall-nolock-${Date.now()}`;

    const shard = (manager as any).shards[shardIndex(queueName)];
    shard.setStallConfig(queueName, {
      enabled: true,
      stallInterval: 200,
      maxStalls: 5,
      gracePeriod: 50,
    });

    // 1. Push job
    await manager.push(queueName, { data: { task: 'slow' } });

    // 2. Pull WITHOUT lock (simulates worker with useLocks=false)
    const pulled = await manager.pull(queueName);
    expect(pulled).not.toBeNull();

    // 3. Observe the two-phase detector instead of assuming timer callbacks are punctual.
    expect(await waitForLocationType(manager, pulled!.id, 'queue')).toBe('queue');

    // 4. Ack WITHOUT token — should succeed by completing from queue
    await manager.ack(pulled!.id, { result: 'done' });

    // 5. Job should be completed
    const finalLoc = (manager as any).jobIndex.get(pulled!.id);
    expect(finalLoc?.type).toBe('completed');
  }, 10000);

  /**
   * Scenario 2: With locks, NO re-pull — ack with original token T1
   * Expected: Issue #33 handler detects job in queue, completes from queue.
   */
  test('WITH locks, no re-pull: Issue #33 handles stall correctly', async () => {
    manager = new QueueManager({ stallCheckMs: 150 });
    const queueName = `stall-lock-ok-${Date.now()}`;

    const shard = (manager as any).shards[shardIndex(queueName)];
    shard.setStallConfig(queueName, {
      enabled: true,
      stallInterval: 200,
      maxStalls: 5,
      gracePeriod: 50,
    });

    // 1. Push job
    const job = await manager.push(queueName, { data: { task: 'slow-locked' } });
    console.log(`\n1. Pushed job: ${job.id}`);

    // 2. Pull WITH lock
    const result = await manager.pullWithLock(queueName, 'worker-1', 0, 30000);
    expect(result.job).not.toBeNull();
    const token1 = result.token!;
    console.log(`2. Pulled with lock: job=${result.job!.id}, token=${token1.substring(0, 8)}...`);

    // 3. Observe the two-phase detector instead of assuming timer callbacks are punctual.
    const locationType = await waitForLocationType(manager, result.job!.id, 'queue');
    console.log(`3. After stall: type=${locationType}`);
    expect(locationType).toBe('queue');

    // 4. Ack with ORIGINAL token T1 (no re-pull happened)
    let ackOk = false;
    try {
      await manager.ack(result.job!.id, { result: 'done' }, token1);
      ackOk = true;
      console.log('4. ✅ Ack with T1 succeeded — Issue #33 handled it');
    } catch (e) {
      console.log(`4. ❌ Ack with T1 failed: ${e}`);
    }

    // Verify: job completed from queue
    const finalLoc = (manager as any).jobIndex.get(result.job!.id);
    console.log(`5. Final: type=${finalLoc?.type}`);

    expect(ackOk).toBe(true);
    expect(finalLoc?.type).toBe('completed');
  }, 10000);

  /**
   * Scenario 3: With locks + re-pull — createLock returns null (T1 still active)
   *
   * When stall detection re-queues a job but doesn't release the lock:
   *   - Re-pull succeeds (job dequeued, moved to processing)
   *   - createLock returns null (T1 still in lock manager)
   *   - pullWithLock returns { job, token: null }
   *   - Original T1 ack should still work (T1 is valid, job is in processing)
   */
  test('re-pull after stall: lock not recreated, T1 ack still works', async () => {
    manager = new QueueManager({ stallCheckMs: 150 });
    const queueName = `stall-lock-repull-${Date.now()}`;

    const shard = (manager as any).shards[shardIndex(queueName)];
    shard.setStallConfig(queueName, {
      enabled: true,
      stallInterval: 200,
      maxStalls: 5,
      gracePeriod: 50,
    });

    // 1. Push job with zero backoff
    const job = await manager.push(queueName, { data: { task: 'slow' }, backoff: 0 });
    console.log(`\n1. Pushed job: ${job.id}`);

    // 2. Pull WITH lock → T1
    const result1 = await manager.pullWithLock(queueName, 'worker-1', 0, 30000);
    expect(result1.job).not.toBeNull();
    const token1 = result1.token!;
    const jobId = result1.job!.id;
    console.log(`2. Pull #1: token=${token1.substring(0, 8)}...`);

    // 3. Observe the two-phase detector instead of assuming timer callbacks are punctual.
    const locationType = await waitForLocationType(manager, jobId, 'queue');
    console.log(`3. After stall: type=${locationType}`);
    expect(locationType).toBe('queue');

    // 4. Wait for backoff=0 to settle, then re-pull
    await Bun.sleep(100);

    const result2 = await manager.pullWithLock(queueName, 'worker-2', 0, 30000);

    if (!result2.job) {
      console.log('4. Second pull returned null');
      await manager.ack(jobId, { result: 'done' }, token1);
      return;
    }

    const sameJob = String(result2.job.id) === String(jobId);
    console.log(`4. Pull #2: sameJob=${sameJob}, token=${result2.token ?? 'null'}`);

    expect(sameJob).toBe(true);
    // Key: createLock returns null because T1 still exists
    expect(result2.token).toBeNull();

    // 5. Job is now in processing. Ack with original T1 should work.
    const locAfterRepull = (manager as any).jobIndex.get(jobId);
    console.log(`   After re-pull: type=${locAfterRepull?.type}`);
    expect(locAfterRepull?.type).toBe('processing');

    await manager.ack(jobId, { result: 'done' }, token1);
    console.log('5. ✅ Ack with T1 succeeded (lock was never overwritten)');

    const finalLoc = (manager as any).jobIndex.get(jobId);
    console.log(`6. Final: type=${finalLoc?.type}`);
    expect(finalLoc?.type).toBe('completed');
  }, 10000);

  /**
   * Scenario 4: Verify stall detection actually fires and re-queues.
   */
  test('stall detection fires and re-queues correctly', async () => {
    manager = new QueueManager({ stallCheckMs: 150 });
    const queueName = `stall-verify-${Date.now()}`;

    const shard = (manager as any).shards[shardIndex(queueName)];
    shard.setStallConfig(queueName, {
      enabled: true,
      stallInterval: 200,
      maxStalls: 5,
      gracePeriod: 50,
    });

    await manager.push(queueName, { data: { task: 'test' } });
    const pulled = await manager.pull(queueName);
    expect(pulled).not.toBeNull();

    expect(await waitForLocationType(manager, pulled!.id, 'queue')).toBe('queue');
  }, 10000);
});
