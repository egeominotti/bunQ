import { describe, expect, test } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import type { Shard } from '../src/domain/queue/shard';
import type { Job } from '../src/domain/types/job';
import { shardIndex } from '../src/shared/hash';

function countQueuePushes(manager: QueueManager, queueName: string): () => number {
  const internals = manager as unknown as { shards: Shard[] };
  const queue = internals.shards[shardIndex(queueName)].getQueue(queueName);
  const originalPush = queue.push.bind(queue);
  let pushes = 0;

  queue.push = (job: Job): void => {
    pushes++;
    originalPush(job);
  };

  return () => pushes;
}

describe('pullBatch ineligible candidate parking', () => {
  test('restores blocked FIFO-group jobs only once for the whole batch', async () => {
    const manager = new QueueManager();
    const queue = 'batch-single-parking-group';
    const blockedCount = 64;
    const batchSize = 8;
    const timestamp = Date.now() - 10_000;

    try {
      const active = await manager.push(queue, {
        data: { kind: 'active' },
        groupId: 'blocked',
        priority: 100,
        timestamp,
      });
      await manager.pushBatch(queue, [
        ...Array.from({ length: blockedCount }, (_, index) => ({
          data: { kind: 'blocked', index },
          groupId: 'blocked',
          priority: 100,
          timestamp: timestamp + index + 1,
        })),
        ...Array.from({ length: batchSize }, (_, index) => ({
          data: { kind: 'eligible', index },
          groupId: `free-${index}`,
          timestamp: timestamp + blockedCount + index + 1,
        })),
      ]);

      expect((await manager.pull(queue, 0))?.id).toBe(active.id);
      const getPushCount = countQueuePushes(manager, queue);
      const batch = await manager.pullBatch(queue, batchSize, 0);

      expect(batch.map((job) => (job.data as { kind: string }).kind)).toEqual(
        Array(batchSize).fill('eligible')
      );
      expect(getPushCount()).toBe(blockedCount);
      expect(manager.count(queue)).toBe(blockedCount);
    } finally {
      manager.shutdown();
    }
  });

  test('restores future delayed jobs only once for the whole batch', async () => {
    const manager = new QueueManager();
    const queue = 'batch-single-parking-delayed';
    const delayedCount = 64;
    const batchSize = 8;
    const timestamp = Date.now() - 10_000;

    try {
      await manager.pushBatch(queue, [
        ...Array.from({ length: delayedCount }, (_, index) => ({
          data: { kind: 'delayed', index },
          delay: 60_000,
          priority: 100,
          timestamp: timestamp + index,
        })),
        ...Array.from({ length: batchSize }, (_, index) => ({
          data: { kind: 'eligible', index },
          timestamp: timestamp + delayedCount + index,
        })),
      ]);

      const getPushCount = countQueuePushes(manager, queue);
      const batch = await manager.pullBatch(queue, batchSize, 0);

      expect(batch.map((job) => (job.data as { kind: string }).kind)).toEqual(
        Array(batchSize).fill('eligible')
      );
      expect(getPushCount()).toBe(delayedCount);
      expect(manager.count(queue)).toBe(delayedCount);
    } finally {
      manager.shutdown();
    }
  });

  test('keeps the earliest parked runAt for a delayed long-poll', async () => {
    const manager = new QueueManager();
    const queue = 'batch-single-parking-wakeup';
    const timestamp = Date.now();

    try {
      const first = await manager.push(queue, {
        data: { order: 1 },
        delay: 80,
        priority: 100,
        timestamp,
      });
      await manager.push(queue, {
        data: { order: 2 },
        delay: 800,
        priority: 100,
        timestamp,
      });

      const startedAt = Date.now();
      const batch = await manager.pullBatch(queue, 2, 1_000);
      const elapsed = Date.now() - startedAt;

      expect(batch.map((job) => job.id)).toEqual([first.id]);
      expect(elapsed).toBeGreaterThanOrEqual(30);
      expect(elapsed).toBeLessThan(500);
      expect(manager.count(queue)).toBe(1);
    } finally {
      manager.shutdown();
    }
  });

  test('restores parked jobs once when concurrency partially fills the batch', async () => {
    const manager = new QueueManager();
    const queue = 'batch-single-parking-concurrency';
    const delayedCount = 32;
    const timestamp = Date.now() - 1_000;

    try {
      manager.setConcurrency(queue, 2);
      await manager.pushBatch(queue, [
        ...Array.from({ length: delayedCount }, (_, index) => ({
          data: { kind: 'delayed', index },
          delay: 60_000,
          priority: 100,
          timestamp: timestamp + index,
        })),
        ...Array.from({ length: 3 }, (_, index) => ({
          data: { kind: 'eligible', index },
          timestamp: timestamp + delayedCount + index,
        })),
      ]);

      const getPushCount = countQueuePushes(manager, queue);
      const batch = await manager.pullBatch(queue, 3, 0);

      expect(batch).toHaveLength(2);
      expect(getPushCount()).toBe(delayedCount);
      expect(manager.count(queue)).toBe(delayedCount + 1);

      await manager.ackBatch(batch.map((job) => job.id));
      const remainingReady = await manager.pull(queue, 0);
      expect(remainingReady?.data).toEqual({ kind: 'eligible', index: 2 });
    } finally {
      manager.shutdown();
    }
  });

  test('restores already parked jobs when a later expired-job event throws', async () => {
    const manager = new QueueManager();
    const queue = 'batch-single-parking-finally';
    const timestamp = Date.now() - 10_000;

    try {
      await manager.pushBatch(queue, [
        {
          data: { kind: 'delayed' },
          delay: 60_000,
          priority: 200,
          timestamp,
        },
        {
          data: { kind: 'expired' },
          priority: 100,
          timestamp,
          ttl: 1,
        },
        { data: { kind: 'eligible' }, timestamp },
      ]);
      const getPushCount = countQueuePushes(manager, queue);
      manager.setDashboardEmit((event) => {
        if (event === 'job:expired') throw new Error('dashboard failure');
      });

      await expect(manager.pullBatch(queue, 1, 0)).rejects.toThrow('dashboard failure');
      expect(getPushCount()).toBe(1);
      expect(manager.count(queue)).toBe(2);

      manager.setDashboardEmit(() => undefined);
      const eligible = await manager.pull(queue, 0);
      expect(eligible?.data).toEqual({ kind: 'eligible' });
      expect(manager.count(queue)).toBe(1);
    } finally {
      manager.shutdown();
    }
  });
});
