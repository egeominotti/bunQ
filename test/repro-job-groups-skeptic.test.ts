import { afterEach, describe, expect, spyOn, test, setSystemTime } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { cleanup } from '../src/application/cleanupTasks';
import { QueueManager } from '../src/application/queueManager';
import type { BackgroundContext } from '../src/application/types';
import { validateAtomicFlowBatch } from '../src/application/operations/flowValidation';
import { Queue, shutdownManager } from '../src/client';
import { shardIndex } from '../src/shared/hash';

function queueName(label: string): string {
  return `groups-regression-${label}-${Bun.randomUUIDv7()}`;
}

function backgroundContext(manager: QueueManager): BackgroundContext {
  return (
    manager as unknown as {
      contextFactory: { getBackgroundContext(): BackgroundContext };
    }
  ).contextFactory.getBackgroundContext();
}

interface GroupPolicyStorage {
  db: { exec(sql: string): void };
  saveGroupRateLimit(queue: string, groupId: string, max: number, duration: number): void;
  removeGroupRateLimit(queue: string, groupId: string): void;
  saveGroupConcurrency(queue: string, groupId: string, concurrency: number): void;
  removeGroupConcurrency(queue: string, groupId: string): void;
}

function groupPolicyStorage(manager: QueueManager): GroupPolicyStorage {
  return (manager as unknown as { storage: GroupPolicyStorage }).storage;
}

function failGroupStateDeletes(manager: QueueManager): void {
  groupPolicyStorage(manager).db.exec(`
    CREATE TEMP TRIGGER fail_group_state_delete
    BEFORE DELETE ON group_state
    BEGIN
      SELECT RAISE(ABORT, 'injected group state delete failure');
    END
  `);
}

afterEach(() => {
  setSystemTime();
  shutdownManager();
});

describe('job group regressions', () => {
  test('preserves insertion FIFO when custom IDs sort in the opposite order', async () => {
    const manager = new QueueManager();
    const queue = queueName('fifo');
    const timestamp = Date.now();
    try {
      await manager.push(queue, {
        customId: 'z-first',
        data: { label: 'first' },
        groupId: 'A',
        timestamp,
      });
      await manager.push(queue, {
        customId: 'a-second',
        data: { label: 'second' },
        groupId: 'A',
        timestamp,
      });

      const jobs = await manager.pullBatch(queue, 2);
      expect(jobs.map((job) => (job.data as { label: string }).label)).toEqual(['first', 'second']);
    } finally {
      manager.shutdown();
    }
  });

  test('preserves grouped FIFO through priority and delay replacements', async () => {
    const manager = new QueueManager();
    const queue = queueName('fifo-updates');
    const timestamp = Date.now();
    try {
      const first = await manager.push(queue, {
        customId: 'z-first-update',
        data: { label: 'first' },
        groupId: 'A',
        timestamp,
      });
      const second = await manager.push(queue, {
        customId: 'a-second-update',
        data: { label: 'second' },
        groupId: 'A',
        timestamp,
      });
      expect(await manager.changePriority(first.id, 1)).toBe(true);
      expect(await manager.changePriority(second.id, 99)).toBe(true);
      expect(await manager.changeDelay(first.id, 0)).toBe(true);
      expect(await manager.changeDelay(second.id, 0)).toBe(true);

      const jobs = await manager.pullBatch(queue, 2);
      expect(jobs.map((job) => (job.data as { label: string }).label)).toEqual(['first', 'second']);
    } finally {
      manager.shutdown();
    }
  });

  test('restores insertion FIFO from durable SQLite state', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'bunqueue-group-fifo-'));
    const database = join(directory, 'queue.db');
    const queue = queueName('fifo-restart');
    const timestamp = Date.now();
    let manager = new QueueManager({ dataPath: database });
    try {
      await manager.push(queue, {
        customId: 'z-first-restart',
        data: { label: 'first' },
        durable: true,
        groupId: 'A',
        timestamp,
      });
      await manager.push(queue, {
        customId: 'a-second-restart',
        data: { label: 'second' },
        durable: true,
        groupId: 'A',
        timestamp,
      });
      manager.shutdown();

      manager = new QueueManager({ dataPath: database });
      const jobs = await manager.pullBatch(queue, 2);
      expect(jobs.map((job) => (job.data as { label: string }).label)).toEqual(['first', 'second']);
    } finally {
      manager.shutdown();
      rmSync(directory, { recursive: true, force: true });
    }
  });

  test('keeps secondary scheduling lazy for plain queues and releases it after groups drain', async () => {
    const manager = new QueueManager();
    const plainQueue = queueName('lazy-plain');
    const groupedQueue = queueName('lazy-grouped');
    try {
      await manager.pushBatch(
        plainQueue,
        Array.from({ length: 100 }, (_, index) => ({ data: { index } }))
      );
      const context = backgroundContext(manager);
      const plainShard = context.shards[shardIndex(plainQueue)] as unknown as {
        groupScheduler: { queues: Map<string, unknown> };
      };
      expect(plainShard.groupScheduler.queues.has(plainQueue)).toBe(false);

      await manager.push(groupedQueue, { data: {}, groupId: 'A' });
      const groupedShard = context.shards[shardIndex(groupedQueue)] as unknown as {
        groupScheduler: { queues: Map<string, unknown> };
      };
      expect(groupedShard.groupScheduler.queues.has(groupedQueue)).toBe(true);
      const claimed = await manager.pull(groupedQueue);
      expect(claimed).not.toBeNull();
      expect(groupedShard.groupScheduler.queues.has(groupedQueue)).toBe(false);
      await manager.ack(claimed!.id, {});
    } finally {
      manager.shutdown();
    }
  });

  test('does not deliver a delayed grouped job at its exact TTL boundary', async () => {
    const manager = new QueueManager();
    const queue = queueName('ttl-boundary');
    const createdAt = Date.now();
    try {
      const pushed = await manager.push(queue, {
        data: { label: 'delayed' },
        delay: 200,
        groupId: 'A',
        timestamp: createdAt,
        ttl: 100,
      });

      setSystemTime(new Date(createdAt + 100));
      expect(await manager.pull(queue)).toBeNull();
      expect(await manager.getJobState(pushed.id)).toBe('delayed');

      setSystemTime(new Date(createdAt + 101));
      expect(await manager.pull(queue)).toBeNull();
      expect(await manager.getJobState(pushed.id)).toBe('unknown');
    } finally {
      manager.shutdown();
    }
  });

  test('does not deliver a delayed ungrouped job at its exact TTL boundary', async () => {
    const manager = new QueueManager();
    const queue = queueName('plain-ttl-boundary');
    const createdAt = Date.now();
    try {
      const pushed = await manager.push(queue, {
        data: { label: 'delayed' },
        delay: 200,
        timestamp: createdAt,
        ttl: 100,
      });

      setSystemTime(new Date(createdAt + 100));
      expect(await manager.pull(queue)).toBeNull();
      expect(await manager.getJobState(pushed.id)).toBe('delayed');

      setSystemTime(new Date(createdAt + 101));
      expect(await manager.pull(queue)).toBeNull();
      expect(await manager.getJobState(pushed.id)).toBe('unknown');
    } finally {
      manager.shutdown();
    }
  });

  test('raising a group concurrency override wakes a blocked pull', async () => {
    const manager = new QueueManager();
    const queue = queueName('concurrency-wakeup');
    try {
      await manager.setGroupConcurrency(queue, 'A', 1);
      await manager.push(queue, { data: { label: 'first' }, groupId: 'A' });
      await manager.push(queue, { data: { label: 'second' }, groupId: 'A' });
      expect(await manager.pull(queue, 0, undefined, { concurrency: 4 })).not.toBeNull();

      const startedAt = performance.now();
      const waiting = manager.pull(queue, 1_000, undefined, { concurrency: 4 });
      await Bun.sleep(50);
      await manager.setGroupConcurrency(queue, 'A', 2);

      expect((await waiting)?.data).toEqual({ label: 'second' });
      expect(performance.now() - startedAt).toBeLessThan(500);
    } finally {
      manager.shutdown();
    }
  });

  test('removing a group rate override wakes a blocked pull', async () => {
    const manager = new QueueManager();
    const queue = queueName('rate-wakeup');
    const defaults = { limit: { max: 10, duration: 60_000 } };
    try {
      await manager.setGroupRateLimit(queue, 'A', 1, 60_000);
      await manager.push(queue, { data: { label: 'first' }, groupId: 'A' });
      await manager.push(queue, { data: { label: 'second' }, groupId: 'A' });
      expect(await manager.pull(queue, 0, undefined, defaults)).not.toBeNull();

      const startedAt = performance.now();
      const waiting = manager.pull(queue, 1_000, undefined, defaults);
      await Bun.sleep(50);
      expect(await manager.removeGroupRateLimit(queue, 'A')).toBe(1);

      expect((await waiting)?.data).toEqual({ label: 'second' });
      expect(performance.now() - startedAt).toBeLessThan(500);
    } finally {
      manager.shutdown();
    }
  });

  test('rejects invalid public group control IDs before coercion or mutation', async () => {
    const queue = new Queue(queueName('control-id'), { embedded: true });
    const setConcurrency = queue.setGroupConcurrency.bind(queue) as unknown as (
      groupId: unknown,
      concurrency: number
    ) => Promise<void>;
    try {
      for (const value of [undefined, null, {}, Number.MAX_SAFE_INTEGER + 1, Symbol('A'), 'A\0B']) {
        await expect(setConcurrency(value, 1)).rejects.toThrow(/groupId/i);
      }
      expect(await queue.getGroupConcurrency('undefined')).toBeNull();
      expect(await queue.getGroupConcurrency('[object Object]')).toBeNull();
    } finally {
      queue.close();
    }
  });

  test('rejects invalid group IDs on every direct QueueManager group operation', () => {
    const manager = new QueueManager();
    const queue = queueName('direct-control-id');
    const direct = manager as unknown as Record<string, (...args: unknown[]) => unknown>;
    const operations = [
      ['getGroupJobsCount', []],
      ['getGroupActiveCount', []],
      ['setGroupRateLimit', [1, 1_000]],
      ['getGroupRateLimit', []],
      ['removeGroupRateLimit', []],
      ['getGroupRateLimitTtl', []],
      ['setGroupConcurrency', [1]],
      ['getGroupConcurrency', []],
      ['removeGroupConcurrency', []],
    ] as const;
    try {
      for (const groupId of ['', 'A\0B', 'A'.repeat(257), undefined, {}]) {
        for (const [method, trailingArgs] of operations) {
          expect(() => direct[method]!(queue, groupId, ...trailingArgs)).toThrow(/groupId/i);
        }
      }
    } finally {
      manager.shutdown();
    }
  });

  test('does not publish a group concurrency change when SQLite persistence fails', async () => {
    const manager = new QueueManager({ dataPath: ':memory:' });
    const queue = queueName('concurrency-save-failure');
    const shard = backgroundContext(manager).shards[shardIndex(queue)];
    const notify = spyOn(shard, 'notifyBatch');
    groupPolicyStorage(manager).saveGroupConcurrency = () => {
      throw new Error('injected concurrency save failure');
    };
    try {
      await expect(
        Promise.resolve().then(() => manager.setGroupConcurrency(queue, 'A', 2))
      ).rejects.toThrow('injected concurrency save failure');
      expect(await manager.getGroupConcurrency(queue, 'A')).toBeNull();
      expect(notify).not.toHaveBeenCalled();
    } finally {
      notify.mockRestore();
      manager.shutdown();
    }
  });

  test('does not remove group concurrency in memory when SQLite persistence fails', async () => {
    const manager = new QueueManager({ dataPath: ':memory:' });
    const queue = queueName('concurrency-remove-failure');
    try {
      await manager.setGroupConcurrency(queue, 'A', 2);
      groupPolicyStorage(manager).removeGroupConcurrency = () => {
        throw new Error('injected concurrency remove failure');
      };

      await expect(
        Promise.resolve().then(() => manager.removeGroupConcurrency(queue, 'A'))
      ).rejects.toThrow('injected concurrency remove failure');
      expect(await manager.getGroupConcurrency(queue, 'A')).toBe(2);
    } finally {
      manager.shutdown();
    }
  });

  test('does not publish a group rate limit when SQLite persistence fails', async () => {
    const manager = new QueueManager({ dataPath: ':memory:' });
    const queue = queueName('rate-save-failure');
    try {
      groupPolicyStorage(manager).saveGroupRateLimit = () => {
        throw new Error('injected rate save failure');
      };

      await expect(
        Promise.resolve().then(() => manager.setGroupRateLimit(queue, 'A', 2, 1_000))
      ).rejects.toThrow('injected rate save failure');
      expect(await manager.getGroupRateLimit(queue, 'A')).toBeNull();
    } finally {
      manager.shutdown();
    }
  });

  test('does not remove a group rate limit in memory when SQLite persistence fails', async () => {
    const manager = new QueueManager({ dataPath: ':memory:' });
    const queue = queueName('rate-remove-failure');
    try {
      await manager.setGroupRateLimit(queue, 'A', 2, 1_000);
      groupPolicyStorage(manager).removeGroupRateLimit = () => {
        throw new Error('injected rate remove failure');
      };

      await expect(
        Promise.resolve().then(() => manager.removeGroupRateLimit(queue, 'A'))
      ).rejects.toThrow('injected rate remove failure');
      expect(await manager.getGroupRateLimit(queue, 'A')).toEqual({ max: 2, duration: 1_000 });
    } finally {
      manager.shutdown();
    }
  });

  test('rolls back the durable rate policy when empty-row deletion fails', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'bunqueue-group-rate-remove-'));
    const database = join(directory, 'queue.db');
    const queue = queueName('rate-remove-transaction');
    let manager = new QueueManager({ dataPath: database });
    try {
      await manager.setGroupRateLimit(queue, 'A', 2, 1_000);
      failGroupStateDeletes(manager);
      const notify = spyOn(backgroundContext(manager).shards[shardIndex(queue)], 'notifyBatch');
      try {
        await expect(
          Promise.resolve().then(() => manager.removeGroupRateLimit(queue, 'A'))
        ).rejects.toThrow('injected group state delete failure');
        expect(await manager.getGroupRateLimit(queue, 'A')).toEqual({ max: 2, duration: 1_000 });
        expect(notify).not.toHaveBeenCalled();
      } finally {
        notify.mockRestore();
      }

      manager.shutdown();
      manager = new QueueManager({ dataPath: database });
      expect(await manager.getGroupRateLimit(queue, 'A')).toEqual({ max: 2, duration: 1_000 });
    } finally {
      manager.shutdown();
      rmSync(directory, { recursive: true, force: true });
    }
  });

  test('rolls back the durable concurrency policy when empty-row deletion fails', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'bunqueue-group-concurrency-remove-'));
    const database = join(directory, 'queue.db');
    const queue = queueName('concurrency-remove-transaction');
    let manager = new QueueManager({ dataPath: database });
    try {
      await manager.setGroupConcurrency(queue, 'A', 2);
      failGroupStateDeletes(manager);
      const notify = spyOn(backgroundContext(manager).shards[shardIndex(queue)], 'notifyBatch');
      try {
        await expect(
          Promise.resolve().then(() => manager.removeGroupConcurrency(queue, 'A'))
        ).rejects.toThrow('injected group state delete failure');
        expect(await manager.getGroupConcurrency(queue, 'A')).toBe(2);
        expect(notify).not.toHaveBeenCalled();
      } finally {
        notify.mockRestore();
      }

      manager.shutdown();
      manager = new QueueManager({ dataPath: database });
      expect(await manager.getGroupConcurrency(queue, 'A')).toBe(2);
    } finally {
      manager.shutdown();
      rmSync(directory, { recursive: true, force: true });
    }
  });

  test('rejects a PostgreSQL-incompatible NUL group ID before adding a job', async () => {
    const queue = new Queue(queueName('add-id'), { embedded: true });
    try {
      await expect(queue.add('job', {}, { group: { id: 'A\0B' } })).rejects.toThrow(/groupId/i);
      expect(await queue.countAsync()).toBe(0);
    } finally {
      queue.close();
    }
  });

  test('rejects empty and NUL group IDs in atomic flow validation', () => {
    for (const groupId of ['', 'A\0B']) {
      expect(() =>
        validateAtomicFlowBatch({
          jobs: [
            {
              id: 'flow-job',
              queue: 'flow-queue',
              input: { data: {}, groupId },
            },
          ],
        })
      ).toThrow(/groupId/i);
    }
  });

  test('periodic cleanup releases secondary group indexes and expired windows', async () => {
    const manager = new QueueManager();
    const queue = queueName('cleanup');
    try {
      await manager.push(queue, { data: { label: 'only' }, groupId: 'A' });
      const pulled = await manager.pull(queue, 0, undefined, {
        limit: { max: 1, duration: 1 },
      });
      expect(pulled).not.toBeNull();
      await manager.ack(pulled!.id, {});
      await Bun.sleep(5);

      const context = backgroundContext(manager);
      const shard = context.shards[shardIndex(queue)] as unknown as {
        queues: Map<string, unknown>;
        groupScheduler: { queues: Map<string, unknown> };
        groupLimiterManager: { windows: Map<string, unknown> };
      };
      await cleanup(context);

      expect(shard.queues.has(queue)).toBe(false);
      expect(shard.groupScheduler.queues.has(queue)).toBe(false);
      expect([...shard.groupLimiterManager.windows.keys()]).not.toContain(`${queue}\0A`);
    } finally {
      manager.shutdown();
    }
  });

  test('periodic cleanup preserves durable group overrides for an empty queue', async () => {
    const manager = new QueueManager();
    const queue = queueName('cleanup-overrides');
    try {
      await manager.setGroupRateLimit(queue, 'A', 3, 60_000);
      await manager.setGroupConcurrency(queue, 'A', 2);
      await manager.push(queue, { data: {}, groupId: 'A' });
      const pulled = await manager.pull(queue);
      expect(pulled).not.toBeNull();
      await manager.ack(pulled!.id, {});

      await cleanup(backgroundContext(manager));

      expect(await manager.getGroupRateLimit(queue, 'A')).toEqual({ max: 3, duration: 60_000 });
      expect(await manager.getGroupConcurrency(queue, 'A')).toBe(2);
    } finally {
      manager.shutdown();
    }
  });
});
