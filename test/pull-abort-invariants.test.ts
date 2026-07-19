import { afterEach, describe, expect, test } from 'bun:test';
import { rmSync } from 'node:fs';
import { QueueManager } from '../src/application/queueManager';
import { shardIndex } from '../src/shared/hash';
import type { RWLock } from '../src/shared/lock';

const databases: string[] = [];

afterEach(() => {
  for (const path of databases.splice(0)) {
    for (const suffix of ['', '-wal', '-shm']) {
      try {
        rmSync(path + suffix);
      } catch {
        // already removed
      }
    }
  }
});

describe('connection-scoped pull cancellation invariants', () => {
  test('pre-aborted single and batch pulls cannot claim queued jobs', async () => {
    const manager = new QueueManager();
    try {
      const queue = `abort-ready-${process.pid}`;
      const first = await manager.push(queue, { data: { n: 1 } });
      const second = await manager.push(queue, { data: { n: 2 } });
      const before = manager.getStats().totalPulled;
      const controller = new AbortController();
      controller.abort();

      expect(await manager.pull(queue, 100, controller.signal)).toBeNull();
      expect(await manager.pullBatch(queue, 2, 100, controller.signal)).toEqual([]);
      expect(manager.getStats().totalPulled).toBe(before);

      const pulled = await manager.pullBatch(queue, 2);
      expect(pulled.map((job) => job.id)).toEqual([first.id, second.id]);
    } finally {
      manager.shutdown();
    }
  });

  test('aborted owner pulls create neither jobs nor lock tokens', async () => {
    const manager = new QueueManager();
    try {
      const queue = `abort-owner-${process.pid}`;
      const job = await manager.push(queue, { data: {} });
      const controller = new AbortController();
      controller.abort();

      const single = await manager.pullWithLock(queue, 'owner', 100, undefined, controller.signal);
      expect(single).toEqual({ job: null, token: null });
      const batch = await manager.pullBatchWithLock(
        queue,
        1,
        'owner',
        100,
        undefined,
        controller.signal
      );
      expect(batch).toEqual({ jobs: [], tokens: [] });
      expect(manager.getLockInfo(job.id)).toBeNull();
      expect((await manager.pull(queue))?.id).toBe(job.id);
    } finally {
      manager.shutdown();
    }
  });

  test('aborted durable long-poll leaves the next job waiting across restart', async () => {
    const path = `/tmp/bunqueue-pull-abort-${process.pid}-${Date.now()}.db`;
    databases.push(path);
    const queue = `abort-durable-${process.pid}`;
    const first = new QueueManager({ dataPath: path });
    const controller = new AbortController();
    const pending = first.pull(queue, 5000, controller.signal);
    await Bun.sleep(10);
    controller.abort();
    expect(await pending).toBeNull();

    const job = await first.push(queue, { data: { durable: true }, durable: true });
    expect(await first.getJobState(job.id)).toBe('waiting');
    first.shutdown();

    const recovered = new QueueManager({ dataPath: path });
    try {
      expect(await recovered.getJobState(job.id)).toBe('waiting');
      expect((await recovered.pull(queue))?.id).toBe(job.id);
    } finally {
      recovered.shutdown();
    }
  });

  test('abort while waiting for the shard lock consumes no rate token', async () => {
    const manager = new QueueManager();
    try {
      const queue = `abort-lock-${process.pid}`;
      const job = await manager.push(queue, { data: {} });
      manager.setRateLimit(queue, 1, 60_000);
      const internals = manager as unknown as { shardLocks: RWLock[] };
      const guard = await internals.shardLocks[shardIndex(queue)].acquireWrite();
      const controller = new AbortController();
      const pending = manager.pull(queue, 5_000, controller.signal);

      await Bun.sleep(5);
      controller.abort();
      guard.release();

      expect(await pending).toBeNull();
      expect(manager.getStats().totalPulled).toBe(0n);
      expect((await manager.pull(queue))?.id).toBe(job.id);
    } finally {
      manager.shutdown();
    }
  });

  test('batch abort while waiting for the shard lock consumes no resources', async () => {
    const manager = new QueueManager();
    try {
      const queue = `abort-batch-lock-${process.pid}`;
      const jobs = await Promise.all([
        manager.push(queue, { data: { n: 1 } }),
        manager.push(queue, { data: { n: 2 } }),
      ]);
      manager.setRateLimit(queue, 2, 60_000);
      const internals = manager as unknown as { shardLocks: RWLock[] };
      const guard = await internals.shardLocks[shardIndex(queue)].acquireWrite();
      const controller = new AbortController();
      const pending = manager.pullBatch(queue, 2, 5_000, controller.signal);

      await Bun.sleep(5);
      controller.abort();
      guard.release();

      expect(await pending).toEqual([]);
      expect(manager.getStats().totalPulled).toBe(0n);
      expect((await manager.pullBatch(queue, 2)).map((job) => job.id)).toEqual(
        jobs.map((job) => job.id)
      );
    } finally {
      manager.shutdown();
    }
  });
});
