/**
 * Regression from queue-model seed 676055362, path 119.
 *
 * A locked job was discarded, retried from the DLQ, then submitted again with
 * the same custom ID. The retry did not restore custom-ID ownership, allowing
 * the later submission to change the priority of an already-live generation.
 */

import { expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';

test('DLQ retry restores live custom-ID idempotency', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'bunqueue-dlq-custom-id-'));
  const dataPath = join(directory, 'queue.db');
  const queue = 'model-dlq-custom-id';
  const customId = 'model-job-3';
  let manager = new QueueManager({ dataPath });

  try {
    const original = await manager.push(queue, {
      customId,
      data: { generation: 1 },
      durable: true,
      priority: 1,
    });
    const lease = await manager.pullWithLock(queue, 'model-worker', 0, 60_000);
    expect(lease.job?.id).toBe(original.id);
    expect(await manager.discard(original.id, lease.token)).toBe(true);
    expect(manager.retryDlq(queue)).toBe(1);

    expect(manager.getJobByCustomId(customId)?.id).toBe(original.id);
    const duplicate = await manager.push(queue, {
      customId,
      data: { generation: 2 },
      durable: true,
      priority: 2,
    });
    expect(duplicate.id).toBe(original.id);
    expect((await manager.getJob(original.id))?.priority).toBe(1);
    expect(manager.count(queue)).toBe(1);
    expect(manager.getDlqCount(queue)).toBe(0);

    manager.shutdown();
    manager = new QueueManager({ dataPath });
    expect(manager.getJobByCustomId(customId)?.id).toBe(original.id);
    expect((await manager.getJob(original.id))?.priority).toBe(1);
    expect(manager.count(queue)).toBe(1);
  } finally {
    manager.shutdown();
    rmSync(directory, { force: true, recursive: true });
  }
});

test('completed retry restores live custom-ID idempotency', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'bunqueue-completed-custom-id-'));
  const dataPath = join(directory, 'queue.db');
  const queue = 'model-completed-custom-id';
  const customId = 'model-completed-job';
  let manager: QueueManager | null = new QueueManager({ dataPath });

  try {
    const original = await manager.push(queue, {
      customId,
      data: { generation: 1 },
      durable: true,
      priority: 1,
    });
    const lease = await manager.pullWithLock(queue, 'model-worker', 0, 60_000);
    expect(lease.job?.id).toBe(original.id);
    await manager.ack(original.id, { done: true }, lease.token);
    expect(await manager.getJobState(original.id)).toBe('completed');
    expect(manager.retryCompleted(queue)).toBe(1);

    expect(manager.getJobByCustomId(customId)?.id).toBe(original.id);
    const duplicate = await manager.push(queue, {
      customId,
      data: { generation: 2 },
      durable: true,
      priority: 2,
    });
    expect(duplicate.id).toBe(original.id);
    expect((await manager.getJob(original.id))?.priority).toBe(1);
    expect(manager.count(queue)).toBe(1);
  } finally {
    manager.shutdown();
    rmSync(directory, { force: true, recursive: true });
  }
});

test('DLQ retry restores deduplication ownership when the key is free', async () => {
  const manager = new QueueManager();
  const queue = 'model-dlq-dedup-free';
  const uniqueKey = 'retry-dedup-free';

  try {
    const original = await manager.push(queue, {
      data: { generation: 1 },
      dedup: { ttl: 60_000 },
      uniqueKey,
    });
    expect(await manager.discard(original.id)).toBe(true);
    expect(manager.retryDlq(queue, original.id)).toBe(1);
    expect(manager.getDeduplicationJobId(queue, uniqueKey)).toBe(original.id);

    const duplicate = await manager.push(queue, {
      data: { generation: 2 },
      uniqueKey,
    });
    expect(duplicate.id).toBe(original.id);
    expect(manager.count(queue)).toBe(1);
  } finally {
    manager.shutdown();
  }
});

test('DLQ retry leaves the terminal job untouched when another job owns its key', async () => {
  const manager = new QueueManager();
  const queue = 'model-dlq-dedup-conflict';
  const uniqueKey = 'retry-dedup-conflict';

  try {
    const terminal = await manager.push(queue, { data: { generation: 1 }, uniqueKey });
    expect(await manager.discard(terminal.id)).toBe(true);
    const live = await manager.push(queue, { data: { generation: 2 }, uniqueKey });
    expect(live.id).not.toBe(terminal.id);

    expect(manager.retryDlq(queue, terminal.id)).toBe(0);
    expect(manager.getDeduplicationJobId(queue, uniqueKey)).toBe(live.id);
    expect(manager.getDlq(queue).map((job) => job.id)).toContain(terminal.id);
    expect(manager.count(queue)).toBe(1);
  } finally {
    manager.shutdown();
  }
});

test('failed SQLite DLQ retry leaves memory and disk terminal', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'bunqueue-dlq-retry-failure-'));
  const dataPath = join(directory, 'queue.db');
  let manager: QueueManager | null = new QueueManager({ dataPath });
  const queue = 'model-dlq-retry-failure';

  try {
    const terminal = await manager.push(queue, {
      customId: 'retry-storage-failure',
      data: { generation: 1 },
      durable: true,
    });
    expect(await manager.discard(terminal.id)).toBe(true);
    const storage = (
      manager as unknown as {
        storage: { requeueDlqJob(job: unknown): void };
      }
    ).storage;
    const originalRequeue = storage.requeueDlqJob.bind(storage);
    storage.requeueDlqJob = () => {
      throw new Error('injected retry persistence failure');
    };
    try {
      expect(() => manager.retryDlq(queue, terminal.id)).toThrow(
        'injected retry persistence failure'
      );
    } finally {
      storage.requeueDlqJob = originalRequeue;
    }

    expect(await manager.getJobState(terminal.id)).toBe('failed');
    expect(manager.getDlq(queue).map((job) => job.id)).toEqual([terminal.id]);
    expect(manager.count(queue)).toBe(0);

    manager.shutdown();
    manager = null;
    const recovered = new QueueManager({ dataPath });
    try {
      expect(await recovered.getJobState(terminal.id)).toBe('failed');
      expect(recovered.getDlq(queue).map((job) => job.id)).toEqual([terminal.id]);
      expect(recovered.count(queue)).toBe(0);
    } finally {
      recovered.shutdown();
    }
  } finally {
    manager?.shutdown();
    rmSync(directory, { force: true, recursive: true });
  }
});
