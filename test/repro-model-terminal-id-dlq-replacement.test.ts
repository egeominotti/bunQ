import { afterEach, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';

let manager: QueueManager | undefined;
let directory: string | undefined;

afterEach(async () => {
  manager?.shutdown();
  if (directory) await rm(directory, { force: true, recursive: true });
  manager = undefined;
  directory = undefined;
});

test('re-pushing a terminal custom ID replaces its DLQ generation exactly once', async () => {
  directory = await mkdtemp(join(tmpdir(), 'bunqueue-terminal-id-reuse-'));
  const dataPath = join(directory, 'queue.db');
  manager = new QueueManager({ dataPath });
  const queue = 'repro-model-terminal-id-replacement';
  const customId = 'recycled-terminal-id';
  const first = await manager.push(queue, {
    customId,
    data: { generation: 1 },
    durable: true,
  });

  expect(await manager.discard(first.id)).toBe(true);
  expect(manager.getQueueJobCounts(queue).failed).toBe(1);
  expect(manager.getDlq(queue)).toHaveLength(1);

  const second = await manager.push(queue, {
    customId,
    data: { generation: 2 },
    durable: true,
  });

  expect(second.id).toBe(first.id);
  expect(second.data).toEqual({ generation: 2 });
  expect(await manager.getJobState(second.id)).toBe('waiting');
  expect(manager.getQueueJobCounts(queue).failed).toBe(0);
  expect(manager.getDlq(queue)).toHaveLength(0);
  expect(manager.getMemoryStats().jobIndex).toBe(1);

  const db = new Database(dataPath, { readonly: true });
  expect(
    db
      .query<{ count: number }, []>('SELECT COUNT(*) AS count FROM dlq WHERE job_id = ?')
      .get(first.id)?.count
  ).toBe(0);
  expect(
    db.query<{ count: number }, []>('SELECT COUNT(*) AS count FROM jobs WHERE id = ?').get(first.id)
      ?.count
  ).toBe(1);
  db.close();

  manager.shutdown();
  manager = new QueueManager({ dataPath });
  expect(await manager.getJobState(second.id)).toBe('waiting');
  expect(manager.getQueueJobCounts(queue).failed).toBe(0);
  expect(manager.getDlq(queue)).toHaveLength(0);
  expect((await manager.getJob(second.id))?.data).toEqual({ generation: 2 });
});

test('PUSHB terminal ID reuse retires only the matching queue generation', async () => {
  manager = new QueueManager();
  const oldQueue = 'repro-model-terminal-id-old-queue';
  const targetQueue = 'repro-model-terminal-id-target-queue';
  const preservedQueue = 'repro-model-terminal-id-preserved-queue';
  const recycled = await manager.push(oldQueue, {
    customId: 'recycled-across-queues',
    data: { generation: 1 },
    durable: true,
  });
  const preserved = await manager.push(preservedQueue, {
    customId: 'preserved-terminal-id',
    data: { preserved: true },
    durable: true,
  });
  expect(await manager.discard(recycled.id)).toBe(true);
  expect(await manager.discard(preserved.id)).toBe(true);

  const ids = await manager.pushBatch(targetQueue, [
    {
      customId: 'recycled-across-queues',
      data: { generation: 2 },
      durable: true,
    },
    {
      customId: 'recycled-across-queues',
      data: { duplicateInBatch: true },
      durable: true,
    },
  ]);

  expect(ids).toEqual([recycled.id, recycled.id]);
  expect(manager.getQueueJobCounts(oldQueue).failed).toBe(0);
  expect(manager.getDlq(oldQueue)).toHaveLength(0);
  expect(manager.getQueueJobCounts(targetQueue).waiting).toBe(1);
  expect(manager.getQueueJobCounts(preservedQueue).failed).toBe(1);
  expect(manager.getDlq(preservedQueue).map((job) => job.id)).toEqual([preserved.id]);
  expect((await manager.getJob(recycled.id))?.data).toEqual({ generation: 2 });
});

test('a live custom ID remains idempotent when the target queue hashes elsewhere', async () => {
  manager = new QueueManager();
  const first = await manager.push('repro-model-live-id-owner', {
    customId: 'cross-shard-live-id',
    data: { generation: 1 },
    durable: true,
  });

  const duplicate = await manager.push('repro-model-live-id-other-target', {
    customId: 'cross-shard-live-id',
    data: { generation: 2 },
    durable: true,
  });

  expect(duplicate.id).toBe(first.id);
  expect(duplicate.data).toEqual({ generation: 1 });
  expect(manager.getQueueJobCounts('repro-model-live-id-owner').waiting).toBe(1);
  expect(manager.getQueueJobCounts('repro-model-live-id-other-target').waiting).toBe(0);
  expect(manager.getMemoryStats().jobIndex).toBe(1);
});
