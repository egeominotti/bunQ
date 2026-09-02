import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import type { Job } from '../src/domain/types/job';

interface MutableStorage {
  insertJob(job: Job, durable?: boolean, admission?: unknown): void;
  insertJobsBatch(jobs: Job[], durable?: boolean): void;
  commitFlowLink(...args: unknown[]): void;
}

let directory = '';
let dataPath = '';
let manager: QueueManager;

function storageOf(queueManager: QueueManager): MutableStorage {
  return (queueManager as unknown as { storage: MutableStorage }).storage;
}

beforeEach(async () => {
  directory = await mkdtemp(join(tmpdir(), 'bunqueue-durable-rejection-'));
  dataPath = join(directory, 'queue.db');
  manager = new QueueManager({ dataPath });
});

afterEach(async () => {
  manager.shutdown();
  await rm(directory, { force: true, recursive: true });
});

describe('durable persistence rejection is not an in-memory acceptance', () => {
  test('single push rolls back queue, index, custom ID, and dedup ownership', async () => {
    const queue = 'durable-rejection-single';
    const storage = storageOf(manager);
    const insertJob = storage.insertJob.bind(storage);
    storage.insertJob = () => {
      throw new Error('injected durable insert failure');
    };

    try {
      await expect(
        manager.push(queue, {
          customId: 'durable-rejection-single-id',
          uniqueKey: 'durable-rejection-single-key',
          data: { generation: 1 },
          durable: true,
        })
      ).rejects.toThrow('injected durable insert failure');

      expect(manager.getQueueJobCounts(queue).waiting).toBe(0);
      expect(await manager.getJobState('durable-rejection-single-id')).toBe('unknown');
      expect(manager.getMemoryStats().jobIndex).toBe(0);
    } finally {
      storage.insertJob = insertJob;
    }

    const retried = await manager.push(queue, {
      customId: 'durable-rejection-single-id',
      uniqueKey: 'durable-rejection-single-key',
      data: { generation: 2 },
      durable: true,
    });
    expect(retried.data).toEqual({ generation: 2 });
    expect(manager.getQueueJobCounts(queue).waiting).toBe(1);
  });

  test('failed terminal custom-ID reuse preserves the committed generation auxiliaries', async () => {
    const oldQueue = 'durable-rejection-generation-old';
    const newQueue = 'durable-rejection-generation-new';
    const old = await manager.push(oldQueue, {
      customId: 'durable-rejection-generation-id',
      data: { generation: 'old' },
      durable: true,
    });
    expect(manager.addLog(old.id, 'committed old log')).toBe(true);
    expect((await manager.pull(oldQueue, 0))?.id).toBe(old.id);
    await manager.ack(old.id, { generation: 'old' });

    const storage = storageOf(manager);
    const insertJob = storage.insertJob.bind(storage);
    storage.insertJob = () => {
      throw new Error('injected generation admission failure');
    };
    try {
      await expect(
        manager.push(newQueue, {
          customId: String(old.id),
          data: { generation: 'rejected' },
          durable: true,
        })
      ).rejects.toThrow('injected generation admission failure');
    } finally {
      storage.insertJob = insertJob;
    }

    expect(await manager.getJobState(old.id)).toBe('completed');
    expect(manager.getResult(old.id)).toEqual({ generation: 'old' });
    expect(manager.getLogs(old.id).map((entry) => entry.message)).toEqual(['committed old log']);
    expect(manager.getQueueJobCounts(oldQueue).completed).toBe(1);
    expect(manager.getQueueJobCounts(newQueue).waiting).toBe(0);
  });

  test('batch rejection leaves no phantom accepted prefix or reserved identities', async () => {
    const queue = 'durable-rejection-batch';
    const storage = storageOf(manager);
    const insertJob = storage.insertJob.bind(storage);
    const insertJobsBatch = storage.insertJobsBatch.bind(storage);
    const rejectInsert = () => {
      throw new Error('injected durable batch failure');
    };
    storage.insertJob = rejectInsert;
    storage.insertJobsBatch = rejectInsert;

    const inputs = Array.from({ length: 3 }, (_, index) => ({
      customId: `durable-rejection-batch-${index}`,
      uniqueKey: `durable-rejection-batch-key-${index}`,
      data: { index },
      durable: true,
    }));

    try {
      await expect(manager.pushBatch(queue, inputs)).rejects.toThrow(
        'injected durable batch failure'
      );
      expect(manager.getQueueJobCounts(queue).waiting).toBe(0);
      expect(manager.getMemoryStats().jobIndex).toBe(0);
      for (const input of inputs) {
        expect(await manager.getJobState(input.customId)).toBe('unknown');
      }
    } finally {
      storage.insertJob = insertJob;
      storage.insertJobsBatch = insertJobsBatch;
    }

    const ids = await manager.pushBatch(
      queue,
      inputs.map((input) => ({ ...input, data: { ...input.data, retried: true } }))
    );
    expect(ids).toEqual(inputs.map((input) => input.customId));
    expect(manager.getQueueJobCounts(queue).waiting).toBe(3);
  });

  test('a later durable failure preserves the committed buffered batch prefix', async () => {
    const queue = 'durable-rejection-prefix';
    const storage = storageOf(manager);
    const insertJob = storage.insertJob.bind(storage);
    storage.insertJob = (job, durable, admission) => {
      if (job.customId === 'durable-rejection-prefix-fail') {
        throw new Error('injected later durable failure');
      }
      insertJob(job, durable, admission);
    };

    try {
      await expect(
        manager.pushBatch(queue, [
          {
            customId: 'durable-rejection-prefix-ok',
            data: { accepted: true },
          },
          {
            customId: 'durable-rejection-prefix-fail',
            data: { accepted: false },
            durable: true,
          },
        ])
      ).rejects.toThrow('injected later durable failure');
    } finally {
      storage.insertJob = insertJob;
    }

    expect(manager.getQueueJobCounts(queue).waiting).toBe(1);
    expect(await manager.getJobState('durable-rejection-prefix-ok')).toBe('waiting');
    expect(await manager.getJobState('durable-rejection-prefix-fail')).toBe('unknown');

    manager.shutdown();
    manager = new QueueManager({ dataPath });
    expect(await manager.getJobState('durable-rejection-prefix-ok')).toBe('waiting');
    expect(await manager.getJobState('durable-rejection-prefix-fail')).toBe('unknown');
    expect(manager.getQueueJobCounts(queue).waiting).toBe(1);
  });

  test('parent-link persistence failure leaves both parent and child topology unchanged', async () => {
    const queue = 'durable-rejection-parent';
    const parent = await manager.push(queue, {
      customId: 'durable-rejection-parent-id',
      data: { role: 'parent' },
      durable: true,
    });
    const storage = storageOf(manager);
    const commitFlowLink = storage.commitFlowLink.bind(storage);
    storage.commitFlowLink = () => {
      throw new Error('injected parent-link transaction failure');
    };

    try {
      await expect(
        manager.push(queue, {
          customId: 'durable-rejection-child-id',
          parentId: parent.id,
          data: {
            role: 'child',
            __parentId: parent.id,
            __parentQueue: queue,
          },
          durable: true,
        })
      ).rejects.toThrow('injected parent-link transaction failure');
    } finally {
      storage.commitFlowLink = commitFlowLink;
    }

    expect(await manager.getJobState(parent.id)).toBe('waiting');
    expect((await manager.getJob(parent.id))?.dependsOn).toEqual([]);
    expect((await manager.getJob(parent.id))?.childrenIds).toEqual([]);
    expect(await manager.getJobState('durable-rejection-child-id')).toBe('unknown');
    expect(manager.getQueueJobCounts(queue)['waiting-children']).toBe(0);

    manager.shutdown();
    manager = new QueueManager({ dataPath });
    expect(await manager.getJobState(parent.id)).toBe('waiting');
    expect((await manager.getJob(parent.id))?.dependsOn).toEqual([]);
    expect(await manager.getJobState('durable-rejection-child-id')).toBe('unknown');
  });

  test('failed waiting-dependency admission does not leak completion pins or reverse edges', async () => {
    const queue = 'durable-rejection-pins';
    const completed = await manager.push(queue, {
      customId: 'durable-rejection-completed-proof',
      data: { proof: true },
      durable: true,
      removeOnComplete: true,
    });
    const pulled = await manager.pull(queue);
    expect(pulled?.id).toBe(completed.id);
    await manager.ack(completed.id, { proof: true });
    const blocker = await manager.push(queue, {
      customId: 'durable-rejection-blocker',
      data: { blocker: true },
      durable: true,
    });

    const storage = storageOf(manager);
    const insertJob = storage.insertJob.bind(storage);
    storage.insertJob = () => {
      throw new Error('injected pin admission failure');
    };
    try {
      await expect(
        manager.push(queue, {
          customId: 'durable-rejection-dependent',
          data: { dependent: true },
          dependsOn: [completed.id, blocker.id],
          durable: true,
        })
      ).rejects.toThrow('injected pin admission failure');
    } finally {
      storage.insertJob = insertJob;
    }

    expect(await manager.getJobState('durable-rejection-dependent')).toBe('unknown');
    expect(manager.getQueueJobCounts(queue)['waiting-children']).toBe(0);
    const db = new Database(dataPath, { readonly: true });
    try {
      const row = db
        .query<{ pinned: number }, [string]>(
          'SELECT pinned FROM dependency_completions WHERE job_id = ?'
        )
        .get(completed.id);
      expect(row?.pinned).toBe(0);
    } finally {
      db.close();
    }

    const dependent = await manager.push(queue, {
      customId: 'durable-rejection-dependent',
      data: { dependent: true },
      dependsOn: [completed.id, blocker.id],
      durable: true,
    });
    expect(await manager.getJobState(dependent.id)).toBe('waiting-children');
    expect(manager.getQueueJobCounts(queue)['waiting-children']).toBe(1);
  });
});
