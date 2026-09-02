import { afterEach, describe, expect, spyOn, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import type { BackgroundContext } from '../src/application/types';
import { shardIndex } from '../src/shared/hash';

function backgroundContext(manager: QueueManager): BackgroundContext {
  return (
    manager as unknown as {
      contextFactory: { getBackgroundContext(): BackgroundContext };
    }
  ).contextFactory.getBackgroundContext();
}

describe('pushBatch pending persistence index', () => {
  let manager: QueueManager | undefined;
  let directory: string | undefined;

  afterEach(async () => {
    manager?.shutdown();
    if (directory) await rm(directory, { recursive: true, force: true });
  });

  test('persists only the latest generation after reverse-order replacements', async () => {
    directory = await mkdtemp(join(tmpdir(), 'bunqueue-batch-dedup-index-'));
    const dataPath = join(directory, 'queue.db');
    const queue = 'batch-dedup-index';
    const keyCount = 64;
    manager = new QueueManager({ dataPath });

    const firstGeneration = Array.from({ length: keyCount }, (_, key) => ({
      data: { generation: 1, key },
      uniqueKey: `key-${key}`,
      dedup: { replace: true },
    }));
    const secondGeneration = Array.from({ length: keyCount }, (_, offset) => {
      const key = keyCount - offset - 1;
      return {
        data: { generation: 2, key },
        uniqueKey: `key-${key}`,
        dedup: { replace: true },
      };
    });
    const thirdGeneration = {
      data: { generation: 3, key: 0 },
      uniqueKey: 'key-0',
      dedup: { replace: true },
    };
    const context = backgroundContext(manager);
    const insertJobsBatch = spyOn(context.storage!, 'insertJobsBatch');
    const notifyBatch = spyOn(context.shards[shardIndex(queue)], 'notifyBatch');
    let ids: Awaited<ReturnType<QueueManager['pushBatch']>>;

    try {
      ids = await manager.pushBatch(queue, [
        ...firstGeneration,
        ...secondGeneration,
        thirdGeneration,
      ]);

      expect(ids).toHaveLength(keyCount * 2 + 1);
      expect(manager.count(queue)).toBe(keyCount);
      expect(insertJobsBatch).not.toHaveBeenCalled();
      expect(notifyBatch).toHaveBeenCalledTimes(1);
      expect(notifyBatch).toHaveBeenCalledWith(queue, keyCount);
    } finally {
      insertJobsBatch.mockRestore();
      notifyBatch.mockRestore();
    }

    const firstGenerationIds = ids.slice(0, keyCount);
    const secondGenerationIds = ids.slice(keyCount, keyCount * 2);
    const thirdGenerationId = ids.at(-1)!;
    for (const supersededId of [...firstGenerationIds, secondGenerationIds.at(-1)!]) {
      expect(await manager.getJob(supersededId)).toBeNull();
    }

    manager.shutdown();
    manager = new QueueManager({ dataPath });

    expect(manager.count(queue)).toBe(keyCount);
    for (let offset = 0; offset < secondGenerationIds.length - 1; offset++) {
      const job = await manager.getJob(secondGenerationIds[offset]);
      expect(job?.data).toEqual({ generation: 2, key: keyCount - offset - 1 });
    }
    expect((await manager.getJob(thirdGenerationId))?.data).toEqual({ generation: 3, key: 0 });
  });
});
