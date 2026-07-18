import { Database } from 'bun:sqlite';
import { afterEach, expect, test } from 'bun:test';
import { mkdirSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { cleanup } from '../src/application/cleanupTasks';
import type { BackgroundContext } from '../src/application/types';
import { jobId } from '../src/domain/types/job';
import { shardIndex } from '../src/shared/hash';

const tempDir = join(import.meta.dir, '.tmp-stale-dependency-gc');
const dbPath = join(tempDir, 'queue.db');
let manager: QueueManager | null = null;

afterEach(() => {
  manager?.shutdown();
  manager = null;
  rmSync(tempDir, { recursive: true, force: true });
});

test('stale dependency GC releases ownership and deletes durable state', async () => {
  mkdirSync(tempDir, { recursive: true });
  manager = new QueueManager({ dataPath: dbPath, cleanupIntervalMs: 60_000 });
  const queue = 'stale-dependency-gc';
  const first = await manager.push(queue, {
    data: { generation: 1 },
    dependsOn: [jobId('missing-dependency')],
    uniqueKey: 'shared-key',
    durable: true,
  });

  const internals = manager as unknown as BackgroundContext;
  const shard = internals.shards[shardIndex(queue)];
  const waiting = shard.waitingDeps.get(first.id);
  expect(waiting).toBeDefined();
  (waiting as { createdAt: number }).createdAt = Date.now() - 61 * 60 * 1_000;

  await cleanup(internals);

  expect(await manager.getJob(first.id)).toBeNull();
  expect(shard.getUniqueKeyEntry(queue, 'shared-key')).toBeNull();

  const second = await manager.push(queue, {
    data: { generation: 2 },
    uniqueKey: 'shared-key',
    durable: true,
  });
  expect(second.id).not.toBe(first.id);

  const db = new Database(dbPath, { readonly: true });
  try {
    const row = db
      .query<{ count: number }, [string]>('SELECT COUNT(*) AS count FROM jobs WHERE id = ?')
      .get(String(first.id));
    expect(row?.count).toBe(0);
  } finally {
    db.close();
  }
});
