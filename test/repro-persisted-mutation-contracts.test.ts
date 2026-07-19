import { afterEach, describe, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { Queue, shutdownManager } from '../src/client';
import { MIGRATION_TABLE, SCHEMA } from '../src/infrastructure/persistence/schema';

const managers: QueueManager[] = [];
const databasePaths: string[] = [];

function createDatabasePath(label: string): string {
  const path = join(tmpdir(), `bunqueue-${label}-${process.pid}-${crypto.randomUUID()}.db`);
  databasePaths.push(path);
  return path;
}

function openManager(dataPath: string): QueueManager {
  const manager = new QueueManager({ dataPath });
  managers.push(manager);
  return manager;
}

function closeManager(manager: QueueManager): void {
  manager.shutdown();
  const index = managers.indexOf(manager);
  if (index >= 0) managers.splice(index, 1);
}

afterEach(() => {
  shutdownManager();
  for (const manager of managers.splice(0)) manager.shutdown();
  for (const path of databasePaths.splice(0)) {
    for (const suffix of ['', '-wal', '-shm']) {
      const candidate = `${path}${suffix}`;
      if (existsSync(candidate)) rmSync(candidate, { force: true });
    }
  }
});

describe('persisted mutation contracts', () => {
  test('durable progress and message survive active-job crash recovery', async () => {
    const dataPath = createDatabasePath('progress-restart');
    let manager = openManager(dataPath);
    const pushed = await manager.push('progress-restart', {
      data: { operation: 'checkpointed' },
      durable: true,
    });
    const pulled = await manager.pull('progress-restart');
    expect(pulled?.id).toBe(pushed.id);
    expect(await manager.updateProgress(pushed.id, 73, 'checkpoint')).toBe(true);
    expect(await manager.updateProgress(pushed.id, 150)).toBe(true);

    closeManager(manager);
    manager = openManager(dataPath);

    const recovered = await manager.getJob(pushed.id);
    expect(recovered?.progress).toBe(100);
    expect(recovered?.progressMessage).toBe('checkpoint');
  });

  test('per-queue DLQ configuration survives restart', () => {
    const dataPath = createDatabasePath('dlq-config-restart');
    let manager = openManager(dataPath);
    manager.setDlqConfig('configured-dlq', {
      autoRetry: true,
      autoRetryInterval: 12_345,
      maxAge: null,
      maxAutoRetries: 9,
      maxEntries: 321,
    });
    manager.pause('configured-dlq');

    closeManager(manager);
    manager = openManager(dataPath);

    expect(manager.isPaused('configured-dlq')).toBe(true);
    expect(manager.getDlqConfig('configured-dlq')).toEqual({
      autoRetry: true,
      autoRetryInterval: 12_345,
      maxAge: null,
      maxAutoRetries: 9,
      maxEntries: 321,
    });
  });

  test('schema v21 databases migrate before persisting DLQ configuration', () => {
    const dataPath = createDatabasePath('dlq-config-migration');
    const legacy = new Database(dataPath, { create: true });
    legacy.run(SCHEMA.replace(',\n    dlq_config BLOB', ''));
    legacy.run(MIGRATION_TABLE);
    legacy.run('INSERT INTO migrations (version, applied_at) VALUES (21, ?)', Date.now());
    legacy.close();

    let manager = openManager(dataPath);
    manager.setDlqConfig('migrated-dlq', { autoRetry: true, maxEntries: 456 });
    closeManager(manager);
    manager = openManager(dataPath);

    expect(manager.getDlqConfig('migrated-dlq').autoRetry).toBe(true);
    expect(manager.getDlqConfig('migrated-dlq').maxEntries).toBe(456);

    const migrated = new Database(dataPath, { readonly: true });
    const columns = migrated
      .query<{ name: string }, []>('PRAGMA table_info(queue_state)')
      .all()
      .map((column) => column.name);
    migrated.close();
    expect(columns).toContain('dlq_config');
  });

  test('the public embedded Queue setter uses the durable configuration path', () => {
    const dataPath = createDatabasePath('public-dlq-config');
    let queue = new Queue('public-dlq-config', { dataPath, embedded: true });
    queue.setDlqConfig({ autoRetry: true, maxAutoRetries: 7 });
    shutdownManager();

    queue = new Queue('public-dlq-config', { dataPath, embedded: true });
    expect(queue.getDlqConfig().autoRetry).toBe(true);
    expect(queue.getDlqConfig().maxAutoRetries).toBe(7);
  });
});
