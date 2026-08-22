import { afterEach, describe, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { unlinkSync } from 'node:fs';
import { processAutoRetry, type DlqContext } from '../src/application/dlqManager';
import { QueueManager } from '../src/application/queueManager';
import { getDlqRetryState } from '../src/domain/types/dlq';
import { SqliteStorage } from '../src/infrastructure/persistence/sqlite';
import { MIGRATION_TABLE, SCHEMA, SCHEMA_VERSION } from '../src/infrastructure/persistence/schema';
import { shardIndex } from '../src/shared/hash';

const managers: QueueManager[] = [];
const databasePaths: string[] = [];

afterEach(() => {
  for (const manager of managers.splice(0)) manager.shutdown();
  for (const path of databasePaths.splice(0)) {
    for (const suffix of ['', '-wal', '-shm']) {
      try {
        unlinkSync(path + suffix);
      } catch {
        // SQLite sidecars are optional.
      }
    }
  }
});

function createManager(dataPath?: string): QueueManager {
  const manager = new QueueManager(dataPath ? { dataPath } : undefined);
  managers.push(manager);
  if (dataPath && !databasePaths.includes(dataPath)) databasePaths.push(dataPath);
  return manager;
}

function shutdownManager(manager: QueueManager): void {
  manager.shutdown();
  const index = managers.indexOf(manager);
  if (index !== -1) managers.splice(index, 1);
}

function dlqContext(manager: QueueManager): DlqContext {
  const internals = manager as unknown as {
    storage: SqliteStorage | null;
  };

  return {
    shards: manager.getShards(),
    jobIndex: manager.getJobIndex(),
    jobResults: new Map(),
    dependencyResults: { releaseConsumer: () => {} },
    customIdMap: new Map(),
    jobLogs: new Map(),
    storage: internals.storage,
  };
}

async function failOnce(manager: QueueManager, queue: string, error: string): Promise<void> {
  const job = await manager.pull(queue, 0);
  expect(job).not.toBeNull();
  await manager.fail(job!.id, error);
}

describe('DLQ auto-retry state regression', () => {
  test('schema 29 upgrades add durable DLQ retry state', () => {
    const dataPath = `/tmp/bunqueue-repro-dlq-v29-${crypto.randomUUID()}.db`;
    databasePaths.push(dataPath);
    const legacy = new Database(dataPath, { create: true });
    legacy.run(
      SCHEMA.replace('    stacktrace BLOB,\n    dlq_retry_state BLOB', '    stacktrace BLOB')
    );
    legacy.run(MIGRATION_TABLE);
    legacy.run('INSERT INTO migrations(version, applied_at) VALUES (29, 0)');
    legacy.close();

    const storage = new SqliteStorage({ path: dataPath });
    storage.close();
    const migrated = new Database(dataPath, { readonly: true });
    try {
      const columns = migrated
        .query<{ name: string }, []>('PRAGMA table_info(jobs)')
        .all()
        .map((row) => row.name);
      const version = migrated
        .query<{ version: number }, []>('SELECT MAX(version) AS version FROM migrations')
        .get()?.version;
      expect(columns).toContain('dlq_retry_state');
      expect(version).toBe(SCHEMA_VERSION);
    } finally {
      migrated.close();
    }
  });

  test('preserves the retry chain and stops at maxAutoRetries', async () => {
    const manager = createManager();
    const queue = 'repro-dlq-auto-retry-chain';
    const shard = manager.getShards()[shardIndex(queue)];
    manager.setDlqConfig(queue, {
      autoRetry: true,
      autoRetryInterval: 60_000,
      maxAutoRetries: 2,
      maxAge: null,
    });

    await manager.push(queue, { data: { value: 1 }, maxAttempts: 1, backoff: 0 });
    await failOnce(manager, queue, 'initial failure');

    const initialEntry = shard.getDlqEntries(queue)[0];
    const firstEnteredAt = initialEntry.enteredAt;
    initialEntry.nextRetryAt = 0;

    expect(processAutoRetry(queue, dlqContext(manager))).toBe(1);
    expect(shard.getDlqCount(queue)).toBe(0);
    expect(manager.getStats().waiting).toBe(1);

    await failOnce(manager, queue, 'first auto-retry failure');
    const firstRetryEntry = shard.getDlqEntries(queue)[0];
    expect(firstRetryEntry.enteredAt).toBe(firstEnteredAt);
    expect(firstRetryEntry.retryCount).toBe(1);
    expect(firstRetryEntry.attempts).toHaveLength(2);
    expect(firstRetryEntry.attempts.map((attempt) => attempt.error)).toEqual([
      'initial failure',
      'first auto-retry failure',
    ]);

    firstRetryEntry.nextRetryAt = 0;
    expect(processAutoRetry(queue, dlqContext(manager))).toBe(1);
    await failOnce(manager, queue, 'second auto-retry failure');

    const exhaustedEntry = shard.getDlqEntries(queue)[0];
    expect(exhaustedEntry.retryCount).toBe(2);
    expect(exhaustedEntry.nextRetryAt).toBeNull();
    expect(exhaustedEntry.attempts).toHaveLength(3);

    // Even a corrupt/stale due timestamp must not bypass the retry ceiling.
    exhaustedEntry.nextRetryAt = 0;
    expect(processAutoRetry(queue, dlqContext(manager))).toBe(0);
    expect(shard.getDlqCount(queue)).toBe(1);
    expect(manager.getStats().dlq).toBe(1);
  });

  test('removes the persisted DLQ row while an auto-retried job is waiting', async () => {
    const dataPath = `/tmp/bunqueue-repro-dlq-auto-retry-${Date.now()}-${crypto.randomUUID()}.db`;
    const manager = createManager(dataPath);
    const context = dlqContext(manager);
    const storage = context.storage;
    expect(storage).not.toBeNull();

    const queue = 'repro-dlq-auto-retry-persistence';
    const shard = manager.getShards()[shardIndex(queue)];
    manager.setDlqConfig(queue, {
      autoRetry: true,
      autoRetryInterval: 60_000,
      maxAutoRetries: 2,
      maxAge: null,
    });

    const pushed = await manager.push(queue, {
      data: { durable: true },
      maxAttempts: 1,
      backoff: 0,
      durable: true,
    });
    await failOnce(manager, queue, 'initial durable failure');
    expect(storage!.loadDlq().get(queue)).toHaveLength(1);

    shard.getDlqEntries(queue)[0].nextRetryAt = 0;
    expect(processAutoRetry(queue, context)).toBe(1);
    expect(storage!.loadDlq().get(queue) ?? []).toHaveLength(0);
    expect(manager.getStats().waiting).toBe(1);

    const storedWaitingJob = storage!.getJob(pushed.id);
    expect(storedWaitingJob).not.toBeNull();
    expect(getDlqRetryState(storedWaitingJob!)?.retryCount).toBe(1);

    shutdownManager(manager);
    const restarted = createManager(dataPath);
    expect(restarted.getDlqConfig(queue).maxAutoRetries).toBe(2);
    await failOnce(restarted, queue, 'durable auto-retry failure');

    const restartedStorage = dlqContext(restarted).storage!;
    const persisted = restartedStorage.loadDlq().get(queue) ?? [];
    expect(persisted).toHaveLength(1);
    expect(persisted[0].retryCount).toBe(1);
    expect(persisted[0].attempts).toHaveLength(2);
    expect(restarted.getStats().dlq).toBe(1);
  });

  test('records every normal processing failure before the terminal DLQ move', async () => {
    const manager = createManager();
    const queue = 'repro-dlq-full-attempt-history';
    await manager.push(queue, { data: { value: 1 }, maxAttempts: 3, backoff: 0 });

    await failOnce(manager, queue, 'attempt one');
    await failOnce(manager, queue, 'attempt two');
    await failOnce(manager, queue, 'attempt three');

    const entry = manager.getShards()[shardIndex(queue)].getDlqEntries(queue)[0];
    expect(entry.attempts).toHaveLength(3);
    expect(entry.attempts.map((attempt) => attempt.attempt)).toEqual([1, 2, 3]);
    expect(entry.attempts.map((attempt) => attempt.error)).toEqual([
      'attempt one',
      'attempt two',
      'attempt three',
    ]);
    expect(manager.getStats().dlq).toBe(1);
  });

  test('normalizes maxEntries to a safe positive integer', () => {
    const manager = createManager();
    manager.setDlqConfig('repro-dlq-zero-cap', { maxEntries: 0 });
    expect(manager.getDlqConfig('repro-dlq-zero-cap').maxEntries).toBe(1);
  });

  test('maxEntries evicts terminal ownership from memory and SQLite', async () => {
    const dataPath = `/tmp/bunqueue-repro-dlq-cap-${Date.now()}-${crypto.randomUUID()}.db`;
    const manager = createManager(dataPath);
    const queue = 'repro-dlq-durable-cap';
    manager.setDlqConfig(queue, { maxEntries: 2, maxAge: null });

    const ids = [];
    for (let index = 0; index < 3; index++) {
      const job = await manager.push(queue, {
        data: { index },
        maxAttempts: 1,
        backoff: 0,
        durable: true,
      });
      ids.push(job.id);
      await failOnce(manager, queue, `failure ${index}`);
    }

    const context = dlqContext(manager);
    expect(manager.getStats().dlq).toBe(2);
    expect(context.storage!.loadDlq().get(queue)).toHaveLength(2);
    expect(await manager.getJobState(ids[0])).toBe('unknown');
    expect(await manager.getJobState(ids[1])).toBe('failed');
    expect(await manager.getJobState(ids[2])).toBe('failed');

    shutdownManager(manager);
    const restarted = createManager(dataPath);
    expect(restarted.getStats().dlq).toBe(2);
    expect(restarted.getDlq(queue).map((job) => job.id)).toEqual(ids.slice(1));
    expect(await restarted.getJobState(ids[0])).toBe('unknown');
  });
});
