import { describe, expect, test } from 'bun:test';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { resolveServerConfig } from '../src/config';
import {
  backupStartupError,
  createServerQueueManager,
  shutdownServerQueueManager,
  storageStartupError,
} from '../src/infrastructure/server/storageManager';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;

describe('PostgreSQL server configuration', () => {
  test('does not interpolate a raw database password into broker URLs in Compose', async () => {
    const source = await Bun.file(
      new URL('../docker-compose.postgres.yml', import.meta.url)
    ).text();
    const urlLines = source
      .split('\n')
      .filter((line) => line.trimStart().startsWith('BUNQUEUE_POSTGRES_URL:'));
    expect(urlLines).toHaveLength(2);
    expect(urlLines.every((line) => !line.includes('POSTGRES_PASSWORD'))).toBe(true);
    for (const line of urlLines) {
      const fallback = line.match(/\$\{BUNQUEUE_POSTGRES_URL:-([^}]+)\}/)?.[1];
      expect(fallback).toBeDefined();
      expect(() => new URL(fallback!)).not.toThrow();
    }
  });

  test('keeps SQLite as the inferred backend when only dataPath is configured', () => {
    const config = resolveServerConfig({ storage: { dataPath: '/tmp/bunqueue.sqlite' } });
    expect(config.storageDriver).toBe('sqlite');
    expect(config.dataPath).toBe('/tmp/bunqueue.sqlite');
    expect(config.postgresUrl).toBeUndefined();
    expect(storageStartupError(config)).toBeNull();
  });

  test('selects PostgreSQL only from explicit PostgreSQL configuration', () => {
    const config = resolveServerConfig({
      storage: {
        driver: 'postgres',
        url: 'postgres://bunqueue:test@db:5432/bunqueue',
        namespace: 'production',
        brokerId: 'broker-a',
        poolSize: 20,
        leaseDurationMs: 45_000,
        pollIntervalMs: 50,
        statementTimeoutMs: 20_000,
        lockTimeoutMs: 2_000,
        idleTransactionTimeoutMs: 25_000,
        maxConcurrentOperations: 24,
        maxQueuedOperations: 96,
        maxSnapshotJobs: 75_000,
        maxSnapshotPayloadBytes: 134_217_728,
      },
    });
    expect(config).toMatchObject({
      storageDriver: 'postgres',
      postgresUrl: 'postgres://bunqueue:test@db:5432/bunqueue',
      postgresNamespace: 'production',
      postgresBrokerId: 'broker-a',
      postgresPoolSize: 20,
      postgresLeaseDurationMs: 45_000,
      postgresPollIntervalMs: 50,
      postgresStatementTimeoutMs: 20_000,
      postgresLockTimeoutMs: 2_000,
      postgresIdleTransactionTimeoutMs: 25_000,
      postgresMaxConcurrentOperations: 24,
      postgresMaxQueuedOperations: 96,
      postgresMaxSnapshotJobs: 75_000,
      postgresMaxSnapshotPayloadBytes: 134_217_728,
    });
    expect(storageStartupError(config)).toBeNull();
  });

  test('rejects ambiguous SQLite/PostgreSQL configuration and SQLite-only backups', () => {
    const ambiguous = resolveServerConfig({
      storage: {
        driver: 'postgres',
        url: 'postgres://bunqueue:test@db:5432/bunqueue',
        dataPath: '/tmp/also-sqlite.db',
      },
    });
    expect(storageStartupError(ambiguous)).toContain('cannot be combined');
    expect(backupStartupError({ ...ambiguous, s3BackupEnabled: true })).toContain(
      'requires persistent SQLite'
    );
  });

  test.skipIf(!postgresUrl)(
    'initializes and shuts down the selected PostgreSQL server manager',
    async () => {
      const config = resolveServerConfig({
        storage: {
          driver: 'postgres',
          url: postgresUrl!,
          namespace: `test-bootstrap-${Date.now()}-${crypto.randomUUID()}`,
          brokerId: 'bootstrap-broker',
        },
      });
      const manager = await createServerQueueManager(config);
      try {
        expect(manager).toBeInstanceOf(PostgresQueueManager);
        expect(manager.getStorageStatus()).toEqual({ diskFull: false, error: null, since: null });
      } finally {
        await shutdownServerQueueManager(manager);
      }
    }
  );
});
