/**
 * S3 Backup Bug Reproduction Tests - Round 2
 *
 * BUG 5: maxKeys:100 hardcoded - listBackups misses backups beyond 100
 * BUG 6: WAL file not checkpointed before backup - data loss risk
 * BUG 7: gzipSync blocks the event loop - should use async compression
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Database } from 'bun:sqlite';
import { unlink } from 'fs/promises';
import type { S3BackupConfig } from '../src/infrastructure/backup/s3BackupConfig';
import { DEFAULTS } from '../src/infrastructure/backup/s3BackupConfig';
import {
  performBackup,
  listBackups,
  restoreBackup,
} from '../src/infrastructure/backup/s3BackupOperations';
import type { S3Client } from 'bun';

// --- Mock S3 Client with maxKeys enforcement ---

function createMockS3Client() {
  const storage = new Map<string, Uint8Array>();

  const client = {
    file(key: string) {
      return {
        async write(data: Uint8Array | string, _opts?: { type?: string }) {
          storage.set(
            key,
            typeof data === 'string' ? new TextEncoder().encode(data) : new Uint8Array(data)
          );
        },
        async exists() {
          return storage.has(key);
        },
        async arrayBuffer() {
          const d = storage.get(key);
          if (!d) throw new Error(`Not found: ${key}`);
          return d.buffer.slice(d.byteOffset, d.byteOffset + d.byteLength);
        },
        async json() {
          const d = storage.get(key);
          if (!d) throw new Error(`Not found: ${key}`);
          return JSON.parse(new TextDecoder().decode(d));
        },
      };
    },
    async delete(key: string) {
      storage.delete(key);
    },
    async list(opts: { prefix?: string; maxKeys?: number; continuationToken?: string }) {
      const all: { key: string; size: number; lastModified: Date }[] = [];
      for (const [k, v] of storage) {
        if (opts.prefix && !k.startsWith(opts.prefix)) continue;
        all.push({ key: k, size: v.byteLength, lastModified: new Date() });
      }
      const limit = opts.maxKeys ?? 1000;
      const startIndex = opts.continuationToken ? parseInt(opts.continuationToken, 10) : 0;
      const contents = all.slice(startIndex, startIndex + limit);
      const isTruncated = startIndex + limit < all.length;
      return {
        contents,
        isTruncated,
        nextContinuationToken: isTruncated ? String(startIndex + limit) : undefined,
      };
    },
    _storage: storage,
  };

  return client as unknown as S3Client & { _storage: Map<string, Uint8Array> };
}

// --- Helpers ---

const TMP = Bun.env.TMPDIR ?? '/tmp';
let tempFiles: string[] = [];

function tmpPath(name: string): string {
  const p = `${TMP}/bunqueue-bug2-${name}-${Date.now()}-${Math.random().toString(36).slice(2)}.db`;
  tempFiles.push(p);
  return p;
}

function cfg(overrides: Partial<S3BackupConfig> = {}): S3BackupConfig {
  return {
    enabled: true,
    accessKeyId: 'test-key',
    secretAccessKey: 'test-secret',
    bucket: 'test-bucket',
    region: 'us-east-1',
    intervalMs: DEFAULTS.intervalMs,
    retention: DEFAULTS.retention,
    prefix: DEFAULTS.prefix,
    databasePath: '/tmp/test.db',
    ...overrides,
  };
}

// =============================================================================
// BUG 5: maxKeys:100 prevents listing all backups
// =============================================================================
describe('BUG: listBackups limited to 100 items', () => {
  let client: ReturnType<typeof createMockS3Client>;

  beforeEach(() => {
    client = createMockS3Client();
  });

  test('lists more than 100 backups via pagination', async () => {
    // Seed 120 backup files directly into mock storage
    for (let i = 0; i < 120; i++) {
      const key = `backups/bunqueue-2024-01-01T00-00-${String(i).padStart(3, '0')}.db`;
      await (client.file(key) as any).write(new Uint8Array([1, 2, 3]));
    }

    const backups = await listBackups(cfg(), client);

    // BUG: only returns 100 due to hardcoded maxKeys:100
    expect(backups.length).toBe(120);
  });
});

// =============================================================================
// BUG 6: WAL data not included in backup
// =============================================================================
describe('BUG: backup misses WAL-pending data', () => {
  let client: ReturnType<typeof createMockS3Client>;
  let dbPath: string;

  beforeEach(() => {
    client = createMockS3Client();
    dbPath = tmpPath('wal-bug');
  });

  afterEach(async () => {
    for (const f of tempFiles) {
      try { await unlink(f); } catch { /* ok */ }
      try { await unlink(f + '-wal'); } catch { /* ok */ }
      try { await unlink(f + '-shm'); } catch { /* ok */ }
    }
    tempFiles = [];
  });

  test('backup includes data written in WAL mode', async () => {
    // Create DB with WAL mode and disable auto-checkpoint
    const db = new Database(dbPath);
    db.exec('PRAGMA journal_mode=WAL');
    db.exec('PRAGMA wal_autocheckpoint=0');
    db.exec('CREATE TABLE wal_test (id INTEGER PRIMARY KEY, data TEXT)');

    // Insert data - goes to WAL only (no auto-checkpoint)
    for (let i = 0; i < 50; i++) {
      db.run('INSERT INTO wal_test VALUES (?, ?)', [i, `row-${i}`]);
    }

    // Backup while DB is open (WAL has uncheckpointed data)
    const result = await performBackup(cfg({ databasePath: dbPath }), client);
    db.close();
    expect(result.success).toBe(true);

    // Restore to new path
    const restorePath = tmpPath('restore-wal');
    const restoreResult = await restoreBackup(result.key!, cfg({ databasePath: restorePath }), client);
    expect(restoreResult.success).toBe(true);

    // BUG: restored DB should have all 50 rows, but WAL data is lost
    const restored = new Database(restorePath);
    const count = restored.query('SELECT COUNT(*) as c FROM wal_test').get() as { c: number };
    restored.close();

    expect(count.c).toBe(50);
  });
});

// =============================================================================
// BUG 7: gzipSync blocks event loop - should use async compression
// =============================================================================
describe('BUG: compression should be non-blocking', () => {
  let client: ReturnType<typeof createMockS3Client>;
  let dbPath: string;

  beforeEach(() => {
    client = createMockS3Client();
    dbPath = tmpPath('async-gz');
    const db = new Database(dbPath);
    db.run('CREATE TABLE t (id INTEGER)');
    db.close();
  });

  afterEach(async () => {
    for (const f of tempFiles) {
      try { await unlink(f); } catch { /* ok */ }
    }
    tempFiles = [];
  });

  test('performBackup does not use gzipSync (non-blocking)', async () => {
    // Patch gzipSync to detect if it's called
    const originalGzipSync = Bun.gzipSync;
    let syncCalled = false;
    (Bun as any).gzipSync = (...args: any[]) => {
      syncCalled = true;
      return originalGzipSync(...args);
    };

    try {
      const result = await performBackup(cfg({ databasePath: dbPath }), client);
      expect(result.success).toBe(true);

      // BUG: gzipSync is still being called
      expect(syncCalled).toBe(false);
    } finally {
      (Bun as any).gzipSync = originalGzipSync;
    }
  });
});
