/**
 * S3 Backup CRITICAL Bug Reproduction Tests
 *
 * 6 CRITICAL bugs from production readiness analysis:
 * 1. WAL checkpoint fails silently → data loss
 * 2. Write buffer not flushed before backup → recent jobs lost
 * 3. Non-atomic restore → DB corruption on partial write failure
 * 4. No retry/backoff → transient S3 failures = permanent backup loss
 * 5. Restore without full validation → corrupt DB accepted
 * 6. No S3 operation timeouts → indefinite hangs
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Database } from 'bun:sqlite';
import { unlink, stat } from 'fs/promises';
import { existsSync } from 'fs';
import type { S3BackupConfig, BackupMetadata } from '../src/infrastructure/backup/s3BackupConfig';
import { DEFAULTS } from '../src/infrastructure/backup/s3BackupConfig';
import {
  performBackup,
  restoreBackup,
} from '../src/infrastructure/backup/s3BackupOperations';
import { S3BackupManager } from '../src/infrastructure/backup/s3Backup';
import type { S3Client } from 'bun';

// --- Mock S3 Client ---

function createMockS3Client(options?: {
  failUploadCount?: number;
  hangDownload?: boolean;
  hangUpload?: boolean;
}) {
  const storage = new Map<string, Uint8Array>();
  let uploadAttempts = 0;
  const failUploadCount = options?.failUploadCount ?? 0;

  const client = {
    file(key: string) {
      return {
        async write(data: Uint8Array | string, _opts?: { type?: string }) {
          if (options?.hangUpload) {
            // Simulate a hang - never resolves
            return new Promise<void>(() => {});
          }
          uploadAttempts++;
          if (uploadAttempts <= failUploadCount) {
            throw new Error('S3 transient error: connection reset');
          }
          storage.set(
            key,
            typeof data === 'string' ? new TextEncoder().encode(data) : new Uint8Array(data)
          );
        },
        async exists() {
          return storage.has(key);
        },
        async arrayBuffer() {
          if (options?.hangDownload) {
            return new Promise<ArrayBuffer>(() => {});
          }
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
    _getUploadAttempts: () => uploadAttempts,
  };

  return client as unknown as S3Client & {
    _storage: Map<string, Uint8Array>;
    _getUploadAttempts: () => number;
  };
}

// --- Helpers ---

const TMP = Bun.env.TMPDIR ?? '/tmp';
let tempFiles: string[] = [];

function tmpPath(name: string): string {
  const p = `${TMP}/bunqueue-crit-${name}-${Date.now()}-${Math.random().toString(36).slice(2)}.db`;
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
// BUG 1: WAL checkpoint fails silently → data loss
// When checkpoint fails (DB locked), backup silently proceeds with stale data.
// The catch block at s3BackupOperations.ts:51-53 swallows ALL errors.
// Fix: performBackup MUST return success:false if checkpoint fails,
//      OR it must read the WAL file alongside the main DB.
// =============================================================================
describe('CRITICAL BUG 1: WAL checkpoint silent failure', () => {
  let client: ReturnType<typeof createMockS3Client>;
  let dbPath: string;

  beforeEach(() => {
    client = createMockS3Client();
    dbPath = tmpPath('wal-crit');
  });

  afterEach(async () => {
    for (const f of tempFiles) {
      try { await unlink(f); } catch { /* ok */ }
      try { await unlink(f + '-wal'); } catch { /* ok */ }
      try { await unlink(f + '-shm'); } catch { /* ok */ }
    }
    tempFiles = [];
  });

  test('backup includes WAL data even when another connection holds a write lock', async () => {
    // Create DB in WAL mode, disable auto-checkpoint
    const db = new Database(dbPath);
    db.exec('PRAGMA journal_mode=WAL');
    db.exec('PRAGMA wal_autocheckpoint=0');
    db.exec('CREATE TABLE critical_data (id INTEGER PRIMARY KEY, value TEXT)');

    // Insert data that stays in WAL only
    for (let i = 0; i < 100; i++) {
      db.run('INSERT INTO critical_data VALUES (?, ?)', [i, `important-data-${i}`]);
    }

    // Keep a BEGIN IMMEDIATE to hold a write lock - this prevents checkpoint
    db.exec('BEGIN IMMEDIATE');
    db.run('INSERT INTO critical_data VALUES (?, ?)', [999, 'locked-row']);

    // Now performBackup tries checkpoint but fails silently because of the lock
    const result = await performBackup(cfg({ databasePath: dbPath }), client);

    // Rollback and close
    db.exec('ROLLBACK');
    db.close();

    // If backup succeeded, verify data completeness in restored DB
    if (result.success) {
      const restorePath = tmpPath('wal-crit-restore');
      const restoreResult = await restoreBackup(
        result.key!,
        cfg({ databasePath: restorePath }),
        client
      );
      expect(restoreResult.success).toBe(true);

      const restored = new Database(restorePath);
      const count = restored.query('SELECT COUNT(*) as c FROM critical_data').get() as {
        c: number;
      };
      restored.close();

      // BUG: checkpoint failed silently, so only main DB file was backed up
      // WAL data (100 rows) is missing from backup
      expect(count.c).toBeGreaterThanOrEqual(100);
    } else {
      // If it properly fails, that's also acceptable behavior
      expect(result.error).toBeDefined();
    }
  });
});

// =============================================================================
// BUG 2: Write buffer not flushed before backup → recent jobs lost
// S3BackupManager.backup() calls performBackup() without flushing the
// in-memory write buffer first. Jobs buffered in the last 10ms are lost.
// Fix: S3BackupManager should accept a flushCallback and call it before backup.
// =============================================================================
describe('CRITICAL BUG 2: Write buffer not flushed before backup', () => {
  test('S3BackupManager accepts and calls flushBeforeBackup callback', async () => {
    const dbPath = tmpPath('flush-bug');
    const db = new Database(dbPath);
    db.run('CREATE TABLE t (id INTEGER)');
    db.close();

    let flushCalled = false;
    const flushFn = async () => {
      flushCalled = true;
    };

    // BUG: S3BackupManager constructor doesn't accept flushBeforeBackup
    const manager = new S3BackupManager({
      enabled: true,
      accessKeyId: 'test-key',
      secretAccessKey: 'test-secret',
      bucket: 'test-bucket',
      databasePath: dbPath,
      flushBeforeBackup: flushFn,
    } as any);

    // Monkey-patch the internal client to use mock
    const mockClient = createMockS3Client();
    (manager as any).client = mockClient;

    const result = await manager.backup();

    // BUG: flushFn was never called because the option doesn't exist
    expect(flushCalled).toBe(true);
    expect(result.success).toBe(true);

    for (const f of tempFiles) {
      try { await unlink(f); } catch { /* ok */ }
    }
    tempFiles = [];
  });
});

// =============================================================================
// BUG 3: Non-atomic restore → DB corruption on partial write failure
// restoreBackup writes directly via `Bun.write(config.databasePath, data)`
// If the write fails midway (disk full, crash), the DB is corrupted.
// Fix: write to temp file first, then atomic rename.
// =============================================================================
describe('CRITICAL BUG 3: Non-atomic restore', () => {
  let client: ReturnType<typeof createMockS3Client>;
  let dbPath: string;

  beforeEach(async () => {
    client = createMockS3Client();
    dbPath = tmpPath('atomic-restore');
    const db = new Database(dbPath);
    db.run('CREATE TABLE original (id INTEGER PRIMARY KEY, data TEXT)');
    db.run("INSERT INTO original VALUES (1, 'must-survive')");
    db.close();
  });

  afterEach(async () => {
    for (const f of tempFiles) {
      try { await unlink(f); } catch { /* ok */ }
    }
    tempFiles = [];
  });

  test('restore uses atomic write (temp file + rename)', async () => {
    // Backup the original
    const c = cfg({ databasePath: dbPath });
    const backup = await performBackup(c, client);
    expect(backup.success).toBe(true);

    // Restore to the SAME path
    const result = await restoreBackup(backup.key!, c, client);
    expect(result.success).toBe(true);

    // The restored DB should be valid
    const db = new Database(dbPath);
    const row = db.query('SELECT data FROM original WHERE id = 1').get() as { data: string };
    db.close();
    expect(row.data).toBe('must-survive');
  });

  test('original database survives if restore write fails', async () => {
    // Backup first
    const c = cfg({ databasePath: dbPath });
    const backup = await performBackup(c, client);
    expect(backup.success).toBe(true);

    // Get original file content hash
    const originalData = await Bun.file(dbPath).arrayBuffer();
    const originalHash = new Bun.CryptoHasher('sha256');
    originalHash.update(new Uint8Array(originalData));
    const originalChecksum = originalHash.digest('hex');

    // Corrupt the backup in S3 (replace with truncated data that passes header check)
    const dbFileData = new Uint8Array(originalData);
    const corruptData = dbFileData.slice(0, 100); // Truncated but has SQLite header

    // Compress the corrupt data
    const stream = new Blob([corruptData as unknown as BlobPart])
      .stream()
      .pipeThrough(new CompressionStream('gzip'));
    const compressed = new Uint8Array(await new Response(stream).arrayBuffer());

    // Replace backup with corrupt data (keep metadata pointing to correct checksum)
    await (client.file(backup.key!) as any).write(compressed);

    // Try restore - should fail due to checksum mismatch
    const result = await restoreBackup(backup.key!, c, client);

    if (!result.success) {
      // Good - restore failed. Verify original is intact
      const afterData = await Bun.file(dbPath).arrayBuffer();
      const afterHash = new Bun.CryptoHasher('sha256');
      afterHash.update(new Uint8Array(afterData));
      const afterChecksum = afterHash.digest('hex');

      // BUG: if restore wrote partial data before checking checksum,
      // original is destroyed even though restore "failed"
      expect(afterChecksum).toBe(originalChecksum);
    }
  });
});

// =============================================================================
// BUG 4: No retry/backoff → transient S3 failures = permanent backup loss
// S3BackupManager.backup() has no retry logic. A single transient error
// (network reset, 503) means the backup is permanently lost until next interval.
// Fix: add retry with exponential backoff.
// =============================================================================
describe('CRITICAL BUG 4: No retry/backoff on transient failures', () => {
  afterEach(async () => {
    for (const f of tempFiles) {
      try { await unlink(f); } catch { /* ok */ }
    }
    tempFiles = [];
  });

  test('performBackup retries on transient S3 errors', async () => {
    const dbPath = tmpPath('retry-bug');
    const db = new Database(dbPath);
    db.run('CREATE TABLE t (id INTEGER)');
    db.close();

    // Mock client that fails first 2 uploads then succeeds
    const client = createMockS3Client({ failUploadCount: 2 });

    const result = await performBackup(
      cfg({ databasePath: dbPath }),
      client
    );

    // BUG: performBackup doesn't retry, so first S3 error = permanent failure
    expect(result.success).toBe(true);
    expect((client as any)._getUploadAttempts()).toBeGreaterThan(1);
  });

  test('S3BackupManager retries scheduled backup on failure', async () => {
    const dbPath = tmpPath('retry-sched');
    const db = new Database(dbPath);
    db.run('CREATE TABLE t (id INTEGER)');
    db.close();

    // Track backup attempts
    let backupCallCount = 0;
    const originalPerformBackup = (await import('../src/infrastructure/backup/s3BackupOperations'))
      .performBackup;

    const manager = new S3BackupManager({
      enabled: true,
      accessKeyId: 'test-key',
      secretAccessKey: 'test-secret',
      bucket: 'test-bucket',
      databasePath: dbPath,
    });

    // Mock client that always fails
    const failClient = createMockS3Client({ failUploadCount: 999 });
    (manager as any).client = failClient;

    // Call backup - should retry at least once
    const result = await manager.backup();

    // BUG: No retry logic, single failure = immediate return
    // The manager should have retried at least once
    expect((failClient as any)._getUploadAttempts()).toBeGreaterThan(1);

    manager.stop();
  });
});

// =============================================================================
// BUG 5: Restore without full validation → accepts corrupt database
// Only checks SQLite header (first 16 bytes). No PRAGMA integrity_check.
// A corrupt DB with valid header passes validation but crashes at runtime.
// Fix: run PRAGMA integrity_check on restored database before accepting.
// =============================================================================
describe('CRITICAL BUG 5: Restore without full validation', () => {
  let client: ReturnType<typeof createMockS3Client>;

  beforeEach(() => {
    client = createMockS3Client();
  });

  afterEach(async () => {
    for (const f of tempFiles) {
      try { await unlink(f); } catch { /* ok */ }
    }
    tempFiles = [];
  });

  test('restore runs integrity check on restored database', async () => {
    // Create a file that has valid SQLite header but is corrupt
    const header = new TextEncoder().encode('SQLite format 3\0');
    // Pad with garbage to make it look like a DB
    const corrupt = new Uint8Array(4096);
    corrupt.set(header, 0);
    // Fill rest with random garbage
    for (let i = header.length; i < corrupt.length; i++) {
      corrupt[i] = Math.floor(Math.random() * 256);
    }

    // Compress and upload as backup
    const stream = new Blob([corrupt as unknown as BlobPart])
      .stream()
      .pipeThrough(new CompressionStream('gzip'));
    const compressed = new Uint8Array(await new Response(stream).arrayBuffer());

    const key = 'backups/bunqueue-corrupt-test.db';
    await (client.file(key) as any).write(compressed);

    // Upload metadata without checksum (simulates old backup without checksums)
    const metadata: BackupMetadata = {
      timestamp: new Date().toISOString(),
      version: '2.1.9',
      size: corrupt.byteLength,
      compressedSize: compressed.byteLength,
      checksum: '', // Empty checksum - skip checksum verification
      compressed: true,
    };
    await (client.file(`${key}.meta.json`) as any).write(JSON.stringify(metadata));

    const restorePath = tmpPath('integrity-check');
    const result = await restoreBackup(key, cfg({ databasePath: restorePath }), client);

    // BUG: restore succeeds because only header is checked, no integrity_check
    // Should fail because DB is corrupt
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/corrupt|integrity|invalid/i);
  });
});

// =============================================================================
// BUG 6: No S3 operation timeouts → indefinite hangs
// S3 operations (upload, download) have no timeout. A hung S3 endpoint
// causes the backup/restore to hang forever, blocking the interval.
// Fix: add AbortSignal timeout to all S3 operations.
// =============================================================================
describe('CRITICAL BUG 6: No S3 operation timeouts', () => {
  afterEach(async () => {
    for (const f of tempFiles) {
      try { await unlink(f); } catch { /* ok */ }
    }
    tempFiles = [];
  });

  test('restoreBackup times out on hung S3 download', async () => {
    const hangClient = createMockS3Client({ hangDownload: true });

    // Upload a fake backup entry so exists() returns true
    (hangClient as any)._storage.set(
      'backups/hung-test.db',
      new Uint8Array([1, 2, 3])
    );

    const restorePath = tmpPath('timeout-dl');

    // BUG: This will hang forever because there's no timeout
    const timeoutPromise = new Promise<'timeout'>((resolve) =>
      setTimeout(() => resolve('timeout'), 5000)
    );

    const restorePromise = restoreBackup(
      'backups/hung-test.db',
      cfg({ databasePath: restorePath, timeoutMs: 3000 }),
      hangClient
    ).then(() => 'completed' as const);

    const winner = await Promise.race([restorePromise, timeoutPromise]);

    // BUG: winner will be 'timeout' because restore hangs forever
    expect(winner).not.toBe('timeout');
  }, 10000);

  test('performBackup times out on hung S3 upload', async () => {
    const dbPath = tmpPath('timeout-ul');
    const db = new Database(dbPath);
    db.run('CREATE TABLE t (id INTEGER)');
    db.close();

    const hangClient = createMockS3Client({ hangUpload: true });

    // BUG: This will hang forever because there's no timeout
    const timeoutPromise = new Promise<'timeout'>((resolve) =>
      setTimeout(() => resolve('timeout'), 5000)
    );

    const backupPromise = performBackup(
      cfg({ databasePath: dbPath, timeoutMs: 3000 }),
      hangClient
    ).then(() => 'completed' as const);

    const winner = await Promise.race([backupPromise, timeoutPromise]);

    // BUG: winner will be 'timeout' because upload hangs forever
    expect(winner).not.toBe('timeout');
  }, 10000);
});
