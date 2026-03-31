/**
 * S3 Backup Bug Reproduction Tests
 *
 * Each test reproduces a specific bug found during analysis.
 * Tests are written FIRST (failing), then code is fixed to make them pass.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { unlink } from 'fs/promises';
import { validateConfig, DEFAULTS } from '../src/infrastructure/backup/s3BackupConfig';
import type { S3BackupConfig, BackupMetadata } from '../src/infrastructure/backup/s3BackupConfig';
import {
  performBackup,
  listBackups,
  restoreBackup,
  cleanupOldBackups,
} from '../src/infrastructure/backup/s3BackupOperations';
import type { S3Client } from 'bun';

// --- Mock S3 Client ---

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
    async list(opts: { prefix?: string; maxKeys?: number }) {
      const contents: { key: string; size: number; lastModified: Date }[] = [];
      for (const [k, v] of storage) {
        if (opts.prefix && !k.startsWith(opts.prefix)) continue;
        contents.push({ key: k, size: v.byteLength, lastModified: new Date() });
      }
      return { contents: opts.maxKeys ? contents.slice(0, opts.maxKeys) : contents };
    },
    _storage: storage,
  };

  return client as unknown as S3Client & { _storage: Map<string, Uint8Array> };
}

// --- Helpers ---

const TMP = Bun.env.TMPDIR ?? '/tmp';
let tempFiles: string[] = [];

function tmpPath(name: string): string {
  const p = `${TMP}/bunqueue-bug-${name}-${Date.now()}-${Math.random().toString(36).slice(2)}.db`;
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
// BUG 1: validateConfig accepts retention=0 (deletes all backups)
// =============================================================================
describe('BUG: validateConfig accepts dangerous retention values', () => {
  test('rejects retention of 0', () => {
    const r = validateConfig(cfg({ retention: 0 }));
    expect(r.valid).toBe(false);
    expect(r.errors.some((e) => e.toLowerCase().includes('retention'))).toBe(true);
  });

  test('rejects negative retention', () => {
    const r = validateConfig(cfg({ retention: -5 }));
    expect(r.valid).toBe(false);
    expect(r.errors.some((e) => e.toLowerCase().includes('retention'))).toBe(true);
  });
});

// =============================================================================
// BUG 2: validateConfig accepts intervalMs=0 (CPU spin)
// =============================================================================
describe('BUG: validateConfig accepts dangerous interval values', () => {
  test('rejects intervalMs of 0', () => {
    const r = validateConfig(cfg({ intervalMs: 0 }));
    expect(r.valid).toBe(false);
    expect(r.errors.some((e) => e.toLowerCase().includes('interval'))).toBe(true);
  });

  test('rejects negative intervalMs', () => {
    const r = validateConfig(cfg({ intervalMs: -1000 }));
    expect(r.valid).toBe(false);
    expect(r.errors.some((e) => e.toLowerCase().includes('interval'))).toBe(true);
  });

  test('rejects intervalMs below minimum (60 seconds)', () => {
    const r = validateConfig(cfg({ intervalMs: 1000 }));
    expect(r.valid).toBe(false);
    expect(r.errors.some((e) => e.toLowerCase().includes('interval'))).toBe(true);
  });
});

// =============================================================================
// BUG 3: restoreBackup accepts invalid data without integrity check
// =============================================================================
describe('BUG: restore accepts invalid SQLite data', () => {
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

  test('rejects restore when data is not a valid SQLite database (no metadata)', async () => {
    // Upload garbage data as a "backup" with no metadata
    const key = 'backups/fake-backup.db';
    const garbage = Bun.gzipSync(new TextEncoder().encode('this is not a sqlite database'));
    await (client.file(key) as any).write(garbage);

    const restorePath = tmpPath('restore-invalid');
    const result = await restoreBackup(key, cfg({ databasePath: restorePath }), client);

    // BUG: currently succeeds with garbage data when no metadata exists
    expect(result.success).toBe(false);
    expect(result.error).toMatch(/invalid|corrupt|not.*sqlite/i);
  });
});

// =============================================================================
// BUG 4: cleanupOldBackups with retention=0 deletes everything
// =============================================================================
describe('BUG: cleanup with retention=0 deletes all backups', () => {
  let client: ReturnType<typeof createMockS3Client>;
  let dbPath: string;

  beforeEach(async () => {
    client = createMockS3Client();
    dbPath = tmpPath('cleanup-bug');
    const { Database } = await import('bun:sqlite');
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

  test('retention=0 should keep at least 1 backup', async () => {
    const c = cfg({ databasePath: dbPath, retention: 0 });
    await performBackup(c, client);
    await Bun.sleep(5);
    await performBackup(c, client);

    await cleanupOldBackups(c, client);

    // BUG: retention=0 causes ALL backups to be deleted
    const remaining = await listBackups(c, client);
    expect(remaining.length).toBeGreaterThanOrEqual(1);
  });
});
