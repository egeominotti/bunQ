/**
 * S3 Backup Tests
 * Config validation, backup/restore operations, cleanup, manager lifecycle
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Database } from 'bun:sqlite';
import { unlink } from 'fs/promises';
import { validateConfig, DEFAULTS } from '../src/infrastructure/backup/s3BackupConfig';
import type { S3BackupConfig, BackupMetadata } from '../src/infrastructure/backup/s3BackupConfig';
import {
  performBackup,
  listBackups,
  restoreBackup,
  cleanupOldBackups,
} from '../src/infrastructure/backup/s3BackupOperations';
import { S3BackupManager } from '../src/infrastructure/backup/s3Backup';
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
  const p = `${TMP}/bunqueue-test-${name}-${Date.now()}-${Math.random().toString(36).slice(2)}.db`;
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

// --- Tests ---

describe('S3 Backup Config', () => {
  test('validates correct config', () => {
    const r = validateConfig(cfg());
    expect(r.valid).toBe(true);
    expect(r.errors).toHaveLength(0);
  });

  test('rejects missing accessKeyId', () => {
    const r = validateConfig(cfg({ accessKeyId: '' }));
    expect(r.valid).toBe(false);
    expect(r.errors).toContain('S3_ACCESS_KEY_ID is required');
  });

  test('rejects missing secretAccessKey', () => {
    const r = validateConfig(cfg({ secretAccessKey: '' }));
    expect(r.valid).toBe(false);
    expect(r.errors).toContain('S3_SECRET_ACCESS_KEY is required');
  });

  test('rejects missing bucket', () => {
    const r = validateConfig(cfg({ bucket: '' }));
    expect(r.valid).toBe(false);
    expect(r.errors).toContain('S3_BUCKET is required');
  });

  test('rejects missing databasePath', () => {
    const r = validateConfig(cfg({ databasePath: '' }));
    expect(r.valid).toBe(false);
    expect(r.errors).toContain('Database path is required');
  });

  test('reports all errors at once', () => {
    const r = validateConfig(cfg({ accessKeyId: '', secretAccessKey: '', bucket: '', databasePath: '' }));
    expect(r.valid).toBe(false);
    expect(r.errors).toHaveLength(4);
  });

  test('defaults are correct', () => {
    expect(DEFAULTS.intervalMs).toBe(6 * 60 * 60 * 1000);
    expect(DEFAULTS.retention).toBe(7);
    expect(DEFAULTS.prefix).toBe('backups/');
    expect(DEFAULTS.region).toBe('us-east-1');
  });
});

describe('performBackup', () => {
  let client: ReturnType<typeof createMockS3Client>;
  let dbPath: string;

  beforeEach(async () => {
    client = createMockS3Client();
    dbPath = tmpPath('backup');
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

  test('creates backup successfully', async () => {
    const result = await performBackup(cfg({ databasePath: dbPath }), client);
    expect(result.success).toBe(true);
    expect(result.key).toMatch(/^backups\/bunqueue-.*\.db$/);
    expect(result.size).toBeGreaterThan(0);
    expect(result.duration).toBeGreaterThanOrEqual(0);
  });

  test('uploads compressed data + metadata to S3', async () => {
    await performBackup(cfg({ databasePath: dbPath }), client);
    expect(client._storage.size).toBe(2);

    const metaKey = [...client._storage.keys()].find((k) => k.endsWith('.meta.json'))!;
    expect(metaKey).toBeDefined();

    const meta = (await (client.file(metaKey) as any).json()) as BackupMetadata;
    expect(meta.timestamp).toBeDefined();
    expect(meta.version).toBeDefined();
    expect(meta.size).toBeGreaterThan(0);
    expect(meta.compressedSize).toBeGreaterThan(0);
    expect(meta.checksum).toMatch(/^[a-f0-9]{64}$/);
    expect(meta.compressed).toBe(true);
  });

  test('fails when database file missing', async () => {
    const result = await performBackup(cfg({ databasePath: '/nonexistent/path.db' }), client);
    expect(result.success).toBe(false);
    expect(result.error).toContain('Database file not found');
  });

  test('uses configured prefix', async () => {
    const result = await performBackup(cfg({ databasePath: dbPath, prefix: 'custom/' }), client);
    expect(result.success).toBe(true);
    expect(result.key).toMatch(/^custom\/bunqueue-/);
  });
});

describe('listBackups', () => {
  let client: ReturnType<typeof createMockS3Client>;
  let dbPath: string;

  beforeEach(async () => {
    client = createMockS3Client();
    dbPath = tmpPath('list');
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

  test('returns empty array when no backups', async () => {
    expect(await listBackups(cfg({ databasePath: dbPath }), client)).toHaveLength(0);
  });

  test('lists backups and filters .meta.json', async () => {
    const c = cfg({ databasePath: dbPath });
    await performBackup(c, client);
    await Bun.sleep(5);
    await performBackup(c, client);

    const backups = await listBackups(c, client);
    expect(backups).toHaveLength(2);
    for (const b of backups) {
      expect(b.key).toMatch(/\.db$/);
      expect(b.key).not.toContain('.meta.json');
      expect(b.size).toBeGreaterThan(0);
    }
  });
});

describe('restoreBackup', () => {
  let client: ReturnType<typeof createMockS3Client>;
  let dbPath: string;

  beforeEach(async () => {
    client = createMockS3Client();
    dbPath = tmpPath('restore-src');
    // Create a real SQLite database so restore validation passes
    const db = new Database(dbPath);
    db.run('CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)');
    db.run("INSERT INTO test VALUES (1, 'hello')");
    db.close();
  });

  afterEach(async () => {
    for (const f of tempFiles) {
      try { await unlink(f); } catch { /* ok */ }
    }
    tempFiles = [];
  });

  test('restores backup with correct content', async () => {
    const c = cfg({ databasePath: dbPath });
    const backup = await performBackup(c, client);
    expect(backup.success).toBe(true);

    const restorePath = tmpPath('restore-target');
    const result = await restoreBackup(backup.key!, cfg({ databasePath: restorePath }), client);
    expect(result.success).toBe(true);
    expect(result.size).toBeGreaterThan(0);

    // Verify restored database is valid and has same data
    const restoredDb = new Database(restorePath);
    const rows = restoredDb.query('SELECT value FROM test WHERE id = 1').get() as { value: string };
    restoredDb.close();
    expect(rows.value).toBe('hello');
  });

  test('fails when backup not found', async () => {
    const result = await restoreBackup('nonexistent.db', cfg({ databasePath: dbPath }), client);
    expect(result.success).toBe(false);
    expect(result.error).toContain('Backup not found');
  });

  test('detects checksum mismatch', async () => {
    const c = cfg({ databasePath: dbPath });
    const backup = await performBackup(c, client);

    // Corrupt metadata checksum
    const metaKey = `${backup.key}.meta.json`;
    const meta = (await (client.file(metaKey) as any).json()) as BackupMetadata;
    meta.checksum = 'aaaa'.repeat(16);
    await (client.file(metaKey) as any).write(JSON.stringify(meta));

    const restorePath = tmpPath('restore-corrupt');
    const result = await restoreBackup(backup.key!, cfg({ databasePath: restorePath }), client);
    expect(result.success).toBe(false);
    expect(result.error).toContain('checksum mismatch');
  });
});

describe('cleanupOldBackups', () => {
  let client: ReturnType<typeof createMockS3Client>;
  let dbPath: string;

  beforeEach(async () => {
    client = createMockS3Client();
    dbPath = tmpPath('cleanup');
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

  test('keeps backups within retention limit', async () => {
    const c = cfg({ databasePath: dbPath, retention: 5 });
    await performBackup(c, client);
    await Bun.sleep(5);
    await performBackup(c, client);
    await Bun.sleep(5);
    await performBackup(c, client);

    await cleanupOldBackups(c, client);
    expect(await listBackups(c, client)).toHaveLength(3);
  });

  test('deletes oldest backups exceeding retention', async () => {
    const c = cfg({ databasePath: dbPath, retention: 2 });
    for (let i = 0; i < 4; i++) {
      await performBackup(c, client);
      await Bun.sleep(5);
    }

    expect(await listBackups(c, client)).toHaveLength(4);
    await cleanupOldBackups(c, client);
    expect(await listBackups(c, client)).toHaveLength(2);
  });

  test('deletes metadata alongside backups', async () => {
    const c = cfg({ databasePath: dbPath, retention: 1 });
    await performBackup(c, client);
    await Bun.sleep(5);
    await performBackup(c, client);

    expect(client._storage.size).toBe(4); // 2 .db + 2 .meta.json
    await cleanupOldBackups(c, client);
    expect(client._storage.size).toBe(2); // 1 .db + 1 .meta.json
  });
});

describe('S3BackupManager', () => {
  test('getStatus returns correct info', () => {
    const m = new S3BackupManager({
      enabled: true,
      accessKeyId: 'k',
      secretAccessKey: 's',
      bucket: 'b',
      databasePath: '/tmp/test.db',
    });

    const s = m.getStatus();
    expect(s.enabled).toBe(true);
    expect(s.bucket).toBe('b');
    expect(s.intervalMs).toBe(DEFAULTS.intervalMs);
    expect(s.retention).toBe(DEFAULTS.retention);
    expect(s.isRunning).toBe(false);
  });

  test('validate detects invalid config', () => {
    const m = new S3BackupManager({ accessKeyId: '', databasePath: '/tmp/t.db' });
    const r = m.validate();
    expect(r.valid).toBe(false);
    expect(r.errors.length).toBeGreaterThan(0);
  });

  test('start does nothing when disabled', () => {
    const m = new S3BackupManager({ enabled: false, databasePath: '/tmp/t.db' });
    m.start();
    expect(m.getStatus().isRunning).toBe(false);
    m.stop();
  });

  test('stop is safe when never started', () => {
    const m = new S3BackupManager({ enabled: false, databasePath: '/tmp/t.db' });
    m.stop();
    expect(m.getStatus().isRunning).toBe(false);
  });
});
