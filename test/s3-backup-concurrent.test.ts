/**
 * S3 Backup Edge Cases & Concurrency Tests (Embedded Mode)
 * Tests: empty DB, file creation, sequential uniqueness, concurrent writes,
 * retention, missing paths, status, concurrent mutex, data completeness.
 */
import { describe, test, expect, afterEach } from 'bun:test';
import { Database } from 'bun:sqlite';
import { unlink } from 'fs/promises';
import { DEFAULTS } from '../src/infrastructure/backup/s3BackupConfig';
import type { S3BackupConfig, BackupMetadata } from '../src/infrastructure/backup/s3BackupConfig';
import { performBackup, listBackups, restoreBackup, cleanupOldBackups } from '../src/infrastructure/backup/s3BackupOperations';
import { S3BackupManager } from '../src/infrastructure/backup/s3Backup';
import type { S3Client } from 'bun';

function createMockS3Client(opts?: { writeDelayMs?: number }) {
  const storage = new Map<string, Uint8Array>();
  const client = {
    file(key: string) {
      return {
        async write(data: Uint8Array | string, _o?: { type?: string }) {
          if (opts?.writeDelayMs) await new Promise((r) => setTimeout(r, opts.writeDelayMs));
          storage.set(key, typeof data === 'string' ? new TextEncoder().encode(data) : new Uint8Array(data));
        },
        async exists() { return storage.has(key); },
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
    async delete(key: string) { storage.delete(key); },
    async list(o: { prefix?: string; maxKeys?: number }) {
      const contents: { key: string; size: number; lastModified: Date }[] = [];
      for (const [k, v] of storage) {
        if (o.prefix && !k.startsWith(o.prefix)) continue;
        contents.push({ key: k, size: v.byteLength, lastModified: new Date() });
      }
      return { contents: o.maxKeys ? contents.slice(0, o.maxKeys) : contents };
    },
    _storage: storage,
  };
  return client as unknown as S3Client & { _storage: Map<string, Uint8Array> };
}

const TMP = Bun.env.TMPDIR ?? '/tmp';
let tempFiles: string[] = [];
const tmp = (n: string) => { const p = `${TMP}/bq-edge-${n}-${Date.now()}-${Math.random().toString(36).slice(2)}.db`; tempFiles.push(p); return p; };
const c = (o: Partial<S3BackupConfig> = {}): S3BackupConfig => ({
  enabled: true, accessKeyId: 'k', secretAccessKey: 's', bucket: 'b', region: 'us-east-1',
  intervalMs: DEFAULTS.intervalMs, retention: DEFAULTS.retention, prefix: DEFAULTS.prefix,
  databasePath: '/tmp/test.db', ...o,
});
const mkDb = (path: string, table = 'test_data') => { const db = new Database(path); db.run(`CREATE TABLE ${table} (id INTEGER PRIMARY KEY, value TEXT)`); return db; };

afterEach(async () => {
  for (const f of tempFiles) {
    try { await unlink(f); } catch {} try { await unlink(f + '-wal'); } catch {} try { await unlink(f + '-shm'); } catch {}
  }
  tempFiles = [];
});

describe('1: empty database backup', () => {
  test('succeeds and is restorable', async () => {
    const s3 = createMockS3Client(), p = tmp('empty');
    mkDb(p).close();
    const r = await performBackup(c({ databasePath: p }), s3);
    expect(r.success).toBe(true);
    expect(r.size).toBeGreaterThan(0);
    const rp = tmp('empty-r');
    const rr = await restoreBackup(r.key!, c({ databasePath: rp }), s3);
    expect(rr.success).toBe(true);
    const db = new Database(rp);
    expect(db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='test_data'").all()).toHaveLength(1);
    db.close();
  });
});

describe('2: backup creates valid file', () => {
  test('writes .db and .meta.json with correct metadata', async () => {
    const s3 = createMockS3Client(), p = tmp('valid');
    const db = mkDb(p); db.run("INSERT INTO test_data VALUES (1, 'x')"); db.close();
    const r = await performBackup(c({ databasePath: p }), s3);
    expect(r.success).toBe(true);
    const keys = [...s3._storage.keys()];
    expect(keys.filter((k) => k.endsWith('.db'))).toHaveLength(1);
    const metaKey = keys.find((k) => k.endsWith('.meta.json'))!;
    expect(metaKey).toBeDefined();
    const meta = JSON.parse(new TextDecoder().decode(s3._storage.get(metaKey)!)) as BackupMetadata;
    expect(meta.checksum).toMatch(/^[a-f0-9]{64}$/);
    expect(meta.compressed).toBe(true);
    expect(meta.size).toBeGreaterThan(0);
  });
});

describe('3: sequential backups produce unique files', () => {
  test('three backups yield three distinct keys', async () => {
    const s3 = createMockS3Client(), p = tmp('seq');
    mkDb(p).close();
    const keys: string[] = [];
    for (let i = 0; i < 3; i++) {
      const r = await performBackup(c({ databasePath: p }), s3);
      expect(r.success).toBe(true);
      keys.push(r.key!);
      await Bun.sleep(5);
    }
    expect(new Set(keys).size).toBe(3);
    expect(await listBackups(c({ databasePath: p }), s3)).toHaveLength(3);
  });
});

describe('4: backup during concurrent writes', () => {
  test('produces a valid (non-corrupt) restore', async () => {
    const s3 = createMockS3Client(), p = tmp('cw');
    const db = mkDb(p);
    for (let i = 0; i < 50; i++) db.run('INSERT INTO test_data VALUES (?, ?)', [i, `v${i}`]);
    const bp = performBackup(c({ databasePath: p }), s3);
    for (let i = 50; i < 100; i++) db.run('INSERT INTO test_data VALUES (?, ?)', [i, `v${i}`]);
    const r = await bp;
    db.close();
    expect(r.success).toBe(true);
    const rp = tmp('cw-r');
    expect((await restoreBackup(r.key!, c({ databasePath: rp }), s3)).success).toBe(true);
    const rd = new Database(rp);
    expect((rd.query('SELECT COUNT(*) as n FROM test_data').get() as { n: number }).n).toBeGreaterThanOrEqual(50);
    rd.close();
  });
});

describe('5: retention cleanup', () => {
  test('retention=2 keeps only 2 of 5 backups', async () => {
    const s3 = createMockS3Client(), p = tmp('ret');
    mkDb(p).close();
    const conf = c({ databasePath: p, retention: 2 });
    for (let i = 0; i < 5; i++) { await performBackup(conf, s3); await Bun.sleep(5); }
    expect(await listBackups(conf, s3)).toHaveLength(5);
    await cleanupOldBackups(conf, s3);
    expect(await listBackups(conf, s3)).toHaveLength(2);
  });

  test('retention=1 keeps exactly one backup', async () => {
    const s3 = createMockS3Client(), p = tmp('ret1');
    mkDb(p).close();
    const conf = c({ databasePath: p, retention: 1 });
    for (let i = 0; i < 3; i++) { await performBackup(conf, s3); await Bun.sleep(5); }
    await cleanupOldBackups(conf, s3);
    expect(await listBackups(conf, s3)).toHaveLength(1);
  });
});

describe('6: non-existent database path', () => {
  test('fails gracefully for missing database', async () => {
    const s3 = createMockS3Client();
    const r = await performBackup(c({ databasePath: `/tmp/bq-missing-${Date.now()}.db` }), s3);
    expect(r.success).toBe(false);
    expect(r.error).toContain('Database file not found');
    expect(s3._storage.size).toBe(0);
  });

  test('fails gracefully for empty path', async () => {
    const r = await performBackup(c({ databasePath: '' }), createMockS3Client());
    expect(r.success).toBe(false);
  });
});

describe('7: backup status reporting', () => {
  test('returns correct defaults and custom endpoint', () => {
    const m1 = new S3BackupManager({ enabled: true, accessKeyId: 'k', secretAccessKey: 's', bucket: 'bk', databasePath: '/tmp/t.db' });
    const s1 = m1.getStatus();
    expect(s1.enabled).toBe(true);
    expect(s1.bucket).toBe('bk');
    expect(s1.intervalMs).toBe(DEFAULTS.intervalMs);
    expect(s1.retention).toBe(DEFAULTS.retention);
    expect(s1.isRunning).toBe(false);
    expect(s1.endpoint).toBe('AWS S3');

    const m2 = new S3BackupManager({ enabled: true, accessKeyId: 'k', secretAccessKey: 's', bucket: 'b', endpoint: 'https://r2.cf.com', databasePath: '/tmp/t.db' });
    expect(m2.getStatus().endpoint).toBe('https://r2.cf.com');
  });

  test('isRunning=false when disabled, even after start()', () => {
    const m = new S3BackupManager({ enabled: false, databasePath: '/tmp/t.db' });
    m.start();
    expect(m.getStatus().isRunning).toBe(false);
    m.stop();
  });
});

describe('8: concurrent backup mutex', () => {
  test('second concurrent backup returns "already in progress"', async () => {
    const s3 = createMockS3Client({ writeDelayMs: 100 }), p = tmp('mtx');
    mkDb(p).close();
    const m = new S3BackupManager({ enabled: true, accessKeyId: 'k', secretAccessKey: 's', bucket: 'b', databasePath: p });
    (m as any).client = s3;
    const [r1, r2] = await Promise.all([m.backup(), m.backup()]);
    const results = [r1, r2];
    expect(results.filter((r) => r.success)).toHaveLength(1);
    expect(results.filter((r) => !r.success && r.error?.includes('already in progress'))).toHaveLength(1);
  });

  test('mutex is released after failure so next backup works', async () => {
    const p = tmp('mtx-rel');
    const m = new S3BackupManager({ enabled: true, accessKeyId: 'k', secretAccessKey: 's', bucket: 'b', databasePath: p });
    (m as any).client = createMockS3Client();
    expect((await m.backup()).success).toBe(false); // no DB file yet
    mkDb(p).close();
    expect((await m.backup()).success).toBe(true);
  });
});

describe('9: backup data completeness', () => {
  test('all rows present after backup+restore cycle', async () => {
    const s3 = createMockS3Client(), p = tmp('comp'), N = 200;
    const db = mkDb(p);
    for (let i = 0; i < N; i++) db.run('INSERT INTO test_data VALUES (?, ?)', [i, JSON.stringify({ q: 'e', p: { to: `u${i}@t.c` } })]);
    db.close();
    const r = await performBackup(c({ databasePath: p }), s3);
    expect(r.success).toBe(true);
    const rp = tmp('comp-r');
    expect((await restoreBackup(r.key!, c({ databasePath: rp }), s3)).success).toBe(true);
    const rd = new Database(rp);
    expect((rd.query('SELECT COUNT(*) as n FROM test_data').get() as { n: number }).n).toBe(N);
    expect(JSON.parse((rd.query('SELECT value FROM test_data WHERE id=0').get() as { value: string }).value).q).toBe('e');
    rd.close();
  });

  test('backup preserves multiple tables', async () => {
    const s3 = createMockS3Client(), p = tmp('mt');
    const db = new Database(p);
    db.run('CREATE TABLE jobs (id INTEGER PRIMARY KEY, data TEXT)');
    db.run('CREATE TABLE dlq (id INTEGER PRIMARY KEY, reason TEXT)');
    db.run('CREATE TABLE crons (name TEXT PRIMARY KEY, schedule TEXT)');
    db.run("INSERT INTO jobs VALUES (1, 'jd')");
    db.run("INSERT INTO dlq VALUES (1, 'timeout')");
    db.run("INSERT INTO crons VALUES ('daily', '0 0 * * *')");
    db.close();
    const r = await performBackup(c({ databasePath: p }), s3);
    expect(r.success).toBe(true);
    const rp = tmp('mt-r');
    await restoreBackup(r.key!, c({ databasePath: rp }), s3);
    const rd = new Database(rp);
    expect((rd.query('SELECT data FROM jobs WHERE id=1').get() as { data: string }).data).toBe('jd');
    expect((rd.query('SELECT reason FROM dlq WHERE id=1').get() as { reason: string }).reason).toBe('timeout');
    expect((rd.query("SELECT schedule FROM crons WHERE name='daily'").get() as { schedule: string }).schedule).toBe('0 0 * * *');
    rd.close();
  });
});
