import { afterEach, describe, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import type { S3Client } from 'bun';
import { mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  cleanupOldBackups,
  listBackups,
  performBackup,
  restoreBackup,
} from '../src/infrastructure/backup/s3BackupOperations';
import { configFromEnv, type S3BackupConfig } from '../src/infrastructure/backup/s3BackupConfig';
import { SqliteStorage } from '../src/infrastructure/persistence/sqlite';

class MemoryS3 {
  readonly objects = new Map<string, Uint8Array>();
  readonly writes: string[] = [];
  failList = false;
  failPayloadWrites = false;

  file(key: string) {
    return {
      exists: async () => this.objects.has(key),
      write: async (data: string | Uint8Array | ArrayBuffer) => {
        if (this.failPayloadWrites && key.endsWith('.db')) {
          throw new Error('simulated payload failure');
        }
        const bytes =
          typeof data === 'string'
            ? new TextEncoder().encode(data)
            : data instanceof Uint8Array
              ? data.slice()
              : new Uint8Array(data);
        this.objects.set(key, bytes);
        this.writes.push(key);
        return bytes.byteLength;
      },
      arrayBuffer: async () => {
        const bytes = this.objects.get(key);
        if (!bytes) throw new Error(`missing object: ${key}`);
        return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
      },
      json: async () => {
        const bytes = this.objects.get(key);
        if (!bytes) throw new Error(`missing object: ${key}`);
        return JSON.parse(new TextDecoder().decode(bytes));
      },
    };
  }

  async list() {
    if (this.failList) throw new Error('simulated list outage');
    return { contents: [], isTruncated: false };
  }

  async delete(key: string) {
    this.objects.delete(key);
  }
}

const tempDirs: string[] = [];

afterEach(async () => {
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true, force: true })));
});

async function makeTempDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), 'bunqueue-backup-production-'));
  tempDirs.push(dir);
  return dir;
}

function config(databasePath: string): S3BackupConfig {
  return {
    enabled: true,
    accessKeyId: 'test',
    secretAccessKey: 'test',
    bucket: 'test',
    region: 'us-east-1',
    intervalMs: 60_000,
    retention: 7,
    prefix: 'backups/',
    databasePath,
  };
}

describe('S3 backup production regressions', () => {
  test('a persistence flush fails instead of snapshotting while writes remain buffered', async () => {
    const dir = await makeTempDir();
    const storage = new SqliteStorage({ path: join(dir, 'buffered.db') });
    const internals = storage as unknown as {
      writeBuffer: { flush: () => number; readonly pendingCount: number };
    };
    const originalBuffer = internals.writeBuffer;
    internals.writeBuffer = {
      flush: () => 0,
      get pendingCount() {
        return 1;
      },
    };

    try {
      expect(() => storage.flushWriteBuffer()).toThrow('1 write remains buffered');
    } finally {
      internals.writeBuffer = originalBuffer;
      storage.close();
    }
  });

  test('temporary credentials and virtual-hosted style are read from the environment', () => {
    const previous = {
      awsToken: Bun.env.AWS_SESSION_TOKEN,
      s3Token: Bun.env.S3_SESSION_TOKEN,
      virtualHostedStyle: Bun.env.S3_VIRTUAL_HOSTED_STYLE,
    };
    try {
      Bun.env.AWS_SESSION_TOKEN = 'aws-fallback-token';
      Bun.env.S3_SESSION_TOKEN = 's3-preferred-token';
      Bun.env.S3_VIRTUAL_HOSTED_STYLE = 'true';
      const preferred = configFromEnv('/tmp/bunqueue.db');
      expect(preferred.sessionToken).toBe('s3-preferred-token');
      expect(preferred.virtualHostedStyle).toBe(true);

      delete Bun.env.S3_SESSION_TOKEN;
      expect(configFromEnv('/tmp/bunqueue.db').sessionToken).toBe('aws-fallback-token');
    } finally {
      restoreEnv('AWS_SESSION_TOKEN', previous.awsToken);
      restoreEnv('S3_SESSION_TOKEN', previous.s3Token);
      restoreEnv('S3_VIRTUAL_HOSTED_STYLE', previous.virtualHostedStyle);
    }
  });

  test('a blocked WAL checkpoint cannot produce a successful incomplete snapshot', async () => {
    const dir = await makeTempDir();
    const sourcePath = join(dir, 'source.db');
    const restorePath = join(dir, 'restore.db');
    const writer = new Database(sourcePath);
    writer.exec(
      'PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; ' +
        'CREATE TABLE jobs(id INTEGER PRIMARY KEY, value TEXT); ' +
        'PRAGMA wal_checkpoint(TRUNCATE)'
    );

    const reader = new Database(sourcePath);
    reader.exec('BEGIN');
    reader.query('SELECT COUNT(*) AS count FROM jobs').get();
    for (let id = 1; id <= 25; id++) {
      writer.run('INSERT INTO jobs VALUES (?, ?)', [id, `job-${id}`]);
    }

    const s3 = new MemoryS3();
    try {
      const backup = await performBackup(config(sourcePath), s3 as unknown as S3Client);
      expect(backup.success).toBe(true);

      const restored = await restoreBackup(
        backup.key!,
        config(restorePath),
        s3 as unknown as S3Client
      );
      expect(restored.success).toBe(true);

      const restoredDb = new Database(restorePath);
      const row = restoredDb.query('SELECT COUNT(*) AS count FROM jobs').get() as {
        count: number;
      };
      restoredDb.close();
      expect(row.count).toBe(25);
    } finally {
      reader.exec('ROLLBACK');
      reader.close();
      writer.close();
    }
  });

  test('restore quarantines stale WAL and SHM files before installing the snapshot', async () => {
    const dir = await makeTempDir();
    const sourcePath = join(dir, 'backup-source.db');
    const livePath = join(dir, 'live.db');
    const source = new Database(sourcePath);
    source.exec(
      'PRAGMA journal_mode=DELETE; CREATE TABLE jobs(id INTEGER PRIMARY KEY, value TEXT)'
    );
    source.run('INSERT INTO jobs VALUES (?, ?)', [1, 'from-backup']);
    source.close();

    const s3 = new MemoryS3();
    const backup = await performBackup(config(sourcePath), s3 as unknown as S3Client);
    expect(backup.success).toBe(true);

    const live = new Database(livePath);
    live.exec(
      'PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; ' +
        'CREATE TABLE jobs(id INTEGER PRIMARY KEY, value TEXT); ' +
        'PRAGMA wal_checkpoint(TRUNCATE)'
    );
    live.run('INSERT INTO jobs VALUES (?, ?)', [2, 'post-backup-live-data']);
    const staleWal = await readFile(`${livePath}-wal`);
    const staleShm = await readFile(`${livePath}-shm`);
    live.close();

    // Simulate sidecars retained after an unclean process exit.
    await writeFile(`${livePath}-wal`, staleWal);
    await writeFile(`${livePath}-shm`, staleShm);

    const restored = await restoreBackup(backup.key!, config(livePath), s3 as unknown as S3Client);
    expect(restored.success).toBe(true);

    const restoredDb = new Database(livePath);
    const rows = restoredDb.query('SELECT id, value FROM jobs ORDER BY id').all();
    restoredDb.close();
    expect(rows).toEqual([{ id: 1, value: 'from-backup' }]);
    expect(await Bun.file(`${livePath}-wal`).exists()).toBe(false);
    expect(await Bun.file(`${livePath}-shm`).exists()).toBe(false);
  });

  test('concurrent backups use unique keys and publish metadata before each payload', async () => {
    const dir = await makeTempDir();
    const sourcePath = join(dir, 'concurrent-source.db');
    const source = new Database(sourcePath);
    source.exec('CREATE TABLE jobs(id INTEGER PRIMARY KEY, value TEXT)');
    source.run('INSERT INTO jobs VALUES (?, ?)', [1, 'durable']);
    source.close();
    const s3 = new MemoryS3();

    const results = await Promise.all([
      performBackup(config(sourcePath), s3 as unknown as S3Client),
      performBackup(config(sourcePath), s3 as unknown as S3Client),
    ]);
    expect(results.every(({ success }) => success)).toBe(true);
    expect(new Set(results.map(({ key }) => key)).size).toBe(2);
    for (const { key } of results) {
      expect(s3.writes.indexOf(`${key}.meta.json`)).toBeLessThan(s3.writes.indexOf(key!));
      expect(s3.objects.has(`${key}.meta.json`)).toBe(true);
      expect(s3.objects.has(key!)).toBe(true);
    }
    expect((await readdir(dir)).some((name) => name.includes('backup-snapshot'))).toBe(false);
  });

  test('list failures are observable and retention does not mutate remote objects', async () => {
    const dir = await makeTempDir();
    const sourcePath = join(dir, 'list-source.db');
    const source = new Database(sourcePath);
    source.exec('CREATE TABLE jobs(id INTEGER PRIMARY KEY)');
    source.close();
    const s3 = new MemoryS3();
    const backup = await performBackup(config(sourcePath), s3 as unknown as S3Client);
    expect(backup.success).toBe(true);
    const before = new Map(s3.objects);
    s3.failList = true;

    await expect(listBackups(config(sourcePath), s3 as unknown as S3Client)).rejects.toThrow(
      'Failed to list backups'
    );
    await cleanupOldBackups(config(sourcePath), s3 as unknown as S3Client);
    expect(s3.objects).toEqual(before);
  });

  test('a failed payload publication removes its already-published metadata', async () => {
    const dir = await makeTempDir();
    const sourcePath = join(dir, 'publication-source.db');
    const source = new Database(sourcePath);
    source.exec('CREATE TABLE jobs(id INTEGER PRIMARY KEY)');
    source.close();
    const s3 = new MemoryS3();
    s3.failPayloadWrites = true;

    const backup = await performBackup(config(sourcePath), s3 as unknown as S3Client);
    expect(backup.success).toBe(false);
    expect(s3.objects.size).toBe(0);
    expect((await readdir(dir)).some((name) => name.includes('backup-snapshot'))).toBe(false);
  });

  test('metadata-less legacy SQLite restores but metadata-less gzip is rejected', async () => {
    const dir = await makeTempDir();
    const sourcePath = join(dir, 'legacy-source.db');
    const legacyTarget = join(dir, 'legacy-target.db');
    const gzipTarget = join(dir, 'gzip-target.db');
    const source = new Database(sourcePath);
    source.exec('CREATE TABLE jobs(id INTEGER PRIMARY KEY, value TEXT)');
    source.run('INSERT INTO jobs VALUES (?, ?)', [1, 'legacy']);
    source.close();
    const bytes = new Uint8Array(await Bun.file(sourcePath).arrayBuffer());
    const s3 = new MemoryS3();
    await s3.file('backups/legacy.db').write(bytes);
    await s3.file('backups/orphan-current.db').write(Bun.gzipSync(bytes));

    const legacy = await restoreBackup(
      'backups/legacy.db',
      config(legacyTarget),
      s3 as unknown as S3Client
    );
    expect(legacy.success).toBe(true);
    const restored = new Database(legacyTarget);
    expect(restored.query('SELECT value FROM jobs WHERE id = 1').get()).toEqual({
      value: 'legacy',
    });
    restored.close();

    const rejected = await restoreBackup(
      'backups/orphan-current.db',
      config(gzipTarget),
      s3 as unknown as S3Client
    );
    expect(rejected.success).toBe(false);
    expect(rejected.error).toContain('metadata is missing');
    expect(await Bun.file(gzipTarget).exists()).toBe(false);
    expect((await readdir(dir)).some((name) => name.includes('.restore-'))).toBe(false);
  });

  test('a failed pre-swap validation preserves the live database and sidecars byte-for-byte', async () => {
    const dir = await makeTempDir();
    const sourcePath = join(dir, 'failure-source.db');
    const livePath = join(dir, 'failure-live.db');
    const source = new Database(sourcePath);
    source.exec('CREATE TABLE jobs(id INTEGER PRIMARY KEY, value TEXT)');
    source.run('INSERT INTO jobs VALUES (?, ?)', [1, 'backup']);
    source.close();
    const s3 = new MemoryS3();
    const backup = await performBackup(config(sourcePath), s3 as unknown as S3Client);
    expect(backup.success).toBe(true);

    const live = new Database(livePath);
    live.exec(
      'PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; ' +
        'CREATE TABLE jobs(id INTEGER PRIMARY KEY, value TEXT); ' +
        'PRAGMA wal_checkpoint(TRUNCATE)'
    );
    live.run('INSERT INTO jobs VALUES (?, ?)', [99, 'must-survive']);
    const staleWal = await readFile(`${livePath}-wal`);
    const staleShm = await readFile(`${livePath}-shm`);
    live.close();
    await writeFile(`${livePath}-wal`, staleWal);
    await writeFile(`${livePath}-shm`, staleShm);

    const before = {
      database: await readFile(livePath),
      wal: await readFile(`${livePath}-wal`),
      shm: await readFile(`${livePath}-shm`),
    };
    const metadataKey = `${backup.key}.meta.json`;
    const metadata = JSON.parse(new TextDecoder().decode(s3.objects.get(metadataKey)!));
    metadata.checksum = '0'.repeat(64);
    await s3.file(metadataKey).write(JSON.stringify(metadata));

    const failed = await restoreBackup(backup.key!, config(livePath), s3 as unknown as S3Client);
    expect(failed.success).toBe(false);
    expect(await readFile(livePath)).toEqual(before.database);
    expect(await readFile(`${livePath}-wal`)).toEqual(before.wal);
    expect(await readFile(`${livePath}-shm`)).toEqual(before.shm);
    expect(
      (await readdir(dir)).some(
        (name) => name.includes('.restore-') || name.includes('restore-quarantine')
      )
    ).toBe(false);
  });
});

function restoreEnv(name: string, value: string | undefined): void {
  if (value === undefined) {
    delete Bun.env[name];
  } else {
    Bun.env[name] = value;
  }
}
