import { describe, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import type { S3Client } from 'bun';
import fc, { type AsyncCommand } from 'fast-check';
import { mkdtemp, readFile, readdir, rm, unlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  cleanupOldBackups,
  listBackups,
  performBackup,
  restoreBackup,
} from '../../src/infrastructure/backup/s3BackupOperations';
import type { S3BackupConfig } from '../../src/infrastructure/backup/s3BackupConfig';

interface BackupSnapshot {
  key: string;
  rows: Map<number, string>;
}

interface BackupModel {
  backups: BackupSnapshot[];
  nextId: number;
  readerOpen: boolean;
  sourceRows: Map<number, string>;
}

class ModelS3 {
  readonly objects = new Map<string, { bytes: Uint8Array; modified: Date }>();
  failNextList = false;
  failNextWrite: ((key: string) => boolean) | null = null;
  private clock = 0;

  file(key: string) {
    return {
      exists: async () => this.objects.has(key),
      write: async (data: string | Uint8Array | ArrayBuffer) => {
        if (this.failNextWrite?.(key)) {
          this.failNextWrite = null;
          throw new Error('injected S3 write failure');
        }
        const bytes =
          typeof data === 'string'
            ? new TextEncoder().encode(data)
            : data instanceof Uint8Array
              ? data.slice()
              : new Uint8Array(data);
        this.objects.set(key, { bytes, modified: new Date(++this.clock) });
        return bytes.byteLength;
      },
      arrayBuffer: async () => {
        const object = this.objects.get(key);
        if (!object) throw new Error(`missing object: ${key}`);
        return object.bytes.buffer.slice(
          object.bytes.byteOffset,
          object.bytes.byteOffset + object.bytes.byteLength
        );
      },
      json: async () => {
        const object = this.objects.get(key);
        if (!object) throw new Error(`missing object: ${key}`);
        return JSON.parse(new TextDecoder().decode(object.bytes));
      },
    };
  }

  async list(options: { prefix?: string; maxKeys?: number; continuationToken?: string }) {
    if (this.failNextList) {
      this.failNextList = false;
      throw new Error('injected S3 list failure');
    }
    const offset = Number(options.continuationToken ?? 0);
    const matching = [...this.objects.entries()]
      .filter(([key]) => key.startsWith(options.prefix ?? ''))
      .sort(([a], [b]) => a.localeCompare(b));
    const page = matching.slice(offset, offset + (options.maxKeys ?? 100));
    const next = offset + page.length;
    return {
      contents: page.map(([key, object]) => ({
        key,
        size: object.bytes.byteLength,
        lastModified: object.modified,
      })),
      isTruncated: next < matching.length,
      nextContinuationToken: next < matching.length ? String(next) : undefined,
    };
  }

  async delete(key: string) {
    this.objects.delete(key);
  }
}

interface BackupReal {
  config: S3BackupConfig;
  dir: string;
  reader: Database | null;
  s3: ModelS3;
  source: Database;
  targetPath: string;
}

abstract class BackupCommand implements AsyncCommand<BackupModel, BackupReal> {
  abstract check(model: Readonly<BackupModel>): boolean;
  abstract run(model: BackupModel, real: BackupReal): Promise<void>;

  protected async invariants(model: BackupModel, real: BackupReal): Promise<void> {
    const sourceRows = real.source.query('SELECT id, value FROM jobs ORDER BY id').all() as Array<{
      id: number;
      value: string;
    }>;
    expect(sourceRows).toEqual(
      [...model.sourceRows].map(([id, value]) => ({ id, value })).sort((a, b) => a.id - b.id)
    );

    const listed = await listBackups(real.config, real.s3 as unknown as S3Client);
    expect(new Set(listed.map((backup) => backup.key))).toEqual(
      new Set(model.backups.map((backup) => backup.key))
    );
    for (const backup of model.backups) {
      expect(real.s3.objects.has(backup.key)).toBe(true);
      expect(real.s3.objects.has(`${backup.key}.meta.json`)).toBe(true);
    }

    const leakedArtifacts = (await readdir(real.dir)).filter(
      (name) =>
        name.includes('.backup-snapshot-') ||
        name.includes('.restore-quarantine-') ||
        (name.includes('.restore-') && name.endsWith('.tmp'))
    );
    expect(leakedArtifacts).toEqual([]);
  }
}

class InsertRows extends BackupCommand {
  constructor(private readonly count: number) {
    super();
  }

  check(): boolean {
    return true;
  }

  async run(model: BackupModel, real: BackupReal): Promise<void> {
    for (let index = 0; index < this.count; index++) {
      const id = model.nextId++;
      const value = `value-${id}`;
      real.source.run('INSERT INTO jobs VALUES (?, ?)', [id, value]);
      model.sourceRows.set(id, value);
    }
    await this.invariants(model, real);
  }

  toString(): string {
    return `insert(${this.count})`;
  }
}

class StartReader extends BackupCommand {
  check(model: Readonly<BackupModel>): boolean {
    return !model.readerOpen;
  }

  async run(model: BackupModel, real: BackupReal): Promise<void> {
    real.reader = new Database(real.config.databasePath);
    real.reader.exec('BEGIN');
    real.reader.query('SELECT COUNT(*) FROM jobs').get();
    model.readerOpen = true;
    await this.invariants(model, real);
  }

  toString(): string {
    return 'startReader';
  }
}

class StopReader extends BackupCommand {
  check(model: Readonly<BackupModel>): boolean {
    return model.readerOpen;
  }

  async run(model: BackupModel, real: BackupReal): Promise<void> {
    real.reader?.exec('ROLLBACK');
    real.reader?.close();
    real.reader = null;
    model.readerOpen = false;
    await this.invariants(model, real);
  }

  toString(): string {
    return 'stopReader';
  }
}

class CreateBackup extends BackupCommand {
  check(): boolean {
    return true;
  }

  async run(model: BackupModel, real: BackupReal): Promise<void> {
    const result = await performBackup(real.config, real.s3 as unknown as S3Client);
    expect(result.success).toBe(true);
    expect(model.backups.some((backup) => backup.key === result.key)).toBe(false);
    model.backups.unshift({ key: result.key!, rows: new Map(model.sourceRows) });

    const verifyPath = join(real.dir, `verify-${model.backups.length}.db`);
    const restored = await restoreBackup(
      result.key!,
      { ...real.config, databasePath: verifyPath },
      real.s3 as unknown as S3Client
    );
    expect(restored.success).toBe(true);
    expect(readRows(verifyPath)).toEqual([...model.sourceRows]);
    await this.invariants(model, real);
  }

  toString(): string {
    return 'backup';
  }
}

class RestoreLatest extends BackupCommand {
  constructor(private readonly withStaleWal: boolean) {
    super();
  }

  check(model: Readonly<BackupModel>): boolean {
    return model.backups.length > 0;
  }

  async run(model: BackupModel, real: BackupReal): Promise<void> {
    await removeDatabase(real.targetPath);
    if (this.withStaleWal) await createStaleDatabase(real.targetPath);
    const expected = model.backups[0];
    const result = await restoreBackup(
      expected.key,
      { ...real.config, databasePath: real.targetPath },
      real.s3 as unknown as S3Client
    );
    expect(result.success).toBe(true);
    expect(readRows(real.targetPath)).toEqual([...expected.rows]);
    await this.invariants(model, real);
  }

  toString(): string {
    return `restore(staleWal=${this.withStaleWal})`;
  }
}

class ApplyRetention extends BackupCommand {
  constructor(private readonly retention: number) {
    super();
  }

  check(model: Readonly<BackupModel>): boolean {
    return model.backups.length > 0;
  }

  async run(model: BackupModel, real: BackupReal): Promise<void> {
    real.config.retention = this.retention;
    await cleanupOldBackups(real.config, real.s3 as unknown as S3Client);
    model.backups = model.backups.slice(0, this.retention);
    await this.invariants(model, real);
  }

  toString(): string {
    return `retain(${this.retention})`;
  }
}

class RejectCorruptRestore extends BackupCommand {
  check(model: Readonly<BackupModel>): boolean {
    return model.backups.length > 0;
  }

  async run(model: BackupModel, real: BackupReal): Promise<void> {
    await removeDatabase(real.targetPath);
    const live = new Database(real.targetPath);
    live.exec('PRAGMA journal_mode=DELETE; CREATE TABLE jobs(id INTEGER PRIMARY KEY, value TEXT)');
    live.run('INSERT INTO jobs VALUES (?, ?)', [777_777, 'live-before-failed-restore']);
    live.close();

    const key = model.backups[0].key;
    const stored = real.s3.objects.get(key)!;
    const original = { bytes: stored.bytes.slice(), modified: stored.modified };
    const corrupted = stored.bytes.slice();
    corrupted[0] ^= 0xff;
    real.s3.objects.set(key, { bytes: corrupted, modified: stored.modified });
    try {
      const result = await restoreBackup(
        key,
        { ...real.config, databasePath: real.targetPath },
        real.s3 as unknown as S3Client
      );
      expect(result.success).toBe(false);
      expect(readRows(real.targetPath)).toEqual([[777_777, 'live-before-failed-restore']]);
    } finally {
      real.s3.objects.set(key, original);
    }
    await this.invariants(model, real);
  }

  toString(): string {
    return 'rejectCorruptRestore';
  }
}

class RejectIncompletePublication extends BackupCommand {
  check(): boolean {
    return true;
  }

  async run(model: BackupModel, real: BackupReal): Promise<void> {
    const keysBefore = [...real.s3.objects.keys()].sort();
    real.s3.failNextWrite = (key) => key.endsWith('.db');
    const result = await performBackup(real.config, real.s3 as unknown as S3Client);
    expect(result.success).toBe(false);
    expect([...real.s3.objects.keys()].sort()).toEqual(keysBefore);
    await this.invariants(model, real);
  }

  toString(): string {
    return 'rejectIncompletePublication';
  }
}

class ListFailurePreservesRetention extends BackupCommand {
  check(model: Readonly<BackupModel>): boolean {
    return model.backups.length > 0;
  }

  async run(model: BackupModel, real: BackupReal): Promise<void> {
    const keysBefore = [...real.s3.objects.keys()].sort();
    real.s3.failNextList = true;
    await expect(listBackups(real.config, real.s3 as unknown as S3Client)).rejects.toThrow(
      'Failed to list backups'
    );

    real.s3.failNextList = true;
    await cleanupOldBackups({ ...real.config, retention: 1 }, real.s3 as unknown as S3Client);
    expect([...real.s3.objects.keys()].sort()).toEqual(keysBefore);
    await this.invariants(model, real);
  }

  toString(): string {
    return 'listFailurePreservesRetention';
  }
}

class RejectMetadataLessCompressed extends BackupCommand {
  check(model: Readonly<BackupModel>): boolean {
    return model.backups.length > 0;
  }

  async run(model: BackupModel, real: BackupReal): Promise<void> {
    const key = model.backups[0].key;
    const metadataKey = `${key}.meta.json`;
    const metadata = real.s3.objects.get(metadataKey)!;
    real.s3.objects.delete(metadataKey);
    await removeDatabase(real.targetPath);
    const live = new Database(real.targetPath);
    live.exec('PRAGMA journal_mode=DELETE; CREATE TABLE jobs(id INTEGER PRIMARY KEY, value TEXT)');
    live.run('INSERT INTO jobs VALUES (?, ?)', [888_888, 'live-before-missing-metadata']);
    live.close();

    try {
      const result = await restoreBackup(
        key,
        { ...real.config, databasePath: real.targetPath },
        real.s3 as unknown as S3Client
      );
      expect(result.success).toBe(false);
      expect(readRows(real.targetPath)).toEqual([[888_888, 'live-before-missing-metadata']]);
    } finally {
      real.s3.objects.set(metadataKey, metadata);
    }
    await this.invariants(model, real);
  }

  toString(): string {
    return 'rejectMetadataLessCompressed';
  }
}

class RestoreLegacyRaw extends BackupCommand {
  check(): boolean {
    return true;
  }

  async run(model: BackupModel, real: BackupReal): Promise<void> {
    const legacyPath = join(real.dir, `legacy-${crypto.randomUUID()}.db`);
    const legacy = new Database(legacyPath);
    legacy.exec(
      'PRAGMA journal_mode=DELETE; CREATE TABLE jobs(id INTEGER PRIMARY KEY, value TEXT)'
    );
    legacy.run('INSERT INTO jobs VALUES (?, ?)', [666_666, 'legacy-raw']);
    legacy.close();
    const key = `${real.config.prefix}legacy-${crypto.randomUUID()}.db`;
    await real.s3.file(key).write(new Uint8Array(await Bun.file(legacyPath).arrayBuffer()));

    try {
      await removeDatabase(real.targetPath);
      const result = await restoreBackup(
        key,
        { ...real.config, databasePath: real.targetPath },
        real.s3 as unknown as S3Client
      );
      expect(result.success).toBe(true);
      expect(readRows(real.targetPath)).toEqual([[666_666, 'legacy-raw']]);
    } finally {
      await real.s3.delete(key);
      await removeDatabase(legacyPath);
    }
    await this.invariants(model, real);
  }

  toString(): string {
    return 'restoreLegacyRaw';
  }
}

function readRows(path: string): Array<[number, string]> {
  const database = new Database(path);
  const rows = database.query('SELECT id, value FROM jobs ORDER BY id').all() as Array<{
    id: number;
    value: string;
  }>;
  database.close();
  return rows.map((row) => [row.id, row.value]);
}

async function removeDatabase(path: string): Promise<void> {
  await Promise.all(
    [path, `${path}-wal`, `${path}-shm`].map((file) => unlink(file).catch(() => undefined))
  );
}

async function createStaleDatabase(path: string): Promise<void> {
  const database = new Database(path);
  database.exec(
    'PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; ' +
      'CREATE TABLE jobs(id INTEGER PRIMARY KEY, value TEXT); PRAGMA wal_checkpoint(TRUNCATE)'
  );
  database.run('INSERT INTO jobs VALUES (?, ?)', [999_999, 'stale']);
  const wal = await readFile(`${path}-wal`);
  const shm = await readFile(`${path}-shm`);
  database.close();
  await writeFile(`${path}-wal`, wal);
  await writeFile(`${path}-shm`, shm);
}

function commandArbitraries() {
  return [
    fc.integer({ min: 1, max: 4 }).map((count) => new InsertRows(count)),
    fc.constant(new StartReader()),
    fc.constant(new StopReader()),
    fc.constant(new CreateBackup()),
    fc.boolean().map((stale) => new RestoreLatest(stale)),
    fc.integer({ min: 1, max: 3 }).map((retention) => new ApplyRetention(retention)),
    fc.constant(new RejectCorruptRestore()),
    fc.constant(new RejectIncompletePublication()),
    fc.constant(new ListFailurePreservesRetention()),
    fc.constant(new RejectMetadataLessCompressed()),
    fc.constant(new RestoreLegacyRaw()),
  ];
}

describe('S3 backup state-machine model', () => {
  test('generated histories preserve snapshot, publication, restore and retention invariants', async () => {
    await fc.assert(
      fc.asyncProperty(fc.commands(commandArbitraries(), { maxCommands: 30 }), async (commands) => {
        const dir = await mkdtemp(join(tmpdir(), 'bunqueue-backup-model-'));
        const sourcePath = join(dir, 'source.db');
        const source = new Database(sourcePath);
        source.exec(
          'PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; ' +
            'CREATE TABLE jobs(id INTEGER PRIMARY KEY, value TEXT); ' +
            'PRAGMA wal_checkpoint(TRUNCATE)'
        );
        const real: BackupReal = {
          config: {
            enabled: true,
            accessKeyId: 'test',
            secretAccessKey: 'test',
            bucket: 'test',
            region: 'us-east-1',
            intervalMs: 60_000,
            retention: 3,
            prefix: 'backups/',
            databasePath: sourcePath,
          },
          dir,
          reader: null,
          s3: new ModelS3(),
          source,
          targetPath: join(dir, 'restore.db'),
        };
        const model: BackupModel = {
          backups: [],
          nextId: 1,
          readerOpen: false,
          sourceRows: new Map(),
        };
        try {
          await fc.asyncModelRun(() => ({ model, real }), commands);
        } finally {
          real.reader?.exec('ROLLBACK');
          real.reader?.close();
          source.close();
          await rm(dir, { recursive: true, force: true });
        }
      }),
      {
        endOnFailure: true,
        interruptAfterTimeLimit: 60_000,
        numRuns: 50,
        verbose: 2,
      }
    );
  }, 70_000);
});
