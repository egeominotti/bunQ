import { afterEach, describe, expect, spyOn, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { JobId } from '../src/domain/types/job';
import { MIGRATION_TABLE, SCHEMA, SCHEMA_VERSION } from '../src/infrastructure/persistence/schema';
import { SqliteStorage } from '../src/infrastructure/persistence/sqlite';
import { pack } from '../src/infrastructure/persistence/sqliteSerializer';

const directories: string[] = [];

function tempDatabase(prefix: string): { directory: string; path: string } {
  const directory = mkdtempSync(join(tmpdir(), prefix));
  directories.push(directory);
  return { directory, path: join(directory, 'queue.db') };
}

function seedVersion30(path: string, count: number): void {
  const database = new Database(path, { create: true });
  const legacySchema = SCHEMA.replace('    name TEXT,\n', '').replace('    job_name TEXT,\n', '');
  database.run(legacySchema);
  database.run(MIGRATION_TABLE);
  database.run('INSERT INTO migrations(version, applied_at) VALUES (30, 0)');
  const insert = database.prepare(
    `INSERT INTO jobs
     (id, queue, data, priority, created_at, run_at, attempts, max_attempts, backoff, state)
     VALUES (?, ?, ?, 0, ?, ?, 0, 3, 1000, 'completed')`
  );
  const now = Date.now() - 60_000;
  database.transaction(() => {
    for (let index = 0; index < count; index++) {
      insert.run(
        `legacy-${String(index).padStart(5, '0')}`,
        'legacy-migration',
        pack({ name: 'legacy-job', index }),
        now + index,
        now + index
      );
    }
  })();
  database.close();
}

afterEach(() => {
  for (const directory of directories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

describe('issue #119 SQLite migration observability and startup cost', () => {
  test('logs migration start, bounded backfill progress, and completion', () => {
    const { path } = tempDatabase('bunqueue-issue119-logs-');
    seedVersion30(path, 1_200);
    const messages: string[] = [];
    const stdout: string[] = [];
    const log = spyOn(console, 'log').mockImplementation((...values: unknown[]) => {
      stdout.push(values.map(String).join(' '));
    });
    const error = spyOn(console, 'error').mockImplementation((...values: unknown[]) => {
      messages.push(values.map(String).join(' '));
    });

    try {
      new SqliteStorage({ path }).close();
    } finally {
      log.mockRestore();
      error.mockRestore();
    }

    expect(stdout).toEqual([]);
    expect(messages.some((message) => message.includes('Starting SQLite migration'))).toBe(true);
    expect(messages.some((message) => message.includes('SQLite migration progress'))).toBe(true);
    expect(messages.some((message) => message.includes('Completed SQLite migration'))).toBe(true);

    const database = new Database(path, { readonly: true });
    try {
      const versions = database
        .query<{ version: number }, []>('SELECT version FROM migrations ORDER BY version')
        .all()
        .map((row) => row.version);
      expect(versions).toContain(31);
      expect(versions).toContain(32);
      expect(versions.at(-1)).toBe(SCHEMA_VERSION);
      expect(
        database
          .query<{ name: string }, []>(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'migration_progress'"
          )
          .get()?.name
      ).toBe('migration_progress');
    } finally {
      database.close();
    }
  });

  test('rejects a database created by a newer bunqueue schema', () => {
    const { path } = tempDatabase('bunqueue-issue119-newer-');
    const database = new Database(path, { create: true });
    database.run(SCHEMA);
    database.run(MIGRATION_TABLE);
    database.run('INSERT INTO migrations(version, applied_at) VALUES (?, 0)', [SCHEMA_VERSION + 1]);
    database.close();

    expect(() => new SqliteStorage({ path })).toThrow('newer than supported');
  });

  test('preserves upgrades from historical databases without a version marker', () => {
    const { path } = tempDatabase('bunqueue-issue119-unversioned-');
    seedVersion30(path, 1);
    const legacy = new Database(path);
    legacy.run('DROP TABLE migrations');
    legacy.close();

    const storage = new SqliteStorage({ path });
    try {
      expect(storage.getJob('legacy-00000' as JobId)?.name).toBe('legacy-job');
      const database = new Database(path, { readonly: true });
      try {
        expect(
          database
            .query<{ version: number }, []>('SELECT MAX(version) AS version FROM migrations')
            .get()?.version
        ).toBe(SCHEMA_VERSION);
      } finally {
        database.close();
      }
    } finally {
      storage.close();
    }
  });

  test('completed dependency lookup reads only requested IDs', () => {
    const { path } = tempDatabase('bunqueue-issue119-recovery-');
    const storage = new SqliteStorage({ path });
    const database = (storage as unknown as { db: Database }).db;
    const insert = database.prepare(
      `INSERT INTO jobs
       (id, queue, name, data, priority, created_at, run_at, completed_at,
        attempts, max_attempts, backoff, state)
       VALUES (?, 'recovery', 'job', ?, 0, ?, ?, ?, 0, 3, 1000, 'completed')`
    );
    const now = Date.now();
    database.transaction(() => {
      for (let index = 0; index < 25; index++) {
        insert.run(`completed-${index}`, pack({ index }), now, now, now);
      }
    })();

    const lookup = storage.loadCompletedJobIds as (requested: Iterable<JobId>) => Set<JobId>;
    expect(lookup.call(storage, ['completed-7' as JobId, 'missing' as JobId])).toEqual(
      new Set(['completed-7' as JobId])
    );
    storage.close();
  });

  test('resumes a committed name backfill batch after an interrupted migration', () => {
    const { path } = tempDatabase('bunqueue-issue119-resume-');
    seedVersion30(path, 1_200);
    const before = new Database(path);
    before.run(`
      CREATE TRIGGER fail_second_name_batch
      BEFORE UPDATE OF name ON jobs
      WHEN OLD.id = 'legacy-00500'
      BEGIN
        SELECT RAISE(ABORT, 'issue119_interrupted_backfill');
      END;
    `);
    before.close();

    expect(() => new SqliteStorage({ path })).toThrow('issue119_interrupted_backfill');

    const interrupted = new Database(path);
    const migratedRows = interrupted
      .query<{ count: number }, []>('SELECT COUNT(*) AS count FROM jobs WHERE name IS NOT NULL')
      .get()?.count;
    const progress = interrupted
      .query<{ processed_rows: number; last_key: string }, []>(
        "SELECT processed_rows, last_key FROM migration_progress WHERE version = 31 AND phase = 'job-names'"
      )
      .get();
    const version = interrupted
      .query<{ version: number }, []>('SELECT MAX(version) AS version FROM migrations')
      .get()?.version;
    expect(migratedRows).toBe(500);
    expect(progress).toEqual({ processed_rows: 500, last_key: 'legacy-00499' });
    expect(version).toBe(30);
    interrupted.run('DROP TRIGGER fail_second_name_batch');
    interrupted.close();

    new SqliteStorage({ path }).close();
    const completed = new Database(path, { readonly: true });
    try {
      expect(
        completed
          .query<{ count: number }, []>('SELECT COUNT(*) AS count FROM jobs WHERE name IS NULL')
          .get()?.count
      ).toBe(0);
      expect(
        completed
          .query<{ version: number }, []>('SELECT MAX(version) AS version FROM migrations')
          .get()?.version
      ).toBe(SCHEMA_VERSION);
      expect(
        completed
          .query<{ count: number }, []>('SELECT COUNT(*) AS count FROM migration_progress')
          .get()?.count
      ).toBe(0);
    } finally {
      completed.close();
    }
  });

  test('validation failure after storage construction does not leave the process wedged', async () => {
    const { path } = tempDatabase('bunqueue-issue119-constructor-cleanup-');
    const source = `
      import { QueueManager } from './src/application/queueManager.ts';
      try {
        new QueueManager({ dataPath: ${JSON.stringify(path)}, maxQueueEvents: -1 });
        console.error('unexpected construction success');
        process.exitCode = 2;
      } catch (error) {
        console.error(error instanceof Error ? error.message : String(error));
      }
    `;
    const child = Bun.spawn([process.execPath, '-e', source], {
      cwd: process.cwd(),
      stdout: 'pipe',
      stderr: 'pipe',
    });
    const naturalExit = await Promise.race([
      child.exited.then((exitCode) => ({ exitCode })),
      Bun.sleep(1_000).then(() => null),
    ]);
    if (!naturalExit) {
      child.kill(9);
      await child.exited;
    }
    const stderr = await new Response(child.stderr).text();

    expect(naturalExit).not.toBeNull();
    expect(naturalExit?.exitCode).toBe(0);
    expect(stderr).toContain('maxQueueEvents must be a non-negative safe integer');
  });
});
