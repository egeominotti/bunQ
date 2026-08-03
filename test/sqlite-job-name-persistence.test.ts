import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createJob, jobId, type Job } from '../src/domain/types/job';
import { MIGRATION_TABLE, SCHEMA } from '../src/infrastructure/persistence/schema';
import { SqliteStorage } from '../src/infrastructure/persistence/sqlite';
import { pack, unpack } from '../src/infrastructure/persistence/sqliteSerializer';

type NamedJob = Job & { readonly name: string };

function makeNamedJob(id: string, name: string, data: unknown): NamedJob {
  return { ...createJob(jobId(id), 'named-jobs', { data }), name } as NamedJob;
}

describe('SqliteStorage job name persistence', () => {
  let directory: string;
  let databasePath: string;
  let storage: SqliteStorage;

  beforeEach(async () => {
    directory = await mkdtemp(join(tmpdir(), 'bunqueue-sqlite-job-name-'));
    databasePath = join(directory, 'queue.db');
    storage = new SqliteStorage({
      path: databasePath,
      writeBufferSize: 1_000,
      writeBufferFlushMs: 60_000,
    });
  });

  afterEach(async () => {
    storage.close();
    await rm(directory, { recursive: true, force: true });
  });

  test('round-trips separate job names and user data through single and batch inserts', () => {
    const single = makeNamedJob('named-single', 'send-email', {
      name: 'customer-visible-name',
      recipient: 'user@example.com',
    });
    const batch = [
      makeNamedJob('named-batch-a', 'resize-image', { width: 640 }),
      makeNamedJob('named-batch-b', 'write-audit', { name: 'audit-subject', sequence: 2 }),
    ];
    storage.insertJobImmediate(single);
    storage.insertJobsBatch(batch);
    expect(storage.flushWriteBuffer()).toBe(2);
    storage.close();

    const database = new Database(databasePath, { readonly: true });
    const row = database
      .query<{ name: string; data: Uint8Array }, [string]>(
        'SELECT name, data FROM jobs WHERE id = ?'
      )
      .get(single.id);
    database.close();
    expect(row?.name).toBe('send-email');
    expect(unpack(row?.data ?? null, null, 'named-single')).toEqual(single.data);

    storage = new SqliteStorage({ path: databasePath });
    const recoveredSingle = storage.getJob(single.id) as NamedJob | null;
    const recoveredBatch = batch.map((job) => storage.getJob(job.id) as NamedJob | null);
    expect(recoveredSingle?.name).toBe('send-email');
    expect(recoveredSingle?.data).toEqual(single.data);
    expect(recoveredBatch.map((job) => job?.name)).toEqual(['resize-image', 'write-audit']);
    expect(recoveredBatch.map((job) => job?.data)).toEqual(batch.map((job) => job.data));
  });

  test('migrates a v30 name envelope into the dedicated column and clean data payload', () => {
    storage.close();
    databasePath = join(directory, 'legacy-v30.db');
    const legacy = new Database(databasePath, { create: true });
    const legacySchema = SCHEMA.replace('    name TEXT,\n', '').replace('    job_name TEXT,\n', '');
    legacy.run(legacySchema);
    legacy.run(MIGRATION_TABLE);
    legacy
      .prepare('INSERT INTO migrations (version, applied_at) VALUES (?, ?)')
      .run(30, Date.now());
    legacy
      .prepare(
        `INSERT INTO jobs
         (id, queue, data, priority, created_at, run_at, attempts, max_attempts, backoff, state)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      )
      .run(
        'legacy-name-envelope',
        'named-jobs',
        pack({ name: 'legacy-operation', customerId: 42 }),
        0,
        Date.now(),
        Date.now(),
        0,
        3,
        1_000,
        'waiting'
      );
    legacy.close();

    storage = new SqliteStorage({ path: databasePath });
    const migrated = storage.getJob(jobId('legacy-name-envelope')) as NamedJob | null;
    expect(migrated?.name).toBe('legacy-operation');
    expect(migrated?.data).toEqual({ customerId: 42 });
    storage.close();

    const database = new Database(databasePath, { readonly: true });
    const row = database
      .query<{ name: string; data: Uint8Array }, [string]>(
        'SELECT name, data FROM jobs WHERE id = ?'
      )
      .get('legacy-name-envelope');
    const version = database
      .query<{ version: number }, []>('SELECT MAX(version) AS version FROM migrations')
      .get();
    database.close();
    expect(row?.name).toBe('legacy-operation');
    expect(unpack(row?.data ?? null, null, 'legacy-name-envelope')).toEqual({ customerId: 42 });
    expect(version?.version).toBeGreaterThan(30);

    storage = new SqliteStorage({ path: databasePath });
    const restarted = storage.getJob(jobId('legacy-name-envelope')) as NamedJob | null;
    expect(restarted?.name).toBe('legacy-operation');
    expect(restarted?.data).toEqual({ customerId: 42 });
  });
});
