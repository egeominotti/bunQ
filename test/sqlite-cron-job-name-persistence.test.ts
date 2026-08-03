import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createCronJob, type CronJob } from '../src/domain/types/cron';
import { MIGRATION_TABLE, SCHEMA } from '../src/infrastructure/persistence/schema';
import { SqliteStorage } from '../src/infrastructure/persistence/sqlite';
import { pack, unpack } from '../src/infrastructure/persistence/sqliteSerializer';

function makeNamedCron(name: string, jobName: string, data: unknown): CronJob {
  return createCronJob(
    {
      name,
      jobName,
      queue: 'named-cron-jobs',
      data,
      repeatEvery: 60_000,
    },
    Date.now() + 60_000
  );
}

describe('SqliteStorage cron job name persistence', () => {
  let directory: string;
  let databasePath: string;
  let storage: SqliteStorage | null;

  beforeEach(async () => {
    directory = await mkdtemp(join(tmpdir(), 'bunqueue-sqlite-cron-job-name-'));
    databasePath = join(directory, 'queue.db');
    storage = new SqliteStorage({ path: databasePath });
  });

  afterEach(async () => {
    storage?.close();
    await rm(directory, { recursive: true, force: true });
  });

  test('round-trips the spawned job name separately from scheduler id and user data', () => {
    const cron = makeNamedCron('report-scheduler', 'deliver-report', {
      name: 'customer-visible-name',
      tenantId: 42,
    });
    storage!.saveCron(cron);
    storage!.close();
    storage = null;

    const database = new Database(databasePath, { readonly: true });
    const row = database
      .query<{ job_name: string; data: Uint8Array }, [string]>(
        'SELECT job_name, data FROM cron_jobs WHERE name = ?'
      )
      .get(cron.name);
    database.close();
    expect(row?.job_name).toBe('deliver-report');
    expect(unpack(row?.data ?? null, null, 'named-cron')).toEqual(cron.data);

    storage = new SqliteStorage({ path: databasePath });
    const recovered = storage.loadCronJobs().find((candidate) => candidate.name === cron.name);
    expect(recovered?.name).toBe('report-scheduler');
    expect(recovered?.jobName).toBe('deliver-report');
    expect(recovered?.data).toEqual(cron.data);
  });

  test('migrates a v30 cron name envelope into job_name and clean data', () => {
    storage!.close();
    storage = null;
    databasePath = join(directory, 'legacy-v30.db');
    const legacy = new Database(databasePath, { create: true });
    const legacySchema = SCHEMA.replace('    name TEXT,\n', '').replace('    job_name TEXT,\n', '');
    legacy.run(legacySchema);
    legacy.run(MIGRATION_TABLE);
    legacy
      .prepare('INSERT INTO migrations (version, applied_at) VALUES (?, ?)')
      .run(30, Date.now());
    legacy
      .prepare('INSERT INTO cron_jobs (name, queue, data, next_run) VALUES (?, ?, ?, ?)')
      .run(
        'legacy-report-scheduler',
        'named-cron-jobs',
        pack({ name: 'legacy-deliver-report', tenantId: 7 }),
        Date.now() + 60_000
      );
    legacy.close();

    storage = new SqliteStorage({ path: databasePath });
    const migrated = storage
      .loadCronJobs()
      .find((candidate) => candidate.name === 'legacy-report-scheduler');
    expect(migrated?.name).toBe('legacy-report-scheduler');
    expect(migrated?.jobName).toBe('legacy-deliver-report');
    expect(migrated?.data).toEqual({ tenantId: 7 });
    storage.close();
    storage = null;

    const database = new Database(databasePath, { readonly: true });
    const row = database
      .query<{ job_name: string; data: Uint8Array }, [string]>(
        'SELECT job_name, data FROM cron_jobs WHERE name = ?'
      )
      .get('legacy-report-scheduler');
    const version = database
      .query<{ version: number }, []>('SELECT MAX(version) AS version FROM migrations')
      .get();
    database.close();
    expect(row?.job_name).toBe('legacy-deliver-report');
    expect(unpack(row?.data ?? null, null, 'legacy-cron')).toEqual({ tenantId: 7 });
    expect(version?.version).toBeGreaterThan(30);

    storage = new SqliteStorage({ path: databasePath });
    const restarted = storage
      .loadCronJobs()
      .find((candidate) => candidate.name === 'legacy-report-scheduler');
    expect(restarted?.jobName).toBe('legacy-deliver-report');
    expect(restarted?.data).toEqual({ tenantId: 7 });
  });
});
