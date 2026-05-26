/**
 * Test: Issue #86 - Attempts field is ignored when job fails (cron jobs)
 *
 * upsertJobScheduler / addCron fires jobs without forwarding job options
 * (maxAttempts, backoff, removeOnComplete, removeOnFail, timeout, delay).
 * Scheduled jobs always fall back to JOB_DEFAULTS.maxAttempts = 3, ignoring
 * both the scheduler job template opts and the Queue defaultJobOptions.
 */
import { describe, it, expect, afterEach } from 'bun:test';
import { Database } from 'bun:sqlite';
import { existsSync, unlinkSync } from 'fs';
import { QueueManager } from '../src/application/queueManager';
import { Queue } from '../src/client';
import { getSharedManager } from '../src/client/manager';
import { SqliteStorage } from '../src/infrastructure/persistence/sqlite';
import { createCronJob } from '../src/domain/types/cron';
import type { CronScheduler } from '../src/infrastructure/scheduler/cronScheduler';

function tick(manager: QueueManager): Promise<void> {
  return ((manager as unknown as { cronScheduler: CronScheduler }).cronScheduler as unknown as {
    tick(): Promise<void>;
  }).tick();
}

describe('Issue #86: cron jobs honor job options', () => {
  let manager: QueueManager;

  afterEach(() => {
    manager?.shutdown();
  });

  it('forwards maxAttempts from cron jobOptions into the pushed job', async () => {
    manager = new QueueManager({});

    manager.addCron({
      name: 'mre-cron',
      queue: 'mre-test',
      data: { type: 'scheduled' },
      repeatEvery: 30_000,
      immediately: true,
      jobOptions: { maxAttempts: 1 },
    });

    await tick(manager);

    const jobs = manager.getJobs('mre-test', { state: ['waiting', 'prioritized'] });
    expect(jobs.length).toBe(1);
    expect(jobs[0].maxAttempts).toBe(1);
  });

  it('forwards removeOnFail/removeOnComplete/backoff/timeout from cron jobOptions', async () => {
    manager = new QueueManager({});

    manager.addCron({
      name: 'opts-cron',
      queue: 'opts-test',
      data: { x: 1 },
      repeatEvery: 30_000,
      immediately: true,
      jobOptions: {
        maxAttempts: 5,
        backoff: 2000,
        timeout: 1234,
        removeOnComplete: true,
        removeOnFail: true,
      },
    });

    await tick(manager);

    const jobs = manager.getJobs('opts-test', { state: ['waiting', 'prioritized'] });
    expect(jobs.length).toBe(1);
    const job = jobs[0];
    expect(job.maxAttempts).toBe(5);
    expect(job.backoff).toBe(2000);
    expect(job.timeout).toBe(1234);
    expect(job.removeOnComplete).toBe(true);
    expect(job.removeOnFail).toBe(true);
  });

  it('defaults to JOB_DEFAULTS.maxAttempts when no jobOptions given', async () => {
    manager = new QueueManager({});

    manager.addCron({
      name: 'default-cron',
      queue: 'default-test',
      data: {},
      repeatEvery: 30_000,
      immediately: true,
    });

    await tick(manager);

    const jobs = manager.getJobs('default-test', { state: ['waiting', 'prioritized'] });
    expect(jobs.length).toBe(1);
    expect(jobs[0].maxAttempts).toBe(3);
  });
});

describe('Issue #86: embedded Queue forwards defaultJobOptions to cron', () => {
  it('Queue defaultJobOptions.attempts reaches the cron job template (no template opts)', async () => {
    const queue = new Queue('e2e-test', {
      embedded: true,
      defaultJobOptions: { attempts: 1, removeOnComplete: true, removeOnFail: true },
    });

    await queue.upsertJobScheduler(
      'e2e-cron',
      { every: 30_000 },
      { data: { type: 'scheduled' } }
    );

    const cron = getSharedManager()
      .listCrons()
      .find((c) => c.name === 'e2e-cron');
    expect(cron).toBeDefined();
    expect(cron!.jobOptions?.maxAttempts).toBe(1);
    expect(cron!.jobOptions?.removeOnComplete).toBe(true);
    expect(cron!.jobOptions?.removeOnFail).toBe(true);

    await queue.removeJobScheduler('e2e-cron');
    await queue.close();
  });

  it('template opts override Queue defaultJobOptions', async () => {
    const queue = new Queue('e2e-test2', {
      embedded: true,
      defaultJobOptions: { attempts: 1 },
    });

    await queue.upsertJobScheduler(
      'e2e-cron2',
      { every: 30_000 },
      { data: {}, opts: { attempts: 7 } }
    );

    const cron = getSharedManager()
      .listCrons()
      .find((c) => c.name === 'e2e-cron2');
    expect(cron!.jobOptions?.maxAttempts).toBe(7);

    await queue.removeJobScheduler('e2e-cron2');
    await queue.close();
  });
});

describe('Issue #86: jobOptions survive SQLite persistence', () => {
  const DB = '/tmp/test-issue86-persist.db';
  const cleanup = () => {
    for (const f of [DB, `${DB}-wal`, `${DB}-shm`]) {
      try {
        if (existsSync(f)) unlinkSync(f);
      } catch {}
    }
  };

  afterEach(cleanup);

  it('round-trips jobOptions (incl. nested backoff) through save/load', () => {
    cleanup();
    const storage = new SqliteStorage({ path: DB });
    const cron = createCronJob(
      {
        name: 'persist-cron',
        queue: 'persist-q',
        data: { x: 1 },
        repeatEvery: 30_000,
        jobOptions: {
          maxAttempts: 5,
          backoff: { type: 'exponential', delay: 2000 },
          timeout: 1234,
          removeOnComplete: true,
          removeOnFail: true,
        },
      },
      Date.now() + 30_000
    );
    storage.saveCron(cron);
    storage.close();

    const storage2 = new SqliteStorage({ path: DB });
    const loaded = storage2.loadCronJobs().find((c) => c.name === 'persist-cron');
    storage2.close();

    expect(loaded).toBeDefined();
    expect(loaded!.jobOptions).toEqual({
      maxAttempts: 5,
      backoff: { type: 'exponential', delay: 2000 },
      timeout: 1234,
      removeOnComplete: true,
      removeOnFail: true,
    });
  });

  it('loads cron rows with NULL job_options as null (backward compat)', () => {
    cleanup();
    const storage = new SqliteStorage({ path: DB });
    const cron = createCronJob(
      { name: 'no-opts', queue: 'q', data: {}, repeatEvery: 30_000 },
      Date.now() + 30_000
    );
    storage.saveCron(cron);
    storage.close();

    const storage2 = new SqliteStorage({ path: DB });
    const loaded = storage2.loadCronJobs().find((c) => c.name === 'no-opts');
    storage2.close();
    expect(loaded!.jobOptions).toBeNull();
  });

  it('migration 12 adds job_options column to a pre-v12 cron_jobs table', () => {
    cleanup();
    // Simulate an existing v11 DB: cron_jobs without job_options, migrations at 11.
    const raw = new Database(DB, { create: true });
    raw.run(`CREATE TABLE cron_jobs (
      name TEXT PRIMARY KEY, queue TEXT NOT NULL, data BLOB NOT NULL,
      schedule TEXT, repeat_every INTEGER, priority INTEGER NOT NULL DEFAULT 0,
      next_run INTEGER NOT NULL, executions INTEGER NOT NULL DEFAULT 0,
      max_limit INTEGER, timezone TEXT, unique_key TEXT, dedup BLOB,
      skip_missed_on_restart INTEGER NOT NULL DEFAULT 0,
      skip_if_no_worker INTEGER NOT NULL DEFAULT 0,
      prevent_overlap INTEGER NOT NULL DEFAULT 1
    )`);
    raw.run('CREATE TABLE migrations (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)');
    raw.run('INSERT INTO migrations (version, applied_at) VALUES (11, ?)', [Date.now()]);
    const before = raw
      .query<{ name: string }, []>('PRAGMA table_info(cron_jobs)')
      .all()
      .map((r) => r.name);
    expect(before).not.toContain('job_options');
    raw.close();

    // Opening via SqliteStorage runs migrate() → migration 12.
    const storage = new SqliteStorage({ path: DB });
    const cols = (storage as unknown as { db: Database }).db
      .query<{ name: string }, []>('PRAGMA table_info(cron_jobs)')
      .all()
      .map((r) => r.name);
    storage.close();
    expect(cols).toContain('job_options');
  });
});
