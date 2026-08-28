import { afterEach, describe, expect, spyOn, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { createCronJob } from '../src/domain/types/cron';
import { SqliteStorage } from '../src/infrastructure/persistence/sqlite';
import { CronScheduler } from '../src/infrastructure/scheduler/cronScheduler';

const directories: string[] = [];

function seedLegacyCron(nextRun: number, skipMissedOnRestart: boolean): string {
  const directory = mkdtempSync(join(tmpdir(), 'bunqueue-cron-native-migration-'));
  directories.push(directory);
  const dataPath = join(directory, 'queue.db');
  const storage = new SqliteStorage({ path: dataPath });
  storage.saveCron(
    createCronJob(
      {
        name: 'legacy-last-day',
        queue: 'reports',
        data: {},
        schedule: '0 0 L * *',
        skipMissedOnRestart,
      },
      nextRun
    )
  );
  storage.close();
  return dataPath;
}

afterEach(() => {
  for (const directory of directories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

describe('persisted Croner schedule migration to Bun.cron', () => {
  test.each([-1, 0, 0.5, Number.NaN, Number.POSITIVE_INFINITY, Number.MAX_VALUE])(
    'rejects repeatEvery=%p through the public scheduler path',
    (repeatEvery) => {
      const scheduler = new CronScheduler();

      expect(() =>
        scheduler.add({
          name: 'invalid-interval',
          queue: 'reports',
          data: {},
          repeatEvery,
        })
      ).toThrow(/repeatEvery.*positive.*safe integer/i);
      expect(scheduler.list()).toEqual([]);
    }
  );

  test('rejects a runtime string interval before scheduler mutation', () => {
    const scheduler = new CronScheduler();

    expect(() =>
      scheduler.add({
        name: 'string-interval',
        queue: 'reports',
        data: {},
        repeatEvery: '1000' as unknown as number,
      })
    ).toThrow(/repeatEvery.*positive.*safe integer/i);
    expect(scheduler.list()).toEqual([]);
  });

  test('does not remove an overlap key owner before rejecting invalid timing', async () => {
    const manager = new QueueManager();
    try {
      const job = await manager.push('reports', {
        data: { retained: true },
        uniqueKey: 'cron:invalid-interval',
      });

      expect(() =>
        manager.addCron({
          name: 'invalid-interval',
          queue: 'reports',
          data: {},
          repeatEvery: -1,
          preventOverlap: true,
        })
      ).toThrow(/repeatEvery.*positive.*safe integer/i);

      expect(manager.getQueueJobCounts('reports').waiting).toBe(1);
      expect(await manager.getJobState(job.id)).toBe('waiting');
      expect(manager.getCron('invalid-interval')).toBeUndefined();
    } finally {
      await manager.shutdown();
    }
  });

  test.each([
    { schedule: '0 0 L * *', timezone: undefined, label: 'schedule' },
    { schedule: '@daily', timezone: 'Not/A_Real_Zone', label: 'timezone' },
  ])('does not remove an overlap key owner before rejecting invalid $label', async (timing) => {
    const manager = new QueueManager();
    try {
      const job = await manager.push('reports', {
        data: { retained: true },
        uniqueKey: `cron:invalid-${timing.label}`,
      });

      expect(() =>
        manager.addCron({
          name: `invalid-${timing.label}`,
          queue: 'reports',
          data: {},
          schedule: timing.schedule,
          timezone: timing.timezone,
          preventOverlap: true,
        })
      ).toThrow(/Invalid cron expression/i);

      expect(manager.getQueueJobCounts('reports').waiting).toBe(1);
      expect(await manager.getJobState(job.id)).toBe('waiting');
      expect(manager.getCron(`invalid-${timing.label}`)).toBeUndefined();
    } finally {
      await manager.shutdown();
    }
  });

  test('rejects a persisted non-positive interval before loading scheduler state', () => {
    const directory = mkdtempSync(join(tmpdir(), 'bunqueue-cron-interval-migration-'));
    directories.push(directory);
    const dataPath = join(directory, 'queue.db');
    const storage = new SqliteStorage({ path: dataPath });
    const cron = createCronJob(
      {
        name: 'invalid-interval',
        queue: 'reports',
        data: {},
        repeatEvery: 1,
      },
      Date.now() - 1
    );
    storage.saveCron({ ...cron, repeatEvery: -1 });
    storage.close();

    expect(() => new QueueManager({ dataPath })).toThrow(
      /Persisted cron "invalid-interval".*repeatEvery.*positive.*safe integer/i
    );
  });

  test('rejects an overdue unsupported schedule with actionable upgrade guidance', () => {
    const dataPath = seedLegacyCron(Date.now() - 60_000, true);

    expect(() => new QueueManager({ dataPath })).toThrow(
      /Persisted cron "legacy-last-day".*0 0 L \* \*.*update or remove.*before upgrading to bunqueue 2\.9\.0/i
    );
  });

  test('rejects a future unsupported schedule before it can enter a due retry loop', () => {
    const dataPath = seedLegacyCron(Date.now() + 86_400_000, false);

    expect(() => new QueueManager({ dataPath })).toThrow(
      /Persisted cron "legacy-last-day".*0 0 L \* \*.*update or remove.*before upgrading to bunqueue 2\.9\.0/i
    );
  });

  test('closes SQLite when constructor preflight rejects a persisted schedule', () => {
    const dataPath = seedLegacyCron(Date.now() + 86_400_000, false);
    const close = spyOn(SqliteStorage.prototype, 'close');
    try {
      expect(() => new QueueManager({ dataPath })).toThrow('Persisted cron');
      expect(close).toHaveBeenCalledTimes(1);
    } finally {
      close.mockRestore();
    }
  });

  test('closes SQLite when persisted cron loading fails', () => {
    const dataPath = seedLegacyCron(Date.now() + 86_400_000, false);
    const load = spyOn(SqliteStorage.prototype, 'loadCronJobs').mockImplementation(() => {
      throw new Error('simulated SQLite cron read failure');
    });
    const originalClose = SqliteStorage.prototype.close;
    const close = spyOn(SqliteStorage.prototype, 'close').mockImplementation(function () {
      originalClose.call(this);
      throw new Error('simulated cleanup failure');
    });
    try {
      expect(() => new QueueManager({ dataPath })).toThrow('simulated SQLite cron read failure');
      expect(close).toHaveBeenCalledTimes(1);
    } finally {
      close.mockRestore();
      load.mockRestore();
    }
  });

  test('does not advance valid rows before rejecting an invalid persisted collection', () => {
    const directory = mkdtempSync(join(tmpdir(), 'bunqueue-cron-native-atomic-'));
    directories.push(directory);
    const dataPath = join(directory, 'queue.db');
    const originalNextRun = Date.now() - 120_000;
    const storage = new SqliteStorage({ path: dataPath });
    storage.saveCron(
      createCronJob(
        {
          name: 'valid-overdue',
          queue: 'reports',
          data: {},
          schedule: '@daily',
          skipMissedOnRestart: true,
        },
        originalNextRun
      )
    );
    storage.saveCron(
      createCronJob(
        {
          name: 'invalid-future',
          queue: 'reports',
          data: {},
          schedule: '0 0 L * *',
        },
        Date.now() + 86_400_000
      )
    );
    storage.close();

    expect(() => new QueueManager({ dataPath })).toThrow('Persisted cron');

    const verification = new SqliteStorage({ path: dataPath });
    try {
      expect(
        verification.loadCronJobs().find((cron) => cron.name === 'valid-overdue')?.nextRun
      ).toBe(originalNextRun);
    } finally {
      verification.close();
    }
  });

  test('leaves scheduler state and persistence untouched when batch validation fails', () => {
    const scheduler = new CronScheduler();
    let persistenceCalls = 0;
    scheduler.setPersistCallback(() => persistenceCalls++);
    const now = Date.now();

    expect(() =>
      scheduler.load([
        createCronJob(
          {
            name: 'valid-overdue',
            queue: 'reports',
            data: {},
            schedule: '@daily',
            skipMissedOnRestart: true,
          },
          now - 60_000
        ),
        createCronJob(
          {
            name: 'invalid-future',
            queue: 'reports',
            data: {},
            schedule: '0 0 L * *',
          },
          now + 60_000
        ),
      ])
    ).toThrow('Persisted cron');
    expect(scheduler.list()).toEqual([]);
    expect(persistenceCalls).toBe(0);
  });
});
