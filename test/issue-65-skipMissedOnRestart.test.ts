/**
 * Test: Issue #65 - skipMissedOnRestart does not work on Queue#upsertJobScheduler
 *
 * Reproduces 5 bugs:
 * 1. skipMissedOnRestart is only enforced in load(), never in add()
 * 2. load() recalculates nextRun but never persists it
 * 3. upsertJobScheduler resets executions to 0 on every restart
 * 4. immediately: true is silently ignored
 * 5. Race window: add() after load() ignores existing executions count
 */
import { describe, it, expect } from 'bun:test';
import { CronScheduler } from '../src/infrastructure/scheduler/cronScheduler';
import type { CronJob } from '../src/domain/types/cron';
import type { JobInput } from '../src/domain/types/job';

/** Access private tick() */
function tickScheduler(scheduler: CronScheduler): Promise<void> {
  return (scheduler as any).tick();
}

function makeCronJob(overrides: Partial<CronJob> & { name: string; queue: string }): CronJob {
  return {
    data: {},
    schedule: '* * * * *',
    repeatEvery: null,
    priority: 0,
    timezone: null,
    nextRun: Date.now() + 60000,
    executions: 0,
    maxLimit: null,
    uniqueKey: null,
    dedup: null,
    skipMissedOnRestart: false,
    ...overrides,
  };
}

describe('Issue #65: skipMissedOnRestart in add() path', () => {
  // BUG 1: add() never checks skipMissedOnRestart
  // When add() is called (via upsertJobScheduler), skipMissedOnRestart has no effect.
  // The add() method always calculates nextRun from now, which is usually fine,
  // but createCronJob() always sets executions=0, losing the DB state.
  it('add() should respect skipMissedOnRestart when upserting an existing cron', async () => {
    const pushedJobs: Array<{ queue: string; input: JobInput }> = [];
    const scheduler = new CronScheduler();

    scheduler.setPushCallback(async (queue, input) => {
      pushedJobs.push({ queue, input });
    });
    scheduler.setPersistCallback(() => {});

    // Simulate: load existing cron from DB with past nextRun
    // This simulates what happens after a server restart
    scheduler.load([makeCronJob({
      name: 'my-scheduler',
      queue: 'test-queue',
      nextRun: Date.now() - 3600_000, // 1 hour overdue
      schedule: '0 * * * *',
      skipMissedOnRestart: true,
      executions: 42, // Had 42 executions before restart
    })]);

    // Verify load() correctly adjusted nextRun
    const afterLoad = scheduler.list();
    expect(afterLoad[0].nextRun).toBeGreaterThan(Date.now());
    expect(afterLoad[0].executions).toBe(42);

    // Now simulate upsertJobScheduler calling add() - this is what the app does on startup
    const cron = scheduler.add({
      name: 'my-scheduler',
      queue: 'test-queue',
      data: {},
      schedule: '0 * * * *',
      skipMissedOnRestart: true,
    });

    // BUG: add() resets executions to 0 via createCronJob()
    // Expected: should preserve existing executions count (42)
    expect(cron.executions).toBe(42);

    // nextRun from add() should be in the future
    expect(cron.nextRun).toBeGreaterThan(Date.now());

    scheduler.stop();
  });

  // BUG 2: load() recalculates nextRun in memory but never persists it
  it('load() should persist recalculated nextRun when skipMissedOnRestart adjusts it', async () => {
    const persisted: Array<{ name: string; executions: number; nextRun: number }> = [];
    const scheduler = new CronScheduler();

    scheduler.setPushCallback(async () => {});
    scheduler.setPersistCallback((name, executions, nextRun) => {
      persisted.push({ name, executions, nextRun });
    });

    const pastNextRun = Date.now() - 3600_000;

    scheduler.load([makeCronJob({
      name: 'persist-test',
      queue: 'test-queue',
      nextRun: pastNextRun,
      schedule: '0 * * * *',
      skipMissedOnRestart: true,
      executions: 10,
    })]);

    // BUG: load() adjusts nextRun in memory but never calls persistCron()
    // The DB still has the old (past) nextRun value
    // Expected: persistCron should be called with the new future nextRun
    expect(persisted.length).toBeGreaterThan(0);
    if (persisted.length > 0) {
      expect(persisted[0].name).toBe('persist-test');
      expect(persisted[0].nextRun).toBeGreaterThan(Date.now());
      expect(persisted[0].executions).toBe(10); // Should preserve count
    }

    scheduler.stop();
  });

  // BUG 3: executions counter reset to 0 on every upsert
  it('add() should preserve executions count when upserting existing cron', () => {
    const scheduler = new CronScheduler();
    scheduler.setPushCallback(async () => {});
    scheduler.setPersistCallback(() => {});

    // First: load cron with 100 executions from DB
    scheduler.load([makeCronJob({
      name: 'counter-test',
      queue: 'test-queue',
      schedule: '*/5 * * * *',
      executions: 100,
    })]);

    const beforeUpsert = scheduler.get('counter-test');
    expect(beforeUpsert?.executions).toBe(100);

    // Then: upsertJobScheduler calls add() which creates new CronJob with executions=0
    scheduler.add({
      name: 'counter-test',
      queue: 'test-queue',
      data: {},
      schedule: '*/5 * * * *',
    });

    const afterUpsert = scheduler.get('counter-test');
    // BUG: executions is reset to 0 by createCronJob()
    // Expected: should be preserved as 100
    expect(afterUpsert?.executions).toBe(100);

    scheduler.stop();
  });

  // BUG 4: immediately: true is silently ignored
  // The immediately option is declared in RepeatOpts but never forwarded or handled
  it('immediately option should trigger an immediate first execution', async () => {
    const pushedJobs: Array<{ queue: string; input: JobInput }> = [];
    const scheduler = new CronScheduler();

    scheduler.setPushCallback(async (queue, input) => {
      pushedJobs.push({ queue, input });
    });
    scheduler.setPersistCallback(() => {});

    // Add a cron with immediately=true
    // Currently CronJobInput doesn't even have an 'immediately' field,
    // so this test documents that it needs to be added
    scheduler.add({
      name: 'immediate-cron',
      queue: 'test-queue',
      data: { type: 'immediate' },
      schedule: '0 * * * *', // hourly - but should fire once NOW
      immediately: true,
    } as any); // Cast because 'immediately' doesn't exist on CronJobInput yet

    // Tick to process
    await tickScheduler(scheduler);

    // BUG: immediately is ignored, no job is pushed (nextRun is in the future)
    // Expected: one job should be pushed immediately on creation
    expect(pushedJobs.length).toBe(1);

    scheduler.stop();
  });

  // BUG 4b: immediately should NOT fire on upsert (only first creation)
  // This is the root cause of the TCP-mode report: user passes immediately+skipMissedOnRestart,
  // and on every restart upsertJobScheduler calls add() which re-fires immediately
  it('immediately should NOT fire on upsert, only on first creation', async () => {
    const pushedJobs: Array<{ queue: string; input: JobInput }> = [];
    const scheduler = new CronScheduler();

    scheduler.setPushCallback(async (queue, input) => {
      pushedJobs.push({ queue, input });
    });
    scheduler.setPersistCallback(() => {});

    // First creation: immediately should fire
    scheduler.add({
      name: 'immediate-upsert',
      queue: 'test-queue',
      data: { type: 'immediate' },
      schedule: '0 * * * *',
      immediately: true,
    });

    await tickScheduler(scheduler);
    expect(pushedJobs.length).toBe(1);

    // Upsert (same name): immediately should NOT fire again
    const cron = scheduler.add({
      name: 'immediate-upsert',
      queue: 'test-queue',
      data: { type: 'immediate' },
      schedule: '0 * * * *',
      immediately: true,
      skipMissedOnRestart: true,
    });

    // nextRun should be in the future, NOT Date.now()
    expect(cron.nextRun).toBeGreaterThan(Date.now() + 1000);

    await tickScheduler(scheduler);
    // Should still be 1 (the first creation), not 2
    expect(pushedJobs.length).toBe(1);

    scheduler.stop();
  });

  // BUG 5: add() after load() creates duplicate with reset state
  it('add() after load() should update existing cron, not create fresh one', () => {
    const scheduler = new CronScheduler();
    scheduler.setPushCallback(async () => {});
    scheduler.setPersistCallback(() => {});

    // Simulate restart: load from DB with historical state
    scheduler.load([makeCronJob({
      name: 'upsert-cron',
      queue: 'test-queue',
      schedule: '*/10 * * * *',
      executions: 55,
      skipMissedOnRestart: true,
      nextRun: Date.now() + 300_000, // 5 min from now (correctly in future)
    })]);

    const loaded = scheduler.get('upsert-cron');
    expect(loaded?.executions).toBe(55);
    expect(loaded?.skipMissedOnRestart).toBe(true);

    // App code calls upsertJobScheduler -> add() with same name
    const updated = scheduler.add({
      name: 'upsert-cron',
      queue: 'test-queue',
      data: {},
      schedule: '*/10 * * * *',
      skipMissedOnRestart: true,
    });

    // The cron should retain its historical executions
    expect(updated.executions).toBe(55);
    // skipMissedOnRestart should still be set
    expect(updated.skipMissedOnRestart).toBe(true);

    // Only one cron should exist
    expect(scheduler.list().length).toBe(1);

    scheduler.stop();
  });

  // Integration test: full restart cycle via add() path
  it('full restart simulation: load() -> add() should not lose state', async () => {
    const pushedJobs: Array<{ queue: string }> = [];
    const persisted: Array<{ name: string; executions: number; nextRun: number }> = [];
    const scheduler = new CronScheduler();

    scheduler.setPushCallback(async (queue) => {
      pushedJobs.push({ queue });
    });
    scheduler.setPersistCallback((name, executions, nextRun) => {
      persisted.push({ name, executions, nextRun });
    });

    // Step 1: Server starts, loads cron from DB (was due 2 hours ago)
    scheduler.load([makeCronJob({
      name: 'restart-test',
      queue: 'jobs',
      nextRun: Date.now() - 7200_000,
      schedule: '0 * * * *',
      skipMissedOnRestart: true,
      executions: 200,
    })]);

    // Step 2: App initializes and calls upsertJobScheduler -> add()
    scheduler.add({
      name: 'restart-test',
      queue: 'jobs',
      data: {},
      schedule: '0 * * * *',
      skipMissedOnRestart: true,
    });

    // Step 3: tick should NOT fire (nextRun is in the future after skipMissedOnRestart)
    await tickScheduler(scheduler);

    // No jobs should have been pushed (missed runs were skipped)
    expect(pushedJobs.length).toBe(0);

    // Executions count should be preserved
    const cron = scheduler.get('restart-test');
    expect(cron?.executions).toBe(200);
    expect(cron?.nextRun).toBeGreaterThan(Date.now());

    scheduler.stop();
  });
});
