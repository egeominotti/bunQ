/**
 * Test: Issue #67 - skipIfNoWorker still not working properly on restart
 *
 * Bug: When a cron job has skipIfNoWorker: true and the server restarts
 * with past-due nextRun, the missed cron fires immediately because the
 * worker reconnects before the tick fires. The load() method only checks
 * skipMissedOnRestart to recalculate nextRun, not skipIfNoWorker.
 *
 * Expected: When skipIfNoWorker is true and nextRun is past-due on load(),
 * nextRun should be recalculated to the next future occurrence (same as
 * skipMissedOnRestart behavior), preventing missed crons from firing.
 */
import { describe, it, expect } from 'bun:test';
import { CronScheduler } from '../src/infrastructure/scheduler/cronScheduler';
import type { CronJob } from '../src/domain/types/cron';
import type { JobInput } from '../src/domain/types/job';

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
    skipIfNoWorker: false,
    ...overrides,
  };
}

describe('Issue #67: skipIfNoWorker on restart', () => {
  it('should recalculate nextRun on load() when skipIfNoWorker is true and nextRun is past-due', () => {
    const scheduler = new CronScheduler();
    const persisted: Array<{ name: string; executions: number; nextRun: number }> = [];

    scheduler.setPushCallback(async () => {});
    scheduler.setPersistCallback((name, executions, nextRun) => {
      persisted.push({ name, executions, nextRun });
    });
    scheduler.setWorkerCheckCallback(() => false); // no workers initially

    const now = Date.now();
    // Simulate restart: cron has past-due nextRun (missed 2 minutes ago)
    scheduler.load([makeCronJob({
      name: 'long-worker-cron',
      queue: 'sync-queue',
      schedule: '* * * * *',
      nextRun: now - 120_000, // 2 minutes in the past
      skipIfNoWorker: true,
      executions: 10,
    })]);

    // nextRun should have been recalculated to the future
    const cron = scheduler.get('long-worker-cron');
    expect(cron).toBeDefined();
    expect(cron!.nextRun).toBeGreaterThan(now);

    // Should have persisted the recalculated nextRun
    expect(persisted.length).toBe(1);
    expect(persisted[0].nextRun).toBeGreaterThan(now);
    // Execution count should be preserved (not incremented)
    expect(persisted[0].executions).toBe(10);

    scheduler.stop();
  });

  it('should NOT fire missed cron on restart even if worker reconnects before tick', async () => {
    const pushedJobs: Array<{ queue: string; input: JobInput }> = [];
    const scheduler = new CronScheduler();
    let workerRegistered = false;

    scheduler.setPushCallback(async (queue, input) => {
      pushedJobs.push({ queue, input });
    });
    scheduler.setPersistCallback(() => {});
    scheduler.setWorkerCheckCallback(() => workerRegistered);

    const now = Date.now();
    // Simulate restart: cron missed while server was down
    scheduler.load([makeCronJob({
      name: 'restart-cron',
      queue: 'sync-queue',
      schedule: '* * * * *',
      nextRun: now - 60_000, // 1 minute in the past
      skipIfNoWorker: true,
    })]);

    // Simulate worker reconnecting before tick fires
    workerRegistered = true;

    // Tick should NOT fire the missed cron (nextRun should have been advanced)
    await tickScheduler(scheduler);

    expect(pushedJobs.length).toBe(0);

    scheduler.stop();
  });

  it('should still fire on schedule after nextRun is recalculated on load', async () => {
    const pushedJobs: Array<{ queue: string }> = [];
    const scheduler = new CronScheduler();

    scheduler.setPushCallback(async (queue) => {
      pushedJobs.push({ queue });
    });
    scheduler.setPersistCallback(() => {});
    scheduler.setWorkerCheckCallback(() => true); // workers exist

    const now = Date.now();
    // Simulate restart with past-due cron
    scheduler.load([makeCronJob({
      name: 'schedule-cron',
      queue: 'sync-queue',
      repeatEvery: 60_000,
      schedule: null,
      nextRun: now - 120_000, // past due
      skipIfNoWorker: true,
    })]);

    // nextRun should be in the future
    const cron = scheduler.get('schedule-cron');
    expect(cron!.nextRun).toBeGreaterThan(now);

    // Manually set nextRun to now to simulate next scheduled time arriving
    cron!.nextRun = now - 1;

    // Now tick should fire the job (worker is registered, not a missed run)
    await tickScheduler(scheduler);
    expect(pushedJobs.length).toBe(1);

    scheduler.stop();
  });

  it('should NOT recalculate nextRun when skipIfNoWorker is false', () => {
    const scheduler = new CronScheduler();

    scheduler.setPushCallback(async () => {});
    scheduler.setPersistCallback(() => {});

    const now = Date.now();
    const pastDue = now - 60_000;

    scheduler.load([makeCronJob({
      name: 'no-skip-cron',
      queue: 'sync-queue',
      nextRun: pastDue,
      skipIfNoWorker: false,
      skipMissedOnRestart: false,
    })]);

    // nextRun should NOT have been recalculated
    const cron = scheduler.get('no-skip-cron');
    expect(cron!.nextRun).toBe(pastDue);

    scheduler.stop();
  });

  it('should handle both skipIfNoWorker and skipMissedOnRestart together', () => {
    const scheduler = new CronScheduler();
    const persisted: Array<{ name: string; nextRun: number }> = [];

    scheduler.setPushCallback(async () => {});
    scheduler.setPersistCallback((name, _exec, nextRun) => {
      persisted.push({ name, nextRun });
    });
    scheduler.setWorkerCheckCallback(() => false);

    const now = Date.now();
    scheduler.load([makeCronJob({
      name: 'both-flags-cron',
      queue: 'sync-queue',
      nextRun: now - 120_000,
      skipIfNoWorker: true,
      skipMissedOnRestart: true,
    })]);

    const cron = scheduler.get('both-flags-cron');
    expect(cron!.nextRun).toBeGreaterThan(now);
    // Should only persist once (not double-persist)
    expect(persisted.length).toBe(1);

    scheduler.stop();
  });
});
