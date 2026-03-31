/**
 * Test: skipIfNoWorker feature
 *
 * When skipIfNoWorker is true on a cron job, the scheduler should
 * NOT push jobs when no workers are registered for the queue.
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

describe('skipIfNoWorker', () => {
  it('should NOT push job when no workers are registered', async () => {
    const pushedJobs: Array<{ queue: string; input: JobInput }> = [];
    const scheduler = new CronScheduler();

    scheduler.setPushCallback(async (queue, input) => {
      pushedJobs.push({ queue, input });
    });
    scheduler.setPersistCallback(() => {});
    scheduler.setWorkerCheckCallback((_queue) => false); // no workers

    scheduler.load([makeCronJob({
      name: 'no-worker-cron',
      queue: 'sync-queue',
      nextRun: Date.now() - 1000, // due
      skipIfNoWorker: true,
    })]);

    await tickScheduler(scheduler);

    // Job should NOT be pushed
    expect(pushedJobs.length).toBe(0);

    scheduler.stop();
  });

  it('should push job when workers ARE registered', async () => {
    const pushedJobs: Array<{ queue: string; input: JobInput }> = [];
    const scheduler = new CronScheduler();

    scheduler.setPushCallback(async (queue, input) => {
      pushedJobs.push({ queue, input });
    });
    scheduler.setPersistCallback(() => {});
    scheduler.setWorkerCheckCallback((_queue) => true); // workers exist

    scheduler.load([makeCronJob({
      name: 'has-worker-cron',
      queue: 'sync-queue',
      nextRun: Date.now() + 60000,
      skipIfNoWorker: true,
    })]);

    // Manually set nextRun to past to simulate cron becoming due at runtime (not a restart)
    const cron = scheduler.get('has-worker-cron');
    cron!.nextRun = Date.now() - 1000;

    await tickScheduler(scheduler);

    // Job SHOULD be pushed
    expect(pushedJobs.length).toBe(1);

    scheduler.stop();
  });

  it('should push job when skipIfNoWorker is false (default)', async () => {
    const pushedJobs: Array<{ queue: string }> = [];
    const scheduler = new CronScheduler();

    scheduler.setPushCallback(async (queue) => {
      pushedJobs.push({ queue });
    });
    scheduler.setPersistCallback(() => {});
    scheduler.setWorkerCheckCallback((_queue) => false); // no workers

    scheduler.load([makeCronJob({
      name: 'default-cron',
      queue: 'sync-queue',
      nextRun: Date.now() - 1000,
      skipIfNoWorker: false, // default: push regardless
    })]);

    await tickScheduler(scheduler);

    // Job SHOULD still be pushed (skipIfNoWorker is false)
    expect(pushedJobs.length).toBe(1);

    scheduler.stop();
  });

  it('should respect per-queue worker check', async () => {
    const pushedJobs: Array<{ queue: string }> = [];
    const scheduler = new CronScheduler();

    scheduler.setPushCallback(async (queue) => {
      pushedJobs.push({ queue });
    });
    scheduler.setPersistCallback(() => {});
    scheduler.setWorkerCheckCallback((queue) => queue === 'has-workers');

    scheduler.load([
      makeCronJob({
        name: 'cron-with-workers',
        queue: 'has-workers',
        nextRun: Date.now() + 60000,
        skipIfNoWorker: true,
      }),
      makeCronJob({
        name: 'cron-without-workers',
        queue: 'no-workers',
        nextRun: Date.now() + 60000,
        skipIfNoWorker: true,
      }),
    ]);

    // Manually set nextRun to past to simulate crons becoming due at runtime
    const cronWith = scheduler.get('cron-with-workers');
    cronWith!.nextRun = Date.now() - 1000;
    const cronWithout = scheduler.get('cron-without-workers');
    cronWithout!.nextRun = Date.now() - 1000;

    await tickScheduler(scheduler);

    // Only the queue with workers should get a job
    expect(pushedJobs.length).toBe(1);
    expect(pushedJobs[0].queue).toBe('has-workers');

    scheduler.stop();
  });

  it('cron should still advance nextRun even when skipped', async () => {
    const scheduler = new CronScheduler();

    scheduler.setPushCallback(async () => {});
    scheduler.setPersistCallback(() => {});
    scheduler.setWorkerCheckCallback(() => false);

    scheduler.load([makeCronJob({
      name: 'advance-cron',
      queue: 'q',
      nextRun: Date.now() + 60000,
      schedule: '* * * * *',
      skipIfNoWorker: true,
      executions: 5,
    })]);

    // Manually set nextRun to past to simulate cron becoming due at runtime
    const cron = scheduler.get('advance-cron');
    cron!.nextRun = Date.now() - 1000;

    await tickScheduler(scheduler);

    // nextRun should have advanced (cron state updated even though job was skipped)
    expect(cron!.nextRun).toBeGreaterThan(Date.now());
    // executions should have incremented
    expect(cron!.executions).toBe(6);

    scheduler.stop();
  });
});
