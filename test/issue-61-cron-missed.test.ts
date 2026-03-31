/**
 * Test: Issue #61 comment - Cron jobs missed during downtime
 *
 * Reproduces the bug: when cron jobs with skipMissedOnRestart=true are loaded
 * with nextRun in the past, they should be skipped to the next future run.
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

describe('Issue #61: cron missed jobs on restart', () => {
  it('with skipMissedOnRestart=true, should NOT execute past-due crons after load', async () => {
    const pushedJobs: Array<{ queue: string; input: JobInput }> = [];
    const scheduler = new CronScheduler({ checkIntervalMs: 50 });

    scheduler.setPushCallback(async (queue, input) => {
      pushedJobs.push({ queue, input });
    });
    scheduler.setPersistCallback(() => {});

    // Simulate loading a cron that was due 1 hour ago with skipMissedOnRestart
    scheduler.load([makeCronJob({
      name: 'missed-cron',
      queue: 'test-queue',
      nextRun: Date.now() - 3600_000,
      schedule: '0 * * * *',
      skipMissedOnRestart: true,
    })]);

    await tickScheduler(scheduler);

    // Should NOT execute missed run
    expect(pushedJobs.length).toBe(0);

    // nextRun should be recalculated to a future time
    const loaded = scheduler.list();
    expect(loaded[0].nextRun).toBeGreaterThan(Date.now());

    scheduler.stop();
  });

  it('with skipMissedOnRestart=false (default), SHOULD execute past-due crons', async () => {
    const pushedJobs: Array<{ queue: string; input: JobInput }> = [];
    const scheduler = new CronScheduler({ checkIntervalMs: 50 });

    scheduler.setPushCallback(async (queue, input) => {
      pushedJobs.push({ queue, input });
    });
    scheduler.setPersistCallback(() => {});

    // Default behavior: execute missed crons (catch-up)
    scheduler.load([makeCronJob({
      name: 'catchup-cron',
      queue: 'test-queue',
      nextRun: Date.now() - 3600_000,
      schedule: '0 * * * *',
      skipMissedOnRestart: false,
    })]);

    await tickScheduler(scheduler);

    // SHOULD execute the missed run (default catch-up behavior)
    expect(pushedJobs.length).toBe(1);

    scheduler.stop();
  });

  it('with skipMissedOnRestart=true on interval-based cron', async () => {
    const pushedJobs: Array<{ queue: string }> = [];
    const scheduler = new CronScheduler({ checkIntervalMs: 50 });

    scheduler.setPushCallback(async (queue) => {
      pushedJobs.push({ queue });
    });
    scheduler.setPersistCallback(() => {});

    scheduler.load([makeCronJob({
      name: 'missed-interval',
      queue: 'test-queue',
      schedule: null,
      repeatEvery: 60000,
      nextRun: Date.now() - 120_000,
      skipMissedOnRestart: true,
    })]);

    await tickScheduler(scheduler);

    expect(pushedJobs.length).toBe(0);

    const loaded = scheduler.list();
    expect(loaded[0].nextRun).toBeGreaterThan(Date.now());

    scheduler.stop();
  });
});
