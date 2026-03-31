/**
 * Test: Issue #65 - skipMissedOnRestart in TCP/server mode
 *
 * Simulates the full server restart cycle:
 * 1. Server starts, cron is created and runs
 * 2. Server stops (simulated)
 * 3. Server restarts with stale cron in DB (nextRun in the past)
 * 4. Verify: missed runs are NOT executed when skipMissedOnRestart=true
 *
 * This tests the QueueManager.addCron() -> CronScheduler.add() -> storage.saveCron()
 * flow that the TCP Cron command uses.
 */
import { describe, it, expect } from 'bun:test';
import { CronScheduler } from '../src/infrastructure/scheduler/cronScheduler';
import type { CronJob, CronJobInput } from '../src/domain/types/cron';
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
    ...overrides,
  };
}

describe('Issue #65: TCP mode - full server restart simulation', () => {
  /**
   * Simulates the exact QueueManager flow:
   * 1. Constructor: setPersistCallback -> load(storage.loadCronJobs()) -> startBackgroundTasks()
   * 2. Client request: handleCron -> addCron() -> add() + saveCron()
   */
  it('server restart: load() then addCron() should not fire missed runs', async () => {
    const pushedJobs: Array<{ queue: string; input: JobInput }> = [];
    const persisted: Array<{ name: string; executions: number; nextRun: number }> = [];
    const savedCrons: CronJob[] = [];

    const scheduler = new CronScheduler();

    // Step 1: setPersistCallback (like QueueManager constructor line 160)
    scheduler.setPushCallback(async (queue, input) => {
      pushedJobs.push({ queue, input });
    });
    scheduler.setPersistCallback((name, executions, nextRun) => {
      persisted.push({ name, executions, nextRun });
    });

    // Step 2: load() from DB (like QueueManager constructor line 180)
    // Simulates: server was down for 2 hours, cron was due 2h ago
    const dbCrons: CronJob[] = [makeCronJob({
      name: 'my-scheduler',
      queue: 'my-queue',
      nextRun: Date.now() - 7200_000, // 2 hours overdue
      schedule: '* * * * *',
      skipMissedOnRestart: true,
      executions: 50,
      timezone: 'America/New_York',
    })];

    scheduler.load(dbCrons);

    // Verify load() adjusted nextRun and persisted it
    const afterLoad = scheduler.get('my-scheduler');
    expect(afterLoad).toBeDefined();
    expect(afterLoad!.nextRun).toBeGreaterThan(Date.now());
    expect(afterLoad!.executions).toBe(50);

    // Verify load() persisted the adjusted nextRun
    expect(persisted.length).toBe(1);
    expect(persisted[0].nextRun).toBeGreaterThan(Date.now());
    expect(persisted[0].executions).toBe(50);

    // Step 3: startBackgroundTasks -> cronScheduler.start() (line 182)
    scheduler.start();

    // Step 4: Client sends TCP Cron command -> handleCron -> addCron()
    // This is what happens when the client app calls upsertJobScheduler in TCP mode
    const cronInput: CronJobInput = {
      name: 'my-scheduler',
      queue: 'my-queue',
      data: { task: 'sync' },
      schedule: '* * * * *',
      skipMissedOnRestart: true,
      timezone: 'America/New_York',
    };

    const cron = scheduler.add(cronInput);

    // Simulate saveCron (like QueueManager.addCron line 1005)
    savedCrons.push(cron);

    // Verify: executions preserved
    expect(cron.executions).toBe(50);
    // Verify: nextRun is in the future
    expect(cron.nextRun).toBeGreaterThan(Date.now());
    // Verify: skipMissedOnRestart is set
    expect(cron.skipMissedOnRestart).toBe(true);

    // Step 5: tick should NOT fire (nextRun is in the future)
    await tickScheduler(scheduler);
    expect(pushedJobs.length).toBe(0);

    // Verify saveCron would write correct data
    expect(savedCrons[0].executions).toBe(50);
    expect(savedCrons[0].nextRun).toBeGreaterThan(Date.now());

    scheduler.stop();
  });

  it('server restart: load() without subsequent add() should not fire missed runs', async () => {
    const pushedJobs: Array<{ queue: string }> = [];
    const persisted: Array<{ name: string; nextRun: number }> = [];

    const scheduler = new CronScheduler();
    scheduler.setPushCallback(async (queue) => { pushedJobs.push({ queue }); });
    scheduler.setPersistCallback((name, _exec, nextRun) => { persisted.push({ name, nextRun }); });

    // Server restarts, loads cron from DB - overdue by 30 min
    scheduler.load([makeCronJob({
      name: 'standalone-cron',
      queue: 'jobs',
      nextRun: Date.now() - 1800_000,
      schedule: '0 * * * *',
      skipMissedOnRestart: true,
      executions: 100,
    })]);

    scheduler.start();

    // tick immediately - should NOT fire
    await tickScheduler(scheduler);
    expect(pushedJobs.length).toBe(0);

    // nextRun should be in the future
    const cron = scheduler.get('standalone-cron');
    expect(cron!.nextRun).toBeGreaterThan(Date.now());
    expect(cron!.executions).toBe(100);

    // DB should be updated
    expect(persisted.length).toBe(1);
    expect(persisted[0].nextRun).toBeGreaterThan(Date.now());

    scheduler.stop();
  });

  it('server restart with interval-based cron: load() then add() preserves state', async () => {
    const pushedJobs: Array<{ queue: string }> = [];

    const scheduler = new CronScheduler();
    scheduler.setPushCallback(async (queue) => { pushedJobs.push({ queue }); });
    scheduler.setPersistCallback(() => {});

    // Load interval-based cron from DB (overdue)
    scheduler.load([makeCronJob({
      name: 'interval-cron',
      queue: 'tasks',
      schedule: null,
      repeatEvery: 60000,
      nextRun: Date.now() - 300_000, // 5 min overdue
      skipMissedOnRestart: true,
      executions: 75,
    })]);

    scheduler.start();

    // Client upserts via TCP
    const cron = scheduler.add({
      name: 'interval-cron',
      queue: 'tasks',
      data: {},
      repeatEvery: 60000,
      skipMissedOnRestart: true,
    });

    expect(cron.executions).toBe(75);
    expect(cron.nextRun).toBeGreaterThan(Date.now());

    await tickScheduler(scheduler);
    expect(pushedJobs.length).toBe(0);

    scheduler.stop();
  });

  it('multiple server restarts: DB state stays consistent', async () => {
    // Simulate 3 consecutive restarts
    for (let restart = 0; restart < 3; restart++) {
      const persisted: Array<{ name: string; executions: number; nextRun: number }> = [];
      const pushedJobs: Array<{ queue: string }> = [];

      const scheduler = new CronScheduler();
      scheduler.setPushCallback(async (queue) => { pushedJobs.push({ queue }); });
      scheduler.setPersistCallback((name, executions, nextRun) => {
        persisted.push({ name, executions, nextRun });
      });

      // Each restart loads the same stale DB state
      scheduler.load([makeCronJob({
        name: 'multi-restart',
        queue: 'q',
        nextRun: Date.now() - 3600_000, // always 1h overdue
        schedule: '0 * * * *',
        skipMissedOnRestart: true,
        executions: 200 + restart, // increments each restart to track
      })]);

      scheduler.start();
      await tickScheduler(scheduler);

      // Never fires missed runs
      expect(pushedJobs.length).toBe(0);

      // Always persists adjusted nextRun
      expect(persisted.length).toBe(1);
      expect(persisted[0].nextRun).toBeGreaterThan(Date.now());
      expect(persisted[0].executions).toBe(200 + restart);

      scheduler.stop();
    }
  });

  it('without skipMissedOnRestart: missed runs SHOULD fire after restart', async () => {
    const pushedJobs: Array<{ queue: string }> = [];

    const scheduler = new CronScheduler();
    scheduler.setPushCallback(async (queue) => { pushedJobs.push({ queue }); });
    scheduler.setPersistCallback(() => {});

    // Load cron WITHOUT skipMissedOnRestart (default catch-up behavior)
    scheduler.load([makeCronJob({
      name: 'catchup-cron',
      queue: 'jobs',
      nextRun: Date.now() - 3600_000,
      schedule: '0 * * * *',
      skipMissedOnRestart: false,
      executions: 10,
    })]);

    scheduler.start();
    await tickScheduler(scheduler);

    // SHOULD fire the missed run
    expect(pushedJobs.length).toBe(1);

    scheduler.stop();
  });
});
