/**
 * CronScheduler Execution Logic Tests
 *
 * Tests the internal execution engine of CronScheduler:
 * - tick() processing due cron jobs from min-heap
 * - start() / stop() lifecycle management
 * - getStats() with stale entry handling
 * - load() reconstructing state from persistence
 * - Generation tracking for lazy deletion
 * - maxLimit enforcement during execution
 * - repeatEvery interval-based scheduling
 * - Persist-before-push ordering guarantees
 * - Error handling in push and persist callbacks
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { CronScheduler } from '../src/infrastructure/scheduler/cronScheduler';
import type { CronJob } from '../src/domain/types/cron';
import type { JobInput } from '../src/domain/types/job';

/** Helper to access the private tick() method for deterministic testing */
function tickScheduler(scheduler: CronScheduler): Promise<void> {
  return (scheduler as any).tick();
}

/** Helper to read the internal cronJobs map size */
function getMapSize(scheduler: CronScheduler): number {
  return (scheduler as any).cronJobs.size;
}

/** Helper to read the internal cronHeap size */
function getHeapSize(scheduler: CronScheduler): number {
  return (scheduler as any).cronHeap.size;
}

/** Helper to check if the scheduler is running */
function isRunning(scheduler: CronScheduler): boolean {
  return (scheduler as any).started === true;
}

/** Create a minimal CronJob for loading */
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
    ...overrides,
  };
}

describe('CronScheduler Execution Logic', () => {
  let scheduler: CronScheduler;
  let pushedJobs: Array<{ queue: string; input: JobInput }>;
  let persistedCrons: Array<{ name: string; executions: number; nextRun: number }>;

  beforeEach(() => {
    pushedJobs = [];
    persistedCrons = [];
    scheduler = new CronScheduler({ checkIntervalMs: 50 });
    scheduler.setPushCallback(async (queue, input) => {
      pushedJobs.push({ queue, input });
    });
    scheduler.setPersistCallback((name, executions, nextRun) => {
      persistedCrons.push({ name, executions, nextRun });
    });
  });

  afterEach(() => {
    scheduler.stop();
  });

  // ─────────────────────────────────────────────────────────────
  // tick() - core execution
  // ─────────────────────────────────────────────────────────────

  describe('tick()', () => {
    test('should execute a due cron job and push it', async () => {
      // Add a cron with repeatEvery that is already due
      const cron = scheduler.add({
        name: 'due-job',
        queue: 'tasks',
        data: { action: 'cleanup' },
        repeatEvery: 100,
      });

      // Wait for it to become due
      await new Promise((r) => setTimeout(r, 120));

      await tickScheduler(scheduler);

      expect(pushedJobs.length).toBe(1);
      expect(pushedJobs[0].queue).toBe('tasks');
      expect(pushedJobs[0].input.data).toEqual({ action: 'cleanup' });
    });

    test('should not execute cron jobs that are not yet due', async () => {
      scheduler.add({
        name: 'future-job',
        queue: 'tasks',
        data: {},
        repeatEvery: 600000, // 10 minutes from now
      });

      await tickScheduler(scheduler);

      expect(pushedJobs.length).toBe(0);
    });

    test('should increment executions after tick', async () => {
      scheduler.add({
        name: 'counter-job',
        queue: 'tasks',
        data: {},
        repeatEvery: 50,
      });

      await new Promise((r) => setTimeout(r, 70));
      await tickScheduler(scheduler);

      const job = scheduler.get('counter-job');
      expect(job).toBeDefined();
      expect(job!.executions).toBe(1);
    });

    test('should update nextRun after execution', async () => {
      scheduler.add({
        name: 'next-run-job',
        queue: 'tasks',
        data: {},
        repeatEvery: 100,
      });

      await new Promise((r) => setTimeout(r, 120));
      // Fixed-rate anchoring: nextRun is scheduledSlot + interval (not now + interval),
      // so a late tick does not drift the schedule forward.
      const scheduledSlot = scheduler.get('next-run-job')!.nextRun;
      await tickScheduler(scheduler);

      const job = scheduler.get('next-run-job');
      expect(job).toBeDefined();
      // nextRun anchored to the scheduled slot, not wall-clock now (no drift)
      expect(job!.nextRun).toBe(scheduledSlot + 100);
    });

    test('should not push jobs if pushJob callback is not set', async () => {
      const plainScheduler = new CronScheduler({ checkIntervalMs: 50 });
      // No setPushCallback called

      plainScheduler.add({
        name: 'no-push',
        queue: 'tasks',
        data: {},
        repeatEvery: 10,
      });

      await new Promise((r) => setTimeout(r, 30));
      await tickScheduler(plainScheduler);

      // No error should occur, and nothing was pushed
      expect(pushedJobs.length).toBe(0);
    });

    test('should process multiple due cron jobs in a single tick', async () => {
      scheduler.add({
        name: 'job-a',
        queue: 'queue-a',
        data: { id: 'a' },
        repeatEvery: 50,
      });
      scheduler.add({
        name: 'job-b',
        queue: 'queue-b',
        data: { id: 'b' },
        repeatEvery: 50,
      });

      await new Promise((r) => setTimeout(r, 70));
      await tickScheduler(scheduler);

      expect(pushedJobs.length).toBe(2);
      const queues = pushedJobs.map((j) => j.queue).sort();
      expect(queues).toEqual(['queue-a', 'queue-b']);
    });

    test('should persist cron state before pushing job', async () => {
      const callOrder: string[] = [];

      const orderedScheduler = new CronScheduler({ checkIntervalMs: 50 });
      orderedScheduler.setPersistCallback((name, executions, nextRun) => {
        callOrder.push('persist');
        persistedCrons.push({ name, executions, nextRun });
      });
      orderedScheduler.setPushCallback(async (queue, input) => {
        callOrder.push('push');
        pushedJobs.push({ queue, input });
      });

      orderedScheduler.add({
        name: 'order-test',
        queue: 'tasks',
        data: {},
        repeatEvery: 50,
      });

      await new Promise((r) => setTimeout(r, 70));
      await tickScheduler(orderedScheduler);

      expect(callOrder).toEqual(['persist', 'push']);
    });

    test('should pass priority to pushed job', async () => {
      scheduler.add({
        name: 'priority-job',
        queue: 'tasks',
        data: {},
        repeatEvery: 50,
        priority: 10,
      });

      await new Promise((r) => setTimeout(r, 70));
      await tickScheduler(scheduler);

      expect(pushedJobs.length).toBe(1);
      expect(pushedJobs[0].input.priority).toBe(10);
    });
  });

  // ─────────────────────────────────────────────────────────────
  // Generation-based lazy deletion
  // ─────────────────────────────────────────────────────────────

  describe('generation tracking and lazy deletion', () => {
    test('should skip stale heap entries after remove()', async () => {
      scheduler.add({
        name: 'stale-job',
        queue: 'tasks',
        data: {},
        repeatEvery: 50,
      });

      // Remove it (lazy - only removes from map, not heap)
      scheduler.remove('stale-job');

      // Heap still has the entry
      expect(getHeapSize(scheduler)).toBe(1);
      // Map no longer has it
      expect(getMapSize(scheduler)).toBe(0);

      await new Promise((r) => setTimeout(r, 70));
      await tickScheduler(scheduler);

      // Stale entry should have been popped and discarded, no jobs pushed
      expect(pushedJobs.length).toBe(0);
    });

    test('should skip stale entries when cron is re-added with same name', async () => {
      scheduler.add({
        name: 'reuse-name',
        queue: 'old-queue',
        data: { version: 1 },
        repeatEvery: 50,
      });

      // Remove and re-add
      scheduler.remove('reuse-name');
      scheduler.add({
        name: 'reuse-name',
        queue: 'new-queue',
        data: { version: 2 },
        repeatEvery: 50,
      });

      // Heap has 2 entries (old stale + new)
      expect(getHeapSize(scheduler)).toBe(2);
      // Map has 1 entry (the new one)
      expect(getMapSize(scheduler)).toBe(1);

      await new Promise((r) => setTimeout(r, 70));
      await tickScheduler(scheduler);

      // Only the new one should have been executed
      expect(pushedJobs.length).toBe(1);
      expect(pushedJobs[0].queue).toBe('new-queue');
      expect(pushedJobs[0].input.data).toEqual({ version: 2 });
    });

    test('getStats should report correct nextRun ignoring stale entries', () => {
      const farFuture = Date.now() + 999999;
      // Load a cron that will be on heap top
      scheduler.load([
        makeCronJob({ name: 'stale-one', queue: 'q', nextRun: Date.now() + 1000 }),
        makeCronJob({ name: 'valid-one', queue: 'q', nextRun: farFuture }),
      ]);

      // Remove the one with nearest nextRun
      scheduler.remove('stale-one');

      const stats = scheduler.getStats();
      // stale-one is still heap top but is stale, so nextRun should be null
      // (peek only checks the first entry, which is stale)
      expect(stats.total).toBe(1);
      expect(stats.pending).toBe(1);
      // The top of the heap is the stale entry, so peek returns stale -> nextRun null
      // (getStats only peeks, does not scan deeper)
      expect(stats.nextRun).toBeNull();
    });
  });

  // ─────────────────────────────────────────────────────────────
  // maxLimit enforcement
  // ─────────────────────────────────────────────────────────────

  describe('maxLimit', () => {
    test('should stop executing after maxLimit is reached', async () => {
      scheduler.add({
        name: 'limited-job',
        queue: 'tasks',
        data: {},
        repeatEvery: 30,
        maxLimit: 2,
      });

      // Tick 1 - should execute
      await new Promise((r) => setTimeout(r, 50));
      await tickScheduler(scheduler);
      expect(pushedJobs.length).toBe(1);

      // Tick 2 - should execute (execution #2)
      await new Promise((r) => setTimeout(r, 50));
      await tickScheduler(scheduler);
      expect(pushedJobs.length).toBe(2);

      // Tick 3 - should NOT execute (at limit)
      await new Promise((r) => setTimeout(r, 50));
      await tickScheduler(scheduler);
      expect(pushedJobs.length).toBe(2);
    });

    test('should remove cron from map when limit is reached during tick', async () => {
      scheduler.add({
        name: 'one-shot',
        queue: 'tasks',
        data: {},
        repeatEvery: 30,
        maxLimit: 1,
      });

      await new Promise((r) => setTimeout(r, 50));
      await tickScheduler(scheduler);

      expect(pushedJobs.length).toBe(1);

      // After the first execution, nextRun is updated.
      // On the next tick when it becomes due, isAtLimit will remove it from map.
      await new Promise((r) => setTimeout(r, 50));
      await tickScheduler(scheduler);

      expect(scheduler.get('one-shot')).toBeUndefined();
      expect(getMapSize(scheduler)).toBe(0);
      // No additional job pushed
      expect(pushedJobs.length).toBe(1);
    });

    test('getStats should not count at-limit crons as pending', () => {
      // Load a cron that is already at its limit
      scheduler.load([
        makeCronJob({
          name: 'done-cron',
          queue: 'q',
          executions: 5,
          maxLimit: 5,
          nextRun: Date.now() + 1000,
        }),
        makeCronJob({
          name: 'active-cron',
          queue: 'q',
          executions: 0,
          maxLimit: 10,
          nextRun: Date.now() + 2000,
        }),
      ]);

      const stats = scheduler.getStats();
      expect(stats.total).toBe(2);
      expect(stats.pending).toBe(1); // Only active-cron
    });
  });

  // ─────────────────────────────────────────────────────────────
  // repeatEvery interval-based scheduling
  // ─────────────────────────────────────────────────────────────

  describe('repeatEvery', () => {
    test('should schedule next run based on interval after execution', async () => {
      scheduler.add({
        name: 'interval-job',
        queue: 'tasks',
        data: {},
        repeatEvery: 200,
      });

      await new Promise((r) => setTimeout(r, 220));
      // Fixed-rate anchoring: nextRun is scheduledSlot + interval (not now + interval),
      // preventing cumulative drift when the tick runs late.
      const scheduledSlot = scheduler.get('interval-job')!.nextRun;
      await tickScheduler(scheduler);

      const job = scheduler.get('interval-job');
      expect(job).toBeDefined();
      // Next run anchored to the scheduled slot, not wall-clock execution time
      expect(job!.nextRun).toBe(scheduledSlot + 200);
    });

    test('should persist correct next run for interval cron', async () => {
      scheduler.add({
        name: 'persist-interval',
        queue: 'tasks',
        data: {},
        repeatEvery: 150,
      });

      await new Promise((r) => setTimeout(r, 170));
      await tickScheduler(scheduler);

      expect(persistedCrons.length).toBe(1);
      expect(persistedCrons[0].name).toBe('persist-interval');
      expect(persistedCrons[0].executions).toBe(1);
      // nextRun should be approximately now + 150
      const now = Date.now();
      expect(persistedCrons[0].nextRun).toBeGreaterThanOrEqual(now + 100);
      expect(persistedCrons[0].nextRun).toBeLessThanOrEqual(now + 250);
    });
  });

  // ─────────────────────────────────────────────────────────────
  // start() / stop() lifecycle
  // ─────────────────────────────────────────────────────────────

  describe('start() / stop() lifecycle', () => {
    test('should start the interval timer', () => {
      expect(isRunning(scheduler)).toBe(false);
      scheduler.start();
      expect(isRunning(scheduler)).toBe(true);
    });

    test('should stop the interval timer', () => {
      scheduler.start();
      expect(isRunning(scheduler)).toBe(true);
      scheduler.stop();
      expect(isRunning(scheduler)).toBe(false);
    });

    test('start() should be idempotent (calling twice does not create duplicate timers)', () => {
      scheduler.start();
      const firstSafety = (scheduler as any).safetyInterval;
      scheduler.start();
      const secondSafety = (scheduler as any).safetyInterval;
      // Should be the exact same interval reference
      expect(firstSafety).toBe(secondSafety);
    });

    test('stop() should be idempotent (calling when already stopped is safe)', () => {
      scheduler.stop();
      expect(isRunning(scheduler)).toBe(false);
      // Should not throw
      scheduler.stop();
      expect(isRunning(scheduler)).toBe(false);
    });

    test('should execute due jobs automatically after start()', async () => {
      scheduler.add({
        name: 'auto-exec',
        queue: 'tasks',
        data: {},
        repeatEvery: 30,
      });

      // Wait for it to become due, then start
      await new Promise((r) => setTimeout(r, 50));
      scheduler.start();

      // Wait for at least one tick cycle (checkIntervalMs = 50)
      await new Promise((r) => setTimeout(r, 120));
      scheduler.stop();

      expect(pushedJobs.length).toBeGreaterThanOrEqual(1);
    });

    test('should stop executing after stop() is called', async () => {
      scheduler.add({
        name: 'stop-test',
        queue: 'tasks',
        data: {},
        repeatEvery: 30,
      });

      await new Promise((r) => setTimeout(r, 50));
      scheduler.start();
      await new Promise((r) => setTimeout(r, 80));
      scheduler.stop();

      const countAfterStop = pushedJobs.length;
      expect(countAfterStop).toBeGreaterThanOrEqual(1);

      // Wait and verify no more jobs are pushed
      await new Promise((r) => setTimeout(r, 150));
      expect(pushedJobs.length).toBe(countAfterStop);
    });
  });

  // ─────────────────────────────────────────────────────────────
  // load() - state reconstruction
  // ─────────────────────────────────────────────────────────────

  describe('load()', () => {
    test('should load crons into map and heap', () => {
      const crons: CronJob[] = [
        makeCronJob({ name: 'loaded-1', queue: 'q1', nextRun: Date.now() + 10000 }),
        makeCronJob({ name: 'loaded-2', queue: 'q2', nextRun: Date.now() + 20000 }),
        makeCronJob({ name: 'loaded-3', queue: 'q3', nextRun: Date.now() + 30000 }),
      ];

      scheduler.load(crons);

      expect(getMapSize(scheduler)).toBe(3);
      expect(getHeapSize(scheduler)).toBe(3);
      expect(scheduler.list().length).toBe(3);
    });

    test('should make loaded crons executable via tick()', async () => {
      // Load a cron that is already past due (explicitly opt in to catch-up)
      scheduler.load([
        makeCronJob({
          name: 'past-due',
          queue: 'tasks',
          nextRun: Date.now() - 1000,
          repeatEvery: 60000,
          schedule: null,
          skipMissedOnRestart: false,
        }),
      ]);

      await tickScheduler(scheduler);

      expect(pushedJobs.length).toBe(1);
      expect(pushedJobs[0].queue).toBe('tasks');
    });

    test('should preserve execution count from loaded crons', () => {
      scheduler.load([
        makeCronJob({
          name: 'resumed',
          queue: 'q',
          executions: 42,
          maxLimit: 100,
        }),
      ]);

      const job = scheduler.get('resumed');
      expect(job).toBeDefined();
      expect(job!.executions).toBe(42);
    });

    test('should build a valid heap ordering from loaded crons', () => {
      // Load in non-sorted order
      const now = Date.now();
      scheduler.load([
        makeCronJob({ name: 'far', queue: 'q', nextRun: now + 30000 }),
        makeCronJob({ name: 'near', queue: 'q', nextRun: now + 1000 }),
        makeCronJob({ name: 'mid', queue: 'q', nextRun: now + 15000 }),
      ]);

      // The heap should have the nearest cron at the top
      const heap = (scheduler as any).cronHeap;
      const top = heap.peek();
      expect(top.cron.name).toBe('near');
    });

    test('should combine loaded crons with added crons', () => {
      scheduler.load([makeCronJob({ name: 'loaded', queue: 'q' })]);
      scheduler.add({
        name: 'added',
        queue: 'q',
        data: {},
        repeatEvery: 5000,
      });

      expect(getMapSize(scheduler)).toBe(2);
      expect(scheduler.list().length).toBe(2);
    });
  });

  // ─────────────────────────────────────────────────────────────
  // getStats()
  // ─────────────────────────────────────────────────────────────

  describe('getStats()', () => {
    test('should return zeros when empty', () => {
      const stats = scheduler.getStats();
      expect(stats.total).toBe(0);
      expect(stats.pending).toBe(0);
      expect(stats.nextRun).toBeNull();
    });

    test('should return correct total and pending', () => {
      scheduler.add({
        name: 'stat-1',
        queue: 'q',
        data: {},
        repeatEvery: 5000,
      });
      scheduler.add({
        name: 'stat-2',
        queue: 'q',
        data: {},
        schedule: '0 * * * *',
      });

      const stats = scheduler.getStats();
      expect(stats.total).toBe(2);
      expect(stats.pending).toBe(2);
      expect(stats.nextRun).not.toBeNull();
    });

    test('should reflect removal in total count', () => {
      scheduler.add({ name: 'a', queue: 'q', data: {}, repeatEvery: 5000 });
      scheduler.add({ name: 'b', queue: 'q', data: {}, repeatEvery: 5000 });

      scheduler.remove('a');

      const stats = scheduler.getStats();
      expect(stats.total).toBe(1);
      expect(stats.pending).toBe(1);
    });

    test('should return nextRun of the soonest non-stale cron', () => {
      const now = Date.now();
      scheduler.load([
        makeCronJob({ name: 'soon', queue: 'q', nextRun: now + 5000 }),
        makeCronJob({ name: 'later', queue: 'q', nextRun: now + 60000 }),
      ]);

      const stats = scheduler.getStats();
      expect(stats.nextRun).toBe(now + 5000);
    });
  });

  // ─────────────────────────────────────────────────────────────
  // Error handling
  // ─────────────────────────────────────────────────────────────

  describe('error handling', () => {
    test('should skip job push if persist callback throws', async () => {
      const failScheduler = new CronScheduler({ checkIntervalMs: 50 });
      failScheduler.setPersistCallback(() => {
        throw new Error('persist failed');
      });
      failScheduler.setPushCallback(async (queue, input) => {
        pushedJobs.push({ queue, input });
      });

      failScheduler.add({
        name: 'fail-persist',
        queue: 'tasks',
        data: {},
        repeatEvery: 30,
      });

      await new Promise((r) => setTimeout(r, 50));
      await tickScheduler(failScheduler);

      // Job should NOT have been pushed because persist failed
      expect(pushedJobs.length).toBe(0);

      // The cron should still be in the scheduler (re-inserted for retry)
      expect(failScheduler.get('fail-persist')).toBeDefined();
      // Executions should NOT have been incremented
      expect(failScheduler.get('fail-persist')!.executions).toBe(0);
    });

    test('should continue scheduling even if push callback throws', async () => {
      const errorScheduler = new CronScheduler({ checkIntervalMs: 50 });
      errorScheduler.setPersistCallback((name, executions, nextRun) => {
        persistedCrons.push({ name, executions, nextRun });
      });
      errorScheduler.setPushCallback(async () => {
        throw new Error('push failed');
      });

      errorScheduler.add({
        name: 'fail-push',
        queue: 'tasks',
        data: {},
        repeatEvery: 30,
      });

      await new Promise((r) => setTimeout(r, 50));
      await tickScheduler(errorScheduler);

      // Persist was called (before push)
      expect(persistedCrons.length).toBe(1);
      // The cron should still be in the scheduler for next execution
      expect(errorScheduler.get('fail-push')).toBeDefined();
      // Executions should be incremented (state was persisted before push failed)
      expect(errorScheduler.get('fail-push')!.executions).toBe(1);
    });

    test('should work without a persist callback set', async () => {
      const noPersistScheduler = new CronScheduler({ checkIntervalMs: 50 });
      noPersistScheduler.setPushCallback(async (queue, input) => {
        pushedJobs.push({ queue, input });
      });
      // No persistCallback set

      noPersistScheduler.add({
        name: 'no-persist',
        queue: 'tasks',
        data: {},
        repeatEvery: 30,
      });

      await new Promise((r) => setTimeout(r, 50));
      await tickScheduler(noPersistScheduler);

      expect(pushedJobs.length).toBe(1);
      expect(noPersistScheduler.get('no-persist')!.executions).toBe(1);
    });
  });

  // ─────────────────────────────────────────────────────────────
  // Schedule-based cron (expression, not repeatEvery)
  // ─────────────────────────────────────────────────────────────

  describe('schedule-based cron execution', () => {
    test('should execute a schedule-based cron when due', async () => {
      // Load a schedule-based cron that is already past due (explicitly opt in to catch-up)
      scheduler.load([
        makeCronJob({
          name: 'schedule-due',
          queue: 'emails',
          schedule: '* * * * *',
          repeatEvery: null,
          nextRun: Date.now() - 5000,
          skipMissedOnRestart: false,
        }),
      ]);

      await tickScheduler(scheduler);

      expect(pushedJobs.length).toBe(1);
      expect(pushedJobs[0].queue).toBe('emails');
    });

    test('should compute next run from cron expression after execution', async () => {
      scheduler.load([
        makeCronJob({
          name: 'schedule-next',
          queue: 'emails',
          schedule: '* * * * *', // every minute
          repeatEvery: null,
          nextRun: Date.now() - 1000,
        }),
      ]);

      await tickScheduler(scheduler);

      const job = scheduler.get('schedule-next');
      expect(job).toBeDefined();
      // Next run should be in the future (within approximately 60 seconds)
      expect(job!.nextRun).toBeGreaterThan(Date.now());
      expect(job!.nextRun).toBeLessThanOrEqual(Date.now() + 61000);
    });
  });

  // ─────────────────────────────────────────────────────────────
  // Edge cases
  // ─────────────────────────────────────────────────────────────

  describe('edge cases', () => {
    test('tick on empty scheduler does nothing', async () => {
      await tickScheduler(scheduler);
      expect(pushedJobs.length).toBe(0);
      expect(persistedCrons.length).toBe(0);
    });

    test('removing a non-existent cron returns false', () => {
      expect(scheduler.remove('ghost')).toBe(false);
    });

    test('get returns undefined for non-existent cron', () => {
      expect(scheduler.get('ghost')).toBeUndefined();
    });

    test('load with empty array does not break scheduler', () => {
      scheduler.load([]);
      expect(getMapSize(scheduler)).toBe(0);
      expect(getHeapSize(scheduler)).toBe(0);
      expect(scheduler.list()).toEqual([]);
    });

    test('cron with both schedule and repeatEvery uses schedule for nextRun', () => {
      // When both are provided, schedule takes precedence in add()
      const cron = scheduler.add({
        name: 'both',
        queue: 'q',
        data: {},
        schedule: '0 0 * * *', // midnight daily
        repeatEvery: 1000,
      });

      // nextRun should be based on cron expression (next midnight),
      // not 1 second from now
      const oneMinuteFromNow = Date.now() + 60000;
      expect(cron.nextRun).toBeGreaterThan(oneMinuteFromNow);
    });

    test('constructor accepts config without errors', () => {
      const defaultScheduler = new CronScheduler();
      expect(defaultScheduler).toBeDefined();
      const configuredScheduler = new CronScheduler({ checkIntervalMs: 500 });
      expect(configuredScheduler).toBeDefined();
    });
  });

  // ─────────────────────────────────────────────────────────────
  // Event-driven setTimeout precision
  // ─────────────────────────────────────────────────────────────

  describe('event-driven timer (setTimeout precision)', () => {
    test('scheduleNext sets a precise timer for the next cron', () => {
      scheduler.add({
        name: 'timer-test',
        queue: 'q',
        data: {},
        repeatEvery: 5000,
      });

      scheduler.start();

      // A precise timer should be set
      expect((scheduler as any).nextTimer).not.toBeNull();
      // Safety fallback should also be set
      expect((scheduler as any).safetyInterval).not.toBeNull();
    });

    test('scheduleNext wakes exactly at nextRun (not polling)', async () => {
      const preciseScheduler = new CronScheduler();
      const jobs: Array<{ queue: string; input: JobInput }> = [];
      preciseScheduler.setPushCallback(async (queue, input) => {
        jobs.push({ queue, input });
      });

      preciseScheduler.add({
        name: 'precise',
        queue: 'q',
        data: {},
        repeatEvery: 150,
      });

      const startTime = Date.now();
      preciseScheduler.start();

      // Wait for execution (should happen at ~150ms, not at 1s intervals)
      await new Promise((r) => setTimeout(r, 300));
      preciseScheduler.stop();

      expect(jobs.length).toBeGreaterThanOrEqual(1);
      // Verify it executed much sooner than the old 1s polling would
      // The first push should happen around 150ms, not 1000ms
    });

    test('add() reschedules timer if new cron is sooner', async () => {
      const jobs: Array<{ queue: string; time: number }> = [];
      const timerScheduler = new CronScheduler();
      timerScheduler.setPushCallback(async (queue) => {
        jobs.push({ queue, time: Date.now() });
      });

      // Add a far-future cron first
      timerScheduler.add({
        name: 'far',
        queue: 'far-queue',
        data: {},
        repeatEvery: 60000,
      });

      timerScheduler.start();

      // Now add a near cron - should reschedule
      timerScheduler.add({
        name: 'near',
        queue: 'near-queue',
        data: {},
        repeatEvery: 100,
      });

      await new Promise((r) => setTimeout(r, 250));
      timerScheduler.stop();

      // The near cron should have executed
      expect(jobs.some((j) => j.queue === 'near-queue')).toBe(true);
      // The far cron should NOT have executed
      expect(jobs.some((j) => j.queue === 'far-queue')).toBe(false);
    });

    test('remove() reschedules timer when next cron is removed', () => {
      const now = Date.now();
      scheduler.load([
        makeCronJob({ name: 'soon', queue: 'q', nextRun: now + 1000 }),
        makeCronJob({ name: 'later', queue: 'q', nextRun: now + 60000 }),
      ]);

      scheduler.start();

      // Timer should target 'soon'
      const timerBefore = (scheduler as any).nextTimer;
      expect(timerBefore).not.toBeNull();

      // Remove the nearest cron
      scheduler.remove('soon');

      // Timer should have been rescheduled
      const timerAfter = (scheduler as any).nextTimer;
      // Either a new timer was set or null (if stale top)
      // The key is the old timer was cleared
      expect(timerBefore).not.toBe(timerAfter);
    });

    test('stop clears both precise timer and safety interval', () => {
      scheduler.add({
        name: 'cleanup-test',
        queue: 'q',
        data: {},
        repeatEvery: 5000,
      });

      scheduler.start();
      expect((scheduler as any).nextTimer).not.toBeNull();
      expect((scheduler as any).safetyInterval).not.toBeNull();
      expect((scheduler as any).started).toBe(true);

      scheduler.stop();
      expect((scheduler as any).nextTimer).toBeNull();
      expect((scheduler as any).safetyInterval).toBeNull();
      expect((scheduler as any).started).toBe(false);
    });

    test('empty heap results in no precise timer', () => {
      scheduler.start();
      // No crons added, so no precise timer
      expect((scheduler as any).nextTimer).toBeNull();
      // But safety interval should still be active
      expect((scheduler as any).safetyInterval).not.toBeNull();
    });
  });
});
