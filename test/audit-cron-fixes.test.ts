/**
 * Audit cron fixes — reproducing tests for TWO confirmed bugs in
 * src/infrastructure/scheduler/cronScheduler.ts
 *
 * These tests assert the CORRECT behavior so they FAIL on the current
 * (buggy) code and will pass once the bugs are fixed (red -> green TDD).
 *
 * BUG H10 (cronScheduler.ts:197 + overlap check 407-416):
 *   remove() deletes from cronJobs map but NEVER clears the lastFiredAt
 *   Map. Deleting a cron and re-creating one with the SAME name leaves a
 *   stale lastFiredAt timestamp. The overlap-detection window
 *   (now - lastFire < interval * 0.8) then silently SKIPS the recreated
 *   cron's first fire.
 *
 * BUG M4 (cronScheduler.ts:323,329 + cronParser.getNextIntervalRun):
 *   interval (repeatEvery) crons compute
 *     nextRun = getNextIntervalRun(interval, Date.now())  // executionTime + interval
 *   instead of anchoring to the SCHEDULED time
 *     nextRun = scheduledTime + interval
 *   so a slow job causes cumulative drift.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { CronScheduler } from '../src/infrastructure/scheduler/cronScheduler';
import type { JobInput } from '../src/domain/types/job';

/** Access the private tick() for deterministic testing. */
function tick(scheduler: CronScheduler): Promise<void> {
  return (scheduler as any).tick();
}

/** Force a cron (by name) to be due NOW and rebuild the heap so tick() sees it. */
function forceDue(scheduler: CronScheduler, name: string, nextRun: number): void {
  const s = scheduler as any;
  const entry = s.cronJobs.get(name);
  if (!entry) throw new Error(`cron not found: ${name}`);
  entry.cron.nextRun = nextRun;
  s.cronHeap.buildFrom(
    Array.from(s.cronJobs.values()).map((e: any) => ({
      cron: e.cron,
      generation: e.generation,
    }))
  );
}

describe('Audit cron fixes', () => {
  let scheduler: CronScheduler;

  afterEach(() => {
    scheduler?.stop();
  });

  // ───────────────────────────────────────────────────────────────────
  // BUG H10: stale lastFiredAt after remove() blocks recreated cron's
  //          first fire via overlap detection.
  // ───────────────────────────────────────────────────────────────────
  test('H10: re-adding a removed cron with the same name CAN fire (lastFiredAt cleared on remove)', async () => {
    const pushed: Array<{ queue: string; input: JobInput }> = [];
    scheduler = new CronScheduler();
    scheduler.setPushCallback(async (queue, input) => {
      pushed.push({ queue, input });
    });

    // A 1-second interval cron. Overlap window = interval * 0.8 = 800ms.
    const interval = 1000;

    // 1) Add the cron, force it due, tick -> it should fire and set lastFiredAt.
    scheduler.add({
      name: 'recreate-me',
      queue: 'tasks',
      data: { gen: 1 },
      repeatEvery: interval,
    });
    forceDue(scheduler, 'recreate-me', Date.now() - 1);
    await tick(scheduler);

    expect(pushed.length).toBe(1); // sanity: first fire happened

    // 2) Remove the cron. The FIX must also clear lastFiredAt for this name.
    const removed = scheduler.remove('recreate-me');
    expect(removed).toBe(true);

    // 3) Re-create a cron with the SAME name and fire it IMMEDIATELY
    //    (well within the 800ms overlap window of the stale timestamp).
    scheduler.add({
      name: 'recreate-me',
      queue: 'tasks',
      data: { gen: 2 },
      repeatEvery: interval,
    });
    forceDue(scheduler, 'recreate-me', Date.now() - 1);
    await tick(scheduler);

    // The recreated cron MUST fire. On buggy code the stale lastFiredAt
    // triggers overlap-skip (now - lastFire < 800ms) and pushed.length stays 1.
    expect(pushed.length).toBe(2);
    expect(pushed[1].input.data).toEqual({ gen: 2 });
  });

  // ───────────────────────────────────────────────────────────────────
  // BUG M4: interval crons drift because nextRun anchors to executionTime
  //         (Date.now() at tick) instead of the scheduled time.
  // ───────────────────────────────────────────────────────────────────
  test('M4: interval cron nextRun is anchored to scheduledTime + interval, not now() + interval (no drift)', async () => {
    const persisted: Array<{ name: string; executions: number; nextRun: number }> = [];
    scheduler = new CronScheduler();
    scheduler.setPushCallback(async () => {
      /* push succeeds */
    });
    scheduler.setPersistCallback((name, executions, nextRun) => {
      persisted.push({ name, executions, nextRun });
    });

    const interval = 1000;

    scheduler.add({
      name: 'drift-job',
      queue: 'tasks',
      data: {},
      repeatEvery: interval,
    });

    // Simulate a job that was SCHEDULED to fire well in the past but whose
    // execution is delayed (slow job / busy event loop). scheduledTime is
    // 5s in the past; the tick (execution) happens "now".
    const scheduledTime = Date.now() - 5000;
    forceDue(scheduler, 'drift-job', scheduledTime);

    const tickTime = Date.now();
    await tick(scheduler);

    expect(persisted.length).toBe(1);
    const newNextRun = persisted[0].nextRun;

    // CORRECT (no drift): nextRun anchored to scheduled time.
    const expectedAnchored = scheduledTime + interval; // ~ now - 4000

    // BUGGY: nextRun = executionTime + interval ~ now + 1000.
    const buggyDrifted = tickTime + interval; // ~ now + 1000

    // Anchored expectation, with a small tolerance for clock granularity.
    expect(Math.abs(newNextRun - expectedAnchored)).toBeLessThanOrEqual(50);

    // And it must NOT be the drifted (now + interval) value. The two differ
    // by ~5000ms here, so this is unambiguous.
    expect(Math.abs(newNextRun - buggyDrifted)).toBeGreaterThan(1000);
  });
});
