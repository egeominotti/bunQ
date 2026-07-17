/**
 * Repro: a SKIPPED cron fire must not consume the maxLimit budget.
 *
 * The scheduler increments and persists `executions` BEFORE fireCronJob,
 * but fireCronJob can return without pushing (skipIfNoWorker, 0.8x-interval
 * overlap guard). A skipped fire therefore burns one execution: a cron with
 * maxLimit=N can deliver fewer than N jobs. Under container contention this
 * is exactly the "executions=3 but count=2" flake in cron-timezone-dst.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';

function tickScheduler(mgr: QueueManager): Promise<void> {
  // biome-ignore lint/suspicious/noExplicitAny: reaching the private scheduler for deterministic ticks
  return (mgr as any).cronScheduler.tick();
}

describe('REPRO: skipped cron fires must not consume maxLimit', () => {
  let manager: QueueManager;

  beforeEach(() => {
    manager = new QueueManager();
  });
  afterEach(() => {
    manager.shutdown();
  });

  test('skipIfNoWorker skip leaves executions untouched; all maxLimit jobs are delivered', async () => {
    manager.addCron({
      name: 'skip-budget',
      queue: 'skip-budget-q',
      data: {},
      repeatEvery: 10,
      maxLimit: 2,
      skipIfNoWorker: true,
      preventOverlap: false,
    });

    // No worker registered: this tick must SKIP without burning budget.
    await Bun.sleep(20);
    await tickScheduler(manager);
    expect(manager.getCron('skip-budget')?.executions).toBe(0);
    expect(manager.count('skip-budget-q')).toBe(0);

    // Now a worker exists: the cap's worth of jobs must ALL be delivered.
    manager.registerWorker('w1', ['skip-budget-q']);
    const deadline = Date.now() + 4_000;
    while (Date.now() < deadline) {
      await Bun.sleep(20);
      await tickScheduler(manager);
      const c = manager.getCron('skip-budget');
      if (!c || c.executions >= 2) break;
    }

    // The invariant: executions counted == jobs actually pushed == maxLimit.
    expect(manager.count('skip-budget-q')).toBe(2);
    const cron = manager.getCron('skip-budget');
    if (cron) expect(cron.executions).toBe(2);
  });

  test('overlap-guard skip leaves executions untouched', async () => {
    manager.addCron({
      name: 'overlap-budget',
      queue: 'overlap-budget-q',
      data: {},
      repeatEvery: 10_000,
      maxLimit: 5,
      preventOverlap: false,
    });

    // Force the cron due on demand by mutating the live heap entry: an
    // upsert would reset lastFiredAt (H10) and defeat the overlap guard.
    // biome-ignore lint/suspicious/noExplicitAny: reaching scheduler internals for a deterministic overlap
    const sched = (manager as any).cronScheduler;
    const fireNow = () => {
      const entry = sched.cronJobs.get('overlap-budget');
      if (!entry) throw new Error('cron gone');
      entry.cron.nextRun = Date.now() - 1;
      sched.cronHeap.push({ cron: entry.cron, generation: entry.generation });
      return tickScheduler(manager);
    };

    await fireNow();
    expect(manager.count('overlap-budget-q')).toBe(1);
    expect(manager.getCron('overlap-budget')?.executions).toBe(1);

    // Immediately due again: inside the 0.8x window → overlap skip.
    // Budget must NOT move: executions stays at the delivered count.
    await fireNow();
    expect(manager.count('overlap-budget-q')).toBe(1);
    expect(manager.getCron('overlap-budget')?.executions).toBe(1);
  });
});
