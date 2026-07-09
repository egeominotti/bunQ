/**
 * WorkerManager Cleanup, Counter & Stop Tests
 * Tests for cleanupStale(), stop(), counter consistency, and multi-worker state transitions
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { WorkerManager } from '../src/application/workerManager';

/** Default WORKER_TIMEOUT_MS is 30000, so stale threshold is 30000 * 3 = 90000 */
const WORKER_TIMEOUT_MS = parseInt(Bun.env.WORKER_TIMEOUT_MS ?? '30000', 10);
const STALE_THRESHOLD = WORKER_TIMEOUT_MS * 3;

describe('WorkerManager - cleanupStale', () => {
  let manager: WorkerManager;

  beforeEach(() => {
    manager = new WorkerManager();
  });

  afterEach(() => {
    manager.stop();
  });

  test('worker with recent heartbeat is kept after cleanup', () => {
    const worker = manager.register('active-worker', ['queue1']);
    manager.heartbeat(worker.id);

    // Force cleanup by accessing the private method via bracket notation
    (manager as any).cleanupStale();

    expect(manager.get(worker.id)).toBeDefined();
    expect(manager.list().length).toBe(1);
  });

  test('worker with old heartbeat (3x timeout) is removed', () => {
    const worker = manager.register('stale-worker', ['queue1']);

    // Simulate stale: set lastSeen to well beyond the 3x threshold
    worker.lastSeen = Date.now() - STALE_THRESHOLD - 1000;

    (manager as any).cleanupStale();

    expect(manager.get(worker.id)).toBeUndefined();
    expect(manager.list().length).toBe(0);
  });

  test('worker exactly at the boundary is NOT removed', () => {
    const worker = manager.register('boundary-worker', ['queue1']);

    // Freeze Date.now so the test setup and cleanupStale() observe the SAME
    // `now`. Otherwise the ms can tick between the two reads, making
    // (now - lastSeen) drift from exactly STALE_THRESHOLD to > STALE_THRESHOLD
    // and flakily removing the worker (seen intermittently under CI load).
    const frozen = Date.now();
    const origNow = Date.now;
    Date.now = () => frozen;
    try {
      // Set lastSeen exactly at threshold (not past it)
      worker.lastSeen = frozen - STALE_THRESHOLD;

      (manager as any).cleanupStale();
    } finally {
      Date.now = origNow;
    }

    // Should NOT be removed because condition is > not >=
    expect(manager.get(worker.id)).toBeDefined();
  });

  test('multiple workers: only stale ones are removed', () => {
    const active1 = manager.register('active-1', ['queue1']);
    const active2 = manager.register('active-2', ['queue2']);
    const stale1 = manager.register('stale-1', ['queue1']);
    const stale2 = manager.register('stale-2', ['queue3']);

    // Make two workers stale
    stale1.lastSeen = Date.now() - STALE_THRESHOLD - 5000;
    stale2.lastSeen = Date.now() - STALE_THRESHOLD - 10000;

    // Keep two workers active (they are fresh from registration)
    manager.heartbeat(active1.id);
    manager.heartbeat(active2.id);

    (manager as any).cleanupStale();

    expect(manager.get(active1.id)).toBeDefined();
    expect(manager.get(active2.id)).toBeDefined();
    expect(manager.get(stale1.id)).toBeUndefined();
    expect(manager.get(stale2.id)).toBeUndefined();
    expect(manager.list().length).toBe(2);
  });

  test('after cleanup, stats reflect correct worker count', () => {
    const active = manager.register('active', ['queue1']);
    const stale = manager.register('stale', ['queue1']);

    manager.incrementActive(active.id);
    manager.incrementActive(stale.id);

    // Make one stale
    stale.lastSeen = Date.now() - STALE_THRESHOLD - 1000;

    (manager as any).cleanupStale();

    const stats = manager.getStats();
    expect(stats.total).toBe(1);
    expect(stats.activeJobs).toBe(1); // Only active worker's job remains
  });

  test('cleanup adjusts totalActiveJobsCounter for stale workers with active jobs', () => {
    const stale = manager.register('stale-with-jobs', ['queue1']);

    manager.incrementActive(stale.id);
    manager.incrementActive(stale.id);
    manager.incrementActive(stale.id);

    expect(manager.getStats().activeJobs).toBe(3);

    stale.lastSeen = Date.now() - STALE_THRESHOLD - 1000;

    (manager as any).cleanupStale();

    expect(manager.getStats().activeJobs).toBe(0);
    expect(manager.list().length).toBe(0);
  });

  test('cleanup of multiple stale workers with mixed active jobs adjusts counters correctly', () => {
    const w1 = manager.register('w1', ['q1']);
    const w2 = manager.register('w2', ['q2']);
    const w3 = manager.register('w3', ['q3']);

    // w1: 2 active jobs
    manager.incrementActive(w1.id);
    manager.incrementActive(w1.id);

    // w2: 1 active job
    manager.incrementActive(w2.id);

    // w3: 0 active jobs (fresh)

    expect(manager.getStats().activeJobs).toBe(3);

    // Make w1 and w2 stale
    w1.lastSeen = Date.now() - STALE_THRESHOLD - 1000;
    w2.lastSeen = Date.now() - STALE_THRESHOLD - 1000;

    (manager as any).cleanupStale();

    // Only w3 remains, with 0 active jobs
    expect(manager.list().length).toBe(1);
    expect(manager.getStats().activeJobs).toBe(0);
    expect(manager.getStats().total).toBe(1);
  });

  test('cleanup when no workers exist does nothing', () => {
    (manager as any).cleanupStale();

    const stats = manager.getStats();
    expect(stats.total).toBe(0);
    expect(stats.activeJobs).toBe(0);
  });

  test('cleanup when all workers are fresh removes none', () => {
    manager.register('w1', ['q1']);
    manager.register('w2', ['q2']);
    manager.register('w3', ['q3']);

    (manager as any).cleanupStale();

    expect(manager.list().length).toBe(3);
  });
});

describe('WorkerManager - counter consistency', () => {
  let manager: WorkerManager;

  beforeEach(() => {
    manager = new WorkerManager();
  });

  afterEach(() => {
    manager.stop();
  });

  test('incrementActive increases totalActiveJobsCounter', () => {
    const worker = manager.register('w1', ['q1']);

    manager.incrementActive(worker.id);
    expect(manager.getStats().activeJobs).toBe(1);

    manager.incrementActive(worker.id);
    expect(manager.getStats().activeJobs).toBe(2);

    manager.incrementActive(worker.id);
    expect(manager.getStats().activeJobs).toBe(3);
  });

  test('jobCompleted decreases active, increases processed', () => {
    const worker = manager.register('w1', ['q1']);

    manager.incrementActive(worker.id);
    manager.incrementActive(worker.id);

    manager.jobCompleted(worker.id);

    const stats = manager.getStats();
    expect(stats.activeJobs).toBe(1);
    expect(stats.totalProcessed).toBe(1);
  });

  test('jobFailed decreases active, increases failed', () => {
    const worker = manager.register('w1', ['q1']);

    manager.incrementActive(worker.id);
    manager.incrementActive(worker.id);

    manager.jobFailed(worker.id);

    const stats = manager.getStats();
    expect(stats.activeJobs).toBe(1);
    expect(stats.totalFailed).toBe(1);
  });

  test('counters accurate after many operations', () => {
    const w1 = manager.register('w1', ['q1']);
    const w2 = manager.register('w2', ['q2']);

    // Simulate a flurry of activity
    for (let i = 0; i < 50; i++) {
      manager.incrementActive(w1.id);
      manager.incrementActive(w2.id);
    }

    // Complete 30 on w1, fail 10 on w1
    for (let i = 0; i < 30; i++) {
      manager.jobCompleted(w1.id);
    }
    for (let i = 0; i < 10; i++) {
      manager.jobFailed(w1.id);
    }

    // Complete 25 on w2, fail 15 on w2
    for (let i = 0; i < 25; i++) {
      manager.jobCompleted(w2.id);
    }
    for (let i = 0; i < 15; i++) {
      manager.jobFailed(w2.id);
    }

    const stats = manager.getStats();
    // w1: 50 incremented, 30 completed, 10 failed => 10 active
    // w2: 50 incremented, 25 completed, 15 failed => 10 active
    expect(stats.activeJobs).toBe(20);
    expect(stats.totalProcessed).toBe(55); // 30 + 25
    expect(stats.totalFailed).toBe(25); // 10 + 15
  });

  test('counters accurate after worker unregister', () => {
    const w1 = manager.register('w1', ['q1']);
    const w2 = manager.register('w2', ['q2']);

    manager.incrementActive(w1.id);
    manager.incrementActive(w1.id);
    manager.incrementActive(w1.id);

    manager.incrementActive(w2.id);
    manager.incrementActive(w2.id);

    manager.jobCompleted(w1.id); // w1 active: 2, processed: 1
    manager.jobFailed(w2.id); // w2 active: 1, failed: 1

    // Unregister w1 (has 2 active jobs)
    manager.unregister(w1.id);

    const stats = manager.getStats();
    // w1 had 2 active jobs, now removed
    // w2 still has 1 active job
    expect(stats.activeJobs).toBe(1);
    expect(stats.total).toBe(1);
    // Processed and failed counters are global and persist after unregister
    expect(stats.totalProcessed).toBe(1);
    expect(stats.totalFailed).toBe(1);
  });

  test('jobCompleted on non-existent worker does not affect counters', () => {
    const w1 = manager.register('w1', ['q1']);
    manager.incrementActive(w1.id);

    manager.jobCompleted('non-existent');

    const stats = manager.getStats();
    expect(stats.activeJobs).toBe(1);
    expect(stats.totalProcessed).toBe(0);
  });

  test('jobFailed on non-existent worker does not affect counters', () => {
    const w1 = manager.register('w1', ['q1']);
    manager.incrementActive(w1.id);

    manager.jobFailed('non-existent');

    const stats = manager.getStats();
    expect(stats.activeJobs).toBe(1);
    expect(stats.totalFailed).toBe(0);
  });

  test('incrementActive on non-existent worker does not affect counters', () => {
    manager.incrementActive('non-existent');

    const stats = manager.getStats();
    expect(stats.activeJobs).toBe(0);
  });

  test('jobCompleted does not let active count go negative on the counter', () => {
    const w1 = manager.register('w1', ['q1']);

    // Complete without any active jobs
    manager.jobCompleted(w1.id);
    manager.jobCompleted(w1.id);

    const stats = manager.getStats();
    expect(stats.activeJobs).toBe(0);
    // Processed still increments even if active was already 0
    expect(stats.totalProcessed).toBe(2);
  });

  test('jobFailed does not let active count go negative on the counter', () => {
    const w1 = manager.register('w1', ['q1']);

    // Fail without any active jobs
    manager.jobFailed(w1.id);
    manager.jobFailed(w1.id);

    const stats = manager.getStats();
    expect(stats.activeJobs).toBe(0);
    expect(stats.totalFailed).toBe(2);
  });
});

describe('WorkerManager - stop()', () => {
  test('after stop(), cleanup interval no longer runs', async () => {
    // Use a very short cleanup interval to test this within a reasonable time
    // We cannot change env in-test, so we test that stop() clears the interval
    const manager = new WorkerManager();

    const staleWorker = manager.register('stale', ['q1']);
    staleWorker.lastSeen = Date.now() - STALE_THRESHOLD - 5000;

    // Stop the manager, which clears the cleanup interval
    manager.stop();

    // The stale worker should still be present because cleanup was stopped
    // We verify by waiting a bit and checking the worker is still there
    await Bun.sleep(100);

    expect(manager.get(staleWorker.id)).toBeDefined();
    expect(manager.list().length).toBe(1);

    // Cleanup (stop already called)
  });

  test('stop() is idempotent - can be called multiple times', () => {
    const manager = new WorkerManager();

    manager.stop();
    manager.stop();
    manager.stop();

    // Should not throw
    expect(true).toBe(true);
  });

  test('stop() sets cleanupInterval to null', () => {
    const manager = new WorkerManager();

    // Before stop, interval exists
    expect((manager as any).cleanupInterval).not.toBeNull();

    manager.stop();

    expect((manager as any).cleanupInterval).toBeNull();
  });
});

describe('WorkerManager - multi-worker state transitions', () => {
  let manager: WorkerManager;

  beforeEach(() => {
    manager = new WorkerManager();
  });

  afterEach(() => {
    manager.stop();
  });

  test('register, work, complete cycle across multiple workers', () => {
    const w1 = manager.register('w1', ['emails']);
    const w2 = manager.register('w2', ['emails']);
    const w3 = manager.register('w3', ['tasks']);

    // All start processing
    manager.incrementActive(w1.id);
    manager.incrementActive(w1.id);
    manager.incrementActive(w2.id);
    manager.incrementActive(w3.id);

    expect(manager.getStats().activeJobs).toBe(4);

    // w1 completes both
    manager.jobCompleted(w1.id);
    manager.jobCompleted(w1.id);

    // w2 fails
    manager.jobFailed(w2.id);

    // w3 completes
    manager.jobCompleted(w3.id);

    const stats = manager.getStats();
    expect(stats.activeJobs).toBe(0);
    expect(stats.totalProcessed).toBe(3);
    expect(stats.totalFailed).toBe(1);
    expect(stats.total).toBe(3);
  });

  test('worker unregister mid-processing adjusts counters', () => {
    const w1 = manager.register('w1', ['q1']);
    const w2 = manager.register('w2', ['q1']);

    manager.incrementActive(w1.id);
    manager.incrementActive(w1.id);
    manager.incrementActive(w2.id);

    expect(manager.getStats().activeJobs).toBe(3);

    // Unregister w1 while it has 2 active jobs
    manager.unregister(w1.id);

    const stats = manager.getStats();
    expect(stats.activeJobs).toBe(1); // Only w2's job
    expect(stats.total).toBe(1);
  });

  test('interleaved increment, complete, fail across workers', () => {
    const w1 = manager.register('w1', ['q1']);
    const w2 = manager.register('w2', ['q2']);

    manager.incrementActive(w1.id);
    manager.jobCompleted(w1.id);
    manager.incrementActive(w2.id);
    manager.incrementActive(w1.id);
    manager.jobFailed(w2.id);
    manager.incrementActive(w2.id);
    manager.jobCompleted(w1.id);
    manager.jobCompleted(w2.id);

    const stats = manager.getStats();
    // w1: +1, -1(c), +1, -1(c) => 0 active, 2 processed
    // w2: +1, -1(f), +1, -1(c) => 0 active, 1 processed, 1 failed
    expect(stats.activeJobs).toBe(0);
    expect(stats.totalProcessed).toBe(3);
    expect(stats.totalFailed).toBe(1);
  });

  test('stale cleanup preserves processed and failed counters', () => {
    const w1 = manager.register('w1', ['q1']);

    manager.incrementActive(w1.id);
    manager.jobCompleted(w1.id);
    manager.incrementActive(w1.id);
    manager.jobFailed(w1.id);

    // Make w1 stale
    w1.lastSeen = Date.now() - STALE_THRESHOLD - 1000;

    (manager as any).cleanupStale();

    const stats = manager.getStats();
    expect(stats.total).toBe(0);
    expect(stats.activeJobs).toBe(0);
    // Processed and failed counters should persist even after cleanup
    expect(stats.totalProcessed).toBe(1);
    expect(stats.totalFailed).toBe(1);
  });

  test('heartbeat prevents worker from being cleaned up', () => {
    const worker = manager.register('long-lived', ['q1']);

    // Simulate the worker being around for a while but sending heartbeats
    manager.incrementActive(worker.id);

    // Heartbeat refreshes lastSeen
    manager.heartbeat(worker.id);

    (manager as any).cleanupStale();

    expect(manager.get(worker.id)).toBeDefined();
    expect(manager.getStats().activeJobs).toBe(1);
  });

  test('getForQueue returns correct workers after cleanup removes stale ones', () => {
    const active = manager.register('active', ['shared-queue']);
    const stale = manager.register('stale', ['shared-queue']);

    stale.lastSeen = Date.now() - STALE_THRESHOLD - 1000;

    (manager as any).cleanupStale();

    const workers = manager.getForQueue('shared-queue');
    expect(workers.length).toBe(1);
    expect(workers[0].id).toBe(active.id);
  });

  test('listActive returns empty after all workers become stale and are cleaned', () => {
    const w1 = manager.register('w1', ['q1']);
    const w2 = manager.register('w2', ['q2']);

    w1.lastSeen = Date.now() - STALE_THRESHOLD - 1000;
    w2.lastSeen = Date.now() - STALE_THRESHOLD - 1000;

    (manager as any).cleanupStale();

    expect(manager.listActive().length).toBe(0);
    expect(manager.list().length).toBe(0);
  });
});
