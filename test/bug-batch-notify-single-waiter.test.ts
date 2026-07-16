/**
 * Bug: Batch push only wakes 1 waiting worker
 *
 * When pushJobBatch inserts N jobs, it calls shard.notify() only once.
 * This means only 1 of M waiting workers wakes up immediately.
 * The other M-1 workers stay blocked until their timeout expires.
 *
 * Notifications without active waiters intentionally coalesce: jobs already
 * present in the queue are observed by pull before it starts waiting.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';

const QUEUE = 'batch-notify-test';

describe('Bug: batch push only notifies 1 waiter', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('pushBatch with N jobs should wake multiple waiting workers, not just 1', async () => {
    const WORKER_COUNT = 5;
    const JOB_COUNT = 10;
    const TIMEOUT_MS = 2000;

    // Start 5 workers pulling with long-poll (timeout=2000ms)
    // They will all block in waitForJob
    const pullStartTimes: number[] = [];
    const pullEndTimes: number[] = [];

    const pullPromises = Array.from({ length: WORKER_COUNT }, async (_, i) => {
      pullStartTimes[i] = Date.now();
      const job = await qm.pull(QUEUE, TIMEOUT_MS);
      pullEndTimes[i] = Date.now();
      return { worker: i, job, duration: pullEndTimes[i] - pullStartTimes[i] };
    });

    // Give workers time to enter waitForJob
    await Bun.sleep(20);

    // Push 10 jobs in a batch — should wake all 5 waiting workers
    const inputs = Array.from({ length: JOB_COUNT }, (_, i) => ({
      data: { index: i },
    }));
    await qm.pushBatch(QUEUE, inputs);

    // Wait for all workers to complete
    const results = await Promise.all(pullPromises);

    // ALL 5 workers should have gotten a job
    const workersWithJobs = results.filter((r) => r.job !== null);
    expect(workersWithJobs.length).toBe(WORKER_COUNT);

    // ALL workers should have woken up quickly (< 200ms), not after timeout
    // If the bug exists, only 1 worker wakes up fast, others wait ~2000ms
    for (const result of results) {
      expect(result.duration).toBeLessThan(200);
    }
  });

  test('multiple single pushes with no waiters remain immediately consumable', async () => {
    const PUSH_COUNT = 5;

    // Push 5 jobs while no workers are waiting
    for (let i = 0; i < PUSH_COUNT; i++) {
      await qm.push(QUEUE, { data: { index: i } });
    }

    // Now start 5 workers — each should return immediately (not wait for timeout)
    const pullPromises = Array.from({ length: PUSH_COUNT }, async (_, i) => {
      const start = Date.now();
      // timeout=500ms, but the queued job is observed before waiting
      const job = await qm.pull(QUEUE, 500);
      const duration = Date.now() - start;
      return { worker: i, job, duration };
    });

    const results = await Promise.all(pullPromises);

    // All 5 workers should get a job
    const workersWithJobs = results.filter((r) => r.job !== null);
    expect(workersWithJobs.length).toBe(PUSH_COUNT);

    // All should resolve quickly, not wait for timeout
    for (const result of results) {
      expect(result.duration).toBeLessThan(200);
    }
  });

  test('WaiterManager.notify called once wakes only 1 of N waiters', async () => {
    // Direct test of the underlying mechanism using the Shard
    const { Shard } = await import('../src/domain/queue/shard');
    const shard = new Shard();

    const WAITER_COUNT = 5;
    const WAIT_TIMEOUT = 2000;

    // Start 5 waiters
    const waiterResults: { index: number; duration: number }[] = [];
    const waiterPromises = Array.from({ length: WAITER_COUNT }, async (_, i) => {
      const start = Date.now();
      await shard.waitForJob(WAIT_TIMEOUT);
      const duration = Date.now() - start;
      waiterResults.push({ index: i, duration });
    });

    await Bun.sleep(10); // let all waiters register

    // Notify 5 times (simulating 5 jobs pushed)
    for (let i = 0; i < WAITER_COUNT; i++) {
      shard.notify();
    }

    await Promise.all(waiterPromises);

    // ALL 5 waiters should have been woken up quickly
    const fastWaiters = waiterResults.filter((w) => w.duration < 200);
    expect(fastWaiters.length).toBe(WAITER_COUNT);
  });

  test('notifications without waiters coalesce into one retry hint', async () => {
    const { Shard } = await import('../src/domain/queue/shard');
    const shard = new Shard();

    const NOTIFICATION_COUNT = 3;

    // Send 3 notifications with no waiters
    for (let i = 0; i < NOTIFICATION_COUNT; i++) {
      shard.notify();
    }

    const firstStart = Date.now();
    await shard.waitForJob(100);
    expect(Date.now() - firstStart).toBeLessThan(50);

    const secondStart = Date.now();
    await shard.waitForJob(30);
    expect(Date.now() - secondStart).toBeGreaterThanOrEqual(20);
  });
});
