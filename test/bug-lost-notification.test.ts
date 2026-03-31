/**
 * Test: Lost Notification TOCTOU Fix Verification
 *
 * Issue: Race condition in pull.ts:185-199
 * Between tryPullFromShard() returning null and waitForJob() being called,
 * a job can be pushed and notify() fired - but no one is waiting yet.
 *
 * Fix: Added pendingNotification flag to Shard:
 * - notify(): if no waiters, set pendingNotification = true
 * - waitForJob(): if pendingNotification, clear it and return immediately
 *
 * This test verifies the fix by:
 * 1. Calling notify() before waitForJob() (simulating the race window)
 * 2. Verifying waitForJob() returns immediately instead of timing out
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { Shard } from '../src/domain/queue/shard';

describe('Fix Verification: Lost Notification TOCTOU (pull.ts:185-199)', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('VERIFIES FIX: notification NOT lost between check and wait', async () => {
    const QUEUE = 'lost-notification-queue';
    const TIMEOUT_MS = 500;

    // Track timing
    let pullStarted = 0;
    let pushCompleted = 0;
    let pullReturned = 0;

    // Start pull in background - will check empty queue, then wait
    const pullPromise = (async () => {
      pullStarted = Date.now();
      const job = await qm.pull(QUEUE, TIMEOUT_MS);
      pullReturned = Date.now();
      return job;
    })();

    // Give the pull time to check (finds nothing) and start waiting
    // The race window is very small, so we push immediately
    await Bun.sleep(1); // Minimal delay to let pull start

    // Push a job - this calls notify()
    // With the fix: even if notify() fires before waitForJob() is called,
    // the pendingNotification flag ensures waitForJob() returns immediately
    await qm.push(QUEUE, { data: { test: 'lost-notification' } });
    pushCompleted = Date.now();

    // Wait for pull to complete
    const job = await pullPromise;
    const pullDuration = pullReturned - pullStarted;

    console.log('\n--- Fix Verification Results ---');
    console.log(`Pull started at: ${pullStarted}`);
    console.log(`Push completed at: ${pushCompleted}`);
    console.log(`Pull returned at: ${pullReturned}`);
    console.log(`Pull duration: ${pullDuration}ms`);
    console.log(`Job received: ${job !== null}`);

    // FIX VERIFICATION:
    // With the fix, the pull should receive the job quickly (not timeout)
    // because either:
    // 1. The worker was already waiting when notify() was called, OR
    // 2. The pendingNotification flag was set, so waitForJob() returned immediately

    // The job should be received
    expect(job).not.toBeNull();
    console.log('\nFIX VERIFIED: Pull received job successfully');
    console.log(`Job received in ${pullDuration}ms`);

    // Pull should complete quickly, not wait the full timeout
    // Allow some margin for slow CI environments
    expect(pullDuration).toBeLessThan(TIMEOUT_MS - 50);
    console.log('FIX VERIFIED: Pull did not timeout');
  });

  test('VERIFIES FIX AT SHARD LEVEL: notify before wait returns immediately', async () => {
    // This test verifies the fix at the Shard level
    const shard = new Shard();
    const QUEUE = 'race-demo';

    // Simulate the race scenario:
    // 1. Worker checks queue (empty)
    // 2. Producer pushes and calls notify() - NO WAITER YET
    // 3. Worker calls waitForJob() - should return immediately due to pendingNotification

    // Step 1: Check queue (simulate tryPullFromShard returning null)
    const queue = shard.getQueue(QUEUE);
    expect(queue.size).toBe(0);

    // Step 2: Push happens, notify is called - but no one is waiting!
    // With the fix, this sets pendingNotification = true
    shard.notify();

    // Step 3: Worker starts waiting AFTER notify was called
    const startWait = Date.now();
    const WAIT_TIMEOUT = 100;

    // With the fix, this should return IMMEDIATELY because pendingNotification is set
    await shard.waitForJob(WAIT_TIMEOUT);
    const waitDuration = Date.now() - startWait;

    console.log('\n--- Fix Verification at Shard Level ---');
    console.log(`Wait timeout: ${WAIT_TIMEOUT}ms`);
    console.log(`Actual wait: ${waitDuration}ms`);

    // FIX VERIFIED: should return almost immediately (< 10ms), not wait the full timeout
    expect(waitDuration).toBeLessThan(10);
    console.log('FIX VERIFIED: waitForJob returned immediately due to pendingNotification flag');
  });

  test('pendingNotifications are consumed one at a time by waitForJob', async () => {
    // Verify that pendingNotifications is a counter, not a boolean flag.
    // Each notify() increments the counter, each waitForJob() decrements it.
    const shard = new Shard();
    const WAIT_TIMEOUT = 50;

    // Call notify() twice with no waiters - both notifications should be stored
    shard.notify();
    shard.notify();

    // First waitForJob should return immediately (consumes one pending notification)
    const start1 = Date.now();
    await shard.waitForJob(WAIT_TIMEOUT);
    const duration1 = Date.now() - start1;

    // Second waitForJob should also return immediately (consumes second pending notification)
    const start2 = Date.now();
    await shard.waitForJob(WAIT_TIMEOUT);
    const duration2 = Date.now() - start2;

    // Third waitForJob should wait the full timeout (no pending notifications left)
    const start3 = Date.now();
    await shard.waitForJob(WAIT_TIMEOUT);
    const duration3 = Date.now() - start3;

    console.log('\n--- Pending Notification Consumption Test ---');
    console.log(`First wait duration: ${duration1}ms (expected: <10ms)`);
    console.log(`Second wait duration: ${duration2}ms (expected: <10ms)`);
    console.log(`Third wait duration: ${duration3}ms (expected: ~${WAIT_TIMEOUT}ms)`);

    expect(duration1).toBeLessThan(10); // Should return immediately
    expect(duration2).toBeLessThan(10); // Should also return immediately
    expect(duration3).toBeGreaterThanOrEqual(WAIT_TIMEOUT - 10); // Should wait full timeout
  });

  test('STRESS: multiple pulls racing with pushes - all jobs should be consumed', async () => {
    const QUEUE = 'stress-race-queue';
    const WORKERS = 10;
    const JOBS = 10;
    const TIMEOUT_MS = 200;

    // Start workers pulling (will all find empty queue initially)
    const pullPromises = Array.from({ length: WORKERS }, async (_, i) => {
      const start = Date.now();
      const job = await qm.pull(QUEUE, TIMEOUT_MS);
      return {
        worker: i,
        job,
        duration: Date.now() - start,
        timedOut: job === null,
      };
    });

    // Small delay then push jobs
    await Bun.sleep(5);

    for (let i = 0; i < JOBS; i++) {
      await qm.push(QUEUE, { data: { job: i } });
    }

    const results = await Promise.all(pullPromises);

    const timedOut = results.filter(r => r.timedOut);
    const received = results.filter(r => !r.timedOut);

    console.log('\n--- Stress Test Results ---');
    console.log(`Workers: ${WORKERS}, Jobs pushed: ${JOBS}`);
    console.log(`Jobs received: ${received.length}`);
    console.log(`Timed out: ${timedOut.length}`);

    // With the fix, all jobs should be consumed (no lost notifications)
    // Since WORKERS == JOBS, all jobs should be received
    expect(received.length).toBe(JOBS);

    // Verify no jobs remain in queue
    const stats = qm.getStats();
    expect(stats.waiting).toBe(0);
    console.log('FIX VERIFIED: All jobs consumed, no lost notifications');
  });
});
