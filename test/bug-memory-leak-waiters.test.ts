/**
 * Memory Leak Fix Verification Tests
 *
 * Issue: eventsManager.ts:56-82
 * Previously, when waitForJobCompletion() timed out, the waiter was marked as cancelled
 * but never removed from the completionWaiters map. If the job never completed,
 * the cancelled waiter would stay in memory forever.
 *
 * FIX: The timeout handler now removes the waiter from the set and cleans up
 * empty sets from the map.
 *
 * These tests verify the fix works correctly by:
 * 1. Creating multiple waiters that timeout
 * 2. Verifying the waiters are removed from memory after timeout
 * 3. Confirming memory stays stable over time
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { EventsManager } from '../src/application/eventsManager';
import { WebhookManager } from '../src/application/webhookManager';

type CompletionWaiterState = { cancelled: boolean };
type CompletionWaiterRegistry = Map<string, Set<CompletionWaiterState>>;

function getCompletionWaiters(target: QueueManager | EventsManager): CompletionWaiterRegistry {
  const state = target as unknown as {
    eventsManager?: EventsManager;
    completionWaiters?: CompletionWaiterRegistry;
  };
  const manager = state.eventsManager ?? target;
  return (manager as unknown as { completionWaiters: CompletionWaiterRegistry }).completionWaiters;
}

describe('Fix Verified: Memory Leak in Waiters (eventsManager.ts:56-82)', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('FIXED: cancelled waiters are removed from map on timeout', async () => {
    // Access internal eventsManager via reflection to check state
    const completionWaiters = getCompletionWaiters(qm);

    // Push jobs but DON'T complete them
    const jobs = [];
    for (let i = 0; i < 5; i++) {
      const job = await qm.push('leak-test', { data: { i } });
      jobs.push(job);
    }

    console.log('\n--- Initial State ---');
    console.log(`Jobs pushed: ${jobs.length}`);
    console.log(`Waiters map size: ${completionWaiters.size}`);

    // Create waiters that will timeout
    const TIMEOUT_MS = 50;
    const waitPromises = jobs.map((job) => qm.waitForJobCompletion(job.id, TIMEOUT_MS));

    console.log('\n--- After Creating Waiters ---');
    console.log(`Waiters map size: ${completionWaiters.size}`);

    // Count total waiter entries
    let totalWaiters = 0;
    for (const [key, waiters] of completionWaiters) {
      totalWaiters += waiters.size;
      console.log(`  Job ${key}: ${waiters.size} waiter(s)`);
    }
    console.log(`Total waiter entries: ${totalWaiters}`);

    // Wait for all timeouts
    const results = await Promise.all(waitPromises);
    const timedOut = results.filter((r) => r === false).length;

    console.log('\n--- After Timeouts ---');
    console.log(`Timed out: ${timedOut}/${jobs.length}`);

    // FIX VERIFIED: Waiters are now removed after timeout
    console.log(`Waiters map size: ${completionWaiters.size}`);

    let cancelledWaiters = 0;
    let activeWaiters = 0;
    for (const [, waiters] of completionWaiters) {
      for (const waiter of waiters) {
        if (waiter.cancelled) {
          cancelledWaiters++;
        } else {
          activeWaiters++;
        }
      }
    }

    console.log(`Cancelled waiters still in map: ${cancelledWaiters}`);
    console.log(`Active waiters: ${activeWaiters}`);

    // FIX VERIFIED: No cancelled waiters remain in the map
    if (cancelledWaiters === 0 && completionWaiters.size === 0) {
      console.log('\n*** FIX VERIFIED: All cancelled waiters cleaned up properly');
    }

    // The map should be empty after timeout - fix verified
    expect(completionWaiters.size).toBe(0);
    expect(cancelledWaiters).toBe(0);
  });

  test('FIXED: no memory leak with orphaned jobs', async () => {
    const completionWaiters = getCompletionWaiters(qm);

    const ORPHAN_COUNT = 100;
    const TIMEOUT_MS = 10;

    console.log('\n--- Memory Leak Fix Verification ---');
    console.log(`Creating ${ORPHAN_COUNT} orphan job waiters...`);

    // Create many orphan job IDs (jobs that will never exist/complete)
    const fakeJobIds: bigint[] = [];
    for (let i = 0; i < ORPHAN_COUNT; i++) {
      fakeJobIds.push(BigInt(999999000 + i));
    }

    // Create waiters for non-existent jobs
    const waitPromises = fakeJobIds.map((jobId) => qm.waitForJobCompletion(jobId, TIMEOUT_MS));

    const mapSizeBefore = completionWaiters.size;
    console.log(`Map size during wait: ${mapSizeBefore}`);

    // Wait for all to timeout
    await Promise.all(waitPromises);

    const mapSizeAfter = completionWaiters.size;

    console.log(`Map size after timeout: ${mapSizeAfter}`);

    // Count any remaining entries (should be 0 after fix)
    let remainingEntries = 0;
    for (const [, waiters] of completionWaiters) {
      remainingEntries += waiters.size;
    }

    console.log(`Remaining entries: ${remainingEntries}`);

    // FIX VERIFIED: No entries leak - all cleaned up on timeout
    if (remainingEntries === 0 && mapSizeAfter === 0) {
      console.log('\n*** FIX VERIFIED: No memory leak');
      console.log('All timed-out waiters properly cleaned up');
    }

    expect(remainingEntries).toBe(0);
    expect(mapSizeAfter).toBe(0);
  });

  test('SHOWS CLEANUP ONLY ON COMPLETION: jobs that complete are cleaned', async () => {
    const completionWaiters = getCompletionWaiters(qm);

    // Push a job that WILL complete
    const job = await qm.push('complete-test', { data: {} });

    // Start waiting
    const waitPromise = qm.waitForJobCompletion(job.id, 5000);

    console.log('\n--- Cleanup on Completion ---');
    console.log(
      `Waiters for job before completion: ${completionWaiters.get(String(job.id))?.size ?? 0}`
    );

    // Complete the job
    const pulledJob = await qm.pull('complete-test');
    await qm.ack(pulledJob!.id, { result: 'done' });

    // Wait should resolve
    const completed = await waitPromise;

    console.log(`Wait result: ${completed ? 'completed' : 'timed out'}`);
    console.log(
      `Waiters for job after completion: ${completionWaiters.get(String(job.id))?.size ?? 0}`
    );
    console.log(`Map has key: ${completionWaiters.has(String(job.id))}`);

    // Completion DOES clean up - the key is deleted entirely
    expect(completed).toBe(true);
    expect(completionWaiters.has(String(job.id))).toBe(false);

    console.log('\nCleanup works on both completion AND timeout');
    console.log('Fix verified: both code paths properly clean up waiters');
  });

  test('FIXED: no memory growth in production simulation', async () => {
    const completionWaiters = getCompletionWaiters(qm);

    const ITERATIONS = 10;
    const WAITERS_PER_ITERATION = 20;
    const TIMEOUT_MS = 5;

    console.log('\n--- Production Memory Stability Verification ---');
    console.log(`${ITERATIONS} iterations x ${WAITERS_PER_ITERATION} waiters`);

    for (let iter = 0; iter < ITERATIONS; iter++) {
      // Create waiters for fake jobs (simulates checking non-existent jobs)
      const waitPromises = [];
      for (let i = 0; i < WAITERS_PER_ITERATION; i++) {
        const fakeId = BigInt(iter * 1000000 + i);
        waitPromises.push(qm.waitForJobCompletion(fakeId, TIMEOUT_MS));
      }

      await Promise.all(waitPromises);

      // Check memory stays stable
      let totalEntries = 0;
      for (const [, waiters] of completionWaiters) {
        totalEntries += waiters.size;
      }

      console.log(
        `Iteration ${iter + 1}: Map size=${completionWaiters.size}, Total entries=${totalEntries}`
      );

      // After each iteration, memory should be clean
      expect(totalEntries).toBe(0);
      expect(completionWaiters.size).toBe(0);
    }

    // Final count
    let finalEntries = 0;
    for (const [, waiters] of completionWaiters) {
      finalEntries += waiters.size;
    }

    console.log(`\nFinal entries: ${finalEntries}`);
    console.log('*** FIX VERIFIED: Memory stays stable at 0');

    // FIX VERIFIED: Memory stays at 0
    expect(finalEntries).toBe(0);
  });
});

describe('EventsManager Isolated Tests', () => {
  test('UNIT: waitForJobCompletion properly cleans up on timeout', async () => {
    const webhookManager = new WebhookManager();
    const eventsManager = new EventsManager(webhookManager);
    const completionWaiters = getCompletionWaiters(eventsManager);

    const fakeJobId = BigInt(123456);
    const TIMEOUT_MS = 10;

    // Wait for non-existent job
    const result = await eventsManager.waitForJobCompletion(fakeJobId, TIMEOUT_MS);

    expect(result).toBe(false); // Timed out

    // Check internal state - should be cleaned up
    const waitersSet = completionWaiters.get(String(fakeJobId));

    console.log('\n--- Unit Test Results ---');
    console.log(`Result: ${result ? 'completed' : 'timed out'}`);
    console.log(`Waiters set exists: ${waitersSet !== undefined}`);
    console.log(`Map size: ${completionWaiters.size}`);

    if (!waitersSet) {
      console.log('*** FIX VERIFIED: Waiter entry cleaned up on timeout');
    }

    // FIX VERIFIED: waiter is removed from map after timeout
    expect(waitersSet).toBeUndefined();
    expect(completionWaiters.size).toBe(0);

    // Cleanup
    eventsManager.clear();
  });
});
