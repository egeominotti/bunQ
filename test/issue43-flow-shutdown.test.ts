/**
 * Issue #43: Flow job step lost during graceful shutdown
 *
 * Scenario: Flow has steps download → parse → handle (chained via dependencies).
 * When shutdown happens after "parse" completes, does "handle" get promoted?
 *
 * Two concerns:
 * 1. Is "handle" promoted from waitingDeps to the queue after "parse" ACK?
 * 2. Even if promoted, the worker is closing and won't pull "handle".
 * 3. Without persistence, shutdown() clears all in-memory state → "handle" is lost.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';

const waitForDepCheck = () => Bun.sleep(60);

describe('Issue #43: Flow job dependency during shutdown', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager({ dependencyCheckMs: 50 });
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('dependency is resolved via microtask before shutdown can clear it', async () => {
    // Create flow: jobA → jobB (B depends on A)
    const jobA = await qm.push('flow-test', { data: { step: 'parse' } });
    const jobB = await qm.push('flow-test', {
      data: { step: 'handle' },
      dependsOn: [jobA.id],
    });

    // Pull and process jobA
    const pulled = await qm.pull('flow-test', 0);
    expect(pulled?.id).toBe(jobA.id);

    // ACK jobA - this triggers onJobCompleted → scheduleDependencyFlush (microtask)
    await qm.ack(jobA.id, { result: 'parse done' });

    // The microtask should have run after the await, promoting jobB
    await waitForDepCheck();

    // Verify jobB is now pullable (promoted from waitingDeps)
    const pulledB = await qm.pull('flow-test', 0);
    expect(pulledB?.id).toBe(jobB.id);
  });

  test('promoted job is lost when shutdown clears in-memory state (no persistence)', async () => {
    // Create flow: jobA → jobB → jobC
    const jobA = await qm.push('flow-shutdown', { data: { step: 'download' } });
    const jobB = await qm.push('flow-shutdown', {
      data: { step: 'parse' },
      dependsOn: [jobA.id],
    });
    const jobC = await qm.push('flow-shutdown', {
      data: { step: 'handle' },
      dependsOn: [jobB.id],
    });

    // Process jobA
    const pulledA = await qm.pull('flow-shutdown', 0);
    expect(pulledA?.id).toBe(jobA.id);
    await qm.ack(jobA.id, { result: 'downloaded' });
    await waitForDepCheck();

    // jobB should now be pullable
    const pulledB = await qm.pull('flow-shutdown', 0);
    expect(pulledB?.id).toBe(jobB.id);

    // ACK jobB - jobC should be promoted
    await qm.ack(jobB.id, { result: 'parsed' });
    await waitForDepCheck();

    // jobC is promoted and waiting in the queue, but...
    // Simulate graceful shutdown: worker is closing, won't pull new jobs
    // Then shutdown() clears everything
    qm.shutdown();

    // After shutdown, create a NEW manager (simulating server restart without persistence)
    qm = new QueueManager({ dependencyCheckMs: 50 });

    // jobC is LOST - no persistence means the promoted job is gone
    const pulledC = await qm.pull('flow-shutdown', 0);
    expect(pulledC).toBeNull(); // BUG: jobC was lost during shutdown
  });

  test('shutdown during active job: next flow step depends on dep processor timing', async () => {
    // Use a very long dep check interval to simulate the race condition
    qm.shutdown();
    qm = new QueueManager({ dependencyCheckMs: 999999 }); // Background task won't fire

    const jobA = await qm.push('race-test', { data: { step: 'parse' } });
    const jobB = await qm.push('race-test', {
      data: { step: 'handle' },
      dependsOn: [jobA.id],
    });

    // Pull and ACK jobA
    const pulled = await qm.pull('race-test', 0);
    expect(pulled?.id).toBe(jobA.id);
    await qm.ack(jobA.id, { result: 'done' });

    // Even with dep check disabled (999999ms), microtask flush should promote jobB
    // Give microtask a chance to run
    await Bun.sleep(10);

    const pulledB = await qm.pull('race-test', 0);

    // If microtask path works, jobB should be pullable.
    // If it doesn't, this confirms the race condition.
    if (pulledB) {
      expect(pulledB.id).toBe(jobB.id);
      // Microtask path works - dependency resolved even without background task
    } else {
      // BUG: dependency not resolved despite ACK
      expect(pulledB).toBeNull();
    }
  });

  test('3-step flow completes normally without shutdown interference', async () => {
    const jobA = await qm.push('full-flow', { data: { step: 'download' } });
    const jobB = await qm.push('full-flow', {
      data: { step: 'parse' },
      dependsOn: [jobA.id],
    });
    const jobC = await qm.push('full-flow', {
      data: { step: 'handle' },
      dependsOn: [jobB.id],
    });

    // Step 1: download
    const pA = await qm.pull('full-flow', 0);
    expect(pA?.id).toBe(jobA.id);
    await qm.ack(jobA.id, { result: 'downloaded' });
    await waitForDepCheck();

    // Step 2: parse
    const pB = await qm.pull('full-flow', 0);
    expect(pB?.id).toBe(jobB.id);
    await qm.ack(jobB.id, { result: 'parsed' });
    await waitForDepCheck();

    // Step 3: handle
    const pC = await qm.pull('full-flow', 0);
    expect(pC?.id).toBe(jobC.id);
    await qm.ack(jobC.id, { result: 'handled' });

    // All steps completed successfully
    expect(qm.getResult(jobA.id)).toEqual({ result: 'downloaded' });
    expect(qm.getResult(jobB.id)).toEqual({ result: 'parsed' });
    expect(qm.getResult(jobC.id)).toEqual({ result: 'handled' });
  });
});
