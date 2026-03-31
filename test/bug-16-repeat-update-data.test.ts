/**
 * Bug #16 - Updating repeated job data is not possible
 * https://github.com/egeominotti/bunqueue/issues/16
 *
 * Scenario: User adds a repeated job with `repeat: { every: 1000 }` and
 * `removeOnComplete: true`. After the job processes, they call updateData()
 * on the original job reference. The new data is NOT reflected in the next
 * repeat execution.
 *
 * Root causes:
 * 1. handleRepeat() copies data from the completing job snapshot. If the job
 *    is already acked, updateData on the old ID fails (job removed from index).
 * 2. createSimpleJob (used by addBulk embedded) has updateData as a no-op.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';

describe('Bug #16 - Updating repeated job data', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  // === Scenario 1: User's exact scenario ===
  // Job completes quickly, user calls updateData AFTER completion.
  // With removeOnComplete: true, the original job is gone from the index.

  test('updateData on a completed repeated job (removeOnComplete: true) should update next repeat', async () => {
    // 1. Push a repeated job with removeOnComplete (like the user's setup)
    const job = await qm.push('repeat-q', {
      data: { customerId: 'okeokeokeokeokok' },
      repeat: { every: 5000, limit: 3 },
      removeOnComplete: true,
    });

    const originalJobId = job.id;

    // 2. Pull and ack (simulates Worker processing quickly)
    const pulled = await qm.pull('repeat-q');
    expect(pulled).not.toBeNull();
    await qm.ack(pulled!.id);

    // Allow the repeat push to complete
    await new Promise((r) => setTimeout(r, 50));

    // 3. Now user calls updateData on the original job ID (like in their setTimeout)
    //    This should somehow propagate to the next repeat, but it fails because
    //    the job is removed from the index (removeOnComplete: true)
    const updated = await qm.updateJobData(originalJobId, {
      customerId: 'okeokddksdsjdskjdksjds',
    });

    // BUG: updateData returns false because the original job was removed
    // The user expects this to update the next repeat's data
    expect(updated).toBe(true);

    // 4. The next repeat job should have the updated data
    const delayedJobs = qm.getJobs('repeat-q', { state: 'delayed' });
    expect(delayedJobs.length).toBeGreaterThanOrEqual(1);
    expect(delayedJobs[0].data).toEqual(
      expect.objectContaining({ customerId: 'okeokddksdsjdskjdksjds' })
    );
  });

  // === Scenario 2: Same but without removeOnComplete ===

  test('updateData on a completed repeated job (removeOnComplete: false) should update next repeat', async () => {
    const job = await qm.push('repeat-q2', {
      data: { customerId: 'initial' },
      repeat: { every: 5000, limit: 3 },
      removeOnComplete: false,
    });

    const originalJobId = job.id;

    // Process and complete
    const pulled = await qm.pull('repeat-q2');
    expect(pulled).not.toBeNull();
    await qm.ack(pulled!.id);
    await new Promise((r) => setTimeout(r, 50));

    // User calls updateData on original job after completion
    // Even without removeOnComplete, the jobIndex location is 'completed'
    // and updateJobData doesn't handle that case
    const updated = await qm.updateJobData(originalJobId, { customerId: 'updated' });

    // BUG: returns false because location.type === 'completed' is not handled
    expect(updated).toBe(true);

    const delayedJobs = qm.getJobs('repeat-q2', { state: 'delayed' });
    expect(delayedJobs.length).toBeGreaterThanOrEqual(1);
    expect(delayedJobs[0].data).toEqual(
      expect.objectContaining({ customerId: 'updated' })
    );
  });

  // === Scenario 3: updateData during processing DOES work ===
  // This documents the currently working case for reference

  test('updateData during processing propagates to next repeat (baseline)', async () => {
    await qm.push('repeat-q3', {
      data: { customerId: 'original' },
      repeat: { every: 5000, limit: 3 },
    });

    const pulled = await qm.pull('repeat-q3');
    expect(pulled).not.toBeNull();

    // Update BEFORE ack (while in processing state) - this works
    const updated = await qm.updateJobData(pulled!.id, { customerId: 'updated-during-processing' });
    expect(updated).toBe(true);

    await qm.ack(pulled!.id);
    await new Promise((r) => setTimeout(r, 50));

    const delayedJobs = qm.getJobs('repeat-q3', { state: 'delayed' });
    expect(delayedJobs.length).toBeGreaterThanOrEqual(1);
    expect((delayedJobs[0].data as Record<string, unknown>).customerId).toBe(
      'updated-during-processing'
    );
  });
});
