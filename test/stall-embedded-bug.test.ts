/**
 * Test: Stall Detection Bug in Embedded Mode
 *
 * BUG: In embedded mode, updateProgress does NOT refresh lastHeartbeat.
 * This causes long-running jobs to be incorrectly marked as stalled.
 *
 * This test verifies the fix: updateProgress should update lastHeartbeat.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';
import { processingShardIndex } from '../src/shared/hash';
import { getSharedManager } from '../src/client/manager';
import type { JobId } from '../src/domain/types/job';

describe('Stall Detection - Heartbeat Updates', () => {
  let queue: Queue;
  let worker: Worker | null = null;

  beforeEach(() => {
    queue = new Queue('stall-heartbeat-test', { embedded: true });
    queue.obliterate();

    // Short stall interval for testing
    queue.setStallConfig({
      stallInterval: 500,
      maxStalls: 1,
      gracePeriod: 100,
    });
  });

  afterEach(async () => {
    if (worker) {
      await worker.close();
      worker = null;
    }
    queue.close();
    shutdownManager();
  });

  /**
   * BUG TEST: updateProgress should refresh lastHeartbeat
   *
   * When fixed, this test should PASS.
   * Currently, lastHeartbeat is not updated by updateProgress.
   */
  test('updateProgress should refresh lastHeartbeat', async () => {
    const manager = getSharedManager();
    let heartbeatBefore: number | null = null;
    let heartbeatAfterProgress: number | null = null;
    let jobIdCaptured: string | null = null;

    worker = new Worker(
      'stall-heartbeat-test',
      async (job) => {
        jobIdCaptured = job.id;

        // Get lastHeartbeat from processingShards
        const procIdx = processingShardIndex(job.id as JobId);
        const processingShards = (manager as any).processingShards;
        const processingJob = processingShards[procIdx]?.get(job.id);

        heartbeatBefore = processingJob?.lastHeartbeat ?? null;

        // Wait a bit so timestamps are different
        await Bun.sleep(100);

        // Call updateProgress - this SHOULD refresh heartbeat
        await job.updateProgress(50);

        // Small delay to ensure any async updates complete
        await Bun.sleep(50);

        // Check lastHeartbeat again
        const processingJobAfter = processingShards[procIdx]?.get(job.id);
        heartbeatAfterProgress = processingJobAfter?.lastHeartbeat ?? null;

        return { success: true };
      },
      {
        embedded: true,
        autorun: false,
        concurrency: 1,
      }
    );

    await queue.add('test-job', { value: 1 });
    worker.run();

    await Bun.sleep(1000);

    console.log('Job ID:', jobIdCaptured);
    console.log('Heartbeat before updateProgress:', heartbeatBefore);
    console.log('Heartbeat after updateProgress:', heartbeatAfterProgress);

    expect(heartbeatBefore).not.toBeNull();
    expect(heartbeatAfterProgress).not.toBeNull();

    // EXPECTED after fix: heartbeatAfterProgress > heartbeatBefore
    // This assertion will FAIL until the bug is fixed
    expect(heartbeatAfterProgress).toBeGreaterThan(heartbeatBefore!);
  });

  /**
   * Test that a job with regular progress updates does NOT get stalled
   */
  test('job with progress updates should NOT be stalled', async () => {
    let jobCompleted = false;
    let jobStalled = false;

    worker = new Worker(
      'stall-heartbeat-test',
      async (job) => {
        // Make 5 progress updates over 600ms total
        // With stallInterval=500ms, without heartbeat refresh this would stall
        for (let i = 1; i <= 5; i++) {
          await Bun.sleep(120);
          await job.updateProgress(i * 20);
        }
        return { success: true };
      },
      {
        embedded: true,
        autorun: false,
        concurrency: 1,
      }
    );

    worker.on('completed', () => {
      jobCompleted = true;
    });

    worker.on('stalled', () => {
      jobStalled = true;
    });

    await queue.add('test-job', { value: 1 });
    worker.run();

    // Wait for completion
    await Bun.sleep(2000);

    // After fix: job should complete without being stalled
    expect(jobCompleted).toBe(true);
    expect(jobStalled).toBe(false);
  });
});
