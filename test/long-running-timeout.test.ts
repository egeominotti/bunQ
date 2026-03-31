/**
 * Long-running Jobs & Timeouts (Embedded Mode)
 *
 * Tests for job timeout, heartbeat keep-alive, stall detection,
 * concurrent long-running jobs, progress tracking, and timeout with retry.
 *
 * Note: In embedded mode, job timeouts and stall detection are handled by
 * the QueueManager's background tasks. The timeout check runs every 5s
 * (jobTimeoutCheckMs) and the stall check runs every 5s (stallCheckMs)
 * using two-phase detection (candidate -> confirmed).
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';
import { getSharedManager } from '../src/client/manager';

describe('Long-running Jobs & Timeouts - Embedded', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('1. job timeout — job exceeding timeout is failed', async () => {
    const queue = new Queue('lr-timeout-1', { embedded: true });
    queue.obliterate();

    let capturedJobId: string | null = null;
    let jobTimedOut = false;

    const worker = new Worker(
      'lr-timeout-1',
      async (job) => {
        capturedJobId = job.id;
        // Sleep long enough for the 5s timeout check to fire.
        // The job has timeout=500, so the background check will detect it
        // and call fail() on the job.
        await Bun.sleep(30000);
        return { done: true };
      },
      { embedded: true, concurrency: 1 }
    );

    // The server-side timeout handler removes the job from processing.
    // When the processor finishes, the ACK fails with "not found" which
    // emits an error event on the worker.
    worker.on('error', () => {
      jobTimedOut = true;
    });

    await queue.add('slow-job', { value: 1 }, { timeout: 500, attempts: 1 });

    // Wait for the timeout check to fire (runs every 5s).
    // After ~5s the check detects the job exceeded its 500ms timeout.
    for (let i = 0; i < 120; i++) {
      if (capturedJobId) {
        const state = await queue.getJobState(capturedJobId);
        // Once the timeout handler fires, the job is no longer 'active'
        if (state !== 'active' && state !== 'waiting') {
          jobTimedOut = true;
        }
      }
      if (jobTimedOut) break;
      await Bun.sleep(100);
    }

    await worker.close(true);

    expect(jobTimedOut).toBe(true);

    queue.close();
  }, 30000);

  test('2. heartbeat keeps job alive — long job with heartbeat completes', async () => {
    const queue = new Queue('lr-heartbeat-2', { embedded: true });
    queue.obliterate();

    // Configure stall detection with short interval
    queue.setStallConfig({
      stallInterval: 1000,
      maxStalls: 2,
      gracePeriod: 500,
    });

    let completed = false;
    let stalled = false;

    const worker = new Worker(
      'lr-heartbeat-2',
      async () => {
        // Long-running job (2s) -- heartbeat at 500ms prevents stalling
        await Bun.sleep(2000);
        return { done: true };
      },
      {
        embedded: true,
        concurrency: 1,
        heartbeatInterval: 500,
      }
    );

    worker.on('completed', () => {
      completed = true;
    });
    worker.on('stalled', () => {
      stalled = true;
    });

    await queue.add('long-job', { value: 1 });

    // Wait for the job to complete
    for (let i = 0; i < 100; i++) {
      if (completed) break;
      await Bun.sleep(100);
    }

    await worker.close();

    expect(completed).toBe(true);
    expect(stalled).toBe(false);

    queue.close();
  }, 30000);

  test('3. stall detection — job without heartbeat is detected as stalled', async () => {
    const queue = new Queue('lr-stall-3', { embedded: true });
    queue.obliterate();

    // Configure stall detection
    queue.setStallConfig({
      stallInterval: 1000,
      maxStalls: 2,
      gracePeriod: 500,
    });

    const stalledJobIds: string[] = [];

    // Subscribe to stall events directly from the QueueManager.
    // The two-phase stall detection marks candidates on one check cycle
    // and confirms them on the next (each cycle is 5s by default).
    const manager = getSharedManager();
    const unsubscribe = manager.subscribe((event) => {
      if (event.queue === 'lr-stall-3' && event.eventType === 'stalled') {
        stalledJobIds.push(String(event.jobId));
      }
    });

    const worker = new Worker(
      'lr-stall-3',
      async () => {
        // Block for a long time without heartbeat -- will be stalled.
        await Bun.sleep(25000);
        return { done: true };
      },
      {
        embedded: true,
        concurrency: 1,
        heartbeatInterval: 0, // Disable heartbeat
      }
    );

    await queue.add('stall-job', { value: 1 });

    // Wait for stall detection to fire
    // Phase 1 (~5s): job marked as candidate
    // Phase 2 (~10s): job confirmed as stalled
    for (let i = 0; i < 200; i++) {
      if (stalledJobIds.length > 0) break;
      await Bun.sleep(100);
    }

    unsubscribe();
    await worker.close(true);

    // The job should have been detected as stalled at least once
    expect(stalledJobIds.length).toBeGreaterThan(0);

    queue.close();
  }, 30000);

  test('4. multiple long-running concurrent — 5 jobs in parallel', async () => {
    const queue = new Queue('lr-concurrent-4', { embedded: true });
    queue.obliterate();

    const completedIndices: number[] = [];

    const worker = new Worker(
      'lr-concurrent-4',
      async (job) => {
        const data = job.data as { index: number };
        // Each job takes ~1s
        await Bun.sleep(1000);
        completedIndices.push(data.index);
        return { index: data.index };
      },
      { embedded: true, concurrency: 5 }
    );

    const startTime = Date.now();

    // Push 5 jobs
    for (let i = 0; i < 5; i++) {
      await queue.add(`job-${i}`, { index: i });
    }

    // Wait for all 5 to complete
    for (let i = 0; i < 100; i++) {
      if (completedIndices.length >= 5) break;
      await Bun.sleep(100);
    }

    const elapsed = Date.now() - startTime;

    await worker.close();

    expect(completedIndices.length).toBe(5);
    // With concurrency:5, all 5 jobs should complete in roughly 1s (+ overhead)
    // rather than 5s (sequential). Allow up to 3s for CI overhead.
    expect(elapsed).toBeLessThan(3000);

    // Verify all indices are present
    const sorted = [...completedIndices].sort((a, b) => a - b);
    expect(sorted).toEqual([0, 1, 2, 3, 4]);

    queue.close();
  }, 30000);

  test('5. progress during long job — progress updates are recorded', async () => {
    const queue = new Queue('lr-progress-5', { embedded: true });
    queue.obliterate();

    const progressValues: number[] = [];
    let completed = false;

    const worker = new Worker(
      'lr-progress-5',
      async (job) => {
        // Simulate a 1s job with progress updates at 25%, 50%, 75%, 100%
        await Bun.sleep(250);
        await job.updateProgress(25);
        await Bun.sleep(250);
        await job.updateProgress(50);
        await Bun.sleep(250);
        await job.updateProgress(75);
        await Bun.sleep(250);
        await job.updateProgress(100);
        return { done: true };
      },
      { embedded: true, concurrency: 1 }
    );

    worker.on('progress', (_job, progress) => {
      progressValues.push(progress as number);
    });

    worker.on('completed', () => {
      completed = true;
    });

    await queue.add('progress-job', { value: 1 });

    // Wait for completion
    for (let i = 0; i < 100; i++) {
      if (completed) break;
      await Bun.sleep(100);
    }

    await worker.close();

    expect(completed).toBe(true);
    expect(progressValues).toEqual([25, 50, 75, 100]);

    queue.close();
  }, 30000);

  test('6. timeout with retry — succeeds on 3rd attempt after 2 timeouts', async () => {
    const queue = new Queue('lr-retry-6', { embedded: true });
    queue.obliterate();

    let attemptCount = 0;
    let completed = false;
    let completedResult: unknown = null;

    // In embedded mode, when a job times out, the server-side timeout handler
    // fails the job and retries it. The same worker instance deduplicates by
    // jobId, so it won't re-process a job that is still in activeJobIds.
    // To handle this, we close and recreate the worker between retry cycles
    // so the retried job can be picked up cleanly.

    async function createAndRunWorker(): Promise<Worker> {
      const w = new Worker(
        'lr-retry-6',
        async () => {
          attemptCount++;
          if (attemptCount <= 2) {
            // Attempts 1 & 2: block until the 5s timeout check fires.
            await Bun.sleep(30000);
            return { done: false };
          }
          // 3rd attempt: complete quickly
          return { done: true, attempt: attemptCount };
        },
        { embedded: true, concurrency: 1 }
      );

      w.on('completed', (_job, result) => {
        completed = true;
        completedResult = result;
      });

      return w;
    }

    await queue.add('retry-timeout-job', { value: 1 }, {
      timeout: 500,
      attempts: 3,
      backoff: 100,
    });

    // Run through up to 3 worker cycles (one per attempt).
    // Each cycle: create worker -> wait ~5s for timeout check -> force close.
    // The timeout check interval is 5s, so each timed-out attempt takes ~5s.
    for (let cycle = 0; cycle < 3; cycle++) {
      if (completed) break;

      const w = await createAndRunWorker();

      // Wait for either completion or the timeout check to fire (~5s).
      for (let i = 0; i < 60; i++) {
        if (completed) break;
        await Bun.sleep(100);
      }

      // Force close the worker so the next cycle's worker can pick up
      // the retried job with a fresh activeJobIds set.
      await w.close(true);
    }

    expect(completed).toBe(true);
    expect((completedResult as { done: boolean; attempt: number }).done).toBe(true);
    expect((completedResult as { attempt: number }).attempt).toBe(3);

    queue.close();
  }, 30000);
});
