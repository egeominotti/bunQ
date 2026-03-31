/**
 * Issue #33 - Jobs running twice (duplicate execution)
 *
 * Reproduces the bug where stall detection retries a job while it's still
 * being processed, causing duplicate execution. This happens when:
 * 1. Heartbeats are disabled or not sent (e.g., heavy CPU processing)
 * 2. Processing time exceeds stallInterval
 * 3. Worker concurrency > 1 allows pulling the retried job
 *
 * The stall check runs every 5s. Two-phase detection means:
 * - At ~5s: job marked as stall candidate
 * - At ~10s: stall confirmed, job retried (pushed back to queue)
 * - Original worker still processing → duplicate execution
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';

describe('Issue #33 - Duplicate job execution via stall detection', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('job should not execute twice when processing is slow and heartbeats disabled', async () => {
    const queue = new Queue('issue33-dup-exec', { embedded: true });
    queue.obliterate();

    // Short stall interval so stall detection triggers quickly
    queue.setStallConfig({
      stallInterval: 500, // 500ms - very short
      gracePeriod: 100, // 100ms grace
      maxStalls: 3,
    });

    const executionLog: Array<{ jobId: string; startTime: number }> = [];
    const errors: string[] = [];
    let firstDone: (() => void) | null = null;
    const firstExecDone = new Promise<void>((r) => (firstDone = r));

    const worker = new Worker(
      'issue33-dup-exec',
      async (job) => {
        const startTime = Date.now();
        executionLog.push({ jobId: job.id, startTime });

        // Simulate slow processing (large file)
        // Must span two stall check cycles (5s each = 10s total)
        await Bun.sleep(12000);

        firstDone?.();
        firstDone = null;
        return { processed: true };
      },
      {
        embedded: true,
        concurrency: 2, // Must be > 1 so worker can pull retried job
        heartbeatInterval: 0, // Disabled - simulates blocked event loop
      }
    );

    worker.on('error', (err) => {
      errors.push(err.message);
    });

    await queue.add('process-large-file', { file: 'large-file.zip' });

    // Wait for first execution to complete
    await firstExecDone;
    // Give time for any duplicate to start
    await Bun.sleep(1000);

    // Count how many times the job was executed
    const uniqueJobIds = new Set(executionLog.map((e) => e.jobId));

    // BUG: With stall detection, the same job gets executed multiple times
    // Each job should execute exactly once
    for (const jobId of uniqueJobIds) {
      const executions = executionLog.filter((e) => e.jobId === jobId);
      expect(executions.length).toBe(1);
    }

    await worker.close();
    queue.close();
  }, 25000);

  test('flow chain: stall-retried job breaks flow - last step never reached', async () => {
    const queue = new Queue('issue33-flow-break', { embedded: true });
    const { FlowProducer } = await import('../src/client');
    const flow = new FlowProducer({ embedded: true });
    queue.obliterate();

    queue.setStallConfig({
      stallInterval: 500,
      gracePeriod: 100,
      maxStalls: 3,
    });

    const stepExecutions = new Map<number, number>();
    let lastStepReached = false;
    let resolve: (() => void) | null = null;
    const done = new Promise<void>((r) => (resolve = r));

    // Timeout to detect if flow never completes
    const timeout = setTimeout(() => {
      resolve?.();
    }, 20000);

    const worker = new Worker(
      'issue33-flow-break',
      async (job) => {
        const data = job.data as { step: number };
        const count = (stepExecutions.get(data.step) ?? 0) + 1;
        stepExecutions.set(data.step, count);

        if (data.step === 1) {
          // Step 1 is slow - triggers stall detection
          await Bun.sleep(12000);
        }

        if (data.step === 2) {
          lastStepReached = true;
          clearTimeout(timeout);
          resolve?.();
        }

        return { step: data.step, result: `step-${data.step}-done` };
      },
      {
        embedded: true,
        concurrency: 2,
        heartbeatInterval: 0,
      }
    );

    await flow.addChain([
      { name: 'step-0', queueName: 'issue33-flow-break', data: { step: 0 } },
      { name: 'step-1', queueName: 'issue33-flow-break', data: { step: 1 } },
      { name: 'step-2', queueName: 'issue33-flow-break', data: { step: 2 } },
    ]);

    await done;
    await Bun.sleep(500);

    // Step 1 should execute exactly once (BUG: may execute twice due to stall)
    const step1Execs = stepExecutions.get(1) ?? 0;
    expect(step1Execs).toBe(1);

    // Last step should be reached (BUG: may never reach due to broken flow)
    expect(lastStepReached).toBe(true);

    await worker.close();
    flow.close();
    queue.close();
  }, 30000);
});
