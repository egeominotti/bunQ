/**
 * Issue #30 - DLQ moved my job for no reason (it was still running)
 *
 * Bug: In embedded mode, SandboxedWorker never sends heartbeats:
 * - heartbeatInterval defaults to 0 (timer never starts)
 * - sendHeartbeat in createEmbeddedOps is a no-op
 *
 * But background stall detection checks lastHeartbeat on the job.
 * A long-running job without progress() calls gets detected as stalled
 * after stallInterval, retried, and eventually moved to DLQ.
 */

import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { SandboxedWorker } from '../src/client';
import { QueueManager } from '../src/application/queueManager';
import { shardIndex } from '../src/shared/hash';
import { unlink } from 'fs/promises';

describe('Issue #30 - DLQ stall detection with SandboxedWorker', () => {
  let manager: QueueManager;
  let longRunningProcessorPath: string;

  beforeAll(async () => {
    // Use fast stall check interval to trigger stall detection quickly in tests
    manager = new QueueManager({ stallCheckMs: 200 });

    // Create a long-running processor that does NOT call progress()
    // Simulates the user's scenario: processing lots of data without heartbeats
    longRunningProcessorPath = `${Bun.env.TMPDIR ?? '/tmp'}/test-long-processor-${Date.now()}.ts`;
    await Bun.write(
      longRunningProcessorPath,
      `
      export default async (job: {
        id: string;
        data: any;
        queue: string;
        attempts: number;
        progress: (value: number) => void;
        log: (message: string) => void;
      }) => {
        // Simulate long-running work (e.g., downloading/processing large files)
        // Does NOT call progress() - relies on heartbeats to stay alive
        await Bun.sleep(3000);
        return { processed: true };
      };
    `
    );
  });

  afterAll(async () => {
    await manager.shutdown();
    try {
      await unlink(longRunningProcessorPath);
    } catch {
      // Ignore cleanup errors
    }
  });

  test('embedded SandboxedWorker should keep job alive via heartbeats', async () => {
    const queueName = `issue30-heartbeat-${Date.now()}`;

    // Use a short stall interval to expose the bug quickly
    // Without heartbeats, the job will be detected as stalled after 500ms
    const shard = (manager as any).shards[shardIndex(queueName)];
    shard.setStallConfig(queueName, {
      enabled: true,
      stallInterval: 500, // 500ms
      maxStalls: 3,
      gracePeriod: 200, // 200ms grace
    });

    const worker = new SandboxedWorker(queueName, {
      processor: longRunningProcessorPath,
      concurrency: 1,
      timeout: 0, // Disable worker timeout
      heartbeatInterval: 200, // Must be shorter than stallInterval (500ms)
      manager,
    });

    await worker.start();

    const job = await manager.push(queueName, { data: { test: true } });

    // Wait for the job to finish processing (3s job + buffer)
    await Bun.sleep(5000);

    // The job result should be stored if ack succeeded
    // BUG: stall detection removes the job from processingShards before the
    // worker finishes, so the ack fails and the result is never stored
    const result = manager.getResult(job.id);
    expect(result).toEqual({ processed: true });

    // The job should NOT be in the DLQ
    const dlqEntries = shard.getDlqEntries(queueName);
    expect(dlqEntries).toHaveLength(0);

    await worker.stop();
  }, 15000);

  test('long-running job should NOT be stalled when stall config is disabled', async () => {
    const queueName = `issue30-no-stall-${Date.now()}`;

    // Disable stall detection for this queue
    const shard = (manager as any).shards[shardIndex(queueName)];
    shard.setStallConfig(queueName, { enabled: false });

    const worker = new SandboxedWorker(queueName, {
      processor: longRunningProcessorPath,
      concurrency: 1,
      timeout: 0,
      manager,
    });

    await worker.start();

    const job = await manager.push(queueName, { data: { big: true } });

    // Wait for job to complete (should take ~3s)
    await Bun.sleep(5000);

    // With stall disabled, the job should complete and result should be stored
    const result = manager.getResult(job.id);
    expect(result).toEqual({ processed: true });

    await worker.stop();
  }, 15000);
});
