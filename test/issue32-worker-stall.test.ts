/**
 * Issue #32 - Regular Worker in embedded mode shows stall warnings
 *
 * Bug: In embedded mode, the regular Worker never sent heartbeats
 * (heartbeats only started in TCP mode). Long-running jobs without
 * progress() calls got detected as stalled.
 *
 * Same underlying bug as #30, but for regular Worker instead of SandboxedWorker.
 * Fix: Worker now sends embedded heartbeats via manager.jobHeartbeat().
 */

import { describe, test, expect, afterAll } from 'bun:test';
import { SandboxedWorker } from '../src/client';
import { QueueManager } from '../src/application/queueManager';
import { jobId } from '../src/domain/types/job';
import { shardIndex } from '../src/shared/hash';

describe('Issue #32 - Regular Worker embedded heartbeats', () => {
  let manager: QueueManager;

  afterAll(async () => {
    if (manager) await manager.shutdown();
  });

  test('embedded Worker heartbeat updates job lastHeartbeat', async () => {
    /**
     * Verify the fix: Worker in embedded mode now calls
     * manager.jobHeartbeat() on an interval, updating lastHeartbeat.
     * We test this via the Worker import and checking the mechanism works.
     */
    const { Worker, shutdownManager } = await import('../src/client');
    const { Queue } = await import('../src/client');

    const queueName = `issue32-hb-direct-${Date.now()}`;
    const queue = new Queue(queueName, { embedded: true });
    queue.obliterate();

    // Enable stall detection
    queue.setStallConfig({
      enabled: true,
      stallInterval: 1000,
      maxStalls: 3,
      gracePeriod: 500,
    });

    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));
    const stalledEvents: string[] = [];

    const worker = new Worker(
      queueName,
      async (job) => {
        // Long-running job (3s) - no progress() calls
        await Bun.sleep(3000);
        return { processed: true };
      },
      {
        embedded: true,
        concurrency: 1,
        heartbeatInterval: 300, // Heartbeat every 300ms
      }
    );

    worker.on('stalled', (id: string) => stalledEvents.push(id));
    worker.on('completed', () => resolve());

    await queue.add('long-job', { test: true });

    await done;
    await Bun.sleep(100);

    // Job completed successfully - no stall events
    expect(stalledEvents).toHaveLength(0);

    await worker.close();
    queue.close();
    shutdownManager();
  }, 15000);

  test('embedded Worker heartbeat prevents stall detection (via QueueManager)', async () => {
    /**
     * Direct test: create a QueueManager with fast stall checks,
     * push a job, manually simulate embedded heartbeats, and verify
     * the job is not detected as stalled.
     */
    manager = new QueueManager({ stallCheckMs: 200 });
    const queueName = `issue32-mgr-hb-${Date.now()}`;

    const shard = (manager as any).shards[shardIndex(queueName)];
    shard.setStallConfig(queueName, {
      enabled: true,
      stallInterval: 500,
      maxStalls: 1,
      gracePeriod: 100,
    });

    // Push and pull a job (simulating what Worker does)
    const job = await manager.push(queueName, { data: { test: true } });
    const pulled = await manager.pull(queueName, 0);
    expect(pulled).not.toBeNull();

    // Send heartbeats every 200ms (like the fixed Worker would)
    const hbTimer = setInterval(() => {
      manager.jobHeartbeat(job.id);
    }, 200);

    // Wait 3s - stall detection runs at 200ms intervals
    // Without heartbeats, job would be stalled after ~500ms
    await Bun.sleep(3000);

    clearInterval(hbTimer);

    // Job should NOT be in DLQ
    const dlqEntries = shard.getDlqEntries(queueName);
    expect(dlqEntries).toHaveLength(0);

    // Ack the job
    await manager.ack(job.id, { result: 'ok' });
  }, 15000);

  test('without heartbeats job gets stalled (via QueueManager)', async () => {
    manager = new QueueManager({ stallCheckMs: 200 });
    const queueName = `issue32-no-hb-mgr-${Date.now()}`;

    const shard = (manager as any).shards[shardIndex(queueName)];
    shard.setStallConfig(queueName, {
      enabled: true,
      stallInterval: 400,
      maxStalls: 1,
      gracePeriod: 100,
    });

    const stalledJobIds: string[] = [];
    manager.subscribe((event) => {
      if (event.eventType === 'stalled') {
        stalledJobIds.push(String(event.jobId));
      }
    });

    const job = await manager.push(queueName, { data: { test: true } });
    await manager.pull(queueName, 0);

    // NO heartbeats - wait for stall detection
    await Bun.sleep(3000);

    // Job should have been detected as stalled
    expect(stalledJobIds.length).toBeGreaterThan(0);
    expect(stalledJobIds[0]).toBe(String(job.id));
  }, 15000);
});
