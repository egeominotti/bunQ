/**
 * Issue #41: Sandboxed Worker using a lot of RAM when doing nothing
 *
 * Reproduction: SandboxedWorker spawns N child processes at start() and
 * NEVER terminates them when idle (idleTimeout defaults to 0 = disabled).
 * After all jobs complete, worker processes remain alive consuming memory.
 */

import { describe, test, expect, afterAll, beforeAll } from 'bun:test';
import { SandboxedWorker } from '../src/client';
import { QueueManager } from '../src/application/queueManager';
import { unlink } from 'fs/promises';

let manager: QueueManager;
let processorPath: string;

beforeAll(async () => {
  manager = new QueueManager();
  processorPath = `${Bun.env.TMPDIR ?? '/tmp'}/test-idle-processor-${Date.now()}.ts`;
  await Bun.write(
    processorPath,
    `export default async (job: { id: string; data: any }) => {
      return { done: true };
    };`
  );
});

afterAll(async () => {
  manager.shutdown();
  try { await unlink(processorPath); } catch {}
});

describe('Issue #41: Sandboxed Worker idle RAM', () => {
  test('workers persist indefinitely when idle with default idleTimeout=0', async () => {
    const queueName = `idle-ram-${Date.now()}`;
    const worker = new SandboxedWorker(queueName, {
      processor: processorPath,
      concurrency: 3,
      timeout: 5000,
      manager,
    });

    await worker.start();

    // All 3 workers spawned and idle
    const statsBefore = worker.getStats();
    expect(statsBefore.total).toBe(3);
    expect(statsBefore.idle).toBe(3);
    expect(statsBefore.busy).toBe(0);

    // Add and process a job
    const job = await manager.push(queueName, { data: { value: 1 } });
    for (let i = 0; i < 30; i++) {
      await Bun.sleep(100);
      if (manager.getResult(job.id) !== undefined) break;
    }

    // After job completes, wait a bit for things to settle
    await Bun.sleep(500);

    // BUG: Workers are still alive and consuming memory even though
    // there are no more jobs. With default idleTimeout=0, they NEVER auto-terminate.
    const statsAfter = worker.getStats();
    expect(statsAfter.total).toBe(3); // Still 3 processes alive!
    expect(statsAfter.idle).toBe(3);  // All idle, none terminated
    expect(statsAfter.busy).toBe(0);
    expect(worker.isRunning()).toBe(true); // Pool still running

    // With concurrency:3 and maxMemory:256 (default), this means
    // ~768MB of idle RAM consumed by worker processes doing nothing.

    // Only manual stop() releases the processes
    await worker.stop();
    expect(worker.getStats().total).toBe(0);
  });

  test('idle workers are recycled after idleRecycleMs', async () => {
    const queueName = `recycle-${Date.now()}`;
    const worker = new SandboxedWorker(queueName, {
      processor: processorPath,
      concurrency: 3,
      timeout: 5000,
      idleRecycleMs: 500, // recycle after 500ms idle
      manager,
    });

    await worker.start();
    expect(worker.getStats().total).toBe(3);

    // Add and wait for job to complete
    const job = await manager.push(queueName, { data: { value: 1 } });
    for (let i = 0; i < 30; i++) {
      await Bun.sleep(100);
      if (manager.getResult(job.id) !== undefined) break;
    }

    // Wait for idle recycle to kick in
    await Bun.sleep(1500);

    // Pool is still running but idle workers were recycled (keeping 1 alive)
    expect(worker.isRunning()).toBe(true);
    const stats = worker.getStats();
    expect(stats.idle).toBeLessThanOrEqual(1); // At most 1 alive idle worker
    expect(stats.recycled).toBeGreaterThanOrEqual(2); // At least 2 recycled

    // New job should still work (worker respawned on demand)
    const job2 = await manager.push(queueName, { data: { value: 2 } });
    for (let i = 0; i < 30; i++) {
      await Bun.sleep(100);
      if (manager.getResult(job2.id) !== undefined) break;
    }
    expect(manager.getResult(job2.id)).toEqual({ done: true });

    await worker.stop();
  });

  test('idleRecycleMs=0 disables idle recycling', async () => {
    const queueName = `no-recycle-${Date.now()}`;
    const worker = new SandboxedWorker(queueName, {
      processor: processorPath,
      concurrency: 3,
      timeout: 5000,
      idleRecycleMs: 0, // disabled
      manager,
    });

    await worker.start();
    expect(worker.getStats().total).toBe(3);

    // Add and wait for job to complete
    const job = await manager.push(queueName, { data: { value: 1 } });
    for (let i = 0; i < 30; i++) {
      await Bun.sleep(100);
      if (manager.getResult(job.id) !== undefined) break;
    }

    // Wait - workers should NOT be recycled
    await Bun.sleep(600);

    const stats = worker.getStats();
    expect(stats.idle).toBe(3); // All still alive
    expect(stats.recycled).toBe(0);

    await worker.stop();
  });

  test('idleTimeout properly auto-stops worker when set', async () => {
    const queueName = `idle-timeout-${Date.now()}`;
    const worker = new SandboxedWorker(queueName, {
      processor: processorPath,
      concurrency: 1,
      timeout: 5000,
      idleTimeout: 500, // 500ms idle timeout
      manager,
    });

    await worker.start();
    expect(worker.isRunning()).toBe(true);

    // Add and process a job
    await manager.push(queueName, { data: { value: 1 } });
    for (let i = 0; i < 30; i++) {
      await Bun.sleep(100);
      if (!worker.isRunning()) break;
    }

    // Wait for idle timeout to trigger auto-stop
    await Bun.sleep(1500);

    // Worker should have auto-stopped after idle timeout
    expect(worker.isRunning()).toBe(false);
  });
});
