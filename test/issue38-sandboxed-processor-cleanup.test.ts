/**
 * Issue #38: Sandboxed processor never removed
 *
 * After a sandboxed processor completes a job successfully, the processor
 * (Bun Worker thread) is never terminated and the wrapper script file
 * persists in /tmp. The worker should properly clean up after stop() is
 * called, and the job should not remain in "active" state.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { SandboxedWorker } from '../src/client';
import { QueueManager } from '../src/application/queueManager';
import { unlink } from 'fs/promises';

describe('Issue #38: sandboxed processor never removed', () => {
  let manager: QueueManager;
  let processorPath: string;
  const cleanupFiles: string[] = [];

  beforeEach(async () => {
    manager = new QueueManager();

    // Create a simple processor that completes quickly
    processorPath = `${Bun.env.TMPDIR ?? '/tmp'}/test-processor-issue38-${Date.now()}.ts`;
    await Bun.write(
      processorPath,
      `
      export default async (job: { id: string; data: any; queue: string; progress: (n: number) => void }) => {
        return { processed: true, value: job.data.value * 2 };
      };
    `
    );
    cleanupFiles.push(processorPath);
  });

  afterEach(async () => {
    await manager.shutdown();
    for (const f of cleanupFiles) {
      try {
        await unlink(f);
      } catch {
        // Ignore
      }
    }
    cleanupFiles.length = 0;
  });

  test('worker stats should show 0 total after stop()', async () => {
    const queueName = `issue38-stats-${Date.now()}`;
    const worker = new SandboxedWorker(queueName, {
      processor: processorPath,
      concurrency: 2,
      timeout: 5000,
      manager,
    });

    await worker.start();
    expect(worker.getStats().total).toBe(2);

    // Add and process a job
    await manager.push(queueName, { data: { value: 10 } });

    // Wait for completion
    let completed = false;
    worker.on('completed', () => {
      completed = true;
    });
    for (let i = 0; i < 50 && !completed; i++) {
      await Bun.sleep(100);
    }
    expect(completed).toBe(true);

    await worker.stop();
    expect(worker.getStats().total).toBe(0);
  });

  test('wrapper script file should be deleted after stop()', async () => {
    const queueName = `issue38-wrapper-${Date.now()}`;
    const worker = new SandboxedWorker(queueName, {
      processor: processorPath,
      concurrency: 1,
      timeout: 5000,
      manager,
    });

    await worker.start();

    // Add and process a job
    await manager.push(queueName, { data: { value: 5 } });

    let completed = false;
    worker.on('completed', () => {
      completed = true;
    });
    for (let i = 0; i < 50 && !completed; i++) {
      await Bun.sleep(100);
    }
    expect(completed).toBe(true);

    // Find wrapper files before stop
    const { Glob } = await import('bun');
    const tmpDir = `${Bun.env.TMPDIR ?? '/tmp'}/bunqueue-workers`;
    const wrappersBefore: string[] = [];
    const glob = new Glob(`worker-${queueName}-*.ts`);
    for await (const file of glob.scan(tmpDir)) {
      wrappersBefore.push(file);
    }

    await worker.stop();

    // Verify wrapper is cleaned up
    const wrappersAfter: string[] = [];
    for await (const file of glob.scan(tmpDir)) {
      wrappersAfter.push(file);
    }
    expect(wrappersAfter.length).toBeLessThan(wrappersBefore.length);
  });

  test('job should transition from active to completed after processing', async () => {
    const queueName = `issue38-state-${Date.now()}`;
    const worker = new SandboxedWorker(queueName, {
      processor: processorPath,
      concurrency: 1,
      timeout: 5000,
      manager,
    });

    await worker.start();

    const job = await manager.push(queueName, { data: { value: 7 } });

    // Wait for completion
    let completed = false;
    worker.on('completed', () => {
      completed = true;
    });
    for (let i = 0; i < 50 && !completed; i++) {
      await Bun.sleep(100);
    }
    expect(completed).toBe(true);

    // The job should NOT be in 'active' state anymore
    const state = manager.getJobState(job.id);
    expect(state).not.toBe('active');

    // The result should be available
    const result = manager.getResult(job.id);
    expect(result).toEqual({ processed: true, value: 14 });

    await worker.stop();
  });

  test('no jobs should remain in active state after stop()', async () => {
    const queueName = `issue38-active-${Date.now()}`;
    const worker = new SandboxedWorker(queueName, {
      processor: processorPath,
      concurrency: 2,
      timeout: 5000,
      manager,
    });

    await worker.start();

    // Add multiple jobs
    const jobs = await Promise.all([
      manager.push(queueName, { data: { value: 1 } }),
      manager.push(queueName, { data: { value: 2 } }),
      manager.push(queueName, { data: { value: 3 } }),
    ]);

    // Wait for all to complete
    let completedCount = 0;
    worker.on('completed', () => {
      completedCount++;
    });
    for (let i = 0; i < 100 && completedCount < 3; i++) {
      await Bun.sleep(100);
    }
    expect(completedCount).toBe(3);

    await worker.stop();

    // Verify no jobs stuck in active state
    const counts = manager.getQueueJobCounts(queueName);
    expect(counts.active).toBe(0);

    // All results should be available
    for (const job of jobs) {
      const state = manager.getJobState(job.id);
      expect(state).not.toBe('active');
    }
  });

  test('processor worker threads should not persist after stop()', async () => {
    const queueName = `issue38-persist-${Date.now()}`;
    const worker = new SandboxedWorker(queueName, {
      processor: processorPath,
      concurrency: 3,
      timeout: 5000,
      manager,
    });

    await worker.start();
    expect(worker.getStats().total).toBe(3);
    expect(worker.getStats().idle).toBe(3);

    // Process a job
    await manager.push(queueName, { data: { value: 42 } });

    let completed = false;
    worker.on('completed', () => {
      completed = true;
    });
    for (let i = 0; i < 50 && !completed; i++) {
      await Bun.sleep(100);
    }
    expect(completed).toBe(true);

    // After job completes, workers are still alive (pooled) but should be idle
    expect(worker.getStats().busy).toBe(0);
    expect(worker.getStats().idle).toBe(3);

    // After stop(), ALL worker threads should be terminated
    await worker.stop();
    const stats = worker.getStats();
    expect(stats.total).toBe(0);
    expect(stats.busy).toBe(0);
    expect(stats.idle).toBe(0);
  });

  test('stop() should work correctly even during active job processing', async () => {
    // Create a slow processor
    const slowProcessorPath = `${Bun.env.TMPDIR ?? '/tmp'}/slow-processor-issue38-${Date.now()}.ts`;
    await Bun.write(
      slowProcessorPath,
      `
      export default async (job: any) => {
        await Bun.sleep(2000);
        return { done: true };
      };
    `
    );
    cleanupFiles.push(slowProcessorPath);

    const queueName = `issue38-stop-active-${Date.now()}`;
    const worker = new SandboxedWorker(queueName, {
      processor: slowProcessorPath,
      concurrency: 1,
      timeout: 10000,
      manager,
    });

    await worker.start();

    // Add a job that takes 2s
    await manager.push(queueName, { data: { test: true } });

    // Wait for it to become active
    let active = false;
    worker.on('active', () => {
      active = true;
    });
    for (let i = 0; i < 30 && !active; i++) {
      await Bun.sleep(100);
    }
    expect(active).toBe(true);

    // Stop while job is in progress - should terminate cleanly
    await worker.stop();

    expect(worker.getStats().total).toBe(0);
  });

  test('closed event should fire after stop() completes', async () => {
    const queueName = `issue38-closed-${Date.now()}`;
    const worker = new SandboxedWorker(queueName, {
      processor: processorPath,
      concurrency: 1,
      timeout: 5000,
      manager,
    });

    await worker.start();

    // Process a job first
    await manager.push(queueName, { data: { value: 1 } });
    let completed = false;
    worker.on('completed', () => {
      completed = true;
    });
    for (let i = 0; i < 50 && !completed; i++) {
      await Bun.sleep(100);
    }

    let closedFired = false;
    worker.on('closed', () => {
      closedFired = true;
    });

    await worker.stop();
    expect(closedFired).toBe(true);
  });

  // ============ Core bug: processor never auto-removed ============

  test('should auto-stop when idle and idleTimeout is set', async () => {
    const queueName = `issue38-idle-${Date.now()}`;
    const worker = new SandboxedWorker(queueName, {
      processor: processorPath,
      concurrency: 1,
      timeout: 5000,
      manager,
      idleTimeout: 500, // Auto-stop after 500ms of inactivity
    });

    let closedFired = false;
    worker.on('closed', () => {
      closedFired = true;
    });

    await worker.start();

    // Add and process a job
    await manager.push(queueName, { data: { value: 10 } });

    let completed = false;
    worker.on('completed', () => {
      completed = true;
    });
    for (let i = 0; i < 50 && !completed; i++) {
      await Bun.sleep(100);
    }
    expect(completed).toBe(true);

    // Wait for idle timeout to trigger auto-stop
    for (let i = 0; i < 30 && !closedFired; i++) {
      await Bun.sleep(100);
    }

    // Worker should have auto-stopped after idle timeout
    expect(closedFired).toBe(true);
    expect(worker.getStats().total).toBe(0);
  });

  test('isRunning() should return false after stop()', async () => {
    const queueName = `issue38-running-${Date.now()}`;
    const worker = new SandboxedWorker(queueName, {
      processor: processorPath,
      concurrency: 1,
      timeout: 5000,
      manager,
    });

    expect(worker.isRunning()).toBe(false);

    await worker.start();
    expect(worker.isRunning()).toBe(true);

    await worker.stop();
    expect(worker.isRunning()).toBe(false);
  });

  test('idleTimeout should reset when new jobs arrive', async () => {
    const queueName = `issue38-idle-reset-${Date.now()}`;
    const worker = new SandboxedWorker(queueName, {
      processor: processorPath,
      concurrency: 1,
      timeout: 5000,
      manager,
      idleTimeout: 800, // 800ms idle timeout
    });

    let closedFired = false;
    worker.on('closed', () => {
      closedFired = true;
    });

    await worker.start();

    // Process first job
    await manager.push(queueName, { data: { value: 1 } });
    let completedCount = 0;
    worker.on('completed', () => {
      completedCount++;
    });
    for (let i = 0; i < 50 && completedCount < 1; i++) {
      await Bun.sleep(100);
    }
    expect(completedCount).toBe(1);

    // Wait 400ms (less than timeout), then add another job
    await Bun.sleep(400);
    expect(closedFired).toBe(false); // Should NOT have stopped yet

    await manager.push(queueName, { data: { value: 2 } });
    for (let i = 0; i < 50 && completedCount < 2; i++) {
      await Bun.sleep(100);
    }
    expect(completedCount).toBe(2);

    // Now wait for the idle timeout after the second job
    for (let i = 0; i < 30 && !closedFired; i++) {
      await Bun.sleep(100);
    }
    expect(closedFired).toBe(true);

    // Clean up (already stopped)
    expect(worker.getStats().total).toBe(0);
  });
});
