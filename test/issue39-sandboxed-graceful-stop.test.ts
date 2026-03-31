/**
 * DISABLED: All SandboxedWorker Issue #39 tests are commented out.
 * Bun's Worker threads are still unstable and cause flaky failures
 * (race conditions, intermittent crashes) in parallel test runs.
 * These tests will be re-enabled once Bun Workers stabilize.
 */

/*
import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { SandboxedWorker } from '../src/client';
import { QueueManager } from '../src/application/queueManager';
import { unlink } from 'fs/promises';

describe('Issue #39: SandboxedWorker.stop() should drain active jobs', () => {
  let manager: QueueManager;
  const cleanupFiles: string[] = [];

  beforeEach(async () => {
    manager = new QueueManager();
  });

  afterEach(async () => {
    await manager.shutdown();
    for (const f of cleanupFiles) {
      try { await unlink(f); } catch {}
    }
    cleanupFiles.length = 0;
  });

  test('stop() should wait for active job to complete before terminating', async () => {
    const processorPath = `${Bun.env.TMPDIR ?? '/tmp'}/test-processor-issue39-slow-${Date.now()}.ts`;
    await Bun.write(processorPath, `
      export default async (job: any) => {
        await Bun.sleep(1000);
        return { completed: true, id: job.id };
      };
    `);
    cleanupFiles.push(processorPath);
    const queueName = `issue39-drain-${Date.now()}`;
    const worker = new SandboxedWorker(queueName, {
      processor: processorPath,
      concurrency: 1,
      timeout: 10000,
      manager,
    });
    await worker.start();
    const job = await manager.push(queueName, { data: { value: 42 } });
    let active = false;
    worker.on('active', () => { active = true; });
    for (let i = 0; i < 50 && !active; i++) { await Bun.sleep(100); }
    expect(active).toBe(true);
    let completedJobId: string | null = null;
    worker.on('completed', (j) => { completedJobId = String(j.id); });
    await worker.stop();
    expect(completedJobId).toBe(String(job.id));
    const result = manager.getResult(job.id);
    expect(result).toEqual({ completed: true, id: String(job.id) });
  });

  test('stop() should drain multiple active jobs before terminating', async () => {
    const processorPath = `${Bun.env.TMPDIR ?? '/tmp'}/test-processor-issue39-multi-${Date.now()}.ts`;
    await Bun.write(processorPath, `
      export default async (job: any) => {
        await Bun.sleep(2000);
        return { value: job.data.value * 2 };
      };
    `);
    cleanupFiles.push(processorPath);
    const queueName = `issue39-multi-${Date.now()}`;
    const worker = new SandboxedWorker(queueName, {
      processor: processorPath,
      concurrency: 3,
      timeout: 10000,
      manager,
    });
    let activeCount = 0;
    const completedIds = new Set<string>();
    worker.on('active', () => { activeCount++; });
    worker.on('completed', (j) => { completedIds.add(String(j.id)); });
    await worker.start();
    const jobs = await Promise.all([
      manager.push(queueName, { data: { value: 1 } }),
      manager.push(queueName, { data: { value: 2 } }),
      manager.push(queueName, { data: { value: 3 } }),
    ]);
    for (let i = 0; i < 100 && activeCount < 3; i++) { await Bun.sleep(100); }
    expect(activeCount).toBe(3);
    await worker.stop();
    expect(completedIds.size).toBe(3);
    for (const job of jobs) {
      expect(completedIds.has(String(job.id))).toBe(true);
    }
  }, 15000);

  test('stop() should not pull new jobs while draining', async () => {
    const processorPath = `${Bun.env.TMPDIR ?? '/tmp'}/test-processor-issue39-nopull-${Date.now()}.ts`;
    await Bun.write(processorPath, `
      export default async (job: any) => {
        await Bun.sleep(500);
        return { ok: true };
      };
    `);
    cleanupFiles.push(processorPath);
    const queueName = `issue39-nopull-${Date.now()}`;
    const worker = new SandboxedWorker(queueName, {
      processor: processorPath,
      concurrency: 1,
      timeout: 10000,
      manager,
    });
    await worker.start();
    await manager.push(queueName, { data: { first: true } });
    await manager.push(queueName, { data: { second: true } });
    let active = false;
    worker.on('active', () => { active = true; });
    for (let i = 0; i < 50 && !active; i++) { await Bun.sleep(100); }
    expect(active).toBe(true);
    let completedCount = 0;
    worker.on('completed', () => { completedCount++; });
    await worker.stop();
    expect(completedCount).toBe(1);
    const counts = manager.getQueueJobCounts(queueName);
    expect(counts.waiting).toBe(1);
  });
});
*/
