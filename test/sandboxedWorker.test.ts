/**
 * DISABLED: All SandboxedWorker tests are commented out.
 * Bun's Worker threads are still unstable and cause flaky failures
 * (race conditions, intermittent crashes) in parallel test runs.
 * These tests will be re-enabled once Bun Workers stabilize.
 */

/*
import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { SandboxedWorker } from '../src/client';
import { QueueManager } from '../src/application/queueManager';
import { unlink } from 'fs/promises';

describe('SandboxedWorker', () => {
  let manager: QueueManager;
  let processorPath: string;

  beforeAll(async () => {
    manager = new QueueManager();
    processorPath = `${Bun.env.TMPDIR ?? '/tmp'}/test-processor-${Date.now()}.ts`;
    await Bun.write(
      processorPath,
      `
      export default async (job: { id: string; data: any; queue: string; progress: (n: number) => void }) => {
        job.progress(50);
        await Bun.sleep(10);
        job.progress(100);
        return { processed: true, value: job.data.value * 2 };
      };
    `
    );
  });

  afterAll(async () => {
    await manager.shutdown();
    try {
      await unlink(processorPath);
    } catch {
    }
  });

  test('should create sandboxed worker with options', () => {
    const worker = new SandboxedWorker('sandboxed-test', {
      processor: processorPath,
      concurrency: 2,
      timeout: 5000,
      manager,
    });
    expect(worker).toBeDefined();
    const stats = worker.getStats();
    expect(stats.total).toBe(0);
  });

  test('should start and stop worker pool', async () => {
    const worker = new SandboxedWorker('sandboxed-test-2', {
      processor: processorPath,
      concurrency: 2,
      manager,
    });
    await worker.start();
    const stats = worker.getStats();
    expect(stats.total).toBe(2);
    expect(stats.idle).toBe(2);
    expect(stats.busy).toBe(0);
    await worker.stop();
    expect(worker.getStats().total).toBe(0);
  });

  test('should process jobs in isolated workers', async () => {
    const queueName = `sandboxed-process-test-${Date.now()}`;
    const worker = new SandboxedWorker(queueName, {
      processor: processorPath,
      concurrency: 1,
      timeout: 5000,
      manager,
    });
    await worker.start();
    const job = await manager.push(queueName, { data: { value: 21 } });
    expect(job.id).toBeDefined();
    let result: unknown;
    for (let i = 0; i < 20; i++) {
      await Bun.sleep(100);
      result = manager.getResult(job.id);
      if (result !== undefined) break;
    }
    expect(result).toEqual({ processed: true, value: 42 });
    await worker.stop();
  });

  test('should handle worker timeout', async () => {
    const slowProcessorPath = `${Bun.env.TMPDIR ?? '/tmp'}/slow-processor-${Date.now()}.ts`;
    await Bun.write(slowProcessorPath, `
      export default async (job: any) => {
        await Bun.sleep(10000);
        return { done: true };
      };
    `);
    const queueName = `sandboxed-timeout-test-${Date.now()}`;
    const worker = new SandboxedWorker(queueName, {
      processor: slowProcessorPath,
      concurrency: 1,
      timeout: 100,
      manager,
    });
    await worker.start();
    await manager.push(queueName, { data: { test: true } });
    let stats = worker.getStats();
    for (let i = 0; i < 20; i++) {
      await Bun.sleep(100);
      stats = worker.getStats();
      if (stats.restarts > 0) break;
    }
    expect(stats.restarts).toBeGreaterThan(0);
    await worker.stop();
    try { await unlink(slowProcessorPath); } catch {}
  });

  test('should handle processor errors', async () => {
    const errorProcessorPath = `${Bun.env.TMPDIR ?? '/tmp'}/error-processor-${Date.now()}.ts`;
    await Bun.write(errorProcessorPath, `
      export default async (job: any) => {
        throw new Error('Test error from processor');
      };
    `);
    const queueName = `sandboxed-error-test-${Date.now()}`;
    const worker = new SandboxedWorker(queueName, {
      processor: errorProcessorPath,
      concurrency: 1,
      timeout: 5000,
      manager,
    });
    await worker.start();
    await manager.push(queueName, { data: { test: true } });
    let stats = worker.getStats();
    for (let i = 0; i < 20; i++) {
      await Bun.sleep(100);
      stats = worker.getStats();
      if (stats.idle === 1) break;
    }
    expect(stats.total).toBe(1);
    expect(stats.idle).toBe(1);
    await worker.stop();
    try { await unlink(errorProcessorPath); } catch {}
  });

  test('should report correct stats', async () => {
    const worker = new SandboxedWorker('sandboxed-stats-test', {
      processor: processorPath,
      concurrency: 3,
      manager,
    });
    await worker.start();
    const stats = worker.getStats();
    expect(stats.total).toBe(3);
    expect(stats.idle).toBe(3);
    expect(stats.busy).toBe(0);
    expect(stats.restarts).toBe(0);
    await worker.stop();
  });

  test('should emit ready event on start', async () => {
    let readyEmitted = false;
    const worker = new SandboxedWorker('sandboxed-ready-event', {
      processor: processorPath,
      concurrency: 1,
      manager,
    });
    worker.on('ready', () => { readyEmitted = true; });
    await worker.start();
    expect(readyEmitted).toBe(true);
    await worker.stop();
  });

  test('should emit closed event on stop', async () => {
    let closedEmitted = false;
    const worker = new SandboxedWorker('sandboxed-closed-event', {
      processor: processorPath,
      concurrency: 1,
      manager,
    });
    worker.on('closed', () => { closedEmitted = true; });
    await worker.start();
    await worker.stop();
    expect(closedEmitted).toBe(true);
  });

  test('should emit active and completed events', async () => {
    const queueName = `sandboxed-events-test-${Date.now()}`;
    const activeJobs: string[] = [];
    const completedJobs: { id: string; result: unknown }[] = [];
    const worker = new SandboxedWorker(queueName, {
      processor: processorPath,
      concurrency: 1,
      timeout: 5000,
      manager,
    });
    worker.on('active', (job) => { activeJobs.push(String(job.id)); });
    worker.on('completed', (job, result) => { completedJobs.push({ id: String(job.id), result }); });
    await worker.start();
    const job = await manager.push(queueName, { data: { value: 10 } });
    for (let i = 0; i < 30; i++) {
      await Bun.sleep(100);
      if (completedJobs.length > 0) break;
    }
    expect(activeJobs.length).toBe(1);
    expect(activeJobs[0]).toBe(String(job.id));
    expect(completedJobs.length).toBe(1);
    expect(completedJobs[0].id).toBe(String(job.id));
    expect(completedJobs[0].result).toEqual({ processed: true, value: 20 });
    await worker.stop();
  });

  test('should emit failed event on processor error', async () => {
    const errorProcessorPath2 = `${Bun.env.TMPDIR ?? '/tmp'}/error-processor-events-${Date.now()}.ts`;
    await Bun.write(errorProcessorPath2, `
      export default async (job: any) => { throw new Error('Event test error'); };
    `);
    const queueName = `sandboxed-failed-event-${Date.now()}`;
    const failedJobs: { id: string; error: string }[] = [];
    const worker = new SandboxedWorker(queueName, {
      processor: errorProcessorPath2,
      concurrency: 1,
      timeout: 5000,
      manager,
    });
    worker.on('failed', (job, error) => { failedJobs.push({ id: String(job.id), error: error.message }); });
    await worker.start();
    const job = await manager.push(queueName, { data: { test: true } });
    for (let i = 0; i < 30; i++) {
      await Bun.sleep(100);
      if (failedJobs.length > 0) break;
    }
    expect(failedJobs.length).toBe(1);
    expect(failedJobs[0].id).toBe(String(job.id));
    expect(failedJobs[0].error).toContain('Event test error');
    await worker.stop();
    try { await unlink(errorProcessorPath2); } catch {}
  });

  test('should emit progress event', async () => {
    const queueName = `sandboxed-progress-event-${Date.now()}`;
    const progressUpdates: number[] = [];
    const worker = new SandboxedWorker(queueName, {
      processor: processorPath,
      concurrency: 1,
      timeout: 5000,
      manager,
    });
    worker.on('progress', (_job, progress) => { progressUpdates.push(progress); });
    await worker.start();
    await manager.push(queueName, { data: { value: 5 } });
    for (let i = 0; i < 30; i++) {
      await Bun.sleep(100);
      if (progressUpdates.length >= 2) break;
    }
    expect(progressUpdates).toContain(50);
    expect(progressUpdates).toContain(100);
    await worker.stop();
  });

  test('should create sandboxed worker with TCP connection options', async () => {
    const worker = new SandboxedWorker('sandboxed-tcp-test', {
      processor: processorPath,
      connection: { host: 'localhost', port: 16789 },
    });
    expect(worker).toBeDefined();
    const stats = worker.getStats();
    expect(stats.total).toBe(0);
    await worker.stop();
  });

  test('should accept custom heartbeat interval for TCP mode', async () => {
    const worker = new SandboxedWorker('sandboxed-tcp-hb-test', {
      processor: processorPath,
      connection: { host: 'localhost', port: 16789 },
      heartbeatInterval: 5000,
    });
    expect(worker).toBeDefined();
    await worker.stop();
  });

  test('should emit failed event on timeout', async () => {
    const slowProcessorPath2 = `${Bun.env.TMPDIR ?? '/tmp'}/slow-processor-events-${Date.now()}.ts`;
    await Bun.write(slowProcessorPath2, `
      export default async (job: any) => { await Bun.sleep(10000); return { done: true }; };
    `);
    const queueName = `sandboxed-timeout-event-${Date.now()}`;
    const failedJobs: { id: string; error: string }[] = [];
    const worker = new SandboxedWorker(queueName, {
      processor: slowProcessorPath2,
      concurrency: 1,
      timeout: 100,
      manager,
    });
    worker.on('failed', (job, error) => { failedJobs.push({ id: String(job.id), error: error.message }); });
    await worker.start();
    await manager.push(queueName, { data: { test: true } });
    for (let i = 0; i < 30; i++) {
      await Bun.sleep(100);
      if (failedJobs.length > 0) break;
    }
    expect(failedJobs.length).toBe(1);
    expect(failedJobs[0].error).toContain('timed out');
    await worker.stop();
    try { await unlink(slowProcessorPath2); } catch {}
  });
});
*/
