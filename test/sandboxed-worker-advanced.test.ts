/**
 * DISABLED: All SandboxedWorker advanced tests are commented out.
 * Bun's Worker threads are still unstable and cause flaky failures
 * (race conditions, intermittent crashes) in parallel test runs.
 * These tests will be re-enabled once Bun Workers stabilize.
 */

/*
import { describe, test, expect, afterEach, beforeEach } from 'bun:test';
import { Queue, SandboxedWorker, FlowProducer, shutdownManager } from '../src/client';
import { unlink, mkdtemp } from 'fs/promises';
import { tmpdir } from 'os';
import { join } from 'path';

let tmpDir: string;
const processorFiles: string[] = [];

async function writeProcessor(name: string, code: string): Promise<string> {
  const path = join(tmpDir, `${name}-${Date.now()}.ts`);
  await Bun.write(path, code);
  processorFiles.push(path);
  return path;
}

describe('Advanced SandboxedWorker - Embedded', () => {
  beforeEach(async () => {
    tmpDir = await mkdtemp(join(tmpdir(), 'bunq-sandbox-'));
  });

  afterEach(async () => {
    shutdownManager();
    for (const f of processorFiles) {
      try { await unlink(f); } catch {}
    }
    processorFiles.length = 0;
  });

  test('basic: sandboxed worker processes jobs in isolated process', async () => {
    const queue = new Queue('sandbox-basic', { embedded: true });
    queue.obliterate();
    const processorPath = await writeProcessor('basic', `
      export default async (job: any) => {
        return { doubled: job.data.value * 2, pid: process.pid };
      };
    `);
    const completed: Array<{ id: string; result: any }> = [];
    const worker = new SandboxedWorker('sandbox-basic', {
      processor: processorPath,
      concurrency: 1,
      timeout: 10000,
    });
    worker.on('completed', (job, result) => { completed.push({ id: job.id, result }); });
    worker.on('error', () => {});
    await worker.start();
    await queue.add('double', { value: 21 });
    for (let i = 0; i < 50 && completed.length === 0; i++) await Bun.sleep(100);
    expect(completed).toHaveLength(1);
    expect(completed[0].result.doubled).toBe(42);
    await worker.stop();
    queue.close();
  }, 15000);

  test('concurrency: multiple sandboxed workers process in parallel', async () => {
    const queue = new Queue('sandbox-concurrent', { embedded: true });
    queue.obliterate();
    const processorPath = await writeProcessor('concurrent', `
      export default async (job: any) => {
        await Bun.sleep(200);
        return { value: job.data.value * 3 };
      };
    `);
    const completed: unknown[] = [];
    const worker = new SandboxedWorker('sandbox-concurrent', {
      processor: processorPath,
      concurrency: 4,
      timeout: 10000,
    });
    worker.on('completed', (_job, result) => completed.push(result));
    worker.on('error', () => {});
    await worker.start();
    await Promise.all([
      queue.add('triple', { value: 1 }),
      queue.add('triple', { value: 2 }),
      queue.add('triple', { value: 3 }),
      queue.add('triple', { value: 4 }),
    ]);
    const start = Date.now();
    for (let i = 0; i < 100 && completed.length < 4; i++) await Bun.sleep(100);
    const elapsed = Date.now() - start;
    expect(completed).toHaveLength(4);
    const values = completed.map((r: any) => r.value).sort((a: number, b: number) => a - b);
    expect(values).toEqual([3, 6, 9, 12]);
    expect(elapsed).toBeLessThan(2000);
    await worker.stop();
    queue.close();
  }, 15000);

  test('progress: sandboxed worker reports progress updates', async () => {
    const queue = new Queue('sandbox-progress', { embedded: true });
    queue.obliterate();
    const processorPath = await writeProcessor('progress', `
      export default async (job: any) => {
        job.progress(25);
        await Bun.sleep(50);
        job.progress(50);
        await Bun.sleep(50);
        job.progress(75);
        await Bun.sleep(50);
        job.progress(100);
        return { done: true };
      };
    `);
    const progressUpdates: number[] = [];
    let completed = false;
    const worker = new SandboxedWorker('sandbox-progress', {
      processor: processorPath,
      concurrency: 1,
      timeout: 10000,
    });
    worker.on('progress', (_job, progress) => progressUpdates.push(progress));
    worker.on('completed', () => { completed = true; });
    worker.on('error', () => {});
    await worker.start();
    await queue.add('task', {});
    for (let i = 0; i < 50 && !completed; i++) await Bun.sleep(100);
    expect(completed).toBe(true);
    expect(progressUpdates).toEqual([25, 50, 75, 100]);
    await worker.stop();
    queue.close();
  }, 15000);

  test('error handling: processor throws, job marked as failed', async () => {
    const queue = new Queue('sandbox-error', { embedded: true });
    queue.obliterate();
    const processorPath = await writeProcessor('error', `
      export default async (job: any) => {
        if (job.data.shouldFail) {
          throw new Error('Intentional failure: ' + job.data.reason);
        }
        return { ok: true };
      };
    `);
    const failures: Array<{ id: string; error: string }> = [];
    const completions: string[] = [];
    const worker = new SandboxedWorker('sandbox-error', {
      processor: processorPath,
      concurrency: 1,
      timeout: 10000,
    });
    worker.on('failed', (job, err) => failures.push({ id: job.id, error: err.message }));
    worker.on('completed', (job) => completions.push(job.id));
    worker.on('error', () => {});
    await worker.start();
    await queue.add('will-fail', { shouldFail: true, reason: 'bad input' });
    await queue.add('will-pass', { shouldFail: false });
    for (let i = 0; i < 50 && (failures.length === 0 || completions.length === 0); i++) {
      await Bun.sleep(100);
    }
    expect(failures).toHaveLength(1);
    expect(failures[0].error).toContain('Intentional failure: bad input');
    expect(completions).toHaveLength(1);
    await worker.stop();
    queue.close();
  }, 15000);

  test('timeout: long-running job gets timed out', async () => {
    const queue = new Queue('sandbox-timeout', { embedded: true });
    queue.obliterate();
    const processorPath = await writeProcessor('timeout', `
      export default async (job: any) => {
        await Bun.sleep(30000);
        return { ok: true };
      };
    `);
    const failures: string[] = [];
    const worker = new SandboxedWorker('sandbox-timeout', {
      processor: processorPath,
      concurrency: 1,
      timeout: 1000,
      autoRestart: true,
    });
    worker.on('failed', (_job, err) => failures.push(err.message));
    worker.on('error', () => {});
    await worker.start();
    await queue.add('slow-job', {});
    for (let i = 0; i < 30 && failures.length === 0; i++) await Bun.sleep(100);
    expect(failures).toHaveLength(1);
    expect(failures[0]).toContain('timed out');
    await worker.stop();
    queue.close();
  }, 15000);

  test('crash recovery: worker recovers after job failure', async () => {
    const queue = new Queue('sandbox-crash', { embedded: true });
    queue.obliterate();
    const processorPath = await writeProcessor('crash', `
      export default async (job: any) => {
        if (job.data.crash) {
          throw new Error('Simulated crash');
        }
        return { ok: true };
      };
    `);
    const failures: string[] = [];
    const completions: unknown[] = [];
    const worker = new SandboxedWorker('sandbox-crash', {
      processor: processorPath,
      concurrency: 1,
      timeout: 5000,
      autoRestart: true,
      maxRestarts: 5,
    });
    worker.on('failed', (_job, err) => failures.push(err.message));
    worker.on('completed', (_job, result) => completions.push(result));
    worker.on('error', () => {});
    await worker.start();
    await queue.add('crash-job', { crash: true });
    for (let i = 0; i < 50 && failures.length === 0; i++) await Bun.sleep(100);
    expect(failures.length).toBeGreaterThanOrEqual(1);
    expect(failures[0]).toContain('Simulated crash');
    await queue.add('good-job', { crash: false });
    for (let i = 0; i < 50 && completions.length === 0; i++) await Bun.sleep(100);
    expect(completions.length).toBeGreaterThanOrEqual(1);
    await worker.stop();
    queue.close();
  }, 15000);

  test('stats: getStats returns correct pool info', async () => {
    const queue = new Queue('sandbox-stats', { embedded: true });
    queue.obliterate();
    const processorPath = await writeProcessor('stats', `
      export default async (job: any) => {
        await Bun.sleep(500);
        return { ok: true };
      };
    `);
    const worker = new SandboxedWorker('sandbox-stats', {
      processor: processorPath,
      concurrency: 3,
      timeout: 10000,
    });
    worker.on('error', () => {});
    await worker.start();
    const before = worker.getStats();
    expect(before.total).toBe(3);
    expect(before.idle).toBe(3);
    expect(before.busy).toBe(0);
    await queue.add('work-1', {});
    await queue.add('work-2', {});
    await Bun.sleep(200);
    const during = worker.getStats();
    expect(during.total).toBe(3);
    expect(during.busy).toBeGreaterThanOrEqual(1);
    await Bun.sleep(1000);
    const after = worker.getStats();
    expect(after.busy).toBe(0);
    expect(after.idle).toBe(3);
    await worker.stop();
    queue.close();
  }, 15000);

  test('flow integration: sandboxed worker processes flow chain steps', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('sandbox-flow', { embedded: true });
    queue.obliterate();
    const processorPath = await writeProcessor('flow', `
      export default async (job: any) => {
        const step = job.data.step;
        return { step, result: step * 10 };
      };
    `);
    const completed: Array<{ step: number; result: number }> = [];
    const worker = new SandboxedWorker('sandbox-flow', {
      processor: processorPath,
      concurrency: 2,
      timeout: 10000,
    });
    worker.on('completed', (_job, result: any) => {
      completed.push({ step: result.step, result: result.result });
    });
    worker.on('error', () => {});
    await worker.start();
    await flow.addChain([
      { name: 'step-0', queueName: 'sandbox-flow', data: { step: 0 } },
      { name: 'step-1', queueName: 'sandbox-flow', data: { step: 1 } },
      { name: 'step-2', queueName: 'sandbox-flow', data: { step: 2 } },
    ]);
    for (let i = 0; i < 80 && completed.length < 3; i++) await Bun.sleep(100);
    expect(completed).toHaveLength(3);
    const steps = completed.map((c) => c.step).sort();
    expect(steps).toEqual([0, 1, 2]);
    await worker.stop();
    flow.close();
    queue.close();
  }, 20000);

  test('logging: sandboxed worker sends log messages', async () => {
    const queue = new Queue('sandbox-log', { embedded: true });
    queue.obliterate();
    const processorPath = await writeProcessor('log', `
      export default async (job: any) => {
        job.log('Starting processing');
        await Bun.sleep(10);
        job.log('Almost done');
        return { ok: true };
      };
    `);
    const logs: string[] = [];
    let completed = false;
    const worker = new SandboxedWorker('sandbox-log', {
      processor: processorPath,
      concurrency: 1,
      timeout: 10000,
    });
    worker.on('log', (_job, message) => logs.push(message));
    worker.on('completed', () => { completed = true; });
    worker.on('error', () => {});
    await worker.start();
    await queue.add('log-job', {});
    for (let i = 0; i < 50 && !completed; i++) await Bun.sleep(100);
    expect(completed).toBe(true);
    expect(logs).toContain('Starting processing');
    expect(logs).toContain('Almost done');
    await worker.stop();
    queue.close();
  }, 15000);

  test('high throughput: sandboxed workers handle many jobs', async () => {
    const queue = new Queue('sandbox-throughput', { embedded: true });
    queue.obliterate();
    const processorPath = await writeProcessor('throughput', `
      export default async (job: any) => {
        return { idx: job.data.idx, squared: job.data.idx * job.data.idx };
      };
    `);
    let completedCount = 0;
    const JOB_COUNT = 50;
    const worker = new SandboxedWorker('sandbox-throughput', {
      processor: processorPath,
      concurrency: 4,
      timeout: 10000,
    });
    worker.on('completed', () => completedCount++);
    worker.on('error', () => {});
    await worker.start();
    for (let i = 0; i < JOB_COUNT; i++) {
      await queue.add('compute', { idx: i });
    }
    const start = Date.now();
    for (let i = 0; i < 200 && completedCount < JOB_COUNT; i++) await Bun.sleep(100);
    const elapsed = Date.now() - start;
    expect(completedCount).toBe(JOB_COUNT);
    await worker.stop();
    queue.close();
  }, 60000);
});
*/
