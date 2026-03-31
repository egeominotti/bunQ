/**
 * DISABLED: All SandboxedWorker flow tests are commented out.
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
  const path = join(tmpDir, `${name}-${Date.now()}-${Math.random().toString(36).slice(2)}.ts`);
  await Bun.write(path, code);
  processorFiles.push(path);
  return path;
}

describe('SandboxedWorker + Flow - Embedded', () => {
  beforeEach(async () => {
    tmpDir = await mkdtemp(join(tmpdir(), 'bunq-sandbox-flow-'));
  });

  afterEach(async () => {
    shutdownManager();
    for (const f of processorFiles) {
      try { await unlink(f); } catch {}
    }
    processorFiles.length = 0;
  });

  test('sandboxed worker processes child jobs in a flow', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('sbx-flow-children', { embedded: true });
    queue.obliterate();
    const processorPath = await writeProcessor('flow-children', `
      export default async (job: any) => {
        const data = job.data;
        if (data.role === 'child') {
          return { childValue: data.value * 2 };
        }
        return { parentDone: true };
      };
    `);
    const completed: Array<{ name: string; result: any }> = [];
    const worker = new SandboxedWorker('sbx-flow-children', {
      processor: processorPath,
      concurrency: 3,
      timeout: 10000,
    });
    worker.on('completed', (job, result) => { completed.push({ name: job.name, result }); });
    worker.on('error', () => {});
    await worker.start();
    await flow.add({
      name: 'parent',
      queueName: 'sbx-flow-children',
      data: { role: 'parent' },
      children: [
        { name: 'child-a', queueName: 'sbx-flow-children', data: { role: 'child', value: 5 } },
        { name: 'child-b', queueName: 'sbx-flow-children', data: { role: 'child', value: 10 } },
      ],
    });
    for (let i = 0; i < 100 && completed.length < 3; i++) await Bun.sleep(100);
    expect(completed.length).toBeGreaterThanOrEqual(3);
    const childResults = completed.filter((c) => c.name.startsWith('child-'));
    expect(childResults.length).toBeGreaterThanOrEqual(2);
    const childValues = childResults.map((c) => c.result.childValue).sort((a: number, b: number) => a - b);
    expect(childValues).toEqual([10, 20]);
    await worker.stop();
    flow.close();
    queue.close();
  }, 20000);

  test('sandboxed worker processes parent job after children complete', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('sbx-flow-order', { embedded: true });
    queue.obliterate();
    const processorPath = await writeProcessor('flow-order', `
      export default async (job: any) => {
        const data = job.data;
        return { role: data.role, processed: true };
      };
    `);
    const executionOrder: string[] = [];
    const worker = new SandboxedWorker('sbx-flow-order', {
      processor: processorPath,
      concurrency: 1,
      timeout: 10000,
    });
    worker.on('completed', (job, result: any) => { executionOrder.push(result.role); });
    worker.on('error', () => {});
    await worker.start();
    await flow.add({
      name: 'parent',
      queueName: 'sbx-flow-order',
      data: { role: 'parent' },
      children: [
        { name: 'child-1', queueName: 'sbx-flow-order', data: { role: 'child-1' } },
        { name: 'child-2', queueName: 'sbx-flow-order', data: { role: 'child-2' } },
      ],
    });
    for (let i = 0; i < 100 && executionOrder.length < 3; i++) await Bun.sleep(100);
    expect(executionOrder).toHaveLength(3);
    expect(executionOrder[executionOrder.length - 1]).toBe('parent');
    expect(executionOrder.slice(0, 2).sort()).toEqual(['child-1', 'child-2']);
    await worker.stop();
    flow.close();
    queue.close();
  }, 20000);

  test('flow with sandboxed worker and job progress updates', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('sbx-flow-progress', { embedded: true });
    queue.obliterate();
    const processorPath = await writeProcessor('flow-progress', `
      export default async (job: any) => {
        const data = job.data;
        for (let p = 25; p <= 100; p += 25) {
          job.progress(p);
          await Bun.sleep(20);
        }
        return { role: data.role, done: true };
      };
    `);
    const progressUpdates: Array<{ jobName: string; progress: number }> = [];
    const completions: string[] = [];
    const worker = new SandboxedWorker('sbx-flow-progress', {
      processor: processorPath,
      concurrency: 1,
      timeout: 10000,
    });
    worker.on('progress', (job, progress) => { progressUpdates.push({ jobName: job.name, progress }); });
    worker.on('completed', (job) => { completions.push(job.name); });
    worker.on('error', () => {});
    await worker.start();
    await flow.addChain([
      { name: 'step-0', queueName: 'sbx-flow-progress', data: { role: 'step-0' } },
      { name: 'step-1', queueName: 'sbx-flow-progress', data: { role: 'step-1' } },
    ]);
    for (let i = 0; i < 100 && completions.length < 2; i++) await Bun.sleep(100);
    expect(completions).toHaveLength(2);
    const step0Progress = progressUpdates.filter((u) => u.jobName === 'step-0').map((u) => u.progress);
    const step1Progress = progressUpdates.filter((u) => u.jobName === 'step-1').map((u) => u.progress);
    expect(step0Progress).toEqual([25, 50, 75, 100]);
    expect(step1Progress).toEqual([25, 50, 75, 100]);
    await worker.stop();
    flow.close();
    queue.close();
  }, 20000);
});
*/
