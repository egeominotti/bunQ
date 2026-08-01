#!/usr/bin/env bun
/**
 * Advanced SandboxedWorker Tests (TCP Mode)
 *
 * Real-world scenarios: process isolation, crash recovery, timeout handling,
 * progress reporting, concurrent processing, flow integration with sandboxed workers.
 */

import { Queue, SandboxedWorker, FlowProducer } from '../../src/client';
import { unlink } from 'fs/promises';

const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789', 10);
const connOpts = { port: TCP_PORT };

let passed = 0;
let failed = 0;
const processorFiles: string[] = [];
const queues: Queue[] = [];

function ok(msg: string) {
  console.log(`   ✅ ${msg}`);
  passed++;
}

function fail(msg: string) {
  console.log(`   ❌ ${msg}`);
  failed++;
}

function makeQueue(name: string): Queue {
  const q = new Queue(name, { connection: connOpts });
  queues.push(q);
  return q;
}

async function writeProcessor(name: string, code: string): Promise<string> {
  const path = `${Bun.env.TMPDIR ?? '/tmp'}/tcp-sandbox-adv-${name}-${Date.now()}.ts`;
  await Bun.write(path, code);
  processorFiles.push(path);
  return path;
}

async function cleanup() {
  for (const f of processorFiles) {
    try {
      await unlink(f);
    } catch {
      /* ignore */
    }
  }
  for (const q of queues) {
    q.obliterate();
    q.close();
  }
}

async function main() {
  console.log('=== Advanced SandboxedWorker Tests (TCP) ===\n');

  // ─────────────────────────────────────────────────
  // Test 1: Basic sandboxed worker processes jobs via TCP
  // ─────────────────────────────────────────────────
  console.log('1. Testing BASIC SANDBOXED PROCESSING over TCP...');
  {
    const q = makeQueue('tcp-sbx-basic');
    q.obliterate();
    await Bun.sleep(100);

    const processorPath = await writeProcessor(
      'basic',
      `
      export default async (job: any) => {
        return { doubled: job.data.value * 2, pid: process.pid };
      };
    `
    );

    const completed: Array<{ id: string; result: { doubled: number; pid: number } }> = [];

    const worker = new SandboxedWorker('tcp-sbx-basic', {
      processor: processorPath,
      concurrency: 1,
      timeout: 10000,
      connection: connOpts,
    });

    worker.on('completed', (job, result) => {
      completed.push({ id: job.id, result: result as { doubled: number; pid: number } });
    });
    worker.on('error', () => undefined);

    await worker.start();
    await q.add('double', { value: 21 });

    for (let i = 0; i < 50 && completed.length === 0; i++) await Bun.sleep(100);

    if (completed.length === 1 && completed[0].result.doubled === 42) {
      ok(`Job processed in isolated process: doubled=42, pid=${completed[0].result.pid}`);
    } else {
      fail(`Expected doubled=42, got: ${JSON.stringify(completed)}`);
    }

    await worker.stop();
  }

  // ─────────────────────────────────────────────────
  // Test 2: Concurrent sandboxed workers
  // ─────────────────────────────────────────────────
  console.log('\n2. Testing CONCURRENT SANDBOXED WORKERS over TCP...');
  {
    const q = makeQueue('tcp-sbx-concurrent');
    q.obliterate();
    await Bun.sleep(100);

    const processorPath = await writeProcessor(
      'concurrent',
      `
      export default async (job: any) => {
        await Bun.sleep(200);
        return { value: job.data.value * 3 };
      };
    `
    );

    const completed: unknown[] = [];
    const worker = new SandboxedWorker('tcp-sbx-concurrent', {
      processor: processorPath,
      concurrency: 4,
      timeout: 10000,
      connection: connOpts,
    });

    worker.on('completed', (_job, result) => completed.push(result));
    worker.on('error', () => undefined);

    await worker.start();

    await Promise.all([
      q.add('triple', { value: 1 }),
      q.add('triple', { value: 2 }),
      q.add('triple', { value: 3 }),
      q.add('triple', { value: 4 }),
    ]);

    const start = Date.now();
    for (let i = 0; i < 100 && completed.length < 4; i++) await Bun.sleep(100);
    const elapsed = Date.now() - start;

    if (completed.length === 4) {
      const values = completed
        .map((result) => (result as { value: number }).value)
        .sort((a, b) => a - b);
      if (JSON.stringify(values) === JSON.stringify([3, 6, 9, 12])) {
        ok(`4 concurrent jobs processed in ${elapsed}ms: ${values}`);
      } else {
        fail(`Results incorrect: ${JSON.stringify(values)}`);
      }
    } else {
      fail(`Expected 4 completions, got ${completed.length}`);
    }

    await worker.stop();
  }

  // ─────────────────────────────────────────────────
  // Test 3: Progress reporting over TCP
  // ─────────────────────────────────────────────────
  console.log('\n3. Testing PROGRESS REPORTING over TCP...');
  {
    const q = makeQueue('tcp-sbx-progress');
    q.obliterate();
    await Bun.sleep(100);

    const processorPath = await writeProcessor(
      'progress',
      `
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
    `
    );

    const progressUpdates: number[] = [];
    let completed = false;

    const worker = new SandboxedWorker('tcp-sbx-progress', {
      processor: processorPath,
      concurrency: 1,
      timeout: 10000,
      connection: connOpts,
    });

    worker.on('progress', (_job, progress) => progressUpdates.push(progress));
    worker.on('completed', () => {
      completed = true;
    });
    worker.on('error', () => undefined);

    await worker.start();
    await q.add('task', {});

    for (let i = 0; i < 50 && !completed; i++) await Bun.sleep(100);

    if (completed && JSON.stringify(progressUpdates) === JSON.stringify([25, 50, 75, 100])) {
      ok(`Progress updates: ${progressUpdates.join(' -> ')}%`);
    } else {
      fail(`Expected completion with [25,50,75,100], got [${progressUpdates.join(', ')}]`);
    }

    await worker.stop();
  }

  // ─────────────────────────────────────────────────
  // Test 4: Error handling - processor throws
  // ─────────────────────────────────────────────────
  console.log('\n4. Testing ERROR HANDLING over TCP...');
  {
    const q = makeQueue('tcp-sbx-error');
    q.obliterate();
    await Bun.sleep(100);

    const processorPath = await writeProcessor(
      'error',
      `
      export default async (job: any) => {
        if (job.data.shouldFail) {
          throw new Error('Intentional failure: ' + job.data.reason);
        }
        return { ok: true };
      };
    `
    );

    const failures: Array<{ id: string; error: string }> = [];
    const completions: string[] = [];

    const worker = new SandboxedWorker('tcp-sbx-error', {
      processor: processorPath,
      concurrency: 1,
      timeout: 10000,
      connection: connOpts,
    });

    worker.on('failed', (job, err) => failures.push({ id: job.id, error: err.message }));
    worker.on('completed', (job) => completions.push(job.id));
    worker.on('error', () => undefined);

    await worker.start();

    await q.add('will-fail', { shouldFail: true, reason: 'bad input' });
    await q.add('will-pass', { shouldFail: false });

    for (let i = 0; i < 50 && (failures.length === 0 || completions.length === 0); i++) {
      await Bun.sleep(100);
    }

    if (
      failures.length === 1 &&
      failures[0].error.includes('Intentional failure: bad input') &&
      completions.length === 1
    ) {
      ok(`Error handled: "${failures[0].error}", success: ${completions[0]}`);
    } else {
      fail(
        `Expected 1 fail + 1 success, got fails=${failures.length} completes=${completions.length}`
      );
    }

    await worker.stop();
  }

  // ─────────────────────────────────────────────────
  // Test 5: Timeout - long-running job gets timed out
  // ─────────────────────────────────────────────────
  console.log('\n5. Testing TIMEOUT HANDLING over TCP...');
  {
    const q = makeQueue('tcp-sbx-timeout');
    q.obliterate();
    await Bun.sleep(100);

    const processorPath = await writeProcessor(
      'timeout',
      `
      export default async (job: any) => {
        await Bun.sleep(30000);
        return { ok: true };
      };
    `
    );

    const failures: string[] = [];

    const worker = new SandboxedWorker('tcp-sbx-timeout', {
      processor: processorPath,
      concurrency: 1,
      timeout: 1000,
      autoRestart: true,
      connection: connOpts,
    });

    worker.on('failed', (_job, err) => failures.push(err.message));
    worker.on('error', () => undefined);

    await worker.start();
    await q.add('slow-job', {});

    for (let i = 0; i < 30 && failures.length === 0; i++) await Bun.sleep(100);

    if (failures.length === 1 && failures[0].toLowerCase().includes('timed out')) {
      ok(`Timeout detected: "${failures[0]}"`);
    } else {
      fail(`Expected timeout failure, got: ${JSON.stringify(failures)}`);
    }

    await worker.stop();
  }

  // ─────────────────────────────────────────────────
  // Test 6: Error recovery (worker stays functional after failures)
  // ─────────────────────────────────────────────────
  console.log('\n6. Testing ERROR RECOVERY over TCP...');
  {
    const q = makeQueue('tcp-sbx-crash');
    q.obliterate();
    await Bun.sleep(100);

    const processorPath = await writeProcessor(
      'crash',
      `
      export default async (job: any) => {
        if (job.data.crash) {
          throw new Error('Simulated crash');
        }
        return { ok: true };
      };
    `
    );

    const failures: string[] = [];
    const completions: unknown[] = [];

    const worker = new SandboxedWorker('tcp-sbx-crash', {
      processor: processorPath,
      concurrency: 1,
      timeout: 5000,
      autoRestart: true,
      maxRestarts: 5,
      connection: connOpts,
    });

    worker.on('failed', (_job, err) => failures.push(err.message));
    worker.on('completed', (_job, result) => completions.push(result));
    worker.on('error', () => undefined);

    await worker.start();

    // First job fails
    await q.add('crash-job', { crash: true });
    for (let i = 0; i < 50 && failures.length === 0; i++) await Bun.sleep(100);

    // Worker should still be functional — next job succeeds
    await q.add('good-job', { crash: false });
    for (let i = 0; i < 50 && completions.length === 0; i++) await Bun.sleep(100);

    if (failures.length >= 1 && completions.length >= 1) {
      ok(`Worker recovered: failure="${failures[0]}", then completed ${completions.length} job(s)`);
    } else {
      fail(
        `Worker did not recover: completions=${completions.length}, failures=${failures.length}`
      );
    }

    await worker.stop();
  }

  // ─────────────────────────────────────────────────
  // Test 7: getStats returns correct pool info
  // ─────────────────────────────────────────────────
  console.log('\n7. Testing GET STATS over TCP...');
  {
    const q = makeQueue('tcp-sbx-stats');
    q.obliterate();
    await Bun.sleep(100);

    const processorPath = await writeProcessor(
      'stats',
      `
      export default async (job: any) => {
        await Bun.sleep(500);
        return { ok: true };
      };
    `
    );

    const worker = new SandboxedWorker('tcp-sbx-stats', {
      processor: processorPath,
      concurrency: 3,
      timeout: 10000,
      connection: connOpts,
    });

    worker.on('error', () => undefined);
    await worker.start();

    // Before any jobs
    const before = worker.getStats();
    if (before.total === 3 && before.idle === 3 && before.busy === 0) {
      ok(`Initial stats: total=${before.total}, idle=${before.idle}, busy=${before.busy}`);
    } else {
      fail(`Initial stats wrong: total=${before.total}, idle=${before.idle}, busy=${before.busy}`);
    }

    // Push 2 jobs so workers become busy
    await q.add('work-1', {});
    await q.add('work-2', {});
    await Bun.sleep(200);

    const during = worker.getStats();
    if (during.total === 3 && during.busy >= 1) {
      ok(`During work: busy=${during.busy}`);
    } else {
      fail(`During stats wrong: total=${during.total}, busy=${during.busy}`);
    }

    // Wait for completion
    await Bun.sleep(1000);

    const after = worker.getStats();
    if (after.busy === 0 && after.idle === 3) {
      ok(`After work: idle=${after.idle}, busy=${after.busy}`);
    } else {
      fail(`After stats wrong: idle=${after.idle}, busy=${after.busy}`);
    }

    await worker.stop();
  }

  // ─────────────────────────────────────────────────
  // Test 8: Flow integration with sandboxed worker
  // ─────────────────────────────────────────────────
  console.log('\n8. Testing FLOW CHAIN with SANDBOXED WORKER over TCP...');
  {
    const flow = new FlowProducer({ connection: connOpts, useLocks: false });
    const q = makeQueue('tcp-sbx-flow');
    q.obliterate();
    await Bun.sleep(100);

    const processorPath = await writeProcessor(
      'flow',
      `
      export default async (job: any) => {
        const step = job.data.step;
        return { step, result: step * 10 };
      };
    `
    );

    const completed: Array<{ step: number; result: number }> = [];

    const worker = new SandboxedWorker('tcp-sbx-flow', {
      processor: processorPath,
      concurrency: 2,
      timeout: 10000,
      connection: connOpts,
    });

    worker.on('completed', (_job, result) => {
      const value = result as { step: number; result: number };
      completed.push({ step: value.step, result: value.result });
    });
    worker.on('error', () => undefined);

    await worker.start();

    await flow.addChain([
      { name: 'step-0', queueName: 'tcp-sbx-flow', data: { step: 0 } },
      { name: 'step-1', queueName: 'tcp-sbx-flow', data: { step: 1 } },
      { name: 'step-2', queueName: 'tcp-sbx-flow', data: { step: 2 } },
    ]);

    for (let i = 0; i < 80 && completed.length < 3; i++) await Bun.sleep(100);

    if (completed.length === 3) {
      const steps = completed.map((c) => c.step).sort();
      if (JSON.stringify(steps) === JSON.stringify([0, 1, 2])) {
        ok(
          `Flow chain: all 3 steps completed: ${completed.map((c) => `step${c.step}=${c.result}`).join(', ')}`
        );
      } else {
        fail(`Steps wrong: ${JSON.stringify(steps)}`);
      }
    } else {
      fail(`Expected 3 completions, got ${completed.length}`);
    }

    await worker.stop();
    await flow.close();
  }

  // ─────────────────────────────────────────────────
  // Test 9: Logging from sandboxed worker
  // ─────────────────────────────────────────────────
  console.log('\n9. Testing LOG MESSAGES over TCP...');
  {
    const q = makeQueue('tcp-sbx-log');
    q.obliterate();
    await Bun.sleep(100);

    const processorPath = await writeProcessor(
      'log',
      `
      export default async (job: any) => {
        job.log('Starting processing');
        await Bun.sleep(10);
        job.log('Almost done');
        return { ok: true };
      };
    `
    );

    const logs: string[] = [];
    let completed = false;

    const worker = new SandboxedWorker('tcp-sbx-log', {
      processor: processorPath,
      concurrency: 1,
      timeout: 10000,
      connection: connOpts,
    });

    worker.on('log', (_job, message) => logs.push(message));
    worker.on('completed', () => {
      completed = true;
    });
    worker.on('error', () => undefined);

    await worker.start();
    await q.add('log-job', {});

    for (let i = 0; i < 50 && !completed; i++) await Bun.sleep(100);

    if (completed && logs.includes('Starting processing') && logs.includes('Almost done')) {
      ok(`Logs received: "${logs.join('", "')}"`);
    } else {
      fail(`Expected both log messages before completion, got [${logs.join(', ')}]`);
    }

    await worker.stop();
  }

  // ─────────────────────────────────────────────────
  // Test 10: High throughput - many jobs through sandboxed workers
  // ─────────────────────────────────────────────────
  console.log('\n10. Testing HIGH THROUGHPUT (50 jobs) over TCP...');
  {
    const q = makeQueue('tcp-sbx-throughput');
    q.obliterate();
    await Bun.sleep(100);

    const processorPath = await writeProcessor(
      'throughput',
      `
      export default async (job: any) => {
        return { idx: job.data.idx, squared: job.data.idx * job.data.idx };
      };
    `
    );

    let completedCount = 0;
    const JOB_COUNT = 50;

    const worker = new SandboxedWorker('tcp-sbx-throughput', {
      processor: processorPath,
      concurrency: 4,
      timeout: 10000,
      connection: connOpts,
    });

    worker.on('completed', () => completedCount++);
    worker.on('error', () => undefined);

    await worker.start();

    for (let i = 0; i < JOB_COUNT; i++) {
      await q.add('compute', { idx: i });
    }

    const start = Date.now();
    for (let i = 0; i < 200 && completedCount < JOB_COUNT; i++) await Bun.sleep(100);
    const elapsed = Date.now() - start;

    if (completedCount === JOB_COUNT) {
      ok(`${JOB_COUNT} jobs completed in ${elapsed}ms`);
    } else {
      fail(`Only ${completedCount}/${JOB_COUNT} jobs completed in ${elapsed}ms`);
    }

    await worker.stop();
  }

  // Cleanup temp files and close all queues
  await cleanup();

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch((e) => {
  console.error(`Fatal error: ${e}`);
  process.exit(1);
});
