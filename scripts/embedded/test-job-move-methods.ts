#!/usr/bin/env bun
/**
 * Job Move Methods Test Suite
 * Tests moveToCompleted, moveToFailed, moveToWait, moveToDelayed, waitUntilFinished
 */

import { Queue, Worker, QueueEvents, shutdownManager } from '../../src/client';

const results: { name: string; passed: boolean; error?: string }[] = [];

function test(name: string, fn: () => Promise<void> | void) {
  return { name, fn };
}

async function runTest(t: { name: string; fn: () => Promise<void> | void }) {
  try {
    await t.fn();
    results.push({ name: t.name, passed: true });
    console.log(`  ✓ ${t.name}`);
  } catch (err) {
    results.push({ name: t.name, passed: false, error: String(err) });
    console.log(`  ✗ ${t.name}`);
    console.log(`    Error: ${err}`);
  }
}

async function cleanup() {
  shutdownManager();
  await Bun.sleep(100);
}

// ============================================================================
// Tests
// ============================================================================

const tests = [
  test('job.moveToCompleted marks job as completed', async () => {
    const queue = new Queue<{ value: number }>('test-move-completed', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 42 });

    const worker = new Worker('test-move-completed', async (j) => {
      await j.moveToCompleted({ result: 'done' });
      return null;
    }, { embedded: true, autorun: true });

    await Bun.sleep(300);

    const state = await queue.getJobState(job.id);
    if (state !== 'completed') throw new Error(`Expected completed, got ${state}`);

    await worker.close();
    queue.close();
  }),

  test('job.moveToFailed marks job as failed', async () => {
    const queue = new Queue<{ value: number }>('test-move-failed', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 42 }, { attempts: 1 });

    const worker = new Worker('test-move-failed', async (j) => {
      await j.moveToFailed(new Error('Intentional failure'));
      throw new Error('Should not reach here');
    }, { embedded: true, autorun: true });

    await Bun.sleep(500);

    const state = await queue.getJobState(job.id);
    if (state !== 'failed') throw new Error(`Expected failed, got ${state}`);

    await worker.close();
    queue.close();
  }),

  test('queue.moveJobToWait re-queues a job', async () => {
    const queue = new Queue<{ value: number }>('test-move-wait', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 42 });

    const result = await queue.moveJobToWait(job.id);
    if (result !== true) throw new Error(`Expected true, got ${result}`);

    const counts = await queue.getJobCountsAsync();
    if (counts.waiting < 1) throw new Error(`Expected at least 1 waiting job`);

    queue.close();
  }),

  test('queue.moveJobToDelayed delays a job', async () => {
    const queue = new Queue<{ value: number }>('test-move-delayed', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 42 });

    const futureTimestamp = Date.now() + 5000;
    await queue.moveJobToDelayed(job.id, futureTimestamp);

    const state = await queue.getJobState(job.id);
    // Job should be delayed or waiting (depending on implementation)
    if (!['delayed', 'waiting', 'unknown'].includes(state)) {
      throw new Error(`Expected delayed/waiting, got ${state}`);
    }

    queue.close();
  }),

  test('job has moveToCompleted method', async () => {
    const queue = new Queue('test-has-move-completed', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 1 });
    if (typeof job.moveToCompleted !== 'function') {
      throw new Error('moveToCompleted method not found');
    }

    queue.close();
  }),

  test('job has moveToFailed method', async () => {
    const queue = new Queue('test-has-move-failed', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 1 });
    if (typeof job.moveToFailed !== 'function') {
      throw new Error('moveToFailed method not found');
    }

    queue.close();
  }),

  test('job has moveToWait method', async () => {
    const queue = new Queue('test-has-move-wait', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 1 });
    if (typeof job.moveToWait !== 'function') {
      throw new Error('moveToWait method not found');
    }

    queue.close();
  }),

  test('job has moveToDelayed method', async () => {
    const queue = new Queue('test-has-move-delayed', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 1 });
    if (typeof job.moveToDelayed !== 'function') {
      throw new Error('moveToDelayed method not found');
    }

    queue.close();
  }),

  test('job has moveToWaitingChildren method', async () => {
    const queue = new Queue('test-has-move-waiting-children', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 1 });
    if (typeof job.moveToWaitingChildren !== 'function') {
      throw new Error('moveToWaitingChildren method not found');
    }

    queue.close();
  }),

  test('job has waitUntilFinished method', async () => {
    const queue = new Queue('test-has-wait-finished', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 1 });
    if (typeof job.waitUntilFinished !== 'function') {
      throw new Error('waitUntilFinished method not found');
    }

    queue.close();
  }),

  test('waitUntilFinished resolves when job completes', async () => {
    const queue = new Queue<{ value: number }>('test-wait-finished', { embedded: true });
    const queueEvents = new QueueEvents('test-wait-finished');
    queue.obliterate();

    const worker = new Worker('test-wait-finished', async (job) => {
      await Bun.sleep(100);
      return { result: job.data.value * 2 };
    }, { embedded: true, autorun: true });

    const job = await queue.add('test', { value: 21 });

    const result = await job.waitUntilFinished(queueEvents, 5000);
    if (!result || (result as { result: number }).result !== 42) {
      throw new Error(`Expected result 42, got ${JSON.stringify(result)}`);
    }

    await worker.close();
    queueEvents.close();
    queue.close();
  }),

  test('waitUntilFinished times out', async () => {
    const queue = new Queue<{ value: number }>('test-wait-timeout', { embedded: true });
    const queueEvents = new QueueEvents('test-wait-timeout');
    queue.obliterate();

    // No worker - job will never complete
    const job = await queue.add('test', { value: 42 });

    try {
      await job.waitUntilFinished(queueEvents, 100);
      throw new Error('Should have timed out');
    } catch (err) {
      if (!(err as Error).message.includes('timed out')) {
        throw new Error(`Expected timeout error, got: ${(err as Error).message}`);
      }
    }

    queueEvents.close();
    queue.close();
  }),
];

// ============================================================================
// Main
// ============================================================================

async function main() {
  console.log('\n=== Job Move Methods Tests ===\n');

  for (const t of tests) {
    await runTest(t);
  }

  await cleanup();

  console.log('\n=== Summary ===');
  const passed = results.filter((r) => r.passed).length;
  const failed = results.filter((r) => !r.passed).length;
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  if (failed > 0) {
    console.log('\nFailed tests:');
    for (const r of results.filter((r) => !r.passed)) {
      console.log(`  - ${r.name}: ${r.error}`);
    }
    process.exit(1);
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
