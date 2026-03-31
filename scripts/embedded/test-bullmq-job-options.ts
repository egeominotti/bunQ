#!/usr/bin/env bun
/**
 * BullMQ v5 JobOptions Test Suite
 * Tests all BullMQ v5 compatible job options
 */

import { Queue, Worker, shutdownManager } from '../../src/client';

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
  test('backoff as number (legacy)', async () => {
    const queue = new Queue('test-backoff-num', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 1 }, { backoff: 5000 });
    if (!job.id) throw new Error('Job not created');
    if (job.opts.backoff !== 5000) throw new Error(`Expected backoff 5000, got ${job.opts.backoff}`);

    queue.close();
  }),

  test('backoff as fixed object', async () => {
    const queue = new Queue('test-backoff-fixed', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 1 }, {
      backoff: { type: 'fixed', delay: 3000 }
    });
    if (!job.id) throw new Error('Job not created');
    const opts = job.opts.backoff as { type: string; delay: number };
    if (opts.type !== 'fixed') throw new Error(`Expected type fixed, got ${opts.type}`);
    if (opts.delay !== 3000) throw new Error(`Expected delay 3000, got ${opts.delay}`);

    queue.close();
  }),

  test('backoff as exponential object', async () => {
    const queue = new Queue('test-backoff-exp', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 1 }, {
      backoff: { type: 'exponential', delay: 1000 }
    });
    const opts = job.opts.backoff as { type: string; delay: number };
    if (opts.type !== 'exponential') throw new Error(`Expected type exponential, got ${opts.type}`);

    queue.close();
  }),

  test('lifo option', async () => {
    const queue = new Queue('test-lifo', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 1 }, { lifo: true });
    if (job.opts.lifo !== true) throw new Error(`Expected lifo true, got ${job.opts.lifo}`);

    queue.close();
  }),

  test('lifo processing order', async () => {
    const queue = new Queue<{ order: number }>('test-lifo-order', { embedded: true });
    queue.obliterate();
    queue.pause();

    await queue.add('test', { order: 1 }, { lifo: true });
    await queue.add('test', { order: 2 }, { lifo: true });
    await queue.add('test', { order: 3 }, { lifo: true });

    const processedOrder: number[] = [];
    const worker = new Worker('test-lifo-order', async (job) => {
      processedOrder.push(job.data.order);
      return { done: true };
    }, { embedded: true, autorun: true });

    queue.resume();
    await Bun.sleep(500);

    // LIFO: 3, 2, 1
    if (processedOrder[0] !== 3) throw new Error(`Expected first job order 3, got ${processedOrder[0]}`);

    await worker.close();
    queue.close();
  }),

  test('stackTraceLimit option', async () => {
    const queue = new Queue('test-stack', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 1 }, { stackTraceLimit: 5 });
    if (job.opts.stackTraceLimit !== 5) throw new Error(`Expected stackTraceLimit 5, got ${job.opts.stackTraceLimit}`);

    queue.close();
  }),

  test('keepLogs option', async () => {
    const queue = new Queue('test-logs', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 1 }, { keepLogs: 100 });
    if (job.opts.keepLogs !== 100) throw new Error(`Expected keepLogs 100, got ${job.opts.keepLogs}`);

    queue.close();
  }),

  test('sizeLimit option', async () => {
    const queue = new Queue('test-size', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 1 }, { sizeLimit: 1024 * 1024 });
    if (job.opts.sizeLimit !== 1024 * 1024) throw new Error(`Expected sizeLimit 1MB`);

    queue.close();
  }),

  test('failParentOnFailure option', async () => {
    const queue = new Queue('test-fail-parent', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 1 }, { failParentOnFailure: true });
    if (job.opts.failParentOnFailure !== true) throw new Error('Expected failParentOnFailure true');

    queue.close();
  }),

  test('removeDependencyOnFailure option', async () => {
    const queue = new Queue('test-remove-dep', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 1 }, { removeDependencyOnFailure: true });
    if (job.opts.removeDependencyOnFailure !== true) throw new Error('Expected removeDependencyOnFailure true');

    queue.close();
  }),

  test('deduplication option', async () => {
    const queue = new Queue('test-dedup', { embedded: true });
    queue.obliterate();

    const job1 = await queue.add('test', { value: 1 }, {
      deduplication: { id: 'unique-1', ttl: 60000 }
    });
    const job2 = await queue.add('test', { value: 2 }, {
      deduplication: { id: 'unique-1', ttl: 60000 }
    });

    // Should be deduplicated (same ID)
    if (job1.id !== job2.id) throw new Error('Expected jobs to be deduplicated');

    queue.close();
  }),

  test('debounce option', async () => {
    const queue = new Queue('test-debounce', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 1 }, {
      debounce: { id: 'debounce-1', ttl: 1000 }
    });
    if (!job.id) throw new Error('Job not created');

    queue.close();
  }),

  test('repeat with extended options', async () => {
    const queue = new Queue('test-repeat-ext', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 1 }, {
      repeat: {
        every: 5000,
        limit: 10,
        startDate: Date.now(),
        endDate: Date.now() + 3600000,
        tz: 'UTC',
        immediately: true,
        offset: 100,
        jobId: 'repeat-job-1',
      }
    });
    if (!job.id) throw new Error('Job not created');

    queue.close();
  }),

  test('removeOnComplete as KeepJobs object', async () => {
    const queue = new Queue('test-remove-complete', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 1 }, {
      removeOnComplete: { age: 3600000, count: 1000 }
    });
    if (!job.id) throw new Error('Job not created');

    queue.close();
  }),

  test('removeOnFail as KeepJobs object', async () => {
    const queue = new Queue('test-remove-fail', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 1 }, {
      removeOnFail: { age: 86400000, count: 500 }
    });
    if (!job.id) throw new Error('Job not created');

    queue.close();
  }),

  test('parent option', async () => {
    const queue = new Queue('test-parent', { embedded: true });
    queue.obliterate();

    const parentJob = await queue.add('parent', { value: 0 });
    const childJob = await queue.add('child', { value: 1 }, {
      parent: { id: parentJob.id, queue: 'test-parent' }
    });

    if (!childJob.opts.parent) throw new Error('Expected parent option');
    if (childJob.opts.parent.id !== parentJob.id) throw new Error('Parent ID mismatch');

    queue.close();
  }),

  test('all options combined', async () => {
    const queue = new Queue('test-combined', { embedded: true });
    queue.obliterate();

    const job = await queue.add('comprehensive', { value: 42 }, {
      priority: 10,
      delay: 100,
      attempts: 5,
      timeout: 30000,
      backoff: { type: 'exponential', delay: 1000 },
      lifo: false,
      stackTraceLimit: 15,
      keepLogs: 50,
      sizeLimit: 1024 * 1024,
      failParentOnFailure: false,
      removeDependencyOnFailure: false,
      deduplication: { id: 'combined-dedup', ttl: 60000 },
    });

    if (!job.id) throw new Error('Job not created');
    if (job.opts.priority !== 10) throw new Error('Priority mismatch');
    if (job.opts.stackTraceLimit !== 15) throw new Error('stackTraceLimit mismatch');

    queue.close();
  }),

  test('addBulk with BullMQ v5 options', async () => {
    const queue = new Queue<{ value: number }>('test-bulk-opts', { embedded: true });
    queue.obliterate();

    const jobs = await queue.addBulk([
      { name: 'job1', data: { value: 1 }, opts: { lifo: true, stackTraceLimit: 5 } },
      { name: 'job2', data: { value: 2 }, opts: { failParentOnFailure: true } },
      { name: 'job3', data: { value: 3 }, opts: { backoff: { type: 'fixed', delay: 500 } } },
    ]);

    if (jobs.length !== 3) throw new Error(`Expected 3 jobs, got ${jobs.length}`);
    // Verify jobs were created (addBulk uses simplified job objects)
    for (let i = 0; i < 3; i++) {
      if (!jobs[i].id) throw new Error(`Job ${i + 1} ID not set`);
    }

    // Verify options were applied by fetching full job
    const fullJob = await queue.getJob(jobs[0].id);
    if (!fullJob) throw new Error('Could not fetch job 1');
    if (fullJob.opts.lifo !== true) throw new Error('Job 1 lifo mismatch');

    queue.close();
  }),
];

// ============================================================================
// Main
// ============================================================================

async function main() {
  console.log('\n=== BullMQ v5 JobOptions Tests ===\n');

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
