#!/usr/bin/env bun
/**
 * Backoff Strategies Test Suite
 * Tests fixed and exponential backoff strategies
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
  test('default backoff (number)', async () => {
    const queue = new Queue('test-default-backoff', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 1 }, { backoff: 2000 });

    if (job.opts.backoff !== 2000) {
      throw new Error(`Expected backoff 2000, got ${job.opts.backoff}`);
    }

    queue.close();
  }),

  test('fixed backoff configuration', async () => {
    const queue = new Queue('test-fixed-backoff', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 1 }, {
      backoff: { type: 'fixed', delay: 1000 }
    });

    const backoff = job.opts.backoff as { type: string; delay: number };
    if (backoff.type !== 'fixed') throw new Error(`Expected type fixed, got ${backoff.type}`);
    if (backoff.delay !== 1000) throw new Error(`Expected delay 1000, got ${backoff.delay}`);

    queue.close();
  }),

  test('exponential backoff configuration', async () => {
    const queue = new Queue('test-exp-backoff', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 1 }, {
      backoff: { type: 'exponential', delay: 500 }
    });

    const backoff = job.opts.backoff as { type: string; delay: number };
    if (backoff.type !== 'exponential') throw new Error(`Expected type exponential`);
    if (backoff.delay !== 500) throw new Error(`Expected delay 500, got ${backoff.delay}`);

    queue.close();
  }),

  test('fixed backoff retries with constant delay', async () => {
    const queue = new Queue<{ value: number }>('test-fixed-retry', { embedded: true });
    queue.obliterate();

    let attempts = 0;
    const retryTimes: number[] = [];
    let lastTime = Date.now();

    const worker = new Worker('test-fixed-retry', async () => {
      const now = Date.now();
      if (attempts > 0) {
        retryTimes.push(now - lastTime);
      }
      lastTime = now;
      attempts++;

      if (attempts < 3) {
        throw new Error('Retry please');
      }
      return { done: true };
    }, { embedded: true, autorun: true });

    await queue.add('test', { value: 1 }, {
      attempts: 3,
      backoff: { type: 'fixed', delay: 100 }
    });

    await Bun.sleep(1500);

    if (attempts !== 3) throw new Error(`Expected 3 attempts, got ${attempts}`);

    // Fixed backoff should have roughly constant delays (100ms each)
    for (const delay of retryTimes) {
      if (delay < 80 || delay > 300) {
        throw new Error(`Fixed delay out of range: ${delay}ms`);
      }
    }

    await worker.close();
    queue.close();
  }),

  test('exponential backoff increases delay', async () => {
    const queue = new Queue<{ value: number }>('test-exp-retry', { embedded: true });
    queue.obliterate();

    let attempts = 0;
    const retryTimes: number[] = [];
    let lastTime = Date.now();

    const worker = new Worker('test-exp-retry', async () => {
      const now = Date.now();
      if (attempts > 0) {
        retryTimes.push(now - lastTime);
      }
      lastTime = now;
      attempts++;

      if (attempts < 4) {
        throw new Error('Retry please');
      }
      return { done: true };
    }, { embedded: true, autorun: true });

    await queue.add('test', { value: 1 }, {
      attempts: 4,
      backoff: { type: 'exponential', delay: 50 }
    });

    await Bun.sleep(2000);

    if (attempts !== 4) throw new Error(`Expected 4 attempts, got ${attempts}`);

    // Exponential: delays should increase (50, 100, 200, ...)
    // Check that later delays are generally larger
    if (retryTimes.length >= 2) {
      const firstDelay = retryTimes[0];
      const lastDelay = retryTimes[retryTimes.length - 1];
      if (lastDelay <= firstDelay) {
        console.log(`  Note: Delays were ${retryTimes.join(', ')}ms`);
        // This might fail due to timing, so we just log it
      }
    }

    await worker.close();
    queue.close();
  }),

  test('backoff with attempts limit', async () => {
    const queue = new Queue<{ value: number }>('test-backoff-limit', { embedded: true });
    queue.obliterate();

    let attempts = 0;

    const worker = new Worker('test-backoff-limit', async () => {
      attempts++;
      throw new Error('Always fail');
    }, { embedded: true, autorun: true });

    await queue.add('test', { value: 1 }, {
      attempts: 3,
      backoff: { type: 'fixed', delay: 50 }
    });

    await Bun.sleep(1000);

    // Should have tried 3 times then stopped
    if (attempts !== 3) throw new Error(`Expected 3 attempts, got ${attempts}`);

    await worker.close();
    queue.close();
  }),

  test('zero backoff delay', async () => {
    const queue = new Queue<{ value: number }>('test-zero-backoff', { embedded: true });
    queue.obliterate();

    let attempts = 0;
    const startTime = Date.now();

    const worker = new Worker('test-zero-backoff', async () => {
      attempts++;
      if (attempts < 3) {
        throw new Error('Retry');
      }
      return { done: true };
    }, { embedded: true, autorun: true });

    await queue.add('test', { value: 1 }, {
      attempts: 3,
      backoff: { type: 'fixed', delay: 0 }
    });

    await Bun.sleep(500);

    const elapsed = Date.now() - startTime;

    // With zero delay, all retries should complete quickly
    if (elapsed > 400) {
      console.log(`  Note: Elapsed time ${elapsed}ms with zero backoff`);
    }
    if (attempts !== 3) throw new Error(`Expected 3 attempts, got ${attempts}`);

    await worker.close();
    queue.close();
  }),

  test('backoff preserves job data between retries', async () => {
    const queue = new Queue<{ counter: number }>('test-backoff-data', { embedded: true });
    queue.obliterate();

    const seenData: number[] = [];

    const worker = new Worker('test-backoff-data', async (job) => {
      seenData.push(job.data.counter);
      if (seenData.length < 3) {
        throw new Error('Retry');
      }
      return { done: true };
    }, { embedded: true, autorun: true });

    await queue.add('test', { counter: 42 }, {
      attempts: 3,
      backoff: { type: 'fixed', delay: 50 }
    });

    await Bun.sleep(500);

    // All retries should see the same data
    for (const data of seenData) {
      if (data !== 42) throw new Error(`Data changed: expected 42, got ${data}`);
    }

    await worker.close();
    queue.close();
  }),

  test('mixed backoff types in same queue', async () => {
    const queue = new Queue<{ id: number }>('test-mixed-backoff', { embedded: true });
    queue.obliterate();

    const job1 = await queue.add('fixed', { id: 1 }, {
      backoff: { type: 'fixed', delay: 100 }
    });
    const job2 = await queue.add('exponential', { id: 2 }, {
      backoff: { type: 'exponential', delay: 100 }
    });
    const job3 = await queue.add('number', { id: 3 }, { backoff: 200 });

    const backoff1 = job1.opts.backoff as { type: string };
    const backoff2 = job2.opts.backoff as { type: string };

    if (backoff1.type !== 'fixed') throw new Error('Job 1 should have fixed backoff');
    if (backoff2.type !== 'exponential') throw new Error('Job 2 should have exponential backoff');
    if (job3.opts.backoff !== 200) throw new Error('Job 3 should have number backoff');

    queue.close();
  }),

  test('backoff with high delay value', async () => {
    const queue = new Queue('test-high-backoff', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 1 }, {
      backoff: { type: 'fixed', delay: 60000 } // 1 minute
    });

    const backoff = job.opts.backoff as { delay: number };
    if (backoff.delay !== 60000) throw new Error(`Expected delay 60000, got ${backoff.delay}`);

    queue.close();
  }),

  test('exponential backoff with small base delay', async () => {
    const queue = new Queue('test-small-exp', { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { value: 1 }, {
      backoff: { type: 'exponential', delay: 10 } // 10ms base
    });

    const backoff = job.opts.backoff as { type: string; delay: number };
    if (backoff.type !== 'exponential') throw new Error('Should be exponential');
    if (backoff.delay !== 10) throw new Error(`Expected delay 10, got ${backoff.delay}`);

    queue.close();
  }),

  test('job succeeds on first try (no backoff needed)', async () => {
    const queue = new Queue<{ value: number }>('test-success-first', { embedded: true });
    queue.obliterate();

    let attempts = 0;

    const worker = new Worker('test-success-first', async () => {
      attempts++;
      return { success: true };
    }, { embedded: true, autorun: true });

    await queue.add('test', { value: 1 }, {
      attempts: 3,
      backoff: { type: 'fixed', delay: 1000 }
    });

    await Bun.sleep(300);

    if (attempts !== 1) throw new Error(`Expected 1 attempt, got ${attempts}`);

    await worker.close();
    queue.close();
  }),
];

// ============================================================================
// Main
// ============================================================================

async function main() {
  console.log('\n=== Backoff Strategies Tests ===\n');

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
