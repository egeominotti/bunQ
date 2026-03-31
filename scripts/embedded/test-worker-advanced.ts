/**
 * Test Worker Advanced Methods - BullMQ v5 Compatible
 * Tests: rateLimit, startStalledCheckTimer, delay, isRateLimited
 */

import { Queue, Worker, shutdownManager } from '../../src/client';

const QUEUE_NAME = 'test-worker-advanced';

interface TestResult {
  name: string;
  passed: boolean;
  error?: string;
}

const results: TestResult[] = [];

function test(name: string, fn: () => Promise<void> | void) {
  return async () => {
    try {
      await fn();
      results.push({ name, passed: true });
      console.log(`✅ ${name}`);
    } catch (err) {
      results.push({ name, passed: false, error: String(err) });
      console.log(`❌ ${name}: ${err}`);
    }
  };
}

async function cleanup() {
  const queue = new Queue(QUEUE_NAME, { embedded: true });
  queue.obliterate();
  queue.close();
}

async function runTests() {
  console.log('\n🧪 Worker Advanced Methods Tests (Embedded)\n');

  await cleanup();

  // Test 1: Worker has rateLimit method
  await test('Worker has rateLimit method', async () => {
    const worker = new Worker(
      QUEUE_NAME,
      async () => ({ done: true }),
      { embedded: true, autorun: false }
    );

    if (typeof worker.rateLimit !== 'function') {
      throw new Error('rateLimit is not a function');
    }

    await worker.close();
  })();

  // Test 2: rateLimit applies rate limiting
  await test('rateLimit applies rate limiting', async () => {
    const worker = new Worker(
      QUEUE_NAME,
      async () => ({ done: true }),
      { embedded: true, autorun: false }
    );

    worker.rateLimit(1000);

    if (!worker.isRateLimited()) {
      throw new Error('Worker should be rate limited');
    }

    await worker.close();
  })();

  // Test 3: isRateLimited returns false initially
  await test('isRateLimited returns false initially', async () => {
    const worker = new Worker(
      QUEUE_NAME,
      async () => ({ done: true }),
      { embedded: true, autorun: false }
    );

    if (worker.isRateLimited()) {
      throw new Error('Worker should not be rate limited initially');
    }

    await worker.close();
  })();

  // Test 4: isRateLimited returns false after expiry
  await test('isRateLimited returns false after expiry', async () => {
    const worker = new Worker(
      QUEUE_NAME,
      async () => ({ done: true }),
      { embedded: true, autorun: false }
    );

    worker.rateLimit(50);

    if (!worker.isRateLimited()) {
      throw new Error('Worker should be rate limited');
    }

    await Bun.sleep(100);

    if (worker.isRateLimited()) {
      throw new Error('Rate limit should have expired');
    }

    await worker.close();
  })();

  // Test 5: Worker has startStalledCheckTimer method
  await test('Worker has startStalledCheckTimer method', async () => {
    const worker = new Worker(
      QUEUE_NAME,
      async () => ({ done: true }),
      { embedded: true, autorun: false }
    );

    if (typeof worker.startStalledCheckTimer !== 'function') {
      throw new Error('startStalledCheckTimer is not a function');
    }

    await worker.close();
  })();

  // Test 6: startStalledCheckTimer does not throw
  await test('startStalledCheckTimer does not throw', async () => {
    const worker = new Worker(
      QUEUE_NAME,
      async () => ({ done: true }),
      { embedded: true, autorun: false }
    );

    await worker.startStalledCheckTimer();

    await worker.close();
  })();

  // Test 7: Worker has delay method
  await test('Worker has delay method', async () => {
    const worker = new Worker(
      QUEUE_NAME,
      async () => ({ done: true }),
      { embedded: true, autorun: false }
    );

    if (typeof worker.delay !== 'function') {
      throw new Error('delay is not a function');
    }

    await worker.close();
  })();

  // Test 8: delay with 0 resolves immediately
  await test('delay with 0 resolves immediately', async () => {
    const worker = new Worker(
      QUEUE_NAME,
      async () => ({ done: true }),
      { embedded: true, autorun: false }
    );

    const start = Date.now();
    await worker.delay(0);
    const elapsed = Date.now() - start;

    if (elapsed > 50) {
      throw new Error(`Delay took too long: ${elapsed}ms`);
    }

    await worker.close();
  })();

  // Test 9: delay waits for specified time
  await test('delay waits for specified time', async () => {
    const worker = new Worker(
      QUEUE_NAME,
      async () => ({ done: true }),
      { embedded: true, autorun: false }
    );

    const start = Date.now();
    await worker.delay(100);
    const elapsed = Date.now() - start;

    if (elapsed < 90) {
      throw new Error(`Delay was too short: ${elapsed}ms`);
    }

    await worker.close();
  })();

  // Test 10: delay can be aborted
  await test('delay can be aborted', async () => {
    const worker = new Worker(
      QUEUE_NAME,
      async () => ({ done: true }),
      { embedded: true, autorun: false }
    );

    const abortController = new AbortController();
    const delayPromise = worker.delay(1000, abortController);

    setTimeout(() => abortController.abort(), 50);

    try {
      await delayPromise;
      throw new Error('Delay should have been aborted');
    } catch (err) {
      if (!(err instanceof Error) || !err.message.includes('aborted')) {
        throw new Error('Expected abort error');
      }
    }

    await worker.close();
  })();

  await cleanup();
  shutdownManager();

  // Summary
  console.log('\n📊 Results:');
  const passed = results.filter((r) => r.passed).length;
  const failed = results.filter((r) => !r.passed).length;
  console.log(`   Passed: ${passed}`);
  console.log(`   Failed: ${failed}`);
  console.log(`   Total: ${results.length}`);

  if (failed > 0) {
    console.log('\n❌ Failed tests:');
    results.filter((r) => !r.passed).forEach((r) => console.log(`   - ${r.name}: ${r.error}`));
    process.exit(1);
  }

  console.log('\n✅ All tests passed!\n');
}

runTests().catch(console.error);
