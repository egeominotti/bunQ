/**
 * Test Job Advanced Methods - BullMQ v5 Compatible
 * Tests: discard, getFailedChildrenValues, getIgnoredChildrenFailures,
 *        removeChildDependency, removeDeduplicationKey, removeUnprocessedChildren
 */

import { Queue, shutdownManager } from '../../src/client';

const QUEUE_NAME = 'test-job-advanced';

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
  console.log('\n🧪 Job Advanced Methods Tests (Embedded)\n');

  await cleanup();

  const queue = new Queue<{ value: number }>(QUEUE_NAME, { embedded: true });

  // Test 1: Job has discard method
  await test('Job has discard method', async () => {
    const job = await queue.add('test', { value: 1 });

    if (typeof job.discard !== 'function') {
      throw new Error('discard is not a function');
    }
  })();

  // Test 2: discard does not throw
  await test('discard does not throw', async () => {
    const job = await queue.add('test', { value: 2 });
    job.discard();
    // Should not throw
  })();

  // Test 3: Job has getFailedChildrenValues method
  await test('Job has getFailedChildrenValues method', async () => {
    const job = await queue.add('test', { value: 3 });

    if (typeof job.getFailedChildrenValues !== 'function') {
      throw new Error('getFailedChildrenValues is not a function');
    }
  })();

  // Test 4: getFailedChildrenValues returns empty object
  await test('getFailedChildrenValues returns empty object for job without children', async () => {
    const job = await queue.add('test', { value: 4 });
    const failedValues = await job.getFailedChildrenValues();

    if (typeof failedValues !== 'object' || Object.keys(failedValues).length !== 0) {
      throw new Error('Expected empty object');
    }
  })();

  // Test 5: Job has getIgnoredChildrenFailures method
  await test('Job has getIgnoredChildrenFailures method', async () => {
    const job = await queue.add('test', { value: 5 });

    if (typeof job.getIgnoredChildrenFailures !== 'function') {
      throw new Error('getIgnoredChildrenFailures is not a function');
    }
  })();

  // Test 6: getIgnoredChildrenFailures returns empty object
  await test('getIgnoredChildrenFailures returns empty object', async () => {
    const job = await queue.add('test', { value: 6 });
    const ignoredFailures = await job.getIgnoredChildrenFailures();

    if (typeof ignoredFailures !== 'object' || Object.keys(ignoredFailures).length !== 0) {
      throw new Error('Expected empty object');
    }
  })();

  // Test 7: Job has removeChildDependency method
  await test('Job has removeChildDependency method', async () => {
    const job = await queue.add('test', { value: 7 });

    if (typeof job.removeChildDependency !== 'function') {
      throw new Error('removeChildDependency is not a function');
    }
  })();

  // Test 8: removeChildDependency returns false for job without parent
  await test('removeChildDependency returns false for job without parent', async () => {
    const job = await queue.add('test', { value: 8 });
    const removed = await job.removeChildDependency();

    if (removed !== false) {
      throw new Error('Expected false');
    }
  })();

  // Test 9: Job has removeDeduplicationKey method
  await test('Job has removeDeduplicationKey method', async () => {
    const job = await queue.add('test', { value: 9 });

    if (typeof job.removeDeduplicationKey !== 'function') {
      throw new Error('removeDeduplicationKey is not a function');
    }
  })();

  // Test 10: removeDeduplicationKey returns false for job without dedupe
  await test('removeDeduplicationKey returns false for job without dedupe', async () => {
    const job = await queue.add('test', { value: 10 });
    const removed = await job.removeDeduplicationKey();

    if (removed !== false) {
      throw new Error('Expected false');
    }
  })();

  // Test 11: Job has removeUnprocessedChildren method
  await test('Job has removeUnprocessedChildren method', async () => {
    const job = await queue.add('test', { value: 11 });

    if (typeof job.removeUnprocessedChildren !== 'function') {
      throw new Error('removeUnprocessedChildren is not a function');
    }
  })();

  // Test 12: removeUnprocessedChildren resolves for job without children
  await test('removeUnprocessedChildren resolves for job without children', async () => {
    const job = await queue.add('test', { value: 12 });
    await job.removeUnprocessedChildren();
    // Should not throw
  })();

  queue.close();
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
