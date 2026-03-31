/**
 * Test Parent Failure Handling Options - BullMQ v5 Compatible
 * Tests: continueParentOnFailure, ignoreDependencyOnFailure
 */

import { Queue, shutdownManager } from '../../src/client';

const QUEUE_NAME = 'test-parent-failure';

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
  console.log('\n🧪 Parent Failure Handling Options Tests (Embedded)\n');

  await cleanup();

  const queue = new Queue<{ value: number }>(QUEUE_NAME, { embedded: true });

  // Test 1: Accept continueParentOnFailure option with true
  await test('Accept continueParentOnFailure option with true', async () => {
    const job = await queue.add('test', { value: 1 }, {
      continueParentOnFailure: true,
    });

    if (!job || !job.id) {
      throw new Error('Job not created');
    }
  })();

  // Test 2: Accept continueParentOnFailure option with false
  await test('Accept continueParentOnFailure option with false', async () => {
    const job = await queue.add('test', { value: 2 }, {
      continueParentOnFailure: false,
    });

    if (!job || !job.id) {
      throw new Error('Job not created');
    }
  })();

  // Test 3: Accept ignoreDependencyOnFailure option with true
  await test('Accept ignoreDependencyOnFailure option with true', async () => {
    const job = await queue.add('test', { value: 3 }, {
      ignoreDependencyOnFailure: true,
    });

    if (!job || !job.id) {
      throw new Error('Job not created');
    }
  })();

  // Test 4: Accept ignoreDependencyOnFailure option with false
  await test('Accept ignoreDependencyOnFailure option with false', async () => {
    const job = await queue.add('test', { value: 4 }, {
      ignoreDependencyOnFailure: false,
    });

    if (!job || !job.id) {
      throw new Error('Job not created');
    }
  })();

  // Test 5: Accept both options together
  await test('Accept both failure options together', async () => {
    const job = await queue.add('test', { value: 5 }, {
      continueParentOnFailure: true,
      ignoreDependencyOnFailure: true,
    });

    if (!job || !job.id) {
      throw new Error('Job not created');
    }
  })();

  // Test 6: Accept custom timestamp option
  await test('Accept custom timestamp option', async () => {
    const customTimestamp = Date.now() - 10000; // 10 seconds ago
    const job = await queue.add('test', { value: 6 }, {
      timestamp: customTimestamp,
    });

    if (!job || !job.id) {
      throw new Error('Job not created');
    }

    // Timestamp should be set
    if (typeof job.timestamp !== 'number') {
      throw new Error('Timestamp not set');
    }
  })();

  // Test 7: Accept all failure and dedup options together
  await test('Accept all advanced options together', async () => {
    const job = await queue.add('test', { value: 7 }, {
      continueParentOnFailure: true,
      ignoreDependencyOnFailure: true,
      failParentOnFailure: false,
      removeDependencyOnFailure: false,
      timestamp: Date.now(),
    });

    if (!job || !job.id) {
      throw new Error('Job not created');
    }
  })();

  // Test 8: Deduplication extend option
  await test('Accept deduplication extend option', async () => {
    const job = await queue.add('test', { value: 8 }, {
      deduplication: {
        id: 'test-extend',
        ttl: 1000,
        extend: true,
      },
    });

    if (!job || !job.id) {
      throw new Error('Job not created');
    }
  })();

  // Test 9: Deduplication replace option
  await test('Accept deduplication replace option', async () => {
    const job = await queue.add('test', { value: 9 }, {
      deduplication: {
        id: 'test-replace',
        ttl: 1000,
        replace: true,
      },
    });

    if (!job || !job.id) {
      throw new Error('Job not created');
    }
  })();

  // Test 10: Deduplication with both extend and replace
  await test('Accept deduplication with extend and replace', async () => {
    const job = await queue.add('test', { value: 10 }, {
      deduplication: {
        id: 'test-both',
        ttl: 1000,
        extend: true,
        replace: true,
      },
    });

    if (!job || !job.id) {
      throw new Error('Job not created');
    }
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
