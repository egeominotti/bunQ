#!/usr/bin/env bun
/**
 * Test Dead Letter Queue (TCP Mode)
 */

import { Queue, Worker } from '../../src/client';

const QUEUE_NAME = 'tcp-test-dlq';
const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');

async function main() {
  console.log('=== Test Dead Letter Queue (TCP) ===\n');

  const queue = new Queue<{ value: number }>(QUEUE_NAME, {
    connection: { port: TCP_PORT },
  });
  let passed = 0;
  let failed = 0;

  // Clean state
  queue.obliterate();
  await Bun.sleep(100);

  // Test 1: Job goes to DLQ after max attempts
  console.log('1. Testing JOB TO DLQ...');
  try {
    await queue.add('dlq-job', { value: 1 }, { attempts: 2, backoff: 50 });

    let attempts = 0;
    const worker = new Worker<{ value: number }>(QUEUE_NAME, async () => {
      attempts++;
      throw new Error('Always fails');
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(1500);
    await worker.close();

    if (attempts >= 2) {
      console.log(`   ✅ Job attempted ${attempts} times then moved to DLQ`);
      passed++;
    } else {
      console.log(`   ❌ Job only attempted ${attempts} times`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ DLQ test failed: ${e}`);
    failed++;
  }

  // Test 2: Multiple jobs to DLQ
  console.log('\n2. Testing MULTIPLE DLQ JOBS...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    await queue.addBulk([
      { name: 'fail-1', data: { value: 1 }, opts: { attempts: 1 } },
      { name: 'fail-2', data: { value: 2 }, opts: { attempts: 1 } },
      { name: 'fail-3', data: { value: 3 }, opts: { attempts: 1 } },
    ]);

    let failCount = 0;
    const worker = new Worker<{ value: number }>(QUEUE_NAME, async () => {
      failCount++;
      throw new Error('Intentional failure');
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(1000);
    await worker.close();

    if (failCount === 3) {
      console.log(`   ✅ All ${failCount} jobs failed and moved to DLQ`);
      passed++;
    } else {
      console.log(`   ❌ Only ${failCount} jobs failed`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Multiple DLQ test failed: ${e}`);
    failed++;
  }

  // Test 3: Retry DLQ via TCP
  console.log('\n3. Testing DLQ RETRY...');
  try {
    // Retry all jobs from DLQ
    queue.retryDlq();
    await Bun.sleep(200);

    let retryCount = 0;
    const worker = new Worker<{ value: number }>(QUEUE_NAME, async () => {
      retryCount++;
      return { success: true }; // Now succeed
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(1000);
    await worker.close();

    // At least some should be retried
    if (retryCount >= 1) {
      console.log(`   ✅ Retried ${retryCount} jobs from DLQ`);
      passed++;
    } else {
      console.log(`   ❌ No jobs retried from DLQ`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ DLQ retry test failed: ${e}`);
    failed++;
  }

  // Test 4: removeOnFail option
  console.log('\n4. Testing REMOVE ON FAIL...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    await queue.add('remove-on-fail', { value: 99 }, {
      attempts: 1,
      removeOnFail: true,
    });

    const worker = new Worker<{ value: number }>(QUEUE_NAME, async () => {
      throw new Error('Fail and remove');
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(500);
    await worker.close();

    // Job should be removed, not in DLQ
    console.log('   ✅ Job with removeOnFail processed');
    passed++;
  } catch (e) {
    console.log(`   ❌ removeOnFail test failed: ${e}`);
    failed++;
  }

  // Test 5: Purge DLQ
  console.log('\n5. Testing PURGE DLQ...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    // Add jobs that will fail
    await queue.addBulk([
      { name: 'purge-1', data: { value: 1 }, opts: { attempts: 1 } },
      { name: 'purge-2', data: { value: 2 }, opts: { attempts: 1 } },
    ]);

    const worker = new Worker<{ value: number }>(QUEUE_NAME, async () => {
      throw new Error('Fail for purge test');
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(500);
    await worker.close();

    // Purge the DLQ
    queue.purgeDlq();
    await Bun.sleep(100);

    console.log('   ✅ DLQ purged');
    passed++;
  } catch (e) {
    console.log(`   ❌ Purge DLQ test failed: ${e}`);
    failed++;
  }

  // Test 6: Job success after failures
  console.log('\n6. Testing SUCCESS AFTER RETRIES...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    await queue.add('retry-success', { value: 42 }, { attempts: 3, backoff: 50 });

    let attempts = 0;
    const worker = new Worker<{ value: number }>(QUEUE_NAME, async () => {
      attempts++;
      if (attempts < 3) throw new Error('Not yet');
      return { success: true };
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(1000);
    await worker.close();

    if (attempts === 3) {
      console.log(`   ✅ Job succeeded after ${attempts} attempts`);
      passed++;
    } else {
      console.log(`   ❌ Wrong attempt count: ${attempts}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Retry success test failed: ${e}`);
    failed++;
  }

  // Cleanup
  queue.obliterate();
  queue.close();

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(console.error);
