#!/usr/bin/env bun
/**
 * Test Dead Letter Queue (DLQ): failed jobs, retry, purge
 */

// Force embedded mode BEFORE imports
process.env.BUNQUEUE_EMBEDDED = '1';

import { Queue, Worker } from '../../src/client';

const QUEUE_NAME = 'test-dlq';

async function main() {
  console.log('=== Test Dead Letter Queue ===\n');

  const queue = new Queue<{ attempt: number }>(QUEUE_NAME, {
    defaultJobOptions: {
      attempts: 3,
      backoff: 100,
    },
  });
  let passed = 0;
  let failed = 0;

  // Clean up
  queue.obliterate();
  queue.purgeDlq();

  // Test 1: Job moves to DLQ after max attempts
  console.log('1. Testing MAX ATTEMPTS -> DLQ...');
  try {
    await queue.add('fail-job', { attempt: 0 }, { attempts: 2, backoff: 50 });

    let attempts = 0;
    const worker = new Worker<{ attempt: number }>(QUEUE_NAME, async () => {
      attempts++;
      throw new Error(`Attempt ${attempts} failed`);
    }, { concurrency: 1, embedded: true });

    await Bun.sleep(1000);
    await worker.close();

    const dlq = queue.getDlq();
    if (attempts >= 2 && dlq.length >= 1) {
      console.log(`   ✅ Job in DLQ after ${attempts} attempts`);
      passed++;
    } else {
      console.log(`   ❌ Attempts: ${attempts}, DLQ entries: ${dlq.length}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ DLQ test failed: ${e}`);
    failed++;
  }

  // Test 2: Get DLQ entries
  console.log('\n2. Testing GET DLQ...');
  try {
    const dlq = queue.getDlq();

    if (dlq.length > 0) {
      const entry = dlq[0];
      console.log(`   ✅ DLQ entry found: reason=${entry.reason}, attempts=${entry.attempts.length}`);
      passed++;
    } else {
      console.log('   ❌ No DLQ entries found');
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Get DLQ failed: ${e}`);
    failed++;
  }

  // Test 3: Get DLQ stats
  console.log('\n3. Testing DLQ STATS...');
  try {
    const stats = queue.getDlqStats();

    if (stats.total >= 1) {
      console.log(`   ✅ DLQ stats: total=${stats.total}, byReason=${JSON.stringify(stats.byReason)}`);
      passed++;
    } else {
      console.log(`   ❌ Unexpected stats: ${JSON.stringify(stats)}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ DLQ stats failed: ${e}`);
    failed++;
  }

  // Test 4: Retry from DLQ
  console.log('\n4. Testing RETRY DLQ...');
  try {
    // First add a job that will fail
    queue.obliterate();
    queue.purgeDlq();

    await queue.add('retry-job', { attempt: 0 }, { attempts: 1, backoff: 50 });

    const worker1 = new Worker<{ attempt: number }>(QUEUE_NAME, async () => {
      throw new Error('First failure');
    }, { concurrency: 1, embedded: true });

    await Bun.sleep(500);
    await worker1.close();

    const dlqBefore = queue.getDlq();

    // Retry all DLQ entries
    queue.retryDlq();

    // Now process with success
    let retrySucceeded = false;
    const worker2 = new Worker<{ attempt: number }>(QUEUE_NAME, async () => {
      retrySucceeded = true;
      return { success: true };
    }, { concurrency: 1, embedded: true });

    await Bun.sleep(500);
    await worker2.close();

    if (dlqBefore.length > 0 && retrySucceeded) {
      console.log('   ✅ DLQ retry succeeded');
      passed++;
    } else {
      console.log(`   ❌ DLQ before: ${dlqBefore.length}, retry succeeded: ${retrySucceeded}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Retry DLQ failed: ${e}`);
    failed++;
  }

  // Test 5: Purge DLQ
  console.log('\n5. Testing PURGE DLQ...');
  try {
    queue.obliterate();
    queue.purgeDlq();

    // Add job that will fail and go to DLQ
    await queue.add('purge-job', { attempt: 0 }, { attempts: 1 });

    const worker = new Worker<{ attempt: number }>(QUEUE_NAME, async () => {
      throw new Error('Fail for purge');
    }, { concurrency: 1, embedded: true });

    await Bun.sleep(500);
    await worker.close();

    const dlqBefore = queue.getDlq();
    queue.purgeDlq();
    const dlqAfter = queue.getDlq();

    if (dlqBefore.length > 0 && dlqAfter.length === 0) {
      console.log(`   ✅ DLQ purged: ${dlqBefore.length} -> ${dlqAfter.length}`);
      passed++;
    } else {
      console.log(`   ❌ Purge failed: ${dlqBefore.length} -> ${dlqAfter.length}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Purge DLQ failed: ${e}`);
    failed++;
  }

  // Test 6: DLQ has entries after failure
  console.log('\n6. Testing DLQ ENTRIES...');
  try {
    queue.obliterate();
    queue.purgeDlq();

    // Add job that will fail
    await queue.add('entry-job', { attempt: 0 }, { attempts: 1 });

    const worker = new Worker<{ attempt: number }>(QUEUE_NAME, async () => {
      throw new Error('Entry test failure');
    }, { concurrency: 1, embedded: true });

    await Bun.sleep(500);
    await worker.close();

    const allDlq = queue.getDlq();

    if (allDlq.length >= 1) {
      const entry = allDlq[0];
      console.log(`   ✅ DLQ entry: reason=${entry.reason}, error=${entry.error?.substring(0, 30)}`);
      passed++;
    } else {
      console.log(`   ❌ No DLQ entries found`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ DLQ entries test failed: ${e}`);
    failed++;
  }

  // Cleanup
  queue.purgeDlq();
  queue.obliterate();

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(console.error);
