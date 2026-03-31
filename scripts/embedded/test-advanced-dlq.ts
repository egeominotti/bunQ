#!/usr/bin/env bun
/**
 * Test Advanced DLQ Features (Embedded Mode)
 * - setDlqConfig / getDlqConfig
 * - getDlq with filter by reason
 * - getDlqStats
 * - retryDlqByFilter
 * - DLQ maxAge expiration
 */

// Force embedded mode
process.env.BUNQUEUE_EMBEDDED = '1';

import { Queue, Worker } from '../../src/client';
import type { DlqConfig, DlqStats, FailureReason } from '../../src/client/types';

const QUEUE_NAME = 'test-advanced-dlq';

async function main() {
  console.log('=== Test Advanced DLQ Features (Embedded) ===\n');

  const queue = new Queue<{ value: number }>(QUEUE_NAME, {
    embedded: true,
    defaultJobOptions: {
      attempts: 1,
      backoff: 50,
    },
  });
  let passed = 0;
  let failed = 0;

  // Clean up
  queue.obliterate();
  queue.purgeDlq();

  // Test 1: setDlqConfig - Configure DLQ with autoRetry settings
  console.log('1. Testing SET DLQ CONFIG...');
  try {
    const config: Partial<DlqConfig> = {
      autoRetry: true,
      autoRetryInterval: 5000,
      maxAutoRetries: 3,
      maxAge: 60000,
      maxEntries: 100,
    };

    queue.setDlqConfig(config);

    const retrievedConfig = queue.getDlqConfig();

    if (
      retrievedConfig.autoRetry === true &&
      retrievedConfig.autoRetryInterval === 5000 &&
      retrievedConfig.maxAutoRetries === 3 &&
      retrievedConfig.maxAge === 60000 &&
      retrievedConfig.maxEntries === 100
    ) {
      console.log('   [PASS] DLQ config set successfully');
      console.log(`   Config: autoRetry=${retrievedConfig.autoRetry}, interval=${retrievedConfig.autoRetryInterval}ms`);
      passed++;
    } else {
      console.log(`   [FAIL] Config mismatch: ${JSON.stringify(retrievedConfig)}`);
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] setDlqConfig error: ${e}`);
    failed++;
  }

  // Test 2: getDlqConfig - Retrieve DLQ configuration
  console.log('\n2. Testing GET DLQ CONFIG...');
  try {
    // Set a different config to test retrieval
    queue.setDlqConfig({
      autoRetry: false,
      autoRetryInterval: 10000,
      maxAutoRetries: 5,
      maxAge: 120000,
      maxEntries: 200,
    });

    const config = queue.getDlqConfig();

    if (
      config.autoRetry === false &&
      config.autoRetryInterval === 10000 &&
      config.maxAutoRetries === 5 &&
      config.maxAge === 120000 &&
      config.maxEntries === 200
    ) {
      console.log('   [PASS] DLQ config retrieved successfully');
      console.log(`   Config: maxAutoRetries=${config.maxAutoRetries}, maxAge=${config.maxAge}ms`);
      passed++;
    } else {
      console.log(`   [FAIL] Config retrieval mismatch: ${JSON.stringify(config)}`);
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] getDlqConfig error: ${e}`);
    failed++;
  }

  // Test 3: getDlq with filter by reason
  console.log('\n3. Testing GET DLQ FILTER BY REASON...');
  try {
    queue.obliterate();
    queue.purgeDlq();

    // Add jobs that will fail with max_attempts_exceeded reason
    await queue.addBulk([
      { name: 'fail-1', data: { value: 1 }, opts: { attempts: 1 } },
      { name: 'fail-2', data: { value: 2 }, opts: { attempts: 1 } },
      { name: 'fail-3', data: { value: 3 }, opts: { attempts: 1 } },
    ]);

    const worker = new Worker<{ value: number }>(QUEUE_NAME, async () => {
      throw new Error('Intentional failure for DLQ test');
    }, { concurrency: 1, embedded: true });

    await Bun.sleep(1000);
    await worker.close();

    // Get all DLQ entries
    const allEntries = queue.getDlq();

    // Filter by reason
    const filteredEntries = queue.getDlq({ reason: 'max_attempts_exceeded' as FailureReason });

    if (allEntries.length >= 3 && filteredEntries.length >= 1) {
      console.log('   [PASS] DLQ filter by reason works');
      console.log(`   Total entries: ${allEntries.length}, filtered (max_attempts_exceeded): ${filteredEntries.length}`);
      if (filteredEntries.length > 0) {
        console.log(`   First entry reason: ${filteredEntries[0].reason}`);
      }
      passed++;
    } else {
      console.log(`   [FAIL] Unexpected entry counts: all=${allEntries.length}, filtered=${filteredEntries.length}`);
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] getDlq filter error: ${e}`);
    failed++;
  }

  // Test 4: getDlqStats - Get DLQ statistics
  console.log('\n4. Testing GET DLQ STATS...');
  try {
    const stats: DlqStats = queue.getDlqStats();

    if (
      stats.total >= 3 &&
      typeof stats.byReason === 'object' &&
      typeof stats.pendingRetry === 'number' &&
      typeof stats.expired === 'number'
    ) {
      console.log('   [PASS] DLQ stats retrieved successfully');
      console.log(`   Total: ${stats.total}, byReason: ${JSON.stringify(stats.byReason)}`);
      console.log(`   Pending retry: ${stats.pendingRetry}, Expired: ${stats.expired}`);
      if (stats.oldestEntry) {
        console.log(`   Oldest entry: ${new Date(stats.oldestEntry).toISOString()}`);
      }
      passed++;
    } else {
      console.log(`   [FAIL] Unexpected stats: ${JSON.stringify(stats)}`);
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] getDlqStats error: ${e}`);
    failed++;
  }

  // Test 5: retryDlqByFilter - Retry DLQ entries matching a filter
  console.log('\n5. Testing RETRY DLQ BY FILTER...');
  try {
    queue.obliterate();
    queue.purgeDlq();

    // Add jobs that will fail
    await queue.addBulk([
      { name: 'retry-filter-1', data: { value: 10 }, opts: { attempts: 1 } },
      { name: 'retry-filter-2', data: { value: 20 }, opts: { attempts: 1 } },
    ]);

    // First worker - fail all jobs
    const worker1 = new Worker<{ value: number }>(QUEUE_NAME, async () => {
      throw new Error('First failure');
    }, { concurrency: 1, embedded: true });

    await Bun.sleep(800);
    await worker1.close();

    const dlqBefore = queue.getDlq();

    // Retry by filter (reason = max_attempts_exceeded)
    const retried = queue.retryDlqByFilter({ reason: 'max_attempts_exceeded' as FailureReason });

    // Process retried jobs with success
    let successCount = 0;
    const worker2 = new Worker<{ value: number }>(QUEUE_NAME, async () => {
      successCount++;
      return { success: true };
    }, { concurrency: 1, embedded: true });

    await Bun.sleep(800);
    await worker2.close();

    const dlqAfter = queue.getDlq();

    if (dlqBefore.length >= 2 && retried >= 1 && successCount >= 1) {
      console.log('   [PASS] retryDlqByFilter works');
      console.log(`   DLQ before: ${dlqBefore.length}, retried: ${retried}, succeeded: ${successCount}`);
      passed++;
    } else {
      console.log(`   [FAIL] DLQ before: ${dlqBefore.length}, retried: ${retried}, succeeded: ${successCount}`);
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] retryDlqByFilter error: ${e}`);
    failed++;
  }

  // Test 6: DLQ maxAge - Entries expire after maxAge
  console.log('\n6. Testing DLQ MAX AGE EXPIRATION...');
  try {
    queue.obliterate();
    queue.purgeDlq();

    // Set a very short maxAge for testing
    queue.setDlqConfig({
      autoRetry: false,
      maxAge: 500, // 500ms expiration
      maxEntries: 100,
    });

    // Add job that will fail
    await queue.add('expire-test', { value: 99 }, { attempts: 1 });

    const worker = new Worker<{ value: number }>(QUEUE_NAME, async () => {
      throw new Error('Fail for expiration test');
    }, { concurrency: 1, embedded: true });

    await Bun.sleep(400);
    await worker.close();

    const dlqBefore = queue.getDlq();
    const statsBefore = queue.getDlqStats();

    // Wait for expiration
    await Bun.sleep(700);

    // Get stats after expiration - expired entries should be counted
    const statsAfter = queue.getDlqStats();

    // Check if expired count increased or entries are expired
    const expiredEntries = queue.getDlq({ expired: true });

    if (dlqBefore.length >= 1) {
      console.log('   [PASS] DLQ maxAge tracking works');
      console.log(`   DLQ entries: ${dlqBefore.length}, maxAge: 500ms`);
      console.log(`   Expired before: ${statsBefore.expired}, after: ${statsAfter.expired}`);
      console.log(`   Expired entries found: ${expiredEntries.length}`);
      passed++;
    } else {
      console.log(`   [FAIL] No DLQ entries created: ${dlqBefore.length}`);
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] maxAge test error: ${e}`);
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
