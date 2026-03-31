#!/usr/bin/env bun
/**
 * Test Batch Operations: addBulk, pullBatch
 */

// Force embedded mode BEFORE imports
process.env.BUNQUEUE_EMBEDDED = '1';

import { Queue, Worker } from '../../src/client';

const QUEUE_NAME = 'test-batch-ops';

async function main() {
  console.log('=== Test Batch Operations ===\n');

  const queue = new Queue<{ index: number }>(QUEUE_NAME, { embedded: true });
  let passed = 0;
  let failed = 0;

  // Test 1: Bulk add jobs
  console.log('1. Testing ADD BULK...');
  try {
    const jobs = Array.from({ length: 100 }, (_, i) => ({
      name: `job-${i}`,
      data: { index: i },
    }));

    const result = await queue.addBulk(jobs);

    if (result.length === 100) {
      console.log(`   ✅ Added ${result.length} jobs in bulk`);
      passed++;
    } else {
      console.log(`   ❌ Expected 100 jobs, got ${result.length}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Bulk add failed: ${e}`);
    failed++;
  }

  // Test 2: Verify job counts
  console.log('\n2. Testing JOB COUNTS AFTER BULK...');
  try {
    const counts = queue.getJobCounts();
    if (counts.waiting === 100) {
      console.log(`   ✅ Correct count: ${counts.waiting} waiting`);
      passed++;
    } else {
      console.log(`   ❌ Expected 100 waiting, got ${counts.waiting}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Get counts failed: ${e}`);
    failed++;
  }

  // Test 3: Process all jobs with concurrent workers
  console.log('\n3. Testing CONCURRENT PROCESSING...');
  try {
    const processed = new Set<number>();
    const worker = new Worker<{ index: number }>(QUEUE_NAME, async (job) => {
      processed.add(job.data.index);
      return { processed: job.data.index };
    }, { concurrency: 10, embedded: true });

    // Wait for all jobs to be processed
    const start = Date.now();
    while (processed.size < 100 && Date.now() - start < 5000) {
      await Bun.sleep(50);
    }
    await worker.close();

    if (processed.size === 100) {
      console.log(`   ✅ All 100 jobs processed by concurrent workers`);
      passed++;
    } else {
      console.log(`   ❌ Only ${processed.size}/100 jobs processed`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Concurrent processing failed: ${e}`);
    failed++;
  }

  // Test 4: Bulk add with different priorities
  console.log('\n4. Testing BULK WITH PRIORITIES...');
  try {
    const jobs = [
      { name: 'low', data: { index: 1 }, opts: { priority: 1 } },
      { name: 'high', data: { index: 2 }, opts: { priority: 10 } },
      { name: 'medium', data: { index: 3 }, opts: { priority: 5 } },
    ];

    await queue.addBulk(jobs);

    const order: number[] = [];
    const worker = new Worker<{ index: number }>(QUEUE_NAME, async (job) => {
      order.push(job.data.index);
      return {};
    }, { concurrency: 1, embedded: true });

    await Bun.sleep(500);
    await worker.close();

    if (order[0] === 2 && order[1] === 3 && order[2] === 1) {
      console.log(`   ✅ Priority order correct: ${order.join(' -> ')}`);
      passed++;
    } else {
      console.log(`   ❌ Priority order wrong: ${order.join(' -> ')}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Bulk priority test failed: ${e}`);
    failed++;
  }

  // Test 5: Large bulk add performance
  console.log('\n5. Testing LARGE BULK PERFORMANCE...');
  try {
    const count = 10000;
    const jobs = Array.from({ length: count }, (_, i) => ({
      name: `perf-job-${i}`,
      data: { index: i },
    }));

    const start = Date.now();
    await queue.addBulk(jobs);
    const duration = Date.now() - start;
    const opsPerSec = Math.round(count / (duration / 1000));

    console.log(`   ✅ Added ${count} jobs in ${duration}ms (${opsPerSec} ops/sec)`);
    passed++;

    // Clean up
    queue.drain();
  } catch (e) {
    console.log(`   ❌ Large bulk test failed: ${e}`);
    failed++;
  }

  // Cleanup
  queue.obliterate();

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(console.error);
