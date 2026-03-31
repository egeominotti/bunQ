#!/usr/bin/env bun
/**
 * Test Rate Limiting and Concurrency Control (Embedded Mode)
 */

import { Queue, Worker } from '../../src/client';
import { getSharedManager } from '../../src/client/manager';

const QUEUE_NAME = 'test-rate-limiting';

async function main() {
  console.log('=== Test Rate Limiting (Embedded) ===\n');

  const queue = new Queue<{ index: number }>(QUEUE_NAME, { embedded: true });
  const manager = getSharedManager();
  let passed = 0;
  let failed = 0;

  // Clean state
  queue.obliterate();

  // Test 1: SetConcurrency - Set concurrency limit on queue
  console.log('1. Testing SET CONCURRENCY...');
  try {
    manager.setConcurrency(QUEUE_NAME, 2);
    console.log('   ✅ Concurrency limit set to 2');
    passed++;
  } catch (e) {
    console.log(`   ❌ SetConcurrency failed: ${e}`);
    failed++;
  }

  // Test 2: ClearConcurrency - Clear concurrency limit
  console.log('\n2. Testing CLEAR CONCURRENCY...');
  try {
    manager.clearConcurrency(QUEUE_NAME);
    console.log('   ✅ Concurrency limit cleared');
    passed++;
  } catch (e) {
    console.log(`   ❌ ClearConcurrency failed: ${e}`);
    failed++;
  }

  // Test 3: Concurrency limit enforcement - Jobs respect limit
  console.log('\n3. Testing CONCURRENCY ENFORCEMENT...');
  try {
    queue.obliterate();

    // Set concurrency FIRST before adding jobs
    manager.setConcurrency(QUEUE_NAME, 2);
    await Bun.sleep(50); // Let settings propagate

    // Add 6 jobs
    await queue.addBulk(
      Array.from({ length: 6 }, (_, i) => ({
        name: `conc-job-${i}`,
        data: { index: i },
      }))
    );

    let maxConcurrent = 0;
    let currentConcurrent = 0;
    let processed = 0;

    // Worker starts AFTER jobs added and concurrency set
    const worker = new Worker<{ index: number }>(QUEUE_NAME, async () => {
      currentConcurrent++;
      maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
      await Bun.sleep(50);
      currentConcurrent--;
      processed++;
      return {};
    }, { concurrency: 10, autorun: false, embedded: true }); // Worker allows 10, but queue limit is 2

    // Small delay then start worker
    await Bun.sleep(50);
    worker.run();

    await Bun.sleep(2000);
    await worker.close();
    manager.clearConcurrency(QUEUE_NAME);

    if (maxConcurrent <= 2 && processed >= 4) {
      console.log(`   ✅ Concurrency enforced: max=${maxConcurrent}, processed=${processed}`);
      passed++;
    } else {
      console.log(`   ❌ Concurrency not enforced: max=${maxConcurrent}, processed=${processed}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Concurrency enforcement test failed: ${e}`);
    failed++;
  }

  // Test 4: RateLimit - Set rate limit (jobs per second)
  console.log('\n4. Testing SET RATE LIMIT...');
  try {
    manager.setRateLimit(QUEUE_NAME, 5); // 5 jobs per second
    console.log('   ✅ Rate limit set to 5 jobs/sec');
    passed++;
  } catch (e) {
    console.log(`   ❌ SetRateLimit failed: ${e}`);
    failed++;
  }

  // Test 5: RateLimitClear - Clear rate limit
  console.log('\n5. Testing CLEAR RATE LIMIT...');
  try {
    manager.clearRateLimit(QUEUE_NAME);
    console.log('   ✅ Rate limit cleared');
    passed++;
  } catch (e) {
    console.log(`   ❌ ClearRateLimit failed: ${e}`);
    failed++;
  }

  // Test 6: Rate limit enforcement - Jobs throttled correctly
  console.log('\n6. Testing RATE LIMIT ENFORCEMENT...');
  try {
    queue.obliterate();

    // Set rate limit FIRST before adding jobs
    manager.setRateLimit(QUEUE_NAME, 5); // 5 jobs per second
    await Bun.sleep(50); // Let settings propagate

    // Add 10 jobs
    await queue.addBulk(
      Array.from({ length: 10 }, (_, i) => ({
        name: `rate-job-${i}`,
        data: { index: i },
      }))
    );

    const timestamps: number[] = [];

    const worker = new Worker<{ index: number }>(QUEUE_NAME, async () => {
      timestamps.push(Date.now());
      return {};
    }, { concurrency: 10, autorun: false, embedded: true }); // Allow high concurrency, rate limit should throttle

    // Small delay then start worker
    await Bun.sleep(50);
    worker.run();

    await Bun.sleep(4000); // Wait for jobs to process
    await worker.close();
    manager.clearRateLimit(QUEUE_NAME);

    // With 5 jobs/sec limit and 10 jobs, it should take ~2 seconds
    if (timestamps.length >= 5) {
      const duration = timestamps[timestamps.length - 1] - timestamps[0];
      // At 5 jobs/sec, 10 jobs should take ~1.8-2.5 seconds
      if (duration >= 800) { // Some tolerance for timing
        console.log(`   ✅ Rate limit enforced: ${timestamps.length} jobs in ${duration}ms`);
        passed++;
      } else {
        console.log(`   ❌ Jobs processed too fast: ${timestamps.length} jobs in ${duration}ms`);
        failed++;
      }
    } else {
      console.log(`   ❌ Not enough jobs processed: ${timestamps.length}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Rate limit enforcement test failed: ${e}`);
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
