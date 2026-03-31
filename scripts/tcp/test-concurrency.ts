#!/usr/bin/env bun
/**
 * Test Concurrency Control (TCP Mode)
 */

import { Queue, Worker } from '../../src/client';

const QUEUE_NAME = 'tcp-test-concurrency';
const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');

async function main() {
  console.log('=== Test Concurrency (TCP) ===\n');

  const queue = new Queue<{ delay: number }>(QUEUE_NAME, {
    connection: { port: TCP_PORT },
  });
  let passed = 0;
  let failed = 0;

  // Clean state
  queue.obliterate();
  await Bun.sleep(100);

  // Test 1: Single concurrency (sequential processing)
  console.log('1. Testing SINGLE CONCURRENCY...');
  try {
    await queue.addBulk([
      { name: 'job-1', data: { delay: 100 } },
      { name: 'job-2', data: { delay: 100 } },
      { name: 'job-3', data: { delay: 100 } },
    ]);

    const timestamps: number[] = [];
    const worker = new Worker<{ delay: number }>(QUEUE_NAME, async (job) => {
      timestamps.push(Date.now());
      await Bun.sleep((job.data as { delay: number }).delay);
      return {};
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(1000);
    await worker.close();

    // With concurrency 1, jobs should be spaced out
    if (timestamps.length === 3) {
      const gap1 = timestamps[1] - timestamps[0];
      const gap2 = timestamps[2] - timestamps[1];
      if (gap1 >= 80 && gap2 >= 80) {
        console.log(`   ✅ Sequential processing verified (gaps: ${gap1}ms, ${gap2}ms)`);
        passed++;
      } else {
        console.log(`   ❌ Jobs processed too fast: gaps ${gap1}ms, ${gap2}ms`);
        failed++;
      }
    } else {
      console.log(`   ❌ Not all jobs processed: ${timestamps.length}/3`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Single concurrency test failed: ${e}`);
    failed++;
  }

  // Test 2: High concurrency (parallel processing)
  console.log('\n2. Testing HIGH CONCURRENCY...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    await queue.addBulk([
      { name: 'parallel-1', data: { delay: 200 } },
      { name: 'parallel-2', data: { delay: 200 } },
      { name: 'parallel-3', data: { delay: 200 } },
      { name: 'parallel-4', data: { delay: 200 } },
      { name: 'parallel-5', data: { delay: 200 } },
    ]);

    const startTimes: number[] = [];
    const worker = new Worker<{ delay: number }>(QUEUE_NAME, async (job) => {
      startTimes.push(Date.now());
      await Bun.sleep((job.data as { delay: number }).delay);
      return {};
    }, { concurrency: 5, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(1000);
    await worker.close();

    if (startTimes.length === 5) {
      // All jobs should start within a short window
      const timeSpread = Math.max(...startTimes) - Math.min(...startTimes);
      if (timeSpread < 150) {
        console.log(`   ✅ Parallel processing verified (spread: ${timeSpread}ms)`);
        passed++;
      } else {
        console.log(`   ❌ Jobs not parallel enough: spread ${timeSpread}ms`);
        failed++;
      }
    } else {
      console.log(`   ❌ Not all jobs processed: ${startTimes.length}/5`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ High concurrency test failed: ${e}`);
    failed++;
  }

  // Test 3: Concurrency respects limit
  console.log('\n3. Testing CONCURRENCY LIMIT...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    // Add 6 jobs
    await queue.addBulk(
      Array.from({ length: 6 }, (_, i) => ({
        name: `limit-${i}`,
        data: { delay: 150 },
      }))
    );

    let maxConcurrent = 0;
    let currentConcurrent = 0;

    const worker = new Worker<{ delay: number }>(QUEUE_NAME, async (job) => {
      currentConcurrent++;
      maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
      await Bun.sleep((job.data as { delay: number }).delay);
      currentConcurrent--;
      return {};
    }, { concurrency: 3, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(1500);
    await worker.close();

    if (maxConcurrent <= 3) {
      console.log(`   ✅ Concurrency limit respected (max: ${maxConcurrent})`);
      passed++;
    } else {
      console.log(`   ❌ Concurrency exceeded: max was ${maxConcurrent}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Concurrency limit test failed: ${e}`);
    failed++;
  }

  // Test 4: Multiple workers
  console.log('\n4. Testing MULTIPLE WORKERS...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    await queue.addBulk(
      Array.from({ length: 10 }, (_, i) => ({
        name: `multi-${i}`,
        data: { delay: 50 },
      }))
    );

    let worker1Count = 0;
    let worker2Count = 0;

    const worker1 = new Worker<{ delay: number }>(QUEUE_NAME, async (job) => {
      worker1Count++;
      await Bun.sleep((job.data as { delay: number }).delay);
      return {};
    }, { concurrency: 2, connection: { port: TCP_PORT }, useLocks: false });

    const worker2 = new Worker<{ delay: number }>(QUEUE_NAME, async (job) => {
      worker2Count++;
      await Bun.sleep((job.data as { delay: number }).delay);
      return {};
    }, { concurrency: 2, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(1500);
    await worker1.close();
    await worker2.close();

    const total = worker1Count + worker2Count;
    if (total === 10 && worker1Count > 0 && worker2Count > 0) {
      console.log(`   ✅ Work distributed: W1=${worker1Count}, W2=${worker2Count}`);
      passed++;
    } else {
      console.log(`   ❌ Distribution issue: W1=${worker1Count}, W2=${worker2Count}, total=${total}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Multiple workers test failed: ${e}`);
    failed++;
  }

  // Test 5: Zero delay jobs with concurrency
  console.log('\n5. Testing FAST JOBS...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    await queue.addBulk(
      Array.from({ length: 50 }, (_, i) => ({
        name: `fast-${i}`,
        data: { delay: 0 },
      }))
    );

    let count = 0;
    const worker = new Worker<{ delay: number }>(QUEUE_NAME, async () => {
      count++;
      return {};
    }, { concurrency: 10, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(1500);
    await worker.close();

    if (count === 50) {
      console.log(`   ✅ All ${count} fast jobs completed`);
      passed++;
    } else {
      console.log(`   ❌ Only ${count}/50 jobs completed`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Fast jobs test failed: ${e}`);
    failed++;
  }

  // Test 6: Worker force close (graceful close is tested in embedded mode)
  console.log('\n6. Testing FORCE CLOSE...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    await queue.addBulk(
      Array.from({ length: 3 }, (_, i) => ({
        name: `close-${i}`,
        data: { delay: 50 },
      }))
    );

    let processed = 0;
    const worker = new Worker<{ delay: number }>(QUEUE_NAME, async (job) => {
      await Bun.sleep((job.data as { delay: number }).delay);
      processed++;
      return {};
    }, { concurrency: 2, connection: { port: TCP_PORT }, useLocks: false });

    // Wait a bit then force close
    await Bun.sleep(200);
    await worker.close(true); // Force close

    if (processed >= 1) {
      console.log(`   ✅ Force close: ${processed} jobs completed before close`);
      passed++;
    } else {
      console.log(`   ❌ No jobs completed before close`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Force close test failed: ${e}`);
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
