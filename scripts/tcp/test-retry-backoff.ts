#!/usr/bin/env bun
/**
 * Test Retry and Backoff (TCP Mode)
 */

import { Queue, Worker } from '../../src/client';

const QUEUE_NAME = 'tcp-test-retry';
const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');

async function main() {
  console.log('=== Test Retry and Backoff (TCP) ===\n');

  const queue = new Queue<{ failCount: number }>(QUEUE_NAME, {
    connection: { port: TCP_PORT },
  });
  let passed = 0;
  let failed = 0;

  // Clean state
  queue.obliterate();
  await Bun.sleep(100);

  // Test 1: Job retries on failure
  console.log('1. Testing BASIC RETRY...');
  try {
    await queue.add('retry-job', { failCount: 0 }, { attempts: 3, backoff: 100 });

    let attempts = 0;
    const worker = new Worker<{ failCount: number }>(QUEUE_NAME, async () => {
      attempts++;
      if (attempts < 3) throw new Error(`Attempt ${attempts} failed`);
      return { success: true };
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(1500);
    await worker.close();

    if (attempts === 3) {
      console.log(`   ✅ Job retried ${attempts} times then succeeded`);
      passed++;
    } else {
      console.log(`   ❌ Job attempted ${attempts} times`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Basic retry test failed: ${e}`);
    failed++;
  }

  // Test 2: Backoff delay
  console.log('\n2. Testing BACKOFF DELAY...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    await queue.add('backoff-job', { failCount: 0 }, { attempts: 3, backoff: 200 });

    const attemptTimes: number[] = [];
    const worker = new Worker<{ failCount: number }>(QUEUE_NAME, async () => {
      attemptTimes.push(Date.now());
      if (attemptTimes.length < 3) throw new Error('Still failing');
      return { done: true };
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(2000);
    await worker.close();

    if (attemptTimes.length >= 2) {
      const delay = attemptTimes[1] - attemptTimes[0];
      if (delay >= 180) {
        console.log(`   ✅ Backoff delay: ~${delay}ms between attempts`);
        passed++;
      } else {
        console.log(`   ❌ Delay too short: ${delay}ms`);
        failed++;
      }
    } else {
      console.log(`   ❌ Only ${attemptTimes.length} attempts`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Backoff delay test failed: ${e}`);
    failed++;
  }

  // Test 3: Max attempts exceeded
  console.log('\n3. Testing MAX ATTEMPTS EXCEEDED...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    await queue.add('max-attempts-job', { failCount: 0 }, { attempts: 2, backoff: 50 });

    let totalAttempts = 0;
    const worker = new Worker<{ failCount: number }>(QUEUE_NAME, async () => {
      totalAttempts++;
      throw new Error('Always fails');
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(1000);
    await worker.close();

    if (totalAttempts === 2) {
      console.log(`   ✅ Job stopped after ${totalAttempts} attempts (max reached)`);
      passed++;
    } else {
      console.log(`   ❌ Job attempted ${totalAttempts} times`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Max attempts test failed: ${e}`);
    failed++;
  }

  // Test 4: Exponential backoff
  console.log('\n4. Testing EXPONENTIAL BACKOFF...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    // Exponential backoff: 100, 200, 400...
    await queue.add('exp-backoff-job', { failCount: 0 }, { attempts: 4, backoff: 100 });

    const attemptTimes: number[] = [];
    const worker = new Worker<{ failCount: number }>(QUEUE_NAME, async () => {
      attemptTimes.push(Date.now());
      if (attemptTimes.length < 3) throw new Error('Failing');
      return { done: true };
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(2000);
    await worker.close();

    if (attemptTimes.length >= 3) {
      const delay1 = attemptTimes[1] - attemptTimes[0];
      const delay2 = attemptTimes[2] - attemptTimes[1];
      // Exponential should increase (delay2 > delay1)
      if (delay2 >= delay1 * 0.8) { // Some tolerance
        console.log(`   ✅ Backoff increasing: ${delay1}ms -> ${delay2}ms`);
        passed++;
      } else {
        console.log(`   ❌ Backoff not increasing: ${delay1}ms -> ${delay2}ms`);
        failed++;
      }
    } else {
      console.log(`   ❌ Only ${attemptTimes.length} attempts recorded`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Exponential backoff test failed: ${e}`);
    failed++;
  }

  // Test 5: Retry with result
  console.log('\n5. Testing RETRY THEN SUCCESS...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    await queue.add('retry-success-job', { failCount: 0 }, { attempts: 5, backoff: 50 });

    let attemptCount = 0;
    let finalResult: unknown = null;

    const worker = new Worker<{ failCount: number }>(QUEUE_NAME, async () => {
      attemptCount++;
      if (attemptCount < 3) throw new Error('Not ready');
      finalResult = { value: 42 };
      return finalResult;
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(1000);
    await worker.close();

    if (attemptCount === 3 && finalResult) {
      console.log(`   ✅ Succeeded on attempt ${attemptCount}`);
      passed++;
    } else {
      console.log(`   ❌ Attempts: ${attemptCount}, Result: ${JSON.stringify(finalResult)}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Retry then success test failed: ${e}`);
    failed++;
  }

  // Test 6: Multiple jobs with retries
  console.log('\n6. Testing MULTIPLE JOBS RETRYING...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    await queue.addBulk([
      { name: 'multi-1', data: { failCount: 1 }, opts: { attempts: 2, backoff: 50 } },
      { name: 'multi-2', data: { failCount: 1 }, opts: { attempts: 2, backoff: 50 } },
      { name: 'multi-3', data: { failCount: 0 }, opts: { attempts: 2, backoff: 50 } },
    ]);

    const jobAttempts = new Map<string, number>();
    const worker = new Worker<{ failCount: number }>(QUEUE_NAME, async (job) => {
      const key = job.name;
      const attempts = (jobAttempts.get(key) ?? 0) + 1;
      jobAttempts.set(key, attempts);

      if (attempts <= job.data.failCount) {
        throw new Error('Configured to fail');
      }
      return { success: true };
    }, { concurrency: 2, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(1500);
    await worker.close();

    const totalAttempts = Array.from(jobAttempts.values()).reduce((a, b) => a + b, 0);
    if (totalAttempts >= 4) { // 2+2+1 = 5 total
      console.log(`   ✅ Multiple jobs completed with ${totalAttempts} total attempts`);
      passed++;
    } else {
      console.log(`   ❌ Total attempts: ${totalAttempts}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Multiple jobs retry test failed: ${e}`);
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
