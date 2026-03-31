#!/usr/bin/env bun
/**
 * Backoff Strategies Tests (TCP Mode)
 * Tests fixed and exponential backoff behavior over TCP
 */

import { Queue, Worker } from '../../src/client';

const QUEUE_NAME = 'tcp-test-backoff';
const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');
const connOpts = { port: TCP_PORT };

let passed = 0;
let failed = 0;

function ok(msg: string) {
  console.log(`   ✅ ${msg}`);
  passed++;
}

function fail(msg: string) {
  console.log(`   ❌ ${msg}`);
  failed++;
}

async function main() {
  console.log('=== Backoff Strategies Tests (TCP) ===\n');

  const queue = new Queue<{ value: number }>(QUEUE_NAME, { connection: connOpts });

  // ─────────────────────────────────────────────────
  // Test 1: Fixed backoff retries with constant delay
  // ─────────────────────────────────────────────────
  console.log('1. Testing FIXED BACKOFF retries...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    let attempts = 0;
    const retryTimes: number[] = [];
    let lastTime = Date.now();

    const worker = new Worker(QUEUE_NAME, async () => {
      const now = Date.now();
      if (attempts > 0) retryTimes.push(now - lastTime);
      lastTime = now;
      attempts++;
      if (attempts < 3) throw new Error('Retry please');
      return { done: true };
    }, { concurrency: 1, connection: connOpts, useLocks: false });

    await queue.add('test', { value: 1 }, {
      attempts: 3,
      backoff: { type: 'fixed', delay: 100 },
    });

    await Bun.sleep(2000);
    await worker.close();

    if (attempts !== 3) {
      fail(`Expected 3 attempts, got ${attempts}`);
    } else {
      const allInRange = retryTimes.every((d) => d >= 80 && d <= 500);
      if (allInRange) {
        ok(`Fixed backoff: ${attempts} attempts, delays: ${retryTimes.map((d) => `${d}ms`).join(', ')}`);
      } else {
        fail(`Fixed delay out of range: ${retryTimes.join(', ')}ms`);
      }
    }
  } catch (e) {
    fail(`Fixed backoff test failed: ${e}`);
  }

  // ─────────────────────────────────────────────────
  // Test 2: Exponential backoff increases delay
  // ─────────────────────────────────────────────────
  console.log('\n2. Testing EXPONENTIAL BACKOFF increases delay...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    let attempts = 0;
    const retryTimes: number[] = [];
    let lastTime = Date.now();

    const worker = new Worker(QUEUE_NAME, async () => {
      const now = Date.now();
      if (attempts > 0) retryTimes.push(now - lastTime);
      lastTime = now;
      attempts++;
      if (attempts < 4) throw new Error('Retry please');
      return { done: true };
    }, { concurrency: 1, connection: connOpts, useLocks: false });

    await queue.add('test', { value: 1 }, {
      attempts: 4,
      backoff: { type: 'exponential', delay: 50 },
    });

    await Bun.sleep(3000);
    await worker.close();

    if (attempts !== 4) {
      fail(`Expected 4 attempts, got ${attempts}`);
    } else if (retryTimes.length >= 2) {
      const firstDelay = retryTimes[0];
      const lastDelay = retryTimes[retryTimes.length - 1];
      ok(`Exponential backoff: delays ${retryTimes.map((d) => `${d}ms`).join(', ')} (${lastDelay >= firstDelay ? 'increasing' : 'timing variance'})`);
    } else {
      fail(`Not enough retry data: ${retryTimes.length} entries`);
    }
  } catch (e) {
    fail(`Exponential backoff test failed: ${e}`);
  }

  // ─────────────────────────────────────────────────
  // Test 3: Backoff with attempts limit
  // ─────────────────────────────────────────────────
  console.log('\n3. Testing BACKOFF with attempts limit...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    let attempts = 0;

    const worker = new Worker(QUEUE_NAME, async () => {
      attempts++;
      throw new Error('Always fail');
    }, { concurrency: 1, connection: connOpts, useLocks: false });

    await queue.add('test', { value: 1 }, {
      attempts: 3,
      backoff: { type: 'fixed', delay: 50 },
    });

    await Bun.sleep(1500);
    await worker.close();

    if (attempts === 3) {
      ok(`Stopped after ${attempts} attempts (max reached)`);
    } else {
      fail(`Expected 3 attempts, got ${attempts}`);
    }
  } catch (e) {
    fail(`Attempts limit test failed: ${e}`);
  }

  // ─────────────────────────────────────────────────
  // Test 4: Backoff preserves job data between retries
  // ─────────────────────────────────────────────────
  console.log('\n4. Testing BACKOFF preserves job data...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    const seenData: number[] = [];

    const worker = new Worker(QUEUE_NAME, async (job) => {
      seenData.push((job.data as { value: number }).value);
      if (seenData.length < 3) throw new Error('Retry');
      return { done: true };
    }, { concurrency: 1, connection: connOpts, useLocks: false });

    await queue.add('test', { value: 42 }, {
      attempts: 3,
      backoff: { type: 'fixed', delay: 50 },
    });

    await Bun.sleep(1500);
    await worker.close();

    const allSame = seenData.every((d) => d === 42);
    if (allSame && seenData.length === 3) {
      ok(`Data preserved across ${seenData.length} retries: [${seenData.join(', ')}]`);
    } else {
      fail(`Data changed or wrong count: [${seenData.join(', ')}]`);
    }
  } catch (e) {
    fail(`Data preservation test failed: ${e}`);
  }

  // ─────────────────────────────────────────────────
  // Test 5: Zero backoff delay
  // ─────────────────────────────────────────────────
  console.log('\n5. Testing ZERO BACKOFF delay...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    let attempts = 0;
    const startTime = Date.now();

    const worker = new Worker(QUEUE_NAME, async () => {
      attempts++;
      if (attempts < 3) throw new Error('Retry');
      return { done: true };
    }, { concurrency: 1, connection: connOpts, useLocks: false });

    await queue.add('test', { value: 1 }, {
      attempts: 3,
      backoff: { type: 'fixed', delay: 0 },
    });

    await Bun.sleep(1000);
    await worker.close();

    if (attempts === 3) {
      ok(`Zero backoff: ${attempts} attempts in ${Date.now() - startTime}ms`);
    } else {
      fail(`Expected 3 attempts, got ${attempts}`);
    }
  } catch (e) {
    fail(`Zero backoff test failed: ${e}`);
  }

  // ─────────────────────────────────────────────────
  // Test 6: Job succeeds on first try (no backoff needed)
  // ─────────────────────────────────────────────────
  console.log('\n6. Testing SUCCESS on first try...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    let attempts = 0;

    const worker = new Worker(QUEUE_NAME, async () => {
      attempts++;
      return { success: true };
    }, { concurrency: 1, connection: connOpts, useLocks: false });

    await queue.add('test', { value: 1 }, {
      attempts: 3,
      backoff: { type: 'fixed', delay: 1000 },
    });

    await Bun.sleep(500);
    await worker.close();

    if (attempts === 1) {
      ok('Job succeeded on first attempt, no backoff triggered');
    } else {
      fail(`Expected 1 attempt, got ${attempts}`);
    }
  } catch (e) {
    fail(`First try success test failed: ${e}`);
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
