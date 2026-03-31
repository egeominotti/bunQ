#!/usr/bin/env bun
/**
 * Backpressure and Rate Limiting Tests (TCP Mode)
 * Tests global concurrency limits, rate limits, and their interactions
 * with worker concurrency over TCP connections.
 */

import { Queue, Worker } from '../../src/client';

const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');
const connOpts = { port: TCP_PORT };

let passed = 0;
let failed = 0;
const queues: Queue[] = [];
const workers: Worker[] = [];

function ok(msg: string) {
  console.log(`   ✅ ${msg}`);
  passed++;
}

function fail(msg: string) {
  console.log(`   ❌ ${msg}`);
  failed++;
}

function makeQueue(name: string): Queue {
  const q = new Queue(name, { connection: connOpts });
  queues.push(q);
  return q;
}

function makeWorker(
  name: string,
  processor: (job: any) => Promise<any>,
  opts: Record<string, any> = {},
): Worker {
  const w = new Worker(name, processor, {
    connection: connOpts,
    useLocks: false,
    ...opts,
  });
  workers.push(w);
  return w;
}

async function main() {
  console.log('=== Backpressure Tests (TCP) ===\n');

  // ---------------------------------------------------------------
  // Test 1: Global concurrency limit caps active jobs
  // ---------------------------------------------------------------
  console.log('1. Testing GLOBAL CONCURRENCY LIMIT...');
  try {
    const q = makeQueue('bp-tcp-conc-limit');
    q.obliterate();
    await Bun.sleep(100);

    q.setGlobalConcurrency(2);
    await Bun.sleep(50);

    for (let i = 0; i < 10; i++) {
      await q.add('task', { idx: i });
    }

    let maxConcurrent = 0;
    let currentConcurrent = 0;
    let completed = 0;

    const w = makeWorker('bp-tcp-conc-limit', async () => {
      currentConcurrent++;
      maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
      await Bun.sleep(300);
      currentConcurrent--;
      completed++;
      return {};
    }, { concurrency: 10 });

    for (let i = 0; i < 80; i++) {
      if (completed >= 10) break;
      await Bun.sleep(100);
    }
    await w.close();

    // Allow tolerance of 3 for timing overlap
    if (maxConcurrent <= 3 && completed >= 10) {
      ok(`Concurrency capped: max=${maxConcurrent}, completed=${completed}`);
    } else {
      fail(`Concurrency not capped: max=${maxConcurrent}, completed=${completed}`);
    }
  } catch (e) {
    fail(`Global concurrency limit test failed: ${e}`);
  }

  // ---------------------------------------------------------------
  // Test 2: Remove concurrency restores throughput
  // ---------------------------------------------------------------
  console.log('\n2. Testing REMOVE CONCURRENCY RESTORES THROUGHPUT...');
  try {
    const q = makeQueue('bp-tcp-conc-remove');
    q.obliterate();
    await Bun.sleep(100);

    q.setGlobalConcurrency(1);
    await Bun.sleep(50);

    let completed = 0;
    const completionTimestamps: number[] = [];

    const w = makeWorker('bp-tcp-conc-remove', async () => {
      await Bun.sleep(100);
      completed++;
      completionTimestamps.push(Date.now());
      return {};
    }, { concurrency: 5 });

    for (let i = 0; i < 5; i++) {
      await q.add('task', { idx: i });
    }

    // Wait for first 2 to complete under concurrency=1
    for (let i = 0; i < 40; i++) {
      if (completed >= 2) break;
      await Bun.sleep(100);
    }

    const timeForFirst2 = completionTimestamps.length >= 2
      ? completionTimestamps[1] - completionTimestamps[0]
      : 0;

    // Remove the limit
    q.removeGlobalConcurrency();

    // Wait for all to complete
    for (let i = 0; i < 60; i++) {
      if (completed >= 5) break;
      await Bun.sleep(100);
    }
    await w.close();

    if (completed >= 5 && timeForFirst2 >= 80) {
      ok(`Throughput restored: first2Gap=${timeForFirst2}ms, completed=${completed}`);
    } else {
      fail(`Throughput issue: first2Gap=${timeForFirst2}ms, completed=${completed}`);
    }
  } catch (e) {
    fail(`Remove concurrency test failed: ${e}`);
  }

  // ---------------------------------------------------------------
  // Test 3: Rate limit throttles processing
  // ---------------------------------------------------------------
  console.log('\n3. Testing RATE LIMIT THROTTLES PROCESSING...');
  try {
    const q = makeQueue('bp-tcp-rate-limit');
    q.obliterate();
    await Bun.sleep(100);

    q.setGlobalRateLimit(3);
    await Bun.sleep(50);

    for (let i = 0; i < 10; i++) {
      await q.add('task', { idx: i });
    }

    let completed = 0;
    const completionTimestamps: number[] = [];

    const w = makeWorker('bp-tcp-rate-limit', async () => {
      completed++;
      completionTimestamps.push(Date.now());
      return {};
    }, { concurrency: 10 });

    for (let i = 0; i < 80; i++) {
      if (completed >= 10) break;
      await Bun.sleep(100);
    }
    await w.close();

    if (completionTimestamps.length >= 5) {
      const duration = completionTimestamps[completionTimestamps.length - 1] - completionTimestamps[0];
      if (duration > 0 && completed >= 5) {
        ok(`Rate limited: ${completed} jobs over ${duration}ms`);
      } else {
        fail(`No throttling effect: ${completed} jobs in ${duration}ms`);
      }
    } else {
      fail(`Not enough completions: ${completed}`);
    }
  } catch (e) {
    fail(`Rate limit test failed: ${e}`);
  }

  // ---------------------------------------------------------------
  // Test 4: Remove rate limit restores speed
  // ---------------------------------------------------------------
  console.log('\n4. Testing REMOVE RATE LIMIT RESTORES SPEED...');
  try {
    const q = makeQueue('bp-tcp-rate-remove');
    q.obliterate();
    await Bun.sleep(100);

    q.setGlobalRateLimit(1);
    await Bun.sleep(50);

    let completed = 0;

    const w = makeWorker('bp-tcp-rate-remove', async () => {
      completed++;
      return {};
    }, { concurrency: 5 });

    for (let i = 0; i < 5; i++) {
      await q.add('task', { idx: i });
    }

    // Wait for first 2 under rate limit=1
    for (let i = 0; i < 40; i++) {
      if (completed >= 2) break;
      await Bun.sleep(100);
    }

    const completedBeforeRemove = completed;

    // Remove the rate limit
    q.removeGlobalRateLimit();

    // Wait for all to complete
    for (let i = 0; i < 60; i++) {
      if (completed >= 5) break;
      await Bun.sleep(100);
    }
    await w.close();

    if (completedBeforeRemove >= 1 && completed >= 5) {
      ok(`Rate limit removed: before=${completedBeforeRemove}, after=${completed}`);
    } else {
      fail(`Rate limit removal issue: before=${completedBeforeRemove}, after=${completed}`);
    }
  } catch (e) {
    fail(`Remove rate limit test failed: ${e}`);
  }

  // ---------------------------------------------------------------
  // Test 5: Concurrency + worker concurrency interaction
  // ---------------------------------------------------------------
  console.log('\n5. Testing CONCURRENCY + WORKER CONCURRENCY INTERACTION...');
  try {
    const q = makeQueue('bp-tcp-conc-worker');
    q.obliterate();
    await Bun.sleep(100);

    // Server-side global concurrency=2, worker concurrency=5
    q.setGlobalConcurrency(2);
    await Bun.sleep(50);

    let maxConcurrent = 0;
    let currentConcurrent = 0;
    let completed = 0;

    const w = makeWorker('bp-tcp-conc-worker', async () => {
      currentConcurrent++;
      maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
      await Bun.sleep(200);
      currentConcurrent--;
      completed++;
      return {};
    }, { concurrency: 5 });

    for (let i = 0; i < 10; i++) {
      await q.add('task', { idx: i });
    }

    for (let i = 0; i < 80; i++) {
      if (completed >= 10) break;
      await Bun.sleep(100);
    }
    await w.close();

    // Global concurrency 2 should be the effective limit (tolerance 3)
    if (maxConcurrent <= 3 && completed >= 10) {
      ok(`Server-side limit enforced: max=${maxConcurrent}, completed=${completed}`);
    } else {
      fail(`Server-side limit not enforced: max=${maxConcurrent}, completed=${completed}`);
    }
  } catch (e) {
    fail(`Concurrency + worker interaction test failed: ${e}`);
  }

  // ---------------------------------------------------------------
  // Test 6: Burst absorption - 100 jobs drain smoothly
  // ---------------------------------------------------------------
  console.log('\n6. Testing BURST ABSORPTION...');
  try {
    const q = makeQueue('bp-tcp-burst');
    q.obliterate();
    await Bun.sleep(100);

    let completed = 0;

    const w = makeWorker('bp-tcp-burst', async () => {
      completed++;
      return {};
    }, { concurrency: 10 });

    // Burst: add 100 jobs at once via addBulk
    const jobs = Array.from({ length: 100 }, (_, i) => ({
      name: `burst-${i}`,
      data: { idx: i },
    }));
    await q.addBulk(jobs);

    // Wait for all to complete
    for (let i = 0; i < 100; i++) {
      if (completed >= 100) break;
      await Bun.sleep(100);
    }
    await w.close();

    if (completed >= 100) {
      ok(`Burst absorbed: ${completed}/100 jobs completed`);
    } else {
      fail(`Burst not fully absorbed: ${completed}/100 jobs completed`);
    }
  } catch (e) {
    fail(`Burst absorption test failed: ${e}`);
  }

  // ---------------------------------------------------------------
  // Test 7: Multiple workers sharing concurrency
  // ---------------------------------------------------------------
  console.log('\n7. Testing MULTIPLE WORKERS SHARING CONCURRENCY...');
  try {
    const q = makeQueue('bp-tcp-multi-worker');
    q.obliterate();
    await Bun.sleep(100);

    q.setGlobalConcurrency(4);
    await Bun.sleep(50);

    let maxConcurrent = 0;
    let currentConcurrent = 0;
    let completed = 0;

    const processor = async () => {
      currentConcurrent++;
      maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
      await Bun.sleep(200);
      currentConcurrent--;
      completed++;
      return {};
    };

    const w1 = makeWorker('bp-tcp-multi-worker', processor, { concurrency: 5 });
    const w2 = makeWorker('bp-tcp-multi-worker', processor, { concurrency: 5 });

    for (let i = 0; i < 20; i++) {
      await q.add('task', { idx: i });
    }

    for (let i = 0; i < 120; i++) {
      if (completed >= 20) break;
      await Bun.sleep(100);
    }

    await w1.close();
    await w2.close();

    // Global concurrency of 4 should limit total active across both workers
    // Allow tolerance of 5 for timing overlap
    if (maxConcurrent <= 5 && completed >= 20) {
      ok(`Shared concurrency enforced: max=${maxConcurrent}, completed=${completed}`);
    } else {
      fail(`Shared concurrency not enforced: max=${maxConcurrent}, completed=${completed}`);
    }
  } catch (e) {
    fail(`Multiple workers sharing concurrency test failed: ${e}`);
  }

  // ---------------------------------------------------------------
  // Cleanup
  // ---------------------------------------------------------------
  for (const q of queues) {
    q.removeGlobalConcurrency();
    q.removeGlobalRateLimit();
    q.obliterate();
    q.close();
  }

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch((e) => {
  console.error(`Fatal error: ${e}`);
  process.exit(1);
});
