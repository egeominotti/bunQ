#!/usr/bin/env bun
/**
 * Test Concurrency Control
 */

// Force embedded mode BEFORE imports
process.env.BUNQUEUE_EMBEDDED = '1';

import { Queue, Worker } from '../../src/client';

const QUEUE_NAME = 'test-concurrency';

async function main() {
  console.log('=== Test Concurrency Control ===\n');

  const queue = new Queue<{ index: number }>(QUEUE_NAME, { embedded: true });
  let passed = 0;
  let failed = 0;

  // Test 1: Single concurrency
  console.log('1. Testing CONCURRENCY = 1...');
  try {
    queue.obliterate();

    await queue.addBulk([
      { name: 'job-1', data: { index: 1 } },
      { name: 'job-2', data: { index: 2 } },
      { name: 'job-3', data: { index: 3 } },
    ]);

    let maxConcurrent = 0;
    let currentConcurrent = 0;

    const worker = new Worker<{ index: number }>(QUEUE_NAME, async () => {
      currentConcurrent++;
      maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
      await Bun.sleep(100);
      currentConcurrent--;
      return {};
    }, { concurrency: 1, embedded: true });

    await Bun.sleep(600);
    await worker.close();

    if (maxConcurrent === 1) {
      console.log('   ✅ Max concurrent jobs: 1');
      passed++;
    } else {
      console.log(`   ❌ Max concurrent: ${maxConcurrent}, expected 1`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Concurrency=1 test failed: ${e}`);
    failed++;
  }

  // Test 2: Multiple concurrency
  console.log('\n2. Testing CONCURRENCY = 5...');
  try {
    queue.obliterate();

    await queue.addBulk(
      Array.from({ length: 10 }, (_, i) => ({
        name: `job-${i}`,
        data: { index: i },
      }))
    );

    let maxConcurrent = 0;
    let currentConcurrent = 0;

    const worker = new Worker<{ index: number }>(QUEUE_NAME, async () => {
      currentConcurrent++;
      maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
      await Bun.sleep(200);
      currentConcurrent--;
      return {};
    }, { concurrency: 5, embedded: true });

    await Bun.sleep(800);
    await worker.close();

    if (maxConcurrent >= 4 && maxConcurrent <= 5) {
      console.log(`   ✅ Max concurrent jobs: ${maxConcurrent} (expected ~5)`);
      passed++;
    } else {
      console.log(`   ❌ Max concurrent: ${maxConcurrent}, expected ~5`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Concurrency=5 test failed: ${e}`);
    failed++;
  }

  // Test 3: High concurrency
  console.log('\n3. Testing HIGH CONCURRENCY (20)...');
  try {
    queue.obliterate();

    await queue.addBulk(
      Array.from({ length: 50 }, (_, i) => ({
        name: `job-${i}`,
        data: { index: i },
      }))
    );

    let maxConcurrent = 0;
    let currentConcurrent = 0;
    let processed = 0;

    const worker = new Worker<{ index: number }>(QUEUE_NAME, async () => {
      currentConcurrent++;
      maxConcurrent = Math.max(maxConcurrent, currentConcurrent);
      await Bun.sleep(50);
      currentConcurrent--;
      processed++;
      return {};
    }, { concurrency: 20, embedded: true });

    await Bun.sleep(1000);
    await worker.close();

    if (maxConcurrent >= 15 && processed === 50) {
      console.log(`   ✅ Max concurrent: ${maxConcurrent}, processed: ${processed}`);
      passed++;
    } else {
      console.log(`   ❌ Max concurrent: ${maxConcurrent}, processed: ${processed}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ High concurrency test failed: ${e}`);
    failed++;
  }

  // Test 4: Multiple workers
  console.log('\n4. Testing MULTIPLE WORKERS...');
  try {
    queue.obliterate();

    await queue.addBulk(
      Array.from({ length: 20 }, (_, i) => ({
        name: `job-${i}`,
        data: { index: i },
      }))
    );

    const processedBy = new Map<string, number>();

    const worker1 = new Worker<{ index: number }>(QUEUE_NAME, async () => {
      processedBy.set('worker1', (processedBy.get('worker1') ?? 0) + 1);
      await Bun.sleep(50);
      return {};
    }, { concurrency: 2, embedded: true });

    const worker2 = new Worker<{ index: number }>(QUEUE_NAME, async () => {
      processedBy.set('worker2', (processedBy.get('worker2') ?? 0) + 1);
      await Bun.sleep(50);
      return {};
    }, { concurrency: 2, embedded: true });

    await Bun.sleep(1000);
    await worker1.close();
    await worker2.close();

    const total = (processedBy.get('worker1') ?? 0) + (processedBy.get('worker2') ?? 0);

    if (total === 20) {
      console.log(`   ✅ Jobs distributed: worker1=${processedBy.get('worker1')}, worker2=${processedBy.get('worker2')}`);
      passed++;
    } else {
      console.log(`   ❌ Total processed: ${total}, expected 20`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Multiple workers test failed: ${e}`);
    failed++;
  }

  // Test 5: Worker pause/resume
  console.log('\n5. Testing WORKER PAUSE/RESUME...');
  try {
    queue.obliterate();

    await queue.addBulk(
      Array.from({ length: 5 }, (_, i) => ({
        name: `pause-job-${i}`,
        data: { index: i },
      }))
    );

    let processed = 0;

    const worker = new Worker<{ index: number }>(QUEUE_NAME, async () => {
      processed++;
      await Bun.sleep(50);
      return {};
    }, { concurrency: 1, autorun: false, embedded: true });

    // Process first batch
    worker.run();
    await Bun.sleep(150);
    const afterFirst = processed;

    // Pause
    worker.pause();
    await Bun.sleep(200);
    const afterPause = processed;

    // Resume
    worker.resume();
    await Bun.sleep(400);
    await worker.close();

    if (afterFirst > 0 && afterPause === afterFirst && processed === 5) {
      console.log(`   ✅ Pause/resume works: first=${afterFirst}, paused=${afterPause}, final=${processed}`);
      passed++;
    } else {
      console.log(`   ❌ Pause/resume failed: first=${afterFirst}, paused=${afterPause}, final=${processed}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Pause/resume test failed: ${e}`);
    failed++;
  }

  // Test 6: Queue pause/resume
  console.log('\n6. Testing QUEUE PAUSE/RESUME...');
  try {
    queue.obliterate();

    queue.pause();
    await queue.addBulk(
      Array.from({ length: 3 }, (_, i) => ({
        name: `queue-pause-job-${i}`,
        data: { index: i },
      }))
    );

    let processed = 0;

    const worker = new Worker<{ index: number }>(QUEUE_NAME, async () => {
      processed++;
      return {};
    }, { concurrency: 3, embedded: true });

    // Wait while paused
    await Bun.sleep(300);
    const whilePaused = processed;

    // Resume
    queue.resume();
    await Bun.sleep(300);
    await worker.close();

    if (whilePaused === 0 && processed === 3) {
      console.log(`   ✅ Queue pause/resume: paused=${whilePaused}, resumed=${processed}`);
      passed++;
    } else {
      console.log(`   ❌ Queue pause/resume failed: paused=${whilePaused}, resumed=${processed}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Queue pause/resume test failed: ${e}`);
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
