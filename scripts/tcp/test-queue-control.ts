#!/usr/bin/env bun
/**
 * Test Queue Control (TCP Mode): Pause, Resume, Drain, Obliterate
 */

import { Queue, Worker } from '../../src/client';

const QUEUE_NAME = 'tcp-test-control';
const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789', 10);

async function main() {
  console.log('=== Test Queue Control (TCP) ===\n');

  const queue = new Queue<{ value: number }>(QUEUE_NAME, {
    connection: { port: TCP_PORT },
  });
  let passed = 0;
  let failed = 0;

  // Clean state
  queue.obliterate();
  await Bun.sleep(100);

  // Test 1: Pause queue
  console.log('1. Testing PAUSE...');
  try {
    await queue.addBulk([
      { name: 'pause-1', data: { value: 1 } },
      { name: 'pause-2', data: { value: 2 } },
    ]);

    queue.pause();
    await Bun.sleep(100);

    let processed = 0;
    const worker = new Worker<{ value: number }>(
      QUEUE_NAME,
      () => {
        processed++;
        return {};
      },
      { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false }
    );

    await Bun.sleep(500);
    await worker.close();

    if (processed === 0) {
      console.log('   ✅ Queue paused, no jobs processed');
      passed++;
    } else {
      console.log(`   ❌ ${processed} jobs processed while paused`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Pause test failed: ${e}`);
    failed++;
  }

  // Test 2: Resume queue
  console.log('\n2. Testing RESUME...');
  try {
    queue.resume();
    await Bun.sleep(100);

    let processed = 0;
    const worker = new Worker<{ value: number }>(
      QUEUE_NAME,
      () => {
        processed++;
        return {};
      },
      { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false }
    );

    await Bun.sleep(500);
    await worker.close();

    if (processed >= 1) {
      console.log(`   ✅ Queue resumed, ${processed} jobs processed`);
      passed++;
    } else {
      console.log('   ❌ No jobs processed after resume');
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Resume test failed: ${e}`);
    failed++;
  }

  // Test 3: Drain queue
  console.log('\n3. Testing DRAIN...');
  try {
    await queue.obliterateAsync();

    await queue.addBulk([
      { name: 'drain-1', data: { value: 1 } },
      { name: 'drain-2', data: { value: 2 } },
      { name: 'drain-3', data: { value: 3 } },
    ]);

    queue.drain();
    await Bun.sleep(100);

    let processed = 0;
    const worker = new Worker<{ value: number }>(
      QUEUE_NAME,
      () => {
        processed++;
        return {};
      },
      { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false }
    );

    await Bun.sleep(500);
    await worker.close();

    if (processed === 0) {
      console.log('   ✅ Queue drained, no jobs to process');
      passed++;
    } else {
      console.log(`   ❌ ${processed} jobs processed after drain`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Drain test failed: ${e}`);
    failed++;
  }

  // Test 4: Obliterate queue
  console.log('\n4. Testing OBLITERATE...');
  try {
    await queue.addBulk([
      { name: 'obliterate-1', data: { value: 1 } },
      { name: 'obliterate-2', data: { value: 2 } },
    ]);

    queue.obliterate();
    await Bun.sleep(100);

    let processed = 0;
    const worker = new Worker<{ value: number }>(
      QUEUE_NAME,
      () => {
        processed++;
        return {};
      },
      { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false }
    );

    await Bun.sleep(500);
    await worker.close();

    if (processed === 0) {
      console.log('   ✅ Queue obliterated');
      passed++;
    } else {
      console.log(`   ❌ ${processed} jobs processed after obliterate`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Obliterate test failed: ${e}`);
    failed++;
  }

  // Test 5: Pause during processing
  console.log('\n5. Testing PAUSE DURING PROCESSING...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    await queue.addBulk(
      Array.from({ length: 10 }, (_, i) => ({
        name: `pause-proc-${i}`,
        data: { value: i },
      }))
    );

    let processedBeforePause = 0;
    let processedAfterPause = 0;
    let paused = false;

    const worker = new Worker<{ value: number }>(
      QUEUE_NAME,
      async () => {
        if (!paused) processedBeforePause++;
        else processedAfterPause++;
        await Bun.sleep(100);
        return {};
      },
      { concurrency: 2, connection: { port: TCP_PORT }, useLocks: false }
    );

    await Bun.sleep(300);
    queue.pause();
    paused = true;
    await Bun.sleep(500);
    await worker.close();

    // Should have processed some before pause, none after
    if (processedBeforePause >= 1 && processedAfterPause <= 2) {
      console.log(
        `   ✅ Pause during processing: ${processedBeforePause} before, ${processedAfterPause} after`
      );
      passed++;
    } else {
      console.log(`   ❌ Before: ${processedBeforePause}, After: ${processedAfterPause}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Pause during processing test failed: ${e}`);
    failed++;
  }

  // Test 6: Get job counts (async for TCP)
  console.log('\n6. Testing GET JOB COUNTS...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    await queue.addBulk([
      { name: 'count-1', data: { value: 1 } },
      { name: 'count-2', data: { value: 2 } },
      { name: 'count-3', data: { value: 3 } },
    ]);

    const counts = await queue.getJobCountsAsync();

    if (counts.waiting === 3) {
      console.log('   ✅ Job counts report exactly 3 waiting jobs');
      passed++;
    } else {
      console.log(`   ❌ Expected waiting=3, got ${counts.waiting}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Get job counts test failed: ${e}`);
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
