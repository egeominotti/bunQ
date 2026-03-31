#!/usr/bin/env bun
/**
 * Test Queue Control: pause, resume, drain, obliterate
 */

// Force embedded mode BEFORE imports
process.env.BUNQUEUE_EMBEDDED = '1';

import { Queue, Worker } from '../../src/client';

const QUEUE_NAME = 'test-queue-control';

async function main() {
  console.log('=== Test Queue Control ===\n');

  const queue = new Queue<{ value: number }>(QUEUE_NAME, { embedded: true });
  let passed = 0;
  let failed = 0;

  // Test 1: Pause queue
  console.log('1. Testing PAUSE...');
  try {
    // Add jobs first
    await queue.addBulk([
      { name: 'job-1', data: { value: 1 } },
      { name: 'job-2', data: { value: 2 } },
      { name: 'job-3', data: { value: 3 } },
    ]);

    queue.pause();

    const processed: number[] = [];
    const worker = new Worker<{ value: number }>(QUEUE_NAME, async (job) => {
      processed.push(job.data.value);
      return {};
    }, { concurrency: 1, embedded: true });

    await Bun.sleep(300);

    if (processed.length === 0) {
      console.log('   ✅ Queue paused - no jobs processed');
      passed++;
    } else {
      console.log(`   ❌ Jobs processed while paused: ${processed.length}`);
      failed++;
    }

    await worker.close();
  } catch (e) {
    console.log(`   ❌ Pause test failed: ${e}`);
    failed++;
  }

  // Test 2: Resume queue
  console.log('\n2. Testing RESUME...');
  try {
    queue.resume();

    const processed: number[] = [];
    const worker = new Worker<{ value: number }>(QUEUE_NAME, async (job) => {
      processed.push(job.data.value);
      return {};
    }, { concurrency: 1, embedded: true });

    await Bun.sleep(500);
    await worker.close();

    if (processed.length === 3) {
      console.log(`   ✅ Queue resumed - ${processed.length} jobs processed`);
      passed++;
    } else {
      console.log(`   ❌ Expected 3 jobs, processed ${processed.length}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Resume test failed: ${e}`);
    failed++;
  }

  // Test 3: Drain queue
  console.log('\n3. Testing DRAIN...');
  try {
    // Add more jobs
    await queue.addBulk([
      { name: 'drain-1', data: { value: 10 } },
      { name: 'drain-2', data: { value: 20 } },
      { name: 'drain-3', data: { value: 30 } },
    ]);

    const countsBefore = queue.getJobCounts();
    queue.drain();
    const countsAfter = queue.getJobCounts();

    if (countsBefore.waiting === 3 && countsAfter.waiting === 0) {
      console.log(`   ✅ Queue drained: ${countsBefore.waiting} -> ${countsAfter.waiting} waiting`);
      passed++;
    } else {
      console.log(`   ❌ Drain failed: ${countsBefore.waiting} -> ${countsAfter.waiting}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Drain test failed: ${e}`);
    failed++;
  }

  // Test 4: Obliterate queue
  console.log('\n4. Testing OBLITERATE...');
  try {
    // Add jobs
    await queue.addBulk([
      { name: 'obl-1', data: { value: 100 } },
      { name: 'obl-2', data: { value: 200 } },
    ]);

    const countsBefore = queue.getJobCounts();
    queue.obliterate();

    // Small delay for obliterate to complete
    await Bun.sleep(100);

    const countsAfter = queue.getJobCounts();

    if (countsBefore.waiting === 2 && countsAfter.waiting === 0) {
      console.log(`   ✅ Queue obliterated: ${countsBefore.waiting} -> ${countsAfter.waiting}`);
      passed++;
    } else {
      console.log(`   ❌ Obliterate failed: ${countsBefore.waiting} -> ${countsAfter.waiting}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Obliterate test failed: ${e}`);
    failed++;
  }

  // Test 5: Pause prevents processing
  console.log('\n5. Testing PAUSE PREVENTS PROCESSING...');
  try {
    const queue2 = new Queue<{ value: number }>('test-pause-check');
    queue2.obliterate();

    queue2.pause();
    await queue2.add('check-job', { value: 555 });

    let processed = false;
    const worker = new Worker<{ value: number }>('test-pause-check', async () => {
      processed = true;
      return {};
    }, { concurrency: 1, embedded: true });

    await Bun.sleep(300);
    const whilePaused = processed;

    queue2.resume();
    await Bun.sleep(300);
    const afterResume = processed;

    await worker.close();
    queue2.obliterate();

    if (!whilePaused && afterResume) {
      console.log('   ✅ Pause correctly prevents processing');
      passed++;
    } else {
      console.log(`   ❌ Pause check: whilePaused=${whilePaused}, afterResume=${afterResume}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Pause check test failed: ${e}`);
    failed++;
  }

  // Test 6: Multiple pause/resume cycles
  console.log('\n6. Testing MULTIPLE PAUSE/RESUME CYCLES...');
  try {
    await queue.add('cycle-job', { value: 999 });

    queue.pause();
    queue.resume();
    queue.pause();
    queue.resume();

    const processed: number[] = [];
    const worker = new Worker<{ value: number }>(QUEUE_NAME, async (job) => {
      processed.push(job.data.value);
      return {};
    }, { concurrency: 1, embedded: true });

    await Bun.sleep(300);
    await worker.close();

    if (processed.includes(999)) {
      console.log('   ✅ Job processed after multiple pause/resume cycles');
      passed++;
    } else {
      console.log('   ❌ Job not processed after cycles');
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Cycle test failed: ${e}`);
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
