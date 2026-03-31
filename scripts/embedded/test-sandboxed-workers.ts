#!/usr/bin/env bun
/**
 * Test Sandboxed Workers (Embedded Mode)
 * Tests isolated worker processes with crash recovery
 *
 * Run with: BUNQUEUE_EMBEDDED=1 bun run scripts/embedded/test-sandboxed-workers.ts
 * Or use: bun run scripts/embedded/run-all-tests.ts
 */

import { Queue, SandboxedWorker } from '../../src/client';

const QUEUE_NAME = 'test-sandboxed-workers';
const PROCESSOR_PATH = `${import.meta.dir}/processor.ts`;

async function main() {
  console.log('=== Test Sandboxed Workers (Embedded) ===\n');

  const queue = new Queue<{ message?: string; shouldFail?: boolean; shouldTimeout?: boolean }>(QUEUE_NAME);
  let passed = 0;
  let failed = 0;

  // Clean state
  queue.obliterate();

  // Test 1: SandboxedWorker basic - Create and start sandboxed worker
  console.log('1. Testing SANDBOXED WORKER BASIC...');
  try {
    const worker = new SandboxedWorker(QUEUE_NAME, {
      processor: PROCESSOR_PATH,
      concurrency: 2,
      timeout: 5000,
      autoRestart: true,
      maxRestarts: 3,
    });

    await worker.start();
    await Bun.sleep(100);

    const stats = worker.getStats();
    if (stats.total === 2 && stats.idle === 2 && stats.busy === 0) {
      console.log(`   [PASS] Sandboxed worker created with ${stats.total} workers`);
      passed++;
    } else {
      console.log(`   [FAIL] Worker pool not initialized correctly: ${JSON.stringify(stats)}`);
      failed++;
    }

    await worker.stop();
  } catch (e) {
    console.log(`   [FAIL] Sandboxed worker creation failed: ${e}`);
    failed++;
  }

  // Test 2: SandboxedWorker processes jobs - Jobs are processed by sandboxed worker
  console.log('\n2. Testing SANDBOXED WORKER PROCESSES JOBS...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    // Add jobs first
    await queue.add('job-1', { message: 'Hello from job 1' });
    await queue.add('job-2', { message: 'Hello from job 2' });
    await queue.add('job-3', { message: 'Hello from job 3' });

    const worker = new SandboxedWorker(QUEUE_NAME, {
      processor: PROCESSOR_PATH,
      concurrency: 2,
      timeout: 5000,
    });

    await worker.start();

    // Wait for jobs to be processed
    await Bun.sleep(2000);

    const counts = queue.getJobCounts();
    await worker.stop();

    if (counts.waiting === 0 && counts.completed === 3) {
      console.log('   [PASS] All jobs processed by sandboxed workers');
      passed++;
    } else {
      console.log(`   [FAIL] Jobs not fully processed: waiting=${counts.waiting}, completed=${counts.completed}`);
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] Job processing test failed: ${e}`);
    failed++;
  }

  // Test 3: SandboxedWorker crash recovery - Worker restarts after crash
  console.log('\n3. Testing SANDBOXED WORKER CRASH RECOVERY...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    // Add a job that will cause the processor to fail
    await queue.add('fail-job', { shouldFail: true }, { attempts: 1 });

    const worker = new SandboxedWorker(QUEUE_NAME, {
      processor: PROCESSOR_PATH,
      concurrency: 1,
      timeout: 5000,
      autoRestart: true,
      maxRestarts: 3,
    });

    await worker.start();

    // Wait for the job to be processed and fail
    await Bun.sleep(1500);

    const stats = worker.getStats();
    await worker.stop();

    // Worker should still be running (restarted after failure)
    if (stats.total === 1) {
      console.log(`   [PASS] Worker recovered after job failure (restarts: ${stats.restarts})`);
      passed++;
    } else {
      console.log(`   [FAIL] Worker not recovered: ${JSON.stringify(stats)}`);
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] Crash recovery test failed: ${e}`);
    failed++;
  }

  // Test 4: SandboxedWorker timeout - Jobs timeout and fail
  console.log('\n4. Testing SANDBOXED WORKER TIMEOUT...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    // Add a job that will timeout
    await queue.add('timeout-job', { shouldTimeout: true }, { attempts: 1 });

    const worker = new SandboxedWorker(QUEUE_NAME, {
      processor: PROCESSOR_PATH,
      concurrency: 1,
      timeout: 1000, // 1 second timeout
      autoRestart: true,
      maxRestarts: 3,
    });

    await worker.start();

    // Wait for the job to timeout
    await Bun.sleep(2500);

    const counts = queue.getJobCounts();
    const stats = worker.getStats();
    await worker.stop();

    // Job should have failed due to timeout, and worker should have restarted
    if (counts.failed >= 1 || stats.restarts >= 1) {
      console.log(`   [PASS] Job timed out as expected (restarts: ${stats.restarts}, failed: ${counts.failed})`);
      passed++;
    } else {
      console.log(`   [FAIL] Timeout not triggered: failed=${counts.failed}, restarts=${stats.restarts}`);
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] Timeout test failed: ${e}`);
    failed++;
  }

  // Test 5: SandboxedWorker getStats - Get worker statistics
  console.log('\n5. Testing SANDBOXED WORKER GETSTATS...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    const worker = new SandboxedWorker(QUEUE_NAME, {
      processor: PROCESSOR_PATH,
      concurrency: 3,
      timeout: 5000,
    });

    await worker.start();
    await Bun.sleep(100);

    const stats = worker.getStats();

    if (
      typeof stats.total === 'number' &&
      typeof stats.busy === 'number' &&
      typeof stats.idle === 'number' &&
      typeof stats.restarts === 'number' &&
      stats.total === 3 &&
      stats.busy === 0 &&
      stats.idle === 3
    ) {
      console.log(`   [PASS] Stats returned correctly: total=${stats.total}, busy=${stats.busy}, idle=${stats.idle}, restarts=${stats.restarts}`);
      passed++;
    } else {
      console.log(`   [FAIL] Stats incorrect: ${JSON.stringify(stats)}`);
      failed++;
    }

    await worker.stop();
  } catch (e) {
    console.log(`   [FAIL] GetStats test failed: ${e}`);
    failed++;
  }

  // Test 6: SandboxedWorker stop - Graceful shutdown
  console.log('\n6. Testing SANDBOXED WORKER STOP...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    // Add jobs
    await queue.add('job-1', { message: 'Job 1' });
    await queue.add('job-2', { message: 'Job 2' });

    const worker = new SandboxedWorker(QUEUE_NAME, {
      processor: PROCESSOR_PATH,
      concurrency: 2,
      timeout: 5000,
    });

    await worker.start();
    await Bun.sleep(500);

    // Stop the worker
    await worker.stop();

    // After stop, getStats should return 0 workers
    const stats = worker.getStats();

    if (stats.total === 0) {
      console.log('   [PASS] Worker stopped gracefully');
      passed++;
    } else {
      console.log(`   [FAIL] Worker not stopped: ${JSON.stringify(stats)}`);
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] Stop test failed: ${e}`);
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
