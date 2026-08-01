#!/usr/bin/env bun
/**
 * Test SandboxedWorker lifecycle, processing and recovery over a real TCP broker.
 */

import { Queue, SandboxedWorker } from '../../src/client';

const QUEUE_NAME = 'tcp-test-sandboxed-workers';
const PROCESSOR_PATH = `${import.meta.dir}/../embedded/processor.ts`;
const TCP_PORT = Number.parseInt(process.env.TCP_PORT ?? '16789', 10);
const connection = { port: TCP_PORT };

async function main() {
  console.log('=== Test Sandboxed Workers (TCP) ===\n');

  const queue = new Queue<{ message?: string; shouldFail?: boolean; shouldTimeout?: boolean }>(
    QUEUE_NAME,
    { connection }
  );

  let passed = 0;
  let failed = 0;

  // Clean state
  await queue.obliterateAsync();

  // Test 1: SandboxedWorker basic - Create and start sandboxed worker
  console.log('1. Testing SANDBOXED WORKER BASIC...');
  try {
    const worker = new SandboxedWorker(QUEUE_NAME, {
      processor: PROCESSOR_PATH,
      concurrency: 2,
      timeout: 5000,
      autoRestart: true,
      maxRestarts: 3,
      connection,
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
    await queue.obliterateAsync();

    // Add jobs
    await queue.add('job-1', { message: 'Hello from job 1' });
    await queue.add('job-2', { message: 'Hello from job 2' });
    await queue.add('job-3', { message: 'Hello from job 3' });

    const worker = new SandboxedWorker(QUEUE_NAME, {
      processor: PROCESSOR_PATH,
      concurrency: 2,
      timeout: 5000,
      connection,
    });

    await worker.start();

    // Wait for jobs to be processed
    await Bun.sleep(2000);

    const counts = await queue.getJobCountsAsync();
    await worker.stop();

    if (counts.waiting === 0 && counts.completed === 3) {
      console.log('   [PASS] All jobs processed by sandboxed workers');
      passed++;
    } else {
      console.log(
        `   [FAIL] Jobs not fully processed: waiting=${counts.waiting}, completed=${counts.completed}`
      );
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] Job processing test failed: ${e}`);
    failed++;
  }

  // Test 3: SandboxedWorker crash recovery - Worker restarts after crash
  console.log('\n3. Testing SANDBOXED WORKER CRASH RECOVERY...');
  try {
    await queue.obliterateAsync();

    // Add a job that will cause the processor to fail
    const failedJob = await queue.add('fail-job', { shouldFail: true }, { attempts: 1 });

    const worker = new SandboxedWorker(QUEUE_NAME, {
      processor: PROCESSOR_PATH,
      concurrency: 1,
      timeout: 5000,
      autoRestart: true,
      maxRestarts: 3,
      connection,
    });

    await worker.start();

    // Wait for the job to be processed and fail
    await Bun.sleep(1500);

    const stats = worker.getStats();
    const state = await queue.getJobState(failedJob.id);
    await worker.stop();

    if (stats.total === 1 && state === 'failed') {
      console.log(
        '   [PASS] Processor failure reached the TCP DLQ and the worker stayed available'
      );
      passed++;
    } else {
      console.log(
        `   [FAIL] Failure recovery mismatch: state=${state}, stats=${JSON.stringify(stats)}`
      );
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] Crash recovery test failed: ${e}`);
    failed++;
  }

  // Test 4: SandboxedWorker timeout - Jobs timeout and fail
  console.log('\n4. Testing SANDBOXED WORKER TIMEOUT...');
  try {
    await queue.obliterateAsync();

    // Add a job that will timeout
    const timeoutJob = await queue.add('timeout-job', { shouldTimeout: true }, { attempts: 1 });

    const worker = new SandboxedWorker(QUEUE_NAME, {
      processor: PROCESSOR_PATH,
      concurrency: 1,
      timeout: 1000, // 1 second timeout
      autoRestart: true,
      maxRestarts: 3,
      connection,
    });

    await worker.start();

    // Wait for the job to timeout
    await Bun.sleep(2500);

    const counts = await queue.getJobCountsAsync();
    const stats = worker.getStats();
    const state = await queue.getJobState(timeoutJob.id);
    await worker.stop();

    if (counts.failed === 1 && state === 'failed' && stats.restarts >= 1) {
      console.log(
        `   [PASS] Timeout failed exactly one job and restarted the sandbox (${stats.restarts})`
      );
      passed++;
    } else {
      console.log(
        `   [FAIL] Timeout mismatch: state=${state}, failed=${counts.failed}, restarts=${stats.restarts}`
      );
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] Timeout test failed: ${e}`);
    failed++;
  }

  // Test 5: SandboxedWorker getStats - Get worker statistics
  console.log('\n5. Testing SANDBOXED WORKER GETSTATS...');
  try {
    await queue.obliterateAsync();

    const worker = new SandboxedWorker(QUEUE_NAME, {
      processor: PROCESSOR_PATH,
      concurrency: 3,
      timeout: 5000,
      connection,
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
      console.log(
        `   [PASS] Stats returned correctly: total=${stats.total}, busy=${stats.busy}, idle=${stats.idle}, restarts=${stats.restarts}`
      );
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
    await queue.obliterateAsync();

    // Add jobs
    await queue.add('job-1', { message: 'Job 1' });
    await queue.add('job-2', { message: 'Job 2' });

    const worker = new SandboxedWorker(QUEUE_NAME, {
      processor: PROCESSOR_PATH,
      concurrency: 2,
      timeout: 5000,
      connection,
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
  await queue.obliterateAsync();
  await queue.close();

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(console.error);
