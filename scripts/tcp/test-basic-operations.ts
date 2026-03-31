#!/usr/bin/env bun
/**
 * Test Basic Operations (TCP Mode): Push, Pull, Ack, Fail, Priority, Delay
 */

import { Queue, Worker } from '../../src/client';

const QUEUE_NAME = 'tcp-test-basic-ops';
const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');

async function main() {
  console.log('=== Test Basic Operations (TCP) ===\n');

  const queue = new Queue<{ message: string }>(QUEUE_NAME, {
    connection: { port: TCP_PORT },
  });
  let passed = 0;
  let failed = 0;

  // Clean state
  queue.obliterate();
  await Bun.sleep(100);

  // Test 1: Push a job
  console.log('1. Testing PUSH...');
  try {
    const job = await queue.add('test-job', { message: 'Hello World' });
    if (job.id && job.name === 'test-job') {
      console.log(`   ✅ Job pushed: ${job.id}`);
      passed++;
    } else {
      console.log('   ❌ Job not created correctly');
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Push failed: ${e}`);
    failed++;
  }

  // Test 2: Worker processes job
  console.log('\n2. Testing WORKER PROCESSING...');
  try {
    let processed = false;
    const worker = new Worker<{ message: string }>(QUEUE_NAME, async (job) => {
      console.log(`   Processing job ${job.id}: ${(job.data as { message: string }).message}`);
      processed = true;
      return { success: true };
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(1000);
    await worker.close();

    if (processed) {
      console.log('   ✅ Job processed successfully');
      passed++;
    } else {
      console.log('   ❌ Job was not processed');
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Worker failed: ${e}`);
    failed++;
  }

  // Test 3: Push and fail a job
  console.log('\n3. Testing JOB FAILURE...');
  try {
    queue.obliterate();
    await Bun.sleep(100);
    await queue.add('fail-job', { message: 'This will fail' }, { attempts: 1 });

    let failedJob = false;
    const worker = new Worker<{ message: string }>(QUEUE_NAME, async () => {
      failedJob = true;
      throw new Error('Intentional failure');
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(1000);
    await worker.close();

    if (failedJob) {
      console.log('   ✅ Job failed as expected');
      passed++;
    } else {
      console.log('   ❌ Job was not processed');
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Test failed: ${e}`);
    failed++;
  }

  // Test 4: Push with priority
  console.log('\n4. Testing PRIORITY...');
  try {
    queue.obliterate();
    await Bun.sleep(100);
    await queue.add('low-priority', { message: 'Low' }, { priority: 1 });
    await queue.add('high-priority', { message: 'High' }, { priority: 10 });

    const jobs: string[] = [];
    const worker = new Worker<{ message: string }>(QUEUE_NAME, async (job) => {
      jobs.push((job.data as { message: string }).message);
      return {};
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(1000);
    await worker.close();

    if (jobs[0] === 'High' && jobs[1] === 'Low') {
      console.log('   ✅ Priority ordering correct');
      passed++;
    } else {
      console.log(`   ❌ Priority ordering wrong: ${jobs.join(', ')}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Priority test failed: ${e}`);
    failed++;
  }

  // Test 5: Push with delay
  console.log('\n5. Testing DELAYED JOB...');
  try {
    queue.obliterate();
    await Bun.sleep(100);
    const start = Date.now();
    await queue.add('delayed-job', { message: 'Delayed' }, { delay: 300 });

    let processedAt = 0;
    const worker = new Worker<{ message: string }>(QUEUE_NAME, async () => {
      processedAt = Date.now();
      return {};
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(1000);
    await worker.close();

    const delay = processedAt - start;
    if (processedAt > 0 && delay >= 280) {
      console.log(`   ✅ Job delayed correctly (~${delay}ms)`);
      passed++;
    } else {
      console.log(`   ❌ Job processed too early: ${delay}ms`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Delay test failed: ${e}`);
    failed++;
  }

  // Test 6: Get job by ID
  console.log('\n6. Testing GET JOB...');
  try {
    queue.obliterate();
    await Bun.sleep(100);
    const job = await queue.add('get-test', { message: 'Find me' });
    const retrieved = await queue.getJob(job.id);

    if (retrieved && retrieved.id === job.id) {
      console.log('   ✅ Job retrieved correctly');
      passed++;
    } else {
      console.log('   ❌ Could not retrieve job');
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Get job test failed: ${e}`);
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
