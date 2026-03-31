#!/usr/bin/env bun
/**
 * Test Job Progress (TCP Mode)
 */

import { Queue, Worker } from '../../src/client';

const QUEUE_NAME = 'tcp-test-progress';
const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');

async function main() {
  console.log('=== Test Job Progress (TCP) ===\n');

  const queue = new Queue<{ steps: number }>(QUEUE_NAME, {
    connection: { port: TCP_PORT },
  });
  let passed = 0;
  let failed = 0;

  // Clean state
  queue.obliterate();
  await Bun.sleep(100);

  // Test 1: Update job progress
  console.log('1. Testing PROGRESS UPDATE...');
  try {
    await queue.add('progress-job', { steps: 5 });

    let lastProgress = 0;
    const worker = new Worker<{ steps: number }>(QUEUE_NAME, async (job) => {
      const steps = (job.data as { steps: number }).steps;
      for (let i = 1; i <= steps; i++) {
        await job.updateProgress((i / steps) * 100);
        lastProgress = (i / steps) * 100;
        await Bun.sleep(50);
      }
      return { completed: true };
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(1000);
    await worker.close();

    if (lastProgress === 100) {
      console.log('   ✅ Progress updated to 100%');
      passed++;
    } else {
      console.log(`   ❌ Progress was ${lastProgress}%`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Progress update test failed: ${e}`);
    failed++;
  }

  // Test 2: Progress with message
  console.log('\n2. Testing PROGRESS WITH MESSAGE...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    await queue.add('progress-msg-job', { steps: 3 });

    const messages: string[] = [];
    const worker = new Worker<{ steps: number }>(QUEUE_NAME, async (job) => {
      const msgs = ['Starting...', 'Processing...', 'Finishing...'];
      for (let i = 0; i < msgs.length; i++) {
        await job.updateProgress(((i + 1) / 3) * 100, msgs[i]);
        messages.push(msgs[i]);
        await Bun.sleep(50);
      }
      return { done: true };
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(500);
    await worker.close();

    if (messages.length === 3) {
      console.log('   ✅ Progress messages sent');
      passed++;
    } else {
      console.log(`   ❌ Only ${messages.length} messages`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Progress message test failed: ${e}`);
    failed++;
  }

  // Test 3: Job logging
  console.log('\n3. Testing JOB LOGGING...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    await queue.add('logging-job', { steps: 3 });

    let logCount = 0;
    const worker = new Worker<{ steps: number }>(QUEUE_NAME, async (job) => {
      await job.log('Step 1 started');
      logCount++;
      await job.log('Step 2 processing');
      logCount++;
      await job.log('Step 3 completed');
      logCount++;
      return { logged: true };
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(500);
    await worker.close();

    if (logCount === 3) {
      console.log('   ✅ Job logged 3 messages');
      passed++;
    } else {
      console.log(`   ❌ Logged ${logCount} messages`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Job logging test failed: ${e}`);
    failed++;
  }

  // Test 4: Progress increments
  console.log('\n4. Testing INCREMENTAL PROGRESS...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    await queue.add('incremental-job', { steps: 10 });

    const progressValues: number[] = [];
    const worker = new Worker<{ steps: number }>(QUEUE_NAME, async (job) => {
      for (let i = 1; i <= 10; i++) {
        await job.updateProgress(i * 10);
        progressValues.push(i * 10);
      }
      return {};
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(500);
    await worker.close();

    if (progressValues.length === 10 && progressValues[9] === 100) {
      console.log(`   ✅ Progress incremented correctly: ${progressValues[0]}% -> ${progressValues[9]}%`);
      passed++;
    } else {
      console.log(`   ❌ Progress values: ${progressValues.join(', ')}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Incremental progress test failed: ${e}`);
    failed++;
  }

  // Test 5: Multiple jobs with progress
  console.log('\n5. Testing MULTIPLE JOBS PROGRESS...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    await queue.addBulk([
      { name: 'multi-1', data: { steps: 2 } },
      { name: 'multi-2', data: { steps: 2 } },
      { name: 'multi-3', data: { steps: 2 } },
    ]);

    let completedJobs = 0;
    const worker = new Worker<{ steps: number }>(QUEUE_NAME, async (job) => {
      await job.updateProgress(50);
      await job.updateProgress(100);
      completedJobs++;
      return {};
    }, { concurrency: 3, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(1000);
    await worker.close();

    if (completedJobs === 3) {
      console.log(`   ✅ All ${completedJobs} jobs completed with progress`);
      passed++;
    } else {
      console.log(`   ❌ Only ${completedJobs} jobs completed`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Multiple jobs progress test failed: ${e}`);
    failed++;
  }

  // Test 6: Progress and logging combined
  console.log('\n6. Testing PROGRESS + LOGGING...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    await queue.add('combined-job', { steps: 2 });

    let progressDone = false;
    let logsDone = false;
    const worker = new Worker<{ steps: number }>(QUEUE_NAME, async (job) => {
      await job.log('Starting job');
      await job.updateProgress(25, 'Quarter done');
      await job.log('Still working');
      await job.updateProgress(75, 'Almost there');
      await job.log('Finishing up');
      await job.updateProgress(100, 'Complete');
      progressDone = true;
      logsDone = true;
      return { success: true };
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(500);
    await worker.close();

    if (progressDone && logsDone) {
      console.log('   ✅ Progress and logging combined successfully');
      passed++;
    } else {
      console.log(`   ❌ Progress: ${progressDone}, Logs: ${logsDone}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Combined test failed: ${e}`);
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
