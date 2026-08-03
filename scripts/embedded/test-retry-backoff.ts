#!/usr/bin/env bun
/**
 * Test Retry and Backoff Strategies
 */

// Force embedded mode BEFORE imports
process.env.BUNQUEUE_EMBEDDED = '1';

import { Queue, Worker } from '../../src/client';

const QUEUE_NAME = 'test-retry';

/**
 * Poll until the condition holds or the deadline passes. Fixed sleeps made
 * this suite flaky on loaded CI runners (backoff scheduling + worker poll
 * cadence can exceed a blind budget); polling keeps the fast path fast and
 * gives slow runners room without weakening any assertion.
 */
async function waitFor(cond: () => boolean | Promise<boolean>, timeoutMs = 15000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await cond()) return true;
    await Bun.sleep(25);
  }
  return cond();
}

async function main() {
  console.log('=== Test Retry & Backoff ===\n');

  let passed = 0;
  let failed = 0;

  // Test 1: Fixed backoff retry
  console.log('1. Testing FIXED BACKOFF...');
  try {
    const queue = new Queue<{ attempt: number }>(QUEUE_NAME, { embedded: true });
    queue.obliterate();

    await queue.add('fixed-job', { attempt: 0 }, {
      attempts: 3,
      backoff: { type: 'fixed', delay: 100 },
    });

    const attempts: number[] = [];
    const timestamps: number[] = [];

    const worker = new Worker<{ attempt: number }>(QUEUE_NAME, async () => {
      attempts.push(attempts.length + 1);
      timestamps.push(Date.now());
      if (attempts.length < 3) {
        throw new Error(`Attempt ${attempts.length} failed`);
      }
      return { success: true };
    }, { concurrency: 1, embedded: true });

    await waitFor(() => attempts.length === 3);
    await worker.close();

    if (attempts.length === 3) {
      const delay1 = timestamps[1] - timestamps[0];
      const delay2 = timestamps[2] - timestamps[1];
      if (delay1 >= 72 && delay2 >= 72) {
        console.log(`   ✅ Fixed backoff: ${attempts.length} attempts, delays: ~${delay1}ms, ~${delay2}ms`);
        passed++;
      } else {
        console.log(`   ❌ Fixed backoff below its jitter floor: ${delay1}ms, ${delay2}ms`);
        failed++;
      }
    } else {
      console.log(`   ❌ Expected 3 attempts, got ${attempts.length}`);
      failed++;
    }

    queue.obliterate();
  } catch (e) {
    console.log(`   ❌ Fixed backoff test failed: ${e}`);
    failed++;
  }

  // Test 2: Exponential backoff
  console.log('\n2. Testing EXPONENTIAL BACKOFF...');
  try {
    const queue = new Queue<{ attempt: number }>(QUEUE_NAME, { embedded: true });
    queue.obliterate();

    await queue.add('exp-job', { attempt: 0 }, {
      attempts: 4,
      backoff: 50,
    });

    const timestamps: number[] = [];

    const worker = new Worker<{ attempt: number }>(QUEUE_NAME, async () => {
      timestamps.push(Date.now());
      if (timestamps.length < 4) {
        throw new Error(`Attempt ${timestamps.length} failed`);
      }
      return { success: true };
    }, { concurrency: 1, embedded: true });

    await waitFor(() => timestamps.length >= 4);
    await worker.close();

    if (timestamps.length >= 4) {
      const delays = timestamps.slice(1).map((t, i) => t - timestamps[i]);
      const floors = [45, 90, 180];
      const respectsWindows = floors.every((floor, i) => delays[i] >= floor);
      if (respectsWindows) {
        console.log(`   ✅ Exponential backoff delays: ${delays.map(d => `~${d}ms`).join(', ')}`);
        passed++;
      } else {
        console.log(`   ❌ Backoff below its exponential floor: ${delays.join(', ')}ms`);
        failed++;
      }
    } else {
      console.log(`   ❌ Expected 4 attempts, got ${timestamps.length}`);
      failed++;
    }

    queue.obliterate();
  } catch (e) {
    console.log(`   ❌ Exponential backoff test failed: ${e}`);
    failed++;
  }

  // Test 3: Numeric shorthand
  console.log('\n3. Testing NUMERIC BACKOFF SHORTHAND...');
  try {
    const queue = new Queue<{ attempt: number }>(QUEUE_NAME, { embedded: true });
    queue.obliterate();

    await queue.add('custom-job', { attempt: 0 }, {
      attempts: 3,
      backoff: 200,
    });

    let attemptCount = 0;

    const worker = new Worker<{ attempt: number }>(QUEUE_NAME, async () => {
      attemptCount++;
      if (attemptCount < 3) {
        throw new Error(`Fail ${attemptCount}`);
      }
      return {};
    }, { concurrency: 1, embedded: true });

    await waitFor(() => attemptCount === 3);
    await worker.close();

    if (attemptCount === 3) {
      console.log(`   ✅ Numeric backoff: ${attemptCount} attempts completed`);
      passed++;
    } else {
      console.log(`   ❌ Expected 3 attempts, got ${attemptCount}`);
      failed++;
    }

    queue.obliterate();
  } catch (e) {
    console.log(`   ❌ Numeric backoff test failed: ${e}`);
    failed++;
  }

  // Test 4: No retry (attempts = 1)
  console.log('\n4. Testing NO RETRY (attempts=1)...');
  try {
    const queue = new Queue<{ attempt: number }>(QUEUE_NAME, { embedded: true });
    queue.obliterate();

    await queue.add('no-retry-job', { attempt: 0 }, { attempts: 1 });

    let attemptCount = 0;

    const worker = new Worker<{ attempt: number }>(QUEUE_NAME, async () => {
      attemptCount++;
      throw new Error('Always fails');
    }, { concurrency: 1, embedded: true });

    // Wait for the DLQ entry (positive signal), then settle briefly to catch
    // a spurious second attempt before asserting the count stayed at 1.
    await waitFor(() => queue.getDlq().length === 1);
    await Bun.sleep(150);
    await worker.close();

    const dlq = queue.getDlq();

    if (attemptCount === 1 && dlq.length === 1) {
      console.log('   ✅ No retry: 1 attempt, moved to DLQ');
      passed++;
    } else {
      console.log(`   ❌ Attempts: ${attemptCount}, DLQ: ${dlq.length}`);
      failed++;
    }

    queue.obliterate();
  } catch (e) {
    console.log(`   ❌ No retry test failed: ${e}`);
    failed++;
  }

  // Test 5: Successful on retry
  console.log('\n5. Testing SUCCESS ON RETRY...');
  try {
    const queue = new Queue<{ attempt: number }>(QUEUE_NAME, { embedded: true });
    queue.obliterate();

    await queue.add('success-retry-job', { attempt: 0 }, {
      attempts: 5,
      backoff: 50, // 50ms backoff between retries
    });

    let attemptCount = 0;
    let succeeded = false;

    const worker = new Worker<{ attempt: number }>(QUEUE_NAME, async () => {
      attemptCount++;
      if (attemptCount < 3) {
        throw new Error(`Fail ${attemptCount}`);
      }
      succeeded = true;
      return { success: true };
    }, { concurrency: 1, embedded: true });

    await waitFor(() => succeeded);
    await worker.close();

    if (attemptCount === 3 && succeeded) {
      console.log(`   ✅ Succeeded on attempt ${attemptCount}`);
      passed++;
    } else {
      console.log(`   ❌ Attempts: ${attemptCount}, succeeded: ${succeeded}`);
      failed++;
    }

    queue.obliterate();
  } catch (e) {
    console.log(`   ❌ Success on retry test failed: ${e}`);
    failed++;
  }

  // Test 6: removeOnComplete
  console.log('\n6. Testing REMOVE ON COMPLETE...');
  try {
    const queue = new Queue<{ value: number }>(QUEUE_NAME, { embedded: true });
    queue.obliterate();

    const job = await queue.add('remove-job', { value: 1 }, {
      removeOnComplete: true,
    });

    let completedEvent = false;
    const worker = new Worker<{ value: number }>(QUEUE_NAME, async () => {
      return { done: true };
    }, { concurrency: 1, embedded: true });
    worker.on('completed', () => {
      completedEvent = true;
    });

    // getJob(id) === null alone is NOT a completion signal: during the pull
    // transition (queue -> processing) getJob transiently returns null for a
    // job that was never processed (see
    // test/repro-getjob-false-null-during-pull.test.ts). Wait for the positive
    // 'completed' event first, then for the batched-ack removal to land.
    await waitFor(() => completedEvent);
    await waitFor(async () => (await queue.getJob(job.id)) === null);
    await worker.close();

    // Try to get the job - should be removed
    const retrieved = await queue.getJob(job.id);

    if (!retrieved) {
      console.log('   ✅ Job removed after completion');
      passed++;
    } else {
      console.log('   ❌ Job still exists after completion');
      failed++;
    }

    queue.obliterate();
  } catch (e) {
    console.log(`   ❌ Remove on complete test failed: ${e}`);
    failed++;
  }

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(console.error);
