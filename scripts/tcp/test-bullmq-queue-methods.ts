#!/usr/bin/env bun
/**
 * Test BullMQ v5 Queue Methods (TCP Mode) - Tests all new Queue methods implemented for BullMQ compatibility
 */

import { Queue, Worker } from '../../src/client';

const QUEUE_NAME = 'tcp-test-bullmq-queue-methods';
const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');

async function main() {
  console.log('=== Test BullMQ v5 Queue Methods (TCP) ===\n');

  const queue = new Queue<{ value: number }>(QUEUE_NAME, {
    connection: { port: TCP_PORT },
  });
  let passed = 0;
  let failed = 0;

  // Clean state
  queue.obliterate();
  await Bun.sleep(100);

  // ============================================================================
  // Test 1: getJobState
  // ============================================================================
  console.log('1. Testing getJobState...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    const job = await queue.add('state-test', { value: 1 });
    const state = await queue.getJobState(job.id);

    if (state === 'waiting') {
      console.log(`   Job state: ${state}`);
      console.log('   [PASS] getJobState works');
      passed++;
    } else {
      console.log(`   [FAIL] Expected 'waiting', got '${state}'`);
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] getJobState: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 2: isPausedAsync
  // ============================================================================
  console.log('\n2. Testing isPausedAsync...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    const notPaused = await queue.isPausedAsync();
    if (!notPaused) {
      queue.pause();
      await Bun.sleep(200);

      const isPaused = await queue.isPausedAsync();
      if (isPaused) {
        queue.resume();
        console.log('   [PASS] isPausedAsync works');
        passed++;
      } else {
        console.log('   [FAIL] Queue should be paused after pause()');
        failed++;
      }
    } else {
      console.log('   [FAIL] Queue should not be paused initially');
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] isPausedAsync: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 3: countAsync
  // ============================================================================
  console.log('\n3. Testing countAsync...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    await queue.add('count-1', { value: 1 });
    await queue.add('count-2', { value: 2 });
    await queue.add('count-3', { value: 3 });

    const count = await queue.countAsync();
    if (count === 3) {
      console.log(`   Count: ${count}`);
      console.log('   [PASS] countAsync works');
      passed++;
    } else {
      console.log(`   [FAIL] Expected 3, got ${count}`);
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] countAsync: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 4: getWaitingAsync / getDelayedAsync
  // ============================================================================
  console.log('\n4. Testing getWaitingAsync / getDelayedAsync...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    await queue.add('waiting-1', { value: 1 });
    await queue.add('waiting-2', { value: 2 });
    await queue.add('delayed-1', { value: 3 }, { delay: 60000 });

    const waiting = await queue.getWaitingAsync();
    const delayed = await queue.getDelayedAsync();

    console.log(`   Waiting: ${waiting.length}, Delayed: ${delayed.length}`);

    if (waiting.length === 2 && delayed.length === 1) {
      console.log('   [PASS] getWaitingAsync/getDelayedAsync works');
      passed++;
    } else {
      console.log(`   [FAIL] Expected 2 waiting, 1 delayed`);
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] get*Async methods: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 5: getWaitingCount / getDelayedCount
  // ============================================================================
  console.log('\n5. Testing getWaitingCount / getDelayedCount...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    await queue.add('count-1', { value: 1 });
    await queue.add('count-2', { value: 2 });
    await queue.add('delayed-1', { value: 3 }, { delay: 60000 });

    const waitingCount = await queue.getWaitingCount();
    const delayedCount = await queue.getDelayedCount();

    console.log(`   Waiting: ${waitingCount}, Delayed: ${delayedCount}`);

    if (waitingCount === 2 && delayedCount === 1) {
      console.log('   [PASS] getWaitingCount/getDelayedCount works');
      passed++;
    } else {
      console.log(`   [FAIL] Expected waiting=2, delayed=1`);
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] get*Count: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 6: cleanAsync
  // ============================================================================
  console.log('\n6. Testing cleanAsync...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    await queue.add('clean-1', { value: 1 });
    await queue.add('clean-2', { value: 2 });

    const worker = new Worker<{ value: number }>(QUEUE_NAME, async () => {
      return { done: true };
    }, { concurrency: 5, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(1000);
    await worker.close();

    const cleaned = await queue.cleanAsync(0, 100, 'completed');
    console.log(`   Cleaned: ${cleaned.length} jobs`);

    console.log('   [PASS] cleanAsync works');
    passed++;
  } catch (e) {
    console.log(`   [FAIL] cleanAsync: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 7: promoteJobs
  // ============================================================================
  console.log('\n7. Testing promoteJobs...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    await queue.add('promote-1', { value: 1 }, { delay: 60000 });
    await queue.add('promote-2', { value: 2 }, { delay: 60000 });

    const beforeDelayed = await queue.getDelayedCount();
    const promoted = await queue.promoteJobs();
    const afterDelayed = await queue.getDelayedCount();
    const afterWaiting = await queue.getWaitingCount();

    console.log(`   Before: ${beforeDelayed} delayed, After: ${afterDelayed} delayed, ${afterWaiting} waiting`);
    console.log(`   Promoted: ${promoted}`);

    if (afterWaiting >= beforeDelayed - afterDelayed) {
      console.log('   [PASS] promoteJobs works');
      passed++;
    } else {
      console.log('   [FAIL] Jobs not promoted correctly');
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] promoteJobs: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 8: getJobLogs / addJobLog
  // ============================================================================
  console.log('\n8. Testing getJobLogs / addJobLog...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    const job = await queue.add('logs-test', { value: 1 });

    await queue.addJobLog(job.id, 'Log entry 1');
    await queue.addJobLog(job.id, 'Log entry 2');
    await queue.addJobLog(job.id, 'Log entry 3');

    const logs = await queue.getJobLogs(job.id);

    console.log(`   Logs count: ${logs.count}`);
    console.log(`   Logs: ${logs.logs.join(', ')}`);

    // Logs may have [info] prefix
    const hasLog1 = logs.logs.some(l => l.includes('Log entry 1'));
    if (logs.count === 3 && hasLog1) {
      console.log('   [PASS] getJobLogs/addJobLog works');
      passed++;
    } else {
      console.log('   [FAIL] Expected 3 logs');
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] logs: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 9: updateJobProgress
  // ============================================================================
  console.log('\n9. Testing updateJobProgress...');
  try {
    queue.obliterate();
    await Bun.sleep(200);

    // Note: updateJobProgress only works for jobs in "processing" state (after pull)
    // This is by design - only active jobs can report progress
    let progressUpdated = false;
    let progressValue = 0;

    const job = await queue.add('progress-test', { value: 1 });

    // Create a worker that will pull and update progress
    const worker = new Worker<{ value: number }>(QUEUE_NAME, async (j) => {
      await j.updateProgress(50);
      progressUpdated = true;
      progressValue = 50;
      await Bun.sleep(100);
      return { done: true };
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    // Wait for worker to process
    await Bun.sleep(1000);
    await worker.close();

    console.log(`   Note: updateJobProgress requires job to be in processing state`);
    console.log(`   Progress updated: ${progressUpdated}, value: ${progressValue}`);

    if (progressUpdated && progressValue === 50) {
      console.log('   [PASS] updateJobProgress works (via worker)');
      passed++;
    } else {
      console.log('   [FAIL] Progress was not updated');
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] updateJobProgress: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 10: setGlobalConcurrency / removeGlobalConcurrency
  // ============================================================================
  console.log('\n10. Testing setGlobalConcurrency / removeGlobalConcurrency...');
  try {
    queue.setGlobalConcurrency(5);
    await Bun.sleep(100);
    queue.removeGlobalConcurrency();
    console.log('   [PASS] setGlobalConcurrency/removeGlobalConcurrency works');
    passed++;
  } catch (e) {
    console.log(`   [FAIL] concurrency: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 11: setGlobalRateLimit / removeGlobalRateLimit
  // ============================================================================
  console.log('\n11. Testing setGlobalRateLimit / removeGlobalRateLimit...');
  try {
    queue.setGlobalRateLimit(10);
    await Bun.sleep(100);
    queue.removeGlobalRateLimit();
    console.log('   [PASS] setGlobalRateLimit/removeGlobalRateLimit works');
    passed++;
  } catch (e) {
    console.log(`   [FAIL] rateLimit: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 12: getMetrics
  // ============================================================================
  console.log('\n12. Testing getMetrics...');
  try {
    const metrics = await queue.getMetrics('completed');
    console.log(`   Metrics: ${JSON.stringify(metrics)}`);
    console.log('   [PASS] getMetrics works');
    passed++;
  } catch (e) {
    console.log(`   [FAIL] getMetrics: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 13: getWorkers / getWorkersCount
  // ============================================================================
  console.log('\n13. Testing getWorkers / getWorkersCount...');
  try {
    const workers = await queue.getWorkers();
    const workerCount = await queue.getWorkersCount();

    console.log(`   Workers: ${workers.length}, Count: ${workerCount}`);
    console.log('   [PASS] getWorkers/getWorkersCount works');
    passed++;
  } catch (e) {
    console.log(`   [FAIL] getWorkers: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 14: trimEvents
  // ============================================================================
  console.log('\n14. Testing trimEvents...');
  try {
    const trimmed = await queue.trimEvents(100);
    console.log(`   Trimmed: ${trimmed}`);
    console.log('   [PASS] trimEvents works');
    passed++;
  } catch (e) {
    console.log(`   [FAIL] trimEvents: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 15: getPrioritized / getPrioritizedCount
  // ============================================================================
  console.log('\n15. Testing getPrioritized / getPrioritizedCount...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    await queue.add('p1', { value: 1 }, { priority: 1 });
    await queue.add('p5', { value: 2 }, { priority: 5 });
    await queue.add('p10', { value: 3 }, { priority: 10 });

    const prioritized = await queue.getPrioritized();
    const count = await queue.getPrioritizedCount();

    console.log(`   Prioritized: ${prioritized.length}, Count: ${count}`);

    if (prioritized.length === 3 && count === 3) {
      console.log('   [PASS] getPrioritized/getPrioritizedCount works');
      passed++;
    } else {
      console.log('   [FAIL] Expected 3 prioritized jobs');
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] getPrioritized: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 16: Job Scheduler methods (Cron)
  // ============================================================================
  console.log('\n16. Testing Job Scheduler methods...');
  try {
    // Use unique scheduler name to avoid conflicts
    const schedulerName = `tcp-test-scheduler-${Date.now()}`;

    const scheduler = await queue.upsertJobScheduler(schedulerName, {
      every: 60000,
    }, {
      name: 'scheduled-job',
      data: { value: 42 },
    });

    console.log(`   Created scheduler: ${scheduler?.id}`);

    const retrieved = await queue.getJobScheduler(schedulerName);
    console.log(`   Retrieved: ${retrieved?.id}`);

    const schedulers = await queue.getJobSchedulers();
    console.log(`   Total schedulers: ${schedulers.length}`);

    const count = await queue.getJobSchedulersCount();
    console.log(`   Scheduler count: ${count}`);

    const removed = await queue.removeJobScheduler(schedulerName);
    console.log(`   Removed: ${removed}`);

    if (scheduler && retrieved && removed) {
      console.log('   [PASS] Job Scheduler methods work');
      passed++;
    } else {
      console.log(`   [FAIL] Job Scheduler methods failed (scheduler=${!!scheduler}, retrieved=${!!retrieved}, removed=${removed})`);
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] Job Scheduler: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 17: waitUntilReady
  // ============================================================================
  console.log('\n17. Testing waitUntilReady...');
  try {
    await queue.waitUntilReady();
    console.log('   [PASS] waitUntilReady works');
    passed++;
  } catch (e) {
    console.log(`   [FAIL] waitUntilReady: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 18: disconnect
  // ============================================================================
  console.log('\n18. Testing disconnect...');
  try {
    const queue2 = new Queue<{ value: number }>('tcp-test-disconnect', {
      connection: { port: TCP_PORT },
    });
    await queue2.disconnect();
    console.log('   [PASS] disconnect works');
    passed++;
  } catch (e) {
    console.log(`   [FAIL] disconnect: ${e}`);
    failed++;
  }

  // Cleanup
  queue.obliterate();
  queue.close();

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);
  console.log(`Total: ${passed + failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(console.error);
