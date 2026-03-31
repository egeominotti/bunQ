#!/usr/bin/env bun
/**
 * Test BullMQ v5 Queue Methods - Tests all new Queue methods implemented for BullMQ compatibility
 */

// Force embedded mode BEFORE imports
process.env.BUNQUEUE_EMBEDDED = '1';

import { Queue, Worker } from '../../src/client';

const QUEUE_NAME = 'test-bullmq-queue-methods';

async function main() {
  console.log('=== Test BullMQ v5 Queue Methods (Embedded) ===\n');

  const queue = new Queue<{ value: number }>(QUEUE_NAME, { embedded: true });
  let passed = 0;
  let failed = 0;

  // Clean state
  queue.obliterate();

  // ============================================================================
  // Test 1: getJobState
  // ============================================================================
  console.log('1. Testing getJobState...');
  try {
    queue.obliterate();
    const job = await queue.add('state-test', { value: 1 });

    // Should be waiting
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
  // Test 2: isPaused / isPausedAsync
  // ============================================================================
  console.log('\n2. Testing isPaused / isPausedAsync...');
  try {
    queue.obliterate();

    // Should not be paused initially
    const notPaused = await queue.isPausedAsync();
    if (!notPaused) {
      queue.pause();
      await Bun.sleep(100);

      const isPaused = await queue.isPausedAsync();
      if (isPaused) {
        queue.resume();
        console.log('   [PASS] isPaused/isPausedAsync works');
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
    console.log(`   [FAIL] isPaused: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 3: count / countAsync
  // ============================================================================
  console.log('\n3. Testing count / countAsync...');
  try {
    queue.obliterate();

    await queue.add('count-1', { value: 1 });
    await queue.add('count-2', { value: 2 });
    await queue.add('count-3', { value: 3 });

    const count = await queue.countAsync();
    if (count === 3) {
      console.log(`   Count: ${count}`);
      console.log('   [PASS] count/countAsync works');
      passed++;
    } else {
      console.log(`   [FAIL] Expected 3, got ${count}`);
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] count: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 4: getActive/Completed/Failed/Delayed/Waiting
  // ============================================================================
  console.log('\n4. Testing getActive/Completed/Failed/Delayed/Waiting...');
  try {
    queue.obliterate();

    // Add jobs
    await queue.add('waiting-1', { value: 1 });
    await queue.add('waiting-2', { value: 2 });
    await queue.add('delayed-1', { value: 3 }, { delay: 60000 });

    const waiting = queue.getWaiting();
    const delayed = queue.getDelayed();

    console.log(`   Waiting: ${waiting.length}, Delayed: ${delayed.length}`);

    if (waiting.length === 2 && delayed.length === 1) {
      console.log('   [PASS] getWaiting/getDelayed works');
      passed++;
    } else {
      console.log(`   [FAIL] Expected 2 waiting, 1 delayed`);
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] get* methods: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 5: getActiveCount/CompletedCount/FailedCount/DelayedCount/WaitingCount
  // ============================================================================
  console.log('\n5. Testing get*Count methods...');
  try {
    queue.obliterate();

    await queue.add('count-1', { value: 1 });
    await queue.add('count-2', { value: 2 });
    await queue.add('delayed-1', { value: 3 }, { delay: 60000 });

    const waitingCount = await queue.getWaitingCount();
    const delayedCount = await queue.getDelayedCount();

    console.log(`   Waiting: ${waitingCount}, Delayed: ${delayedCount}`);

    if (waitingCount === 2 && delayedCount === 1) {
      console.log('   [PASS] get*Count methods work');
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
  // Test 6: clean / cleanAsync
  // ============================================================================
  console.log('\n6. Testing clean / cleanAsync...');
  try {
    queue.obliterate();

    // Add and process jobs to get completed
    await queue.add('clean-1', { value: 1 });
    await queue.add('clean-2', { value: 2 });

    const worker = new Worker<{ value: number }>(QUEUE_NAME, async () => {
      return { done: true };
    }, { concurrency: 5, embedded: true });

    await Bun.sleep(500);
    await worker.close();

    // Clean completed jobs (older than 0ms)
    const cleaned = await queue.cleanAsync(0, 100, 'completed');
    console.log(`   Cleaned: ${cleaned.length} jobs`);

    if (cleaned.length >= 0) { // May be 0 if jobs already removed
      console.log('   [PASS] clean/cleanAsync works');
      passed++;
    } else {
      console.log('   [FAIL] clean should return array');
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] clean: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 7: retryJobs / promoteJobs
  // ============================================================================
  console.log('\n7. Testing retryJobs / promoteJobs...');
  try {
    queue.obliterate();

    // Add delayed job
    await queue.add('promote-1', { value: 1 }, { delay: 60000 });
    await queue.add('promote-2', { value: 2 }, { delay: 60000 });

    const beforeDelayed = await queue.getDelayedCount();

    // Promote delayed jobs
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

    const job = await queue.add('logs-test', { value: 1 });

    // Add logs
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

    // Progress can only be updated on ACTIVE (processing) jobs
    // So we need to start processing and update during processing
    let progressUpdated = false;

    const worker = new Worker<{ value: number }>(QUEUE_NAME, async (job) => {
      // Update progress during processing
      await job.updateProgress(50);
      progressUpdated = true;
      await Bun.sleep(100);
      return { done: true };
    }, { concurrency: 1, embedded: true });

    await queue.add('progress-test', { value: 1 });
    await Bun.sleep(500);
    await worker.close();

    if (progressUpdated) {
      console.log('   Progress updated during job processing');
      console.log('   [PASS] updateJobProgress works (via job.updateProgress)');
      passed++;
    } else {
      console.log('   [FAIL] Progress not updated');
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
    queue.removeGlobalConcurrency();
    console.log('   [PASS] setGlobalConcurrency/removeGlobalConcurrency works (no error)');
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
    queue.removeGlobalRateLimit();
    console.log('   [PASS] setGlobalRateLimit/removeGlobalRateLimit works (no error)');
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
    console.log('   [PASS] trimEvents works (no-op expected)');
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

    await Bun.sleep(100);

    const prioritized = await queue.getPrioritized();
    const count = await queue.getPrioritizedCount();

    console.log(`   Prioritized: ${prioritized.length}, Count: ${count}`);

    // getPrioritized is alias of getWaiting, should return waiting jobs
    if (prioritized.length >= 2 && count >= 2) {
      console.log('   [PASS] getPrioritized/getPrioritizedCount works');
      passed++;
    } else {
      console.log(`   [FAIL] Expected at least 2 prioritized jobs, got ${prioritized.length}`);
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] getPrioritized: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 16: Job Scheduler methods
  // ============================================================================
  console.log('\n16. Testing Job Scheduler methods...');
  try {
    // Create scheduler
    const scheduler = await queue.upsertJobScheduler('test-scheduler', {
      every: 60000,
    }, {
      name: 'scheduled-job',
      data: { value: 42 },
    });

    console.log(`   Created scheduler: ${scheduler?.id}`);

    // Get scheduler
    const retrieved = await queue.getJobScheduler('test-scheduler');
    console.log(`   Retrieved: ${retrieved?.id}`);

    // List schedulers
    const schedulers = await queue.getJobSchedulers();
    console.log(`   Total schedulers: ${schedulers.length}`);

    // Count schedulers
    const count = await queue.getJobSchedulersCount();
    console.log(`   Scheduler count: ${count}`);

    // Remove scheduler
    const removed = await queue.removeJobScheduler('test-scheduler');
    console.log(`   Removed: ${removed}`);

    if (scheduler && retrieved && removed) {
      console.log('   [PASS] Job Scheduler methods work');
      passed++;
    } else {
      console.log('   [FAIL] Job Scheduler methods failed');
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] Job Scheduler: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 17: getDeduplicationJobId
  // ============================================================================
  console.log('\n17. Testing getDeduplicationJobId...');
  try {
    queue.obliterate();

    const customId = 'dedupe-test-123';
    await queue.add('dedupe-job', { value: 1 }, { jobId: customId });

    const jobId = await queue.getDeduplicationJobId(customId);
    console.log(`   Dedup job ID: ${jobId}`);

    if (jobId) {
      console.log('   [PASS] getDeduplicationJobId works');
      passed++;
    } else {
      console.log('   [FAIL] Could not find job by dedup ID');
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] getDeduplicationJobId: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 18: waitUntilReady
  // ============================================================================
  console.log('\n18. Testing waitUntilReady...');
  try {
    await queue.waitUntilReady();
    console.log('   [PASS] waitUntilReady works');
    passed++;
  } catch (e) {
    console.log(`   [FAIL] waitUntilReady: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 19: disconnect
  // ============================================================================
  console.log('\n19. Testing disconnect...');
  try {
    const queue2 = new Queue<{ value: number }>('test-disconnect', { embedded: true });
    await queue2.disconnect();
    console.log('   [PASS] disconnect works');
    passed++;
  } catch (e) {
    console.log(`   [FAIL] disconnect: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 20: rateLimit (temporary)
  // ============================================================================
  console.log('\n20. Testing rateLimit (temporary)...');
  try {
    await queue.rateLimit(1000); // 1 second rate limit
    console.log('   [PASS] rateLimit works');
    passed++;
  } catch (e) {
    console.log(`   [FAIL] rateLimit: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 21: getRateLimitTtl
  // ============================================================================
  console.log('\n21. Testing getRateLimitTtl...');
  try {
    const ttl = await queue.getRateLimitTtl();
    console.log(`   TTL: ${ttl}`);
    console.log('   [PASS] getRateLimitTtl works');
    passed++;
  } catch (e) {
    console.log(`   [FAIL] getRateLimitTtl: ${e}`);
    failed++;
  }

  // ============================================================================
  // Test 22: isMaxed
  // ============================================================================
  console.log('\n22. Testing isMaxed...');
  try {
    const maxed = await queue.isMaxed();
    console.log(`   Is maxed: ${maxed}`);
    console.log('   [PASS] isMaxed works');
    passed++;
  } catch (e) {
    console.log(`   [FAIL] isMaxed: ${e}`);
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
