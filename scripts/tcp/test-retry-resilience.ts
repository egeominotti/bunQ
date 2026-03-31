#!/usr/bin/env bun
/**
 * Retry Resilience Tests (TCP Mode)
 * Tests retry behavior, backoff timing, concurrent retries, and failure handling.
 */

import { Queue, Worker } from '../../src/client';

const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');
const connOpts = { port: TCP_PORT };
let passed = 0;
let failed = 0;
const queues: Queue[] = [];

function ok(msg: string) {
  console.log(`   ✅ ${msg}`);
  passed++;
}

function fail(msg: string) {
  console.log(`   ❌ ${msg}`);
  failed++;
}

function makeQueue(name: string): Queue {
  const q = new Queue(name, { connection: connOpts });
  queues.push(q);
  return q;
}

async function main() {
  console.log('=== Retry Resilience Tests (TCP) ===\n');

  // Test 1: Fail then succeed
  console.log('1. Testing FAIL THEN SUCCEED...');
  try {
    const queue = makeQueue('tcp-rr-fail-succeed');
    queue.obliterate();
    await Bun.sleep(100);

    const attemptCounts = new Map<string, number>();
    let completedJobId: string | null = null;

    await queue.add('task', { value: 1 }, { attempts: 3, backoff: 200 });

    const worker = new Worker(
      'tcp-rr-fail-succeed',
      async (job) => {
        const count = (attemptCounts.get(job.id) ?? 0) + 1;
        attemptCounts.set(job.id, count);
        if (count < 3) {
          throw new Error(`Attempt ${count} failed`);
        }
        return { success: true };
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    worker.on('completed', (job) => {
      completedJobId = job.id;
    });

    for (let i = 0; i < 50; i++) {
      await Bun.sleep(100);
      if (completedJobId) break;
    }

    await worker.close();

    if (completedJobId && attemptCounts.get(completedJobId) === 3) {
      ok(`Job failed twice then succeeded on 3rd attempt`);
    } else {
      fail(`Expected completed with 3 attempts, got ${completedJobId ? attemptCounts.get(completedJobId) : 'no completion'}`);
    }
  } catch (e) {
    fail(`Fail then succeed test error: ${e}`);
  }

  // Test 2: Exhaust all retries
  console.log('\n2. Testing EXHAUST ALL RETRIES...');
  try {
    const queue = makeQueue('tcp-rr-exhaust');
    queue.obliterate();
    await Bun.sleep(100);

    let processCount = 0;
    const failedJobAttempts: number[] = [];

    await queue.add('task', { value: 1 }, { attempts: 3, backoff: 100 });

    const worker = new Worker(
      'tcp-rr-exhaust',
      async () => {
        processCount++;
        throw new Error('Always fails');
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    worker.on('failed', (job) => {
      failedJobAttempts.push(job.attemptsMade);
    });

    // Wait for all 3 attempts with exponential backoff
    for (let i = 0; i < 50; i++) {
      await Bun.sleep(100);
      if (processCount >= 3) break;
    }

    await worker.close();

    if (processCount === 3 && failedJobAttempts.length === 3) {
      const sorted = failedJobAttempts.sort((a, b) => a - b);
      ok(`Job exhausted all 3 attempts, failed events fired with attemptsMade [${sorted}]`);
    } else {
      fail(`Expected 3 attempts and 3 failed events, got processCount=${processCount} failedEvents=${failedJobAttempts.length}`);
    }
  } catch (e) {
    fail(`Exhaust all retries test error: ${e}`);
  }

  // Test 3: Exponential backoff timing
  console.log('\n3. Testing EXPONENTIAL BACKOFF TIMING...');
  try {
    const queue = makeQueue('tcp-rr-exp-backoff');
    queue.obliterate();
    await Bun.sleep(100);

    const timestamps: number[] = [];
    let completed = false;

    await queue.add('task', { value: 1 }, { attempts: 4, backoff: 200 });

    const worker = new Worker(
      'tcp-rr-exp-backoff',
      async () => {
        timestamps.push(Date.now());
        if (timestamps.length < 4) {
          throw new Error(`Attempt ${timestamps.length} failed`);
        }
        return { done: true };
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    worker.on('completed', () => {
      completed = true;
    });

    for (let i = 0; i < 80; i++) {
      await Bun.sleep(100);
      if (completed) break;
    }

    await worker.close();

    if (timestamps.length >= 4) {
      const gaps: number[] = [];
      for (let i = 1; i < timestamps.length; i++) {
        gaps.push(timestamps[i] - timestamps[i - 1]);
      }
      // Exponential backoff with jitter (base * 2^attempts * [0.5..1.5]).
      // Due to jitter, adjacent gaps may not be strictly ordered, but the
      // 3rd gap (base*2^3) should reliably exceed the 1st gap (base*2^1).
      if (gaps[2] > gaps[0]) {
        ok(`Backoff grows: ${gaps.map((g) => `${g}ms`).join(' -> ')}`);
      } else {
        fail(`Backoff not growing: ${gaps.map((g) => `${g}ms`).join(' -> ')}`);
      }
    } else {
      fail(`Only ${timestamps.length} attempts recorded, need at least 4`);
    }
  } catch (e) {
    fail(`Exponential backoff timing test error: ${e}`);
  }

  // Test 4: Multiple jobs with different retry configs
  console.log('\n4. Testing MULTIPLE JOBS WITH DIFFERENT RETRY CONFIGS...');
  try {
    const queue = makeQueue('tcp-rr-multi-config');
    queue.obliterate();
    await Bun.sleep(100);

    const attemptCounts = new Map<string, number>();
    const completedIds: string[] = [];

    // Job 1: attempts:1 -> fails on 1st attempt, no retry
    const job1 = await queue.add('task-1a', { id: 1 }, { attempts: 1, backoff: 100 });
    // Job 2: attempts:3 -> fails on 1st, succeeds on 2nd
    const job2 = await queue.add('task-3a', { id: 2 }, { attempts: 3, backoff: 100 });
    // Job 3: attempts:5 -> fails on 1st, succeeds on 2nd
    const job3 = await queue.add('task-5a', { id: 3 }, { attempts: 5, backoff: 100 });

    const worker = new Worker(
      'tcp-rr-multi-config',
      async (job) => {
        const count = (attemptCounts.get(job.id) ?? 0) + 1;
        attemptCounts.set(job.id, count);
        if (count === 1) {
          throw new Error(`First attempt fails for job ${(job.data as any).id}`);
        }
        return { success: true };
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    worker.on('completed', (job) => {
      completedIds.push(job.id);
    });

    // Wait for job2 and job3 to retry and complete
    for (let i = 0; i < 60; i++) {
      await Bun.sleep(100);
      if (completedIds.length >= 2) break;
    }

    await worker.close();

    const job1NotCompleted = !completedIds.includes(job1.id);
    const job2Completed = completedIds.includes(job2.id);
    const job3Completed = completedIds.includes(job3.id);

    if (job1NotCompleted && job2Completed && job3Completed) {
      ok(`Job1 (attempts:1) not completed, Job2 (attempts:3) completed, Job3 (attempts:5) completed`);
    } else {
      fail(`Unexpected: job1NotCompleted=${job1NotCompleted} job2Completed=${job2Completed} job3Completed=${job3Completed}`);
    }
  } catch (e) {
    fail(`Multiple jobs with different retry configs test error: ${e}`);
  }

  // Test 5: Concurrent retries
  console.log('\n5. Testing CONCURRENT RETRIES...');
  try {
    const queue = makeQueue('tcp-rr-concurrent');
    queue.obliterate();
    await Bun.sleep(100);

    const attemptCounts = new Map<string, number>();
    const completedIds: string[] = [];

    for (let i = 0; i < 10; i++) {
      await queue.add(`task-${i}`, { idx: i }, { attempts: 2, backoff: 200 });
    }

    const worker = new Worker(
      'tcp-rr-concurrent',
      async (job) => {
        const count = (attemptCounts.get(job.id) ?? 0) + 1;
        attemptCounts.set(job.id, count);
        if (count === 1) {
          throw new Error(`First attempt fails for idx ${(job.data as any).idx}`);
        }
        return { done: true };
      },
      { concurrency: 5, connection: connOpts, useLocks: false }
    );

    worker.on('completed', (job) => {
      completedIds.push(job.id);
    });

    for (let i = 0; i < 80; i++) {
      await Bun.sleep(100);
      if (completedIds.length >= 10) break;
    }

    await worker.close();

    if (completedIds.length === 10) {
      ok(`All 10 jobs completed after retry with concurrency:5`);
    } else {
      fail(`Only ${completedIds.length}/10 jobs completed`);
    }
  } catch (e) {
    fail(`Concurrent retries test error: ${e}`);
  }

  // Test 6: No retry when attempts:1
  console.log('\n6. Testing NO RETRY WHEN ATTEMPTS:1...');
  try {
    const queue = makeQueue('tcp-rr-no-retry');
    queue.obliterate();
    await Bun.sleep(100);

    let processCount = 0;
    let failedEventCount = 0;

    await queue.add('task', { value: 1 }, { attempts: 1, backoff: 100 });

    const worker = new Worker(
      'tcp-rr-no-retry',
      async () => {
        processCount++;
        throw new Error('Single attempt failure');
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    worker.on('failed', () => {
      failedEventCount++;
    });

    for (let i = 0; i < 20; i++) {
      await Bun.sleep(100);
      if (failedEventCount >= 1) break;
    }

    // Extra wait to confirm no retry happens
    await Bun.sleep(500);

    await worker.close();

    if (failedEventCount === 1 && processCount === 1) {
      ok(`Job failed immediately with no retry (processed exactly once)`);
    } else {
      fail(`Expected 1 process and 1 failed event, got processCount=${processCount} failedEvents=${failedEventCount}`);
    }
  } catch (e) {
    fail(`No retry when attempts:1 test error: ${e}`);
  }

  // Cleanup
  for (const q of queues) {
    q.obliterate();
    q.close();
  }

  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch((e) => {
  console.error(`Fatal error: ${e}`);
  process.exit(1);
});
