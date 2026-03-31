#!/usr/bin/env bun
/**
 * Test Graceful Shutdown (TCP Mode)
 * Tests that worker.close() properly waits for active jobs to finish,
 * handles idle workers, concurrent jobs, and burst load scenarios.
 *
 * Requires a running bunqueue server on TCP_PORT (default: 16789)
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
  console.log('=== Graceful Shutdown Tests (TCP) ===\n');

  // ─────────────────────────────────────────────────────────────────
  // Test 1: Graceful close waits for active job
  // ─────────────────────────────────────────────────────────────────
  console.log('1. Testing GRACEFUL CLOSE WAITS FOR ACTIVE JOB...');
  try {
    const q = makeQueue('tcp-shutdown-wait');
    q.obliterate();
    await Bun.sleep(200);

    let jobCompleted = false;
    let completedViaEvent = false;

    await q.add('slow-job', { value: 1 });

    const worker = new Worker(
      'tcp-shutdown-wait',
      async () => {
        await Bun.sleep(1000);
        jobCompleted = true;
        return { done: true };
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    worker.on('completed', () => {
      completedViaEvent = true;
    });

    // Wait for the worker to pick up the job
    for (let i = 0; i < 30; i++) {
      await Bun.sleep(100);
      const counts = await q.getJobCounts();
      if (counts.active > 0) break;
    }

    // Close while job is still processing - should wait for it
    await worker.close();

    if (jobCompleted && completedViaEvent) {
      ok('Active job completed before close() resolved');
    } else {
      fail(
        `Job not completed: jobCompleted=${jobCompleted}, ` +
          `completedViaEvent=${completedViaEvent}`
      );
    }
  } catch (e) {
    fail(`Graceful close test threw: ${e}`);
  }

  // ─────────────────────────────────────────────────────────────────
  // Test 2: Close with no active jobs resolves quickly
  // ─────────────────────────────────────────────────────────────────
  console.log('\n2. Testing CLOSE WITH NO ACTIVE JOBS...');
  try {
    const q = makeQueue('tcp-shutdown-idle');
    q.obliterate();
    await Bun.sleep(200);

    const worker = new Worker(
      'tcp-shutdown-idle',
      async () => {
        return { done: true };
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    // Give worker time to start polling
    await Bun.sleep(300);

    const start = Date.now();
    await worker.close();
    const elapsed = Date.now() - start;

    if (elapsed < 500) {
      ok(`Idle worker closed in ${elapsed}ms`);
    } else {
      fail(`Idle close took too long: ${elapsed}ms`);
    }
  } catch (e) {
    fail(`Idle close test threw: ${e}`);
  }

  // ─────────────────────────────────────────────────────────────────
  // Test 3: Close with multiple active jobs waits for all
  // ─────────────────────────────────────────────────────────────────
  console.log('\n3. Testing CLOSE WITH MULTIPLE ACTIVE JOBS...');
  try {
    const q = makeQueue('tcp-shutdown-multi');
    q.obliterate();
    await Bun.sleep(200);

    const completedJobs: string[] = [];

    await q.add('job-a', { idx: 0 });
    await q.add('job-b', { idx: 1 });
    await q.add('job-c', { idx: 2 });

    const worker = new Worker(
      'tcp-shutdown-multi',
      async (job) => {
        await Bun.sleep(500);
        completedJobs.push(job.id);
        return { done: true };
      },
      { concurrency: 3, connection: connOpts, useLocks: false }
    );

    // Wait for all jobs to become active
    for (let i = 0; i < 40; i++) {
      await Bun.sleep(100);
      const counts = await q.getJobCounts();
      if (counts.active >= 3) break;
    }

    // Close while all 3 are processing
    await worker.close();

    if (completedJobs.length === 3) {
      ok(`All 3 active jobs completed before close()`);
    } else {
      fail(`Only ${completedJobs.length}/3 jobs completed`);
    }
  } catch (e) {
    fail(`Multiple active jobs test threw: ${e}`);
  }

  // ─────────────────────────────────────────────────────────────────
  // Test 4: Jobs pushed after close are not processed
  // ─────────────────────────────────────────────────────────────────
  console.log('\n4. Testing JOBS PUSHED AFTER CLOSE NOT PROCESSED...');
  try {
    const q = makeQueue('tcp-shutdown-no-new');
    q.obliterate();
    await Bun.sleep(200);

    const processedByWorker1: string[] = [];
    let firstCompleted = false;

    // Add 5 jobs
    for (let i = 0; i < 5; i++) {
      await q.add(`batch1-${i}`, { batch: 1, idx: i });
    }

    const worker1 = new Worker(
      'tcp-shutdown-no-new',
      async (job) => {
        processedByWorker1.push(job.id);
        await Bun.sleep(100);
        return { done: true };
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    worker1.on('completed', () => {
      firstCompleted = true;
    });

    // Wait for at least one job to complete
    for (let i = 0; i < 50; i++) {
      await Bun.sleep(100);
      if (firstCompleted) break;
    }

    // Close worker1
    await worker1.close();
    const worker1Count = processedByWorker1.length;

    // Add 5 more jobs after worker1 is closed
    for (let i = 0; i < 5; i++) {
      await q.add(`batch2-${i}`, { batch: 2, idx: i });
    }

    // Start a new worker to process remaining
    const processedByWorker2: string[] = [];
    const worker2 = new Worker(
      'tcp-shutdown-no-new',
      async (job) => {
        processedByWorker2.push(job.id);
        return { done: true };
      },
      { concurrency: 5, connection: connOpts, useLocks: false }
    );

    // Wait for all remaining to be processed
    for (let i = 0; i < 80; i++) {
      await Bun.sleep(100);
      const counts = await q.getJobCounts();
      if (counts.waiting === 0 && counts.active === 0) break;
    }

    await worker2.close();

    const total = worker1Count + processedByWorker2.length;
    if (total === 10 && processedByWorker2.length > 0) {
      ok(
        `Worker1 processed ${worker1Count}, Worker2 processed ` +
          `${processedByWorker2.length} (total: ${total})`
      );
    } else {
      fail(
        `Unexpected counts: worker1=${worker1Count}, ` +
          `worker2=${processedByWorker2.length}, total=${total}`
      );
    }
  } catch (e) {
    fail(`Post-close jobs test threw: ${e}`);
  }

  // ─────────────────────────────────────────────────────────────────
  // Test 5: Sequential close and restart
  // ─────────────────────────────────────────────────────────────────
  console.log('\n5. Testing SEQUENTIAL CLOSE AND RESTART...');
  try {
    const q = makeQueue('tcp-shutdown-restart');
    q.obliterate();
    await Bun.sleep(200);

    // Add 10 jobs
    for (let i = 0; i < 10; i++) {
      await q.add(`task-${i}`, { idx: i });
    }

    const processedPhase1: string[] = [];

    const worker1 = new Worker(
      'tcp-shutdown-restart',
      async (job) => {
        processedPhase1.push(job.id);
        await Bun.sleep(50);
        return { done: true };
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    // Let it process a few jobs
    for (let i = 0; i < 40; i++) {
      await Bun.sleep(100);
      if (processedPhase1.length >= 3) break;
    }

    await worker1.close();
    const phase1Count = processedPhase1.length;

    if (phase1Count < 1) {
      fail(`Phase 1 processed no jobs`);
    } else {
      // Start a new worker to pick up remaining
      const processedPhase2: string[] = [];
      const worker2 = new Worker(
        'tcp-shutdown-restart',
        async (job) => {
          processedPhase2.push(job.id);
          return { done: true };
        },
        { concurrency: 5, connection: connOpts, useLocks: false }
      );

      // Wait for remaining jobs
      for (let i = 0; i < 80; i++) {
        await Bun.sleep(100);
        const counts = await q.getJobCounts();
        if (counts.waiting === 0 && counts.active === 0) break;
      }

      await worker2.close();

      const total = phase1Count + processedPhase2.length;
      if (total === 10) {
        ok(
          `Phase1: ${phase1Count} jobs, Phase2: ` +
            `${processedPhase2.length} jobs (total: ${total})`
        );
      } else {
        fail(
          `Total mismatch: phase1=${phase1Count}, ` +
            `phase2=${processedPhase2.length}, total=${total}/10`
        );
      }
    }
  } catch (e) {
    fail(`Sequential restart test threw: ${e}`);
  }

  // ─────────────────────────────────────────────────────────────────
  // Test 6: Close under burst load
  // ─────────────────────────────────────────────────────────────────
  console.log('\n6. Testing CLOSE UNDER BURST LOAD...');
  try {
    const q = makeQueue('tcp-shutdown-burst');
    q.obliterate();
    await Bun.sleep(200);

    const totalJobs = 20;

    // Add 20 jobs
    for (let i = 0; i < totalJobs; i++) {
      await q.add(`burst-${i}`, { idx: i });
    }

    let completedCount = 0;

    const worker = new Worker(
      'tcp-shutdown-burst',
      async () => {
        await Bun.sleep(30);
        completedCount++;
        return { done: true };
      },
      { concurrency: 5, connection: connOpts, useLocks: false }
    );

    // Wait for all jobs to complete
    for (let i = 0; i < 100; i++) {
      await Bun.sleep(100);
      if (completedCount >= totalJobs) break;
    }

    // Graceful close after burst is processed
    await worker.close();

    if (completedCount >= totalJobs) {
      ok(`Burst: ${completedCount}/${totalJobs} completed before close`);
    } else {
      fail(`Expected ${totalJobs}, got ${completedCount}`);
    }
  } catch (e) {
    fail(`Burst load test threw: ${e}`);
  }

  // ─────────────────────────────────────────────────────────────────
  // Cleanup
  // ─────────────────────────────────────────────────────────────────
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
