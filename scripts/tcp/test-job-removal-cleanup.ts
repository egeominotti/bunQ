#!/usr/bin/env bun
/**
 * Job Removal & Cleanup Tests (TCP Mode)
 * Tests for removeOnComplete, removeOnFail, job persistence,
 * obliterate, and clean operations over TCP connections.
 */

import { Queue, Worker } from '../../src/client';

const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789', 10);
const connOpts = { port: TCP_PORT };

let passed = 0;
let failed = 0;
const queues: Queue[] = [];
const workers: Worker[] = [];

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

function makeWorker(
  name: string,
  processor: (job: any) => Promise<any>,
  opts: Record<string, any> = {}
): Worker {
  const w = new Worker(name, processor, {
    connection: connOpts,
    useLocks: false,
    ...opts,
  });
  workers.push(w);
  return w;
}

async function main() {
  console.log('=== Job Removal & Cleanup Tests (TCP) ===\n');

  // ---------------------------------------------------------------
  // Test 1: removeOnComplete removes the completed job over TCP.
  // ---------------------------------------------------------------
  console.log('1. Testing REMOVE ON COMPLETE (job completes with result)...');
  try {
    const q = makeQueue('tcp-cleanup-remove-complete');
    q.obliterate();
    await Bun.sleep(200);

    const job = await q.add('task', { value: 1 }, { removeOnComplete: true, durable: true });
    const jobId = String(job.id);

    let completed = false;

    const w = makeWorker(
      'tcp-cleanup-remove-complete',
      async () => {
        completed = true;
        return { done: true };
      },
      { concurrency: 1 }
    );

    // Wait for job to complete
    for (let i = 0; i < 100; i++) {
      if (completed) break;
      await Bun.sleep(50);
    }

    await Bun.sleep(500);
    await w.close();

    if (!completed) {
      fail('Job was not processed');
    } else {
      // The terminal event is observable, but retention removes the job row.
      const fetched = await q.getJob(jobId);
      if (fetched === null) ok('removeOnComplete removed the completed job');
      else fail(`removeOnComplete kept completed job ${fetched.id}`);
    }
  } catch (e) {
    fail(`removeOnComplete test failed: ${e}`);
  }

  // ---------------------------------------------------------------
  // Test 2: removeOnFail removes the failed job without creating a DLQ entry.
  // ---------------------------------------------------------------
  console.log('\n2. Testing JOB FAILURE TRANSITIONS (max attempts exceeded)...');
  try {
    const q = makeQueue('tcp-cleanup-remove-fail');
    q.obliterate();
    await Bun.sleep(200);

    const job = await q.add(
      'fail-task',
      { value: 1 },
      { attempts: 1, removeOnFail: true, durable: true }
    );
    const jobId = String(job.id);

    let jobFailed = false;

    const w = makeWorker(
      'tcp-cleanup-remove-fail',
      async () => {
        throw new Error('Deliberate failure');
      },
      { concurrency: 1 }
    );

    w.on('failed', () => {
      jobFailed = true;
    });

    // Wait for job to fail
    for (let i = 0; i < 100; i++) {
      if (jobFailed) break;
      await Bun.sleep(50);
    }

    await Bun.sleep(500);
    await w.close();

    if (!jobFailed) {
      fail('Job did not fail as expected');
    } else {
      const fetched = await q.getJob(jobId);
      const dlq = await q.getDlqAsync();
      const retainedInDlq = dlq.some((entry) => entry.job.id === jobId);
      if (fetched === null && !retainedInDlq) {
        ok('removeOnFail removed the failed job and bypassed the DLQ');
      } else {
        fail(`removeOnFail retained job=${fetched !== null}, dlq=${retainedInDlq}`);
      }
    }
  } catch (e) {
    fail(`Job failure test failed: ${e}`);
  }

  // ---------------------------------------------------------------
  // Test 3: Job persists without removeOnComplete
  // ---------------------------------------------------------------
  console.log('\n3. Testing JOB PERSISTS WITHOUT REMOVE ON COMPLETE...');
  try {
    const q = makeQueue('tcp-cleanup-persist');
    q.obliterate();
    await Bun.sleep(200);

    const job = await q.add('task', { value: 42 });
    const jobId = String(job.id);

    let completed = false;

    const w = makeWorker(
      'tcp-cleanup-persist',
      async () => {
        completed = true;
        return { result: 'ok' };
      },
      { concurrency: 1 }
    );

    // Wait for job to complete
    for (let i = 0; i < 100; i++) {
      if (completed) break;
      await Bun.sleep(50);
    }

    // Give time for completion to settle
    await Bun.sleep(500);
    await w.close();

    if (!completed) {
      fail('Job was not processed');
    } else {
      const fetched = await q.getJob(jobId);
      if (fetched !== null) {
        ok(`Job persists after completion without removeOnComplete (id: ${fetched.id})`);
      } else {
        fail('Job was removed even without removeOnComplete');
      }
    }
  } catch (e) {
    fail(`Job persistence test failed: ${e}`);
  }

  // ---------------------------------------------------------------
  // Test 4: Obliterate removes waiting/active jobs
  // Note: In TCP mode, completed count may include jobs from the global
  // results cache. We verify obliterate clears waiting + active.
  // ---------------------------------------------------------------
  console.log('\n4. Testing OBLITERATE REMOVES WAITING/ACTIVE JOBS...');
  try {
    const q = makeQueue('tcp-cleanup-obliterate');
    q.obliterate();
    await Bun.sleep(200);

    // Push 10 jobs
    const jobs = Array.from({ length: 10 }, (_, i) => ({
      name: `task-${i}`,
      data: { index: i },
    }));
    await q.addBulk(jobs);

    // Verify jobs exist
    const countsBefore = await q.getJobCountsAsync();
    if (countsBefore.waiting < 1) {
      fail(`Jobs were not created (waiting: ${countsBefore.waiting})`);
    } else {
      // Obliterate the queue
      q.obliterate();
      await Bun.sleep(500);

      const countsAfter = await q.getJobCountsAsync();
      if (countsAfter.waiting === 0 && countsAfter.active === 0) {
        ok(
          `Obliterate removed waiting/active jobs (before: ${countsBefore.waiting} waiting, after: w=${countsAfter.waiting} a=${countsAfter.active})`
        );
      } else {
        fail(
          `Obliterate did not remove jobs (remaining: waiting=${countsAfter.waiting}, active=${countsAfter.active})`
        );
      }
    }
  } catch (e) {
    fail(`Obliterate test failed: ${e}`);
  }

  // ---------------------------------------------------------------
  // Test 5: Clean queue
  // ---------------------------------------------------------------
  console.log('\n5. Testing CLEAN QUEUE...');
  try {
    const q = makeQueue('tcp-cleanup-clean');
    q.obliterate();
    await Bun.sleep(200);

    // Push 10 jobs but do NOT process them
    const jobs = Array.from({ length: 10 }, (_, i) => ({
      name: `task-${i}`,
      data: { index: i },
    }));
    await q.addBulk(jobs);

    // Verify waiting jobs exist
    const countsBefore = await q.getJobCountsAsync();
    if (countsBefore.waiting < 1) {
      fail(`Jobs were not created (waiting: ${countsBefore.waiting})`);
    } else {
      // Clean all jobs with 0 grace period
      const cleaned = await q.cleanAsync(0, 100);
      await Bun.sleep(500);

      const countsAfter = await q.getJobCountsAsync();
      if (countsAfter.waiting === 0) {
        ok(
          `Clean removed waiting jobs (before: ${countsBefore.waiting}, cleaned: ${cleaned.length})`
        );
      } else {
        fail(`Clean did not remove all waiting jobs (remaining: ${countsAfter.waiting})`);
      }
    }
  } catch (e) {
    fail(`Clean queue test failed: ${e}`);
  }

  // ---------------------------------------------------------------
  // Cleanup
  // ---------------------------------------------------------------
  for (const w of workers) {
    try {
      await w.close();
    } catch {
      // ignore
    }
  }

  for (const q of queues) {
    q.obliterate();
    q.close();
  }

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch((e) => {
  console.error(`Fatal error: ${e}`);
  process.exit(1);
});
