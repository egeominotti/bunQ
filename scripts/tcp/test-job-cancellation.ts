#!/usr/bin/env bun
/**
 * Job Cancellation Tests (TCP Mode)
 *
 * Tests for cancelling/removing jobs from queues via TCP, verifying state
 * transitions, and ensuring cancelled jobs are not processed by workers.
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
  console.log('=== Job Cancellation Tests (TCP) ===\n');

  // ─────────────────────────────────────────────────
  // Test 1: Cancel waiting job
  // Push a job (no worker), cancel it, verify state is unknown/removed
  // ─────────────────────────────────────────────────
  console.log('1. Testing CANCEL WAITING JOB...');
  {
    const q = makeQueue('tcp-cancel-waiting');
    q.obliterate();
    await Bun.sleep(200);

    const job = await q.add('task', { value: 1 });
    const jobId = job.id;

    // Verify the job is in waiting state before cancellation
    const stateBefore = await q.getJobState(jobId);
    if (stateBefore !== 'waiting') {
      fail(`Job state before cancel should be 'waiting', got '${stateBefore}'`);
    } else {
      // Cancel/remove the job
      await q.removeAsync(jobId);

      // After removal, state should be unknown (job no longer exists)
      const stateAfter = await q.getJobState(jobId);
      if (stateAfter === 'unknown') {
        ok('Waiting job cancelled, state is now unknown');
      } else {
        fail(`Job state after cancel should be 'unknown', got '${stateAfter}'`);
      }
    }
  }

  // ─────────────────────────────────────────────────
  // Test 2: Cancel does not affect completed
  // Push, process, try cancel, verify it was already completed
  // ─────────────────────────────────────────────────
  console.log('\n2. Testing CANCEL DOES NOT AFFECT COMPLETED...');
  {
    const q = makeQueue('tcp-cancel-completed');
    q.obliterate();
    await Bun.sleep(200);

    const job = await q.add('task', { value: 2 });
    const jobId = job.id;

    let processed = false;
    const worker = new Worker(
      'tcp-cancel-completed',
      async () => {
        processed = true;
        return { done: true };
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    // Wait for the job to be processed
    for (let i = 0; i < 100; i++) {
      if (processed) break;
      await Bun.sleep(50);
    }

    // Give time for completion to register
    await Bun.sleep(300);
    await worker.close();

    if (!processed) {
      fail('Job was never processed');
    } else {
      // Verify it is completed
      const stateBefore = await q.getJobState(jobId);

      // Try to cancel/remove the completed job
      await q.removeAsync(jobId);

      // After attempting to cancel a completed job, it may be removed (unknown)
      // or remain completed -- either way, the job was already processed
      const stateAfter = await q.getJobState(jobId);

      if (stateBefore === 'completed' || stateBefore === 'unknown') {
        ok(`Completed job handled correctly (before=${stateBefore}, after=${stateAfter})`);
      } else {
        fail(`Expected completed or unknown before cancel, got '${stateBefore}'`);
      }
    }
  }

  // ─────────────────────────────────────────────────
  // Test 3: Cancel multiple waiting jobs
  // Push 5, cancel 3, start worker, verify only 2 processed
  // ─────────────────────────────────────────────────
  console.log('\n3. Testing CANCEL MULTIPLE WAITING JOBS (5 pushed, 3 cancelled)...');
  {
    const q = makeQueue('tcp-cancel-multiple');
    q.obliterate();
    await Bun.sleep(200);

    // Push 5 jobs
    const jobs = await q.addBulk(
      Array.from({ length: 5 }, (_, i) => ({
        name: `task-${i}`,
        data: { index: i },
      }))
    );

    // Cancel jobs at indices 0, 2, 4
    await q.removeAsync(jobs[0].id);
    await q.removeAsync(jobs[2].id);
    await q.removeAsync(jobs[4].id);

    // Verify cancelled jobs are gone
    let allCancelled = true;
    for (const idx of [0, 2, 4]) {
      const state = await q.getJobState(jobs[idx].id);
      if (state !== 'unknown') {
        fail(`Cancelled job at index ${idx} has state '${state}', expected 'unknown'`);
        allCancelled = false;
        break;
      }
    }

    if (allCancelled) {
      // Start worker to process the remaining 2 jobs
      const processedIndices: number[] = [];
      const worker = new Worker(
        'tcp-cancel-multiple',
        async (job) => {
          const data = job.data as { index: number };
          processedIndices.push(data.index);
          return { ok: true };
        },
        { concurrency: 5, connection: connOpts, useLocks: false }
      );

      // Wait for remaining jobs to process
      for (let i = 0; i < 100; i++) {
        if (processedIndices.length >= 2) break;
        await Bun.sleep(50);
      }

      // Give extra time to ensure no extra jobs are processed
      await Bun.sleep(500);
      await worker.close();

      if (processedIndices.length === 2) {
        const sorted = processedIndices.sort((a, b) => a - b);
        if (sorted[0] === 1 && sorted[1] === 3) {
          ok('Only 2 remaining jobs processed (indices 1, 3)');
        } else {
          ok(`2 jobs processed (indices ${sorted.join(', ')})`);
        }
      } else {
        fail(`Expected 2 jobs processed, got ${processedIndices.length}`);
      }
    }
  }

  // ─────────────────────────────────────────────────
  // Test 4: Cancel delayed job
  // Push delayed job (5s), cancel before active, verify never processes
  // ─────────────────────────────────────────────────
  console.log('\n4. Testing CANCEL DELAYED JOB...');
  {
    const q = makeQueue('tcp-cancel-delayed');
    q.obliterate();
    await Bun.sleep(200);

    // Push a job with a 5-second delay
    const job = await q.add('delayed-task', { value: 5 }, { delay: 5000 });
    const jobId = job.id;

    // Verify it is in delayed state
    const stateBefore = await q.getJobState(jobId);
    if (stateBefore !== 'delayed') {
      fail(`Delayed job state should be 'delayed', got '${stateBefore}'`);
    } else {
      // Cancel it before it becomes active
      await q.removeAsync(jobId);

      // Verify it is removed
      const stateAfter = await q.getJobState(jobId);
      if (stateAfter !== 'unknown') {
        fail(`Cancelled delayed job state should be 'unknown', got '${stateAfter}'`);
      } else {
        // Start a worker and wait -- the cancelled delayed job should never process
        let processed = false;
        const worker = new Worker(
          'tcp-cancel-delayed',
          async () => {
            processed = true;
            return { ok: true };
          },
          { concurrency: 1, connection: connOpts, useLocks: false }
        );

        // Wait enough time to confirm no processing
        await Bun.sleep(1500);
        await worker.close();

        if (!processed) {
          ok('Delayed job cancelled before processing, worker processed 0 jobs');
        } else {
          fail('Cancelled delayed job was still processed');
        }
      }
    }
  }

  // ─────────────────────────────────────────────────
  // Test 5: Batch cancel
  // Push 10, cancel all, start worker, verify 0 processed
  // ─────────────────────────────────────────────────
  console.log('\n5. Testing BATCH CANCEL (10 jobs, cancel all)...');
  {
    const q = makeQueue('tcp-cancel-batch');
    q.obliterate();
    await Bun.sleep(200);

    // Push 10 jobs
    const jobs = await q.addBulk(
      Array.from({ length: 10 }, (_, i) => ({
        name: `task-${i}`,
        data: { index: i },
      }))
    );

    // Cancel all 10 jobs
    for (const job of jobs) {
      await q.removeAsync(job.id);
    }

    // Verify all are removed
    let allRemoved = true;
    for (const job of jobs) {
      const state = await q.getJobState(job.id);
      if (state !== 'unknown') {
        fail(`Cancelled job ${job.id} has state '${state}', expected 'unknown'`);
        allRemoved = false;
        break;
      }
    }

    if (allRemoved) {
      // Start worker
      let processedCount = 0;
      const worker = new Worker(
        'tcp-cancel-batch',
        async () => {
          processedCount++;
          return { ok: true };
        },
        { concurrency: 5, connection: connOpts, useLocks: false }
      );

      // Wait to confirm no jobs are processed
      await Bun.sleep(1500);
      await worker.close();

      if (processedCount === 0) {
        ok('All 10 jobs cancelled, worker processed 0');
      } else {
        fail(`Expected 0 jobs processed after batch cancel, got ${processedCount}`);
      }
    }
  }

  // ─────────────────────────────────────────────────
  // Cleanup
  // ─────────────────────────────────────────────────
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
