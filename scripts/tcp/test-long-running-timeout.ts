#!/usr/bin/env bun
/**
 * Long-running Jobs & Timeouts (TCP Mode)
 *
 * Tests for concurrent long-running jobs, progress tracking during long jobs,
 * heartbeat keep-alive, and job timeout.
 *
 * Note: Stall detection config (setStallConfig) is embedded-only.
 * Timeout-with-retry is complex in TCP (worker dedup). Those are skipped here.
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
  console.log('=== Long-running Jobs & Timeouts Tests (TCP) ===\n');

  // ─────────────────────────────────────────────────
  // Test 1: Multiple long-running concurrent
  // Push 5 jobs each taking 1s with concurrency:5,
  // verify all complete within ~3s (parallel)
  // ─────────────────────────────────────────────────
  console.log('1. Testing MULTIPLE LONG-RUNNING CONCURRENT (5 jobs x 1s, concurrency:5)...');
  {
    const q = makeQueue('tcp-longrun-concurrent');
    q.obliterate();
    await Bun.sleep(200);

    const completedIndices: number[] = [];

    const worker = new Worker(
      'tcp-longrun-concurrent',
      async (job) => {
        const data = job.data as { index: number };
        // Each job takes ~1s
        await Bun.sleep(1000);
        completedIndices.push(data.index);
        return { index: data.index };
      },
      { concurrency: 5, connection: connOpts, useLocks: false }
    );

    const startTime = Date.now();

    // Push 5 jobs
    for (let i = 0; i < 5; i++) {
      await q.add(`job-${i}`, { index: i });
    }

    // Wait for all 5 to complete
    for (let i = 0; i < 100; i++) {
      if (completedIndices.length >= 5) break;
      await Bun.sleep(100);
    }

    const elapsed = Date.now() - startTime;

    await worker.close();

    if (completedIndices.length !== 5) {
      fail(`Only ${completedIndices.length}/5 jobs completed`);
    } else {
      const sorted = [...completedIndices].sort((a, b) => a - b);
      const allPresent = sorted.length === 5 && sorted[0] === 0 && sorted[4] === 4;
      if (!allPresent) {
        fail(`Not all indices present: ${sorted.join(',')}`);
      } else if (elapsed >= 3000) {
        fail(`Took ${elapsed}ms (expected < 3000ms for parallel execution)`);
      } else {
        ok(`All 5 jobs completed in ${elapsed}ms (parallel, < 3s)`);
      }
    }
  }

  // ─────────────────────────────────────────────────
  // Test 2: Progress during long job
  // Push a job that runs 1s and updates progress at
  // 25/50/75/100, verify progress events fire
  // ─────────────────────────────────────────────────
  console.log('\n2. Testing PROGRESS DURING LONG JOB (1s job, 4 progress updates)...');
  {
    const q = makeQueue('tcp-longrun-progress');
    q.obliterate();
    await Bun.sleep(200);

    const progressValues: number[] = [];
    let completed = false;

    const worker = new Worker(
      'tcp-longrun-progress',
      async (job) => {
        // Simulate a 1s job with progress updates at 25%, 50%, 75%, 100%
        await Bun.sleep(250);
        await job.updateProgress(25);
        await Bun.sleep(250);
        await job.updateProgress(50);
        await Bun.sleep(250);
        await job.updateProgress(75);
        await Bun.sleep(250);
        await job.updateProgress(100);
        return { done: true };
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    worker.on('progress', (_job, progress) => {
      progressValues.push(progress as number);
    });

    worker.on('completed', () => {
      completed = true;
    });

    await q.add('progress-job', { value: 1 });

    // Wait for completion
    for (let i = 0; i < 100; i++) {
      if (completed) break;
      await Bun.sleep(100);
    }

    await worker.close();

    if (!completed) {
      fail('Job did not complete');
    } else if (
      progressValues.length === 4 &&
      progressValues[0] === 25 &&
      progressValues[1] === 50 &&
      progressValues[2] === 75 &&
      progressValues[3] === 100
    ) {
      ok(`Progress events fired correctly: [${progressValues.join(', ')}]`);
    } else {
      fail(`Expected progress [25,50,75,100], got [${progressValues.join(', ')}]`);
    }
  }

  // ─────────────────────────────────────────────────
  // Test 3: Heartbeat keeps job alive
  // Push a long-running job (2s) with heartbeatInterval:500,
  // verify job completes
  // ─────────────────────────────────────────────────
  console.log('\n3. Testing HEARTBEAT KEEPS JOB ALIVE (2s job, heartbeat:500ms)...');
  {
    const q = makeQueue('tcp-longrun-heartbeat');
    q.obliterate();
    await Bun.sleep(200);

    let completed = false;
    let jobFailed = false;

    const worker = new Worker(
      'tcp-longrun-heartbeat',
      async () => {
        // Long-running job (2s) -- heartbeat at 500ms keeps it alive
        await Bun.sleep(2000);
        return { done: true };
      },
      {
        concurrency: 1,
        heartbeatInterval: 500,
        connection: connOpts,
        useLocks: false,
      }
    );

    worker.on('completed', () => {
      completed = true;
    });
    worker.on('failed', () => {
      jobFailed = true;
    });

    await q.add('long-heartbeat-job', { value: 1 });

    // Wait for the job to complete (should take ~2s + overhead)
    for (let i = 0; i < 100; i++) {
      if (completed || jobFailed) break;
      await Bun.sleep(100);
    }

    await worker.close();

    if (completed && !jobFailed) {
      ok('Long-running job with heartbeat completed successfully');
    } else if (jobFailed) {
      fail('Job failed despite heartbeat');
    } else {
      fail('Job did not complete within timeout');
    }
  }

  // ─────────────────────────────────────────────────
  // Test 4: Job timeout
  // Push a job with timeout:500 that tries to sleep 30s,
  // verify it fails. The server-side timeout check runs
  // every ~5s, so wait up to 15s for the timeout to trigger.
  // ─────────────────────────────────────────────────
  console.log('\n4. Testing JOB TIMEOUT (timeout:500, job sleeps 30s)...');
  {
    const q = makeQueue('tcp-longrun-timeout');
    q.obliterate();
    await Bun.sleep(200);

    let capturedJobId: string | null = null;
    let jobTimedOut = false;

    const worker = new Worker(
      'tcp-longrun-timeout',
      async (job) => {
        capturedJobId = job.id;
        // Sleep long enough for the server-side timeout check to fire
        await Bun.sleep(30000);
        return { done: true };
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    // The server-side timeout handler removes the job from processing.
    // When the processor finishes, the ACK fails which emits an error event.
    worker.on('error', () => {
      jobTimedOut = true;
    });

    await q.add('slow-timeout-job', { value: 1 }, { timeout: 500, attempts: 1 });

    // Wait for the timeout check to fire (runs every ~5s).
    // Check job state -- once timeout handler fires, job is no longer 'active'.
    for (let i = 0; i < 150; i++) {
      if (capturedJobId) {
        try {
          const state = await q.getJobState(capturedJobId);
          if (state !== 'active' && state !== 'waiting') {
            jobTimedOut = true;
          }
        } catch {
          // ignore query errors
        }
      }
      if (jobTimedOut) break;
      await Bun.sleep(100);
    }

    await worker.close(true);

    if (jobTimedOut) {
      ok('Job with timeout:500 was timed out by server');
    } else {
      fail('Job was not timed out within 15s');
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
