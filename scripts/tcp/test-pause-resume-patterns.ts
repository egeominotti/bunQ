#!/usr/bin/env bun
/**
 * Pause/Resume Patterns Tests (TCP Mode)
 * Tests for queue pause/resume behavior including processing stops,
 * resume restarts, isPaused state, multiple cycles, concurrent workers,
 * and queued jobs while paused.
 */

import { Queue, Worker } from '../../src/client';

const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');
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
  opts: Record<string, any> = {},
): Worker {
  const w = new Worker(name, processor, {
    connection: connOpts,
    useLocks: false,
    ...opts,
  });
  workers.push(w);
  return w;
}

/** Wait until isPausedAsync returns the expected value */
async function waitForPauseState(q: Queue, expected: boolean, maxMs = 2000): Promise<boolean> {
  const start = Date.now();
  while (Date.now() - start < maxMs) {
    const state = await q.isPausedAsync();
    if (state === expected) return true;
    await Bun.sleep(50);
  }
  return false;
}

async function main() {
  console.log('=== Pause/Resume Patterns Tests (TCP) ===\n');

  // ---------------------------------------------------------------
  // Test 1: Pause stops processing
  // ---------------------------------------------------------------
  console.log('1. Testing PAUSE STOPS PROCESSING...');
  try {
    const q = makeQueue('tcp-pauseres-stops');
    q.obliterate();
    await Bun.sleep(200);

    const processed: number[] = [];

    const w = makeWorker('tcp-pauseres-stops', async (job) => {
      const data = job.data as { index: number };
      processed.push(data.index);
      return { ok: true };
    }, { concurrency: 1 });

    // Push 5 jobs and let the worker process them
    for (let i = 0; i < 5; i++) {
      await q.add(`job-${i}`, { index: i });
    }

    // Wait until all 5 are processed
    for (let i = 0; i < 100; i++) {
      if (processed.length >= 5) break;
      await Bun.sleep(50);
    }

    if (processed.length < 5) {
      fail(`Initial processing failed: only ${processed.length}/5 processed`);
    } else {
      // Pause the queue and confirm it took effect
      q.pause();
      await waitForPauseState(q, true);

      const countAtPause = processed.length;

      // Push more jobs after pause
      for (let i = 10; i < 15; i++) {
        await q.add(`job-${i}`, { index: i });
      }

      // Wait a bit and verify no new jobs are processed
      await Bun.sleep(800);

      if (processed.length === countAtPause) {
        ok(`Pause stopped processing: count stayed at ${countAtPause} after adding 5 more jobs`);
      } else {
        fail(`Jobs processed after pause: expected ${countAtPause}, got ${processed.length}`);
      }
    }

    await w.close();
  } catch (e) {
    fail(`Pause stops processing test failed: ${e}`);
  }

  // ---------------------------------------------------------------
  // Test 2: Resume restarts processing
  // ---------------------------------------------------------------
  console.log('\n2. Testing RESUME RESTARTS PROCESSING...');
  try {
    const q = makeQueue('tcp-pauseres-resume');
    q.obliterate();
    await Bun.sleep(200);

    const processed: number[] = [];

    // Pause the queue BEFORE creating the worker
    q.pause();
    await waitForPauseState(q, true);

    const w = makeWorker('tcp-pauseres-resume', async (job) => {
      const data = job.data as { index: number };
      processed.push(data.index);
      return { ok: true };
    }, { concurrency: 5 });

    // Push 5 jobs while paused
    for (let i = 0; i < 5; i++) {
      await q.add(`job-${i}`, { index: i });
    }

    // Verify nothing is processed while paused
    await Bun.sleep(500);

    if (processed.length > 0) {
      fail(`Jobs processed while paused: ${processed.length}`);
    } else {
      // Resume the queue
      q.resume();

      // Wait for all 5 to be processed
      for (let i = 0; i < 200; i++) {
        if (processed.length >= 5) break;
        await Bun.sleep(50);
      }

      if (processed.length >= 5) {
        const unique = new Set(processed);
        ok(`Resume restarted processing: ${processed.length} jobs, ${unique.size} unique`);
      } else {
        fail(`Not all jobs processed after resume: ${processed.length}/5`);
      }
    }

    await w.close();
  } catch (e) {
    fail(`Resume restarts processing test failed: ${e}`);
  }

  // ---------------------------------------------------------------
  // Test 3: isPaused reflects state
  // ---------------------------------------------------------------
  console.log('\n3. Testing isPaused REFLECTS STATE...');
  try {
    const q = makeQueue('tcp-pauseres-ispaused');
    q.obliterate();
    await Bun.sleep(200);

    // Initially not paused
    const initial = await q.isPausedAsync();

    // Pause
    q.pause();
    await waitForPauseState(q, true);
    const afterPause = await q.isPausedAsync();

    // Resume
    q.resume();
    await waitForPauseState(q, false);
    const afterResume = await q.isPausedAsync();

    if (initial === false && afterPause === true && afterResume === false) {
      ok(`isPaused state correct: initial=${initial}, afterPause=${afterPause}, afterResume=${afterResume}`);
    } else {
      fail(`isPaused state wrong: initial=${initial}, afterPause=${afterPause}, afterResume=${afterResume}`);
    }
  } catch (e) {
    fail(`isPaused reflects state test failed: ${e}`);
  }

  // ---------------------------------------------------------------
  // Test 4: Multiple pause/resume cycles
  // ---------------------------------------------------------------
  console.log('\n4. Testing MULTIPLE PAUSE/RESUME CYCLES...');
  try {
    const q = makeQueue('tcp-pauseres-multi');
    q.obliterate();
    await Bun.sleep(200);

    const processed: number[] = [];

    const w = makeWorker('tcp-pauseres-multi', async (job) => {
      const data = job.data as { index: number };
      processed.push(data.index);
      return { ok: true };
    }, { concurrency: 5 });

    // Cycle 1: pause, push 3 jobs, resume, wait for processing
    q.pause();
    await waitForPauseState(q, true);

    for (let i = 0; i < 3; i++) {
      await q.add(`cycle1-${i}`, { index: i });
    }

    q.resume();
    await waitForPauseState(q, false);

    for (let i = 0; i < 200; i++) {
      if (processed.length >= 3) break;
      await Bun.sleep(50);
    }

    const afterCycle1 = processed.length;

    // Cycle 2: pause again, push 3 more jobs, verify nothing processed, resume
    q.pause();
    await waitForPauseState(q, true);

    for (let i = 3; i < 6; i++) {
      await q.add(`cycle2-${i}`, { index: i });
    }

    // Verify nothing new processed while paused
    await Bun.sleep(500);
    const duringPause = processed.length;

    q.resume();
    await waitForPauseState(q, false);

    for (let i = 0; i < 200; i++) {
      if (processed.length >= 6) break;
      await Bun.sleep(50);
    }

    if (afterCycle1 >= 3 && duringPause === afterCycle1 && processed.length >= 6) {
      const unique = new Set(processed);
      ok(`Multiple cycles: cycle1=${afterCycle1}, paused=${duringPause}, final=${processed.length}, unique=${unique.size}`);
    } else {
      fail(`Multiple cycles failed: cycle1=${afterCycle1}, paused=${duringPause}, final=${processed.length}`);
    }

    await w.close();
  } catch (e) {
    fail(`Multiple pause/resume cycles test failed: ${e}`);
  }

  // ---------------------------------------------------------------
  // Test 5: Pause with concurrent workers
  // ---------------------------------------------------------------
  console.log('\n5. Testing PAUSE WITH CONCURRENT WORKERS...');
  try {
    const q = makeQueue('tcp-pauseres-concurrent');
    q.obliterate();
    await Bun.sleep(200);

    const processed: number[] = [];

    const processor = async (job: any) => {
      const data = job.data as { index: number };
      processed.push(data.index);
      return { ok: true };
    };

    // Create 2 workers on the same queue
    const w1 = makeWorker('tcp-pauseres-concurrent', processor, { concurrency: 2 });
    const w2 = makeWorker('tcp-pauseres-concurrent', processor, { concurrency: 2 });

    // Push initial jobs and let them process
    for (let i = 0; i < 5; i++) {
      await q.add(`job-${i}`, { index: i });
    }

    for (let i = 0; i < 200; i++) {
      if (processed.length >= 5) break;
      await Bun.sleep(50);
    }

    if (processed.length < 5) {
      fail(`Initial processing with 2 workers failed: ${processed.length}/5`);
    } else {
      // Pause the queue and confirm
      q.pause();
      await waitForPauseState(q, true);
      const countAtPause = processed.length;

      // Push more jobs while paused
      for (let i = 10; i < 15; i++) {
        await q.add(`job-${i}`, { index: i });
      }

      // Wait and verify none of the 2 workers process new jobs
      await Bun.sleep(800);

      if (processed.length !== countAtPause) {
        fail(`Workers processed during pause: expected ${countAtPause}, got ${processed.length}`);
      } else {
        // Resume and verify processing resumes
        q.resume();

        for (let i = 0; i < 200; i++) {
          if (processed.length >= countAtPause + 5) break;
          await Bun.sleep(50);
        }

        if (processed.length >= countAtPause + 5) {
          ok(`Concurrent workers respected pause: paused=${countAtPause}, final=${processed.length}`);
        } else {
          fail(`Workers did not resume: expected ${countAtPause + 5}, got ${processed.length}`);
        }
      }
    }

    await w1.close();
    await w2.close();
  } catch (e) {
    fail(`Pause with concurrent workers test failed: ${e}`);
  }

  // ---------------------------------------------------------------
  // Test 6: Jobs added while paused are queued
  // ---------------------------------------------------------------
  console.log('\n6. Testing JOBS ADDED WHILE PAUSED ARE QUEUED...');
  try {
    const q = makeQueue('tcp-pauseres-queued');
    q.obliterate();
    await Bun.sleep(200);

    const processed: number[] = [];

    // Pause the queue BEFORE creating the worker
    q.pause();
    await waitForPauseState(q, true);

    const w = makeWorker('tcp-pauseres-queued', async (job) => {
      const data = job.data as { index: number };
      processed.push(data.index);
      return { ok: true };
    }, { concurrency: 5 });

    // Add 10 jobs
    for (let i = 0; i < 10; i++) {
      await q.add(`job-${i}`, { index: i });
    }

    // Wait for jobs to be persisted
    await Bun.sleep(300);

    // Verify all 10 are queued via async counts. BullMQ semantics (#92): a paused
    // queue reports its ready jobs under `paused`, not `waiting` (no double-count).
    const counts = await q.getJobCountsAsync();

    if (counts.paused < 10 || counts.waiting !== 0) {
      fail(`Jobs not queued properly: waiting=${counts.waiting}, paused=${counts.paused}, expected paused=10/waiting=0`);
    } else {
      // Verify none are processed
      if (processed.length > 0) {
        fail(`Jobs processed while paused: ${processed.length}`);
      } else {
        // Resume
        q.resume();

        // Wait for all 10 to be processed
        for (let i = 0; i < 200; i++) {
          if (processed.length >= 10) break;
          await Bun.sleep(50);
        }

        if (processed.length >= 10) {
          const unique = new Set(processed);
          ok(`Jobs queued while paused: paused=${counts.paused}, processed=${processed.length}, unique=${unique.size}`);
        } else {
          fail(`Not all queued jobs processed: ${processed.length}/10`);
        }
      }
    }

    await w.close();
  } catch (e) {
    fail(`Jobs added while paused test failed: ${e}`);
  }

  // ---------------------------------------------------------------
  // Cleanup
  // ---------------------------------------------------------------
  for (const w of workers) {
    await w.close();
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
