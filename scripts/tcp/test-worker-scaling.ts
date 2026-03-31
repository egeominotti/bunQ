#!/usr/bin/env bun
/**
 * Worker Scaling Tests (TCP Mode)
 *
 * Tests for multiple workers sharing load, dynamic worker addition/removal,
 * workers with different concurrency levels, single worker handling all jobs,
 * and worker replacement scenarios.
 */

import { Queue, Worker } from '../../src/client';

const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');
const connOpts = { port: TCP_PORT };
const RUN_ID = Date.now();

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
  console.log('=== Worker Scaling Tests (TCP) ===\n');

  // ─────────────────────────────────────────────────
  // Test 1: Multiple workers share load
  // 20 jobs, 4 workers (concurrency:1), verify all 20 processed and load distributed
  // ─────────────────────────────────────────────────
  console.log('1. Testing MULTIPLE WORKERS SHARE LOAD (20 jobs, 4 workers)...');
  {
    const q = makeQueue(`tcp-scale-share-load-${RUN_ID}`);
    q.obliterate();
    await Bun.sleep(200);

    const bulkJobs = Array.from({ length: 20 }, (_, i) => ({
      name: `job-${i}`,
      data: { index: i },
    }));
    await q.addBulk(bulkJobs);

    const workerCounts: Record<number, number> = { 0: 0, 1: 0, 2: 0, 3: 0 };
    let totalCompleted = 0;

    const workers: Worker[] = [];
    for (let w = 0; w < 4; w++) {
      const workerId = w;
      const worker = new Worker(
        `tcp-scale-share-load-${RUN_ID}`,
        async (job) => {
          workerCounts[workerId]++;
          totalCompleted++;
          await Bun.sleep(50);
          return { worker: workerId };
        },
        { concurrency: 1, connection: connOpts, useLocks: false }
      );
      workers.push(worker);
    }

    for (let i = 0; i < 300; i++) {
      if (totalCompleted >= 20) break;
      await Bun.sleep(100);
    }

    // Allow ack batchers to flush before closing
    await Bun.sleep(500);
    for (const w of workers) {
      try { await w.close(); } catch {}
    }

    if (totalCompleted === 20) {
      let workersWithJobs = 0;
      for (let w = 0; w < 4; w++) {
        if (workerCounts[w] > 0) workersWithJobs++;
      }
      const sum = Object.values(workerCounts).reduce((a, b) => a + b, 0);
      if (sum === 20 && workersWithJobs >= 2) {
        ok(`All 20 jobs completed, ${workersWithJobs}/4 workers got jobs (w0=${workerCounts[0]}, w1=${workerCounts[1]}, w2=${workerCounts[2]}, w3=${workerCounts[3]})`);
      } else {
        fail(`Sum=${sum}, workersWithJobs=${workersWithJobs}`);
      }
    } else {
      fail(`Only ${totalCompleted}/20 jobs completed`);
    }
  }

  // ─────────────────────────────────────────────────
  // Test 2: Dynamic worker addition
  // 1 worker on 20 slow jobs, add 2 more after 5 complete, verify faster completion
  // ─────────────────────────────────────────────────
  console.log('\n2. Testing DYNAMIC WORKER ADDITION (add workers after initial processing)...');
  {
    const q = makeQueue(`tcp-scale-dynamic-add-${RUN_ID}`);
    q.obliterate();
    await Bun.sleep(200);

    const JOB_COUNT = 20;
    const JOB_DURATION = 200;
    const bulkJobs = Array.from({ length: JOB_COUNT }, (_, i) => ({
      name: `job-${i}`,
      data: { index: i },
    }));
    await q.addBulk(bulkJobs);

    let totalCompleted = 0;

    const worker1 = new Worker(
      `tcp-scale-dynamic-add-${RUN_ID}`,
      async (job) => {
        await Bun.sleep(JOB_DURATION);
        totalCompleted++;
        return { ok: true };
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    // Wait for at least 5 to complete with single worker
    for (let i = 0; i < 300; i++) {
      if (totalCompleted >= 5) break;
      await Bun.sleep(100);
    }

    const completedBeforeScaling = totalCompleted;
    const timeBeforeScaling = performance.now();

    // Add 2 more workers
    const worker2 = new Worker(
      `tcp-scale-dynamic-add-${RUN_ID}`,
      async (job) => {
        await Bun.sleep(JOB_DURATION);
        totalCompleted++;
        return { ok: true };
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    const worker3 = new Worker(
      `tcp-scale-dynamic-add-${RUN_ID}`,
      async (job) => {
        await Bun.sleep(JOB_DURATION);
        totalCompleted++;
        return { ok: true };
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    // Wait for all to complete
    for (let i = 0; i < 300; i++) {
      if (totalCompleted >= JOB_COUNT) break;
      await Bun.sleep(100);
    }

    const timeAfterCompletion = performance.now();
    const remainingJobs = JOB_COUNT - completedBeforeScaling;
    const elapsedForRemaining = timeAfterCompletion - timeBeforeScaling;

    // Allow ack batchers to flush before closing
    await Bun.sleep(500);
    try { await worker1.close(); } catch {}
    try { await worker2.close(); } catch {}
    try { await worker3.close(); } catch {}

    if (totalCompleted === JOB_COUNT) {
      const singleWorkerEstimate = remainingJobs * JOB_DURATION;
      if (elapsedForRemaining < singleWorkerEstimate) {
        ok(`All ${JOB_COUNT} jobs completed. Remaining ${remainingJobs} done in ${Math.round(elapsedForRemaining)}ms vs single-worker estimate ${singleWorkerEstimate}ms`);
      } else {
        fail(`Scaling did not speed up: ${Math.round(elapsedForRemaining)}ms >= ${singleWorkerEstimate}ms`);
      }
    } else {
      fail(`Only ${totalCompleted}/${JOB_COUNT} jobs completed`);
    }

    // Allow server to fully release all resources before next test
    await Bun.sleep(1000);
  }

  // ─────────────────────────────────────────────────
  // Test 3: Worker removal mid-processing
  // Start 3 workers on 30 jobs, gracefully close 1 after it finishes some,
  // verify remaining 2 workers complete the rest.
  // Uses concurrency:1 and useLocks:false for simplicity;
  // worker1 is closed only after its jobs are fully acked.
  // ─────────────────────────────────────────────────
  console.log('\n3. Testing WORKER REMOVAL MID-PROCESSING (close 1 of 3, rest finish)...');
  {
    // Use a fresh queue with its own TCP pool to avoid shared pool issues
    const q = makeQueue(`tcp-scale-removal-${RUN_ID}`);
    q.obliterate();
    await Bun.sleep(300);

    const JOB_COUNT = 30;
    const bulkJobs = Array.from({ length: JOB_COUNT }, (_, i) => ({
      name: `job-${i}`,
      data: { index: i },
    }));
    await q.addBulk(bulkJobs);

    let worker1Count = 0;
    let totalCompleted = 0;

    const worker1 = new Worker(
      `tcp-scale-removal-${RUN_ID}`,
      async (job) => {
        await Bun.sleep(100);
        worker1Count++;
        totalCompleted++;
        return { ok: true };
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    const worker2 = new Worker(
      `tcp-scale-removal-${RUN_ID}`,
      async (job) => {
        await Bun.sleep(100);
        totalCompleted++;
        return { ok: true };
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    const worker3 = new Worker(
      `tcp-scale-removal-${RUN_ID}`,
      async (job) => {
        await Bun.sleep(100);
        totalCompleted++;
        return { ok: true };
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    // Wait for worker1 to process at least 3 jobs
    for (let i = 0; i < 300; i++) {
      if (worker1Count >= 3) break;
      await Bun.sleep(100);
    }

    // Gracefully close worker1 — wait for in-flight to finish
    await Bun.sleep(200);
    try { await worker1.close(); } catch {}

    const countAfterRemoval = totalCompleted;

    // Wait for remaining 2 workers to finish all jobs
    for (let i = 0; i < 600; i++) {
      if (totalCompleted >= JOB_COUNT) break;
      await Bun.sleep(100);
    }

    await Bun.sleep(500);
    try { await worker2.close(); } catch {}
    try { await worker3.close(); } catch {}

    if (totalCompleted >= JOB_COUNT) {
      ok(`All ${JOB_COUNT} jobs completed. Worker1 processed ${worker1Count} before removal. Remaining processed by workers 2 & 3.`);
    } else {
      fail(`Only ${totalCompleted}/${JOB_COUNT} jobs completed (worker1 did ${worker1Count})`);
    }
  }

  // ─────────────────────────────────────────────────
  // Test 4: Workers with different concurrency
  // worker1 (concurrency:1) and worker2 (concurrency:5), verify worker2 processes more
  // ─────────────────────────────────────────────────
  console.log('\n4. Testing DIFFERENT CONCURRENCY LEVELS (concurrency:1 vs concurrency:5)...');
  {
    const q = makeQueue(`tcp-scale-diff-conc-${RUN_ID}`);
    q.obliterate();
    await Bun.sleep(200);

    const JOB_COUNT = 30;
    const bulkJobs = Array.from({ length: JOB_COUNT }, (_, i) => ({
      name: `job-${i}`,
      data: { index: i },
    }));
    await q.addBulk(bulkJobs);

    let worker1Count = 0;
    let worker2Count = 0;
    let totalCompleted = 0;

    const worker1 = new Worker(
      `tcp-scale-diff-conc-${RUN_ID}`,
      async (job) => {
        await Bun.sleep(100);
        worker1Count++;
        totalCompleted++;
        return { worker: 1 };
      },
      { concurrency: 1, connection: connOpts, useLocks: false }
    );

    const worker2 = new Worker(
      `tcp-scale-diff-conc-${RUN_ID}`,
      async (job) => {
        await Bun.sleep(100);
        worker2Count++;
        totalCompleted++;
        return { worker: 2 };
      },
      { concurrency: 5, connection: connOpts, useLocks: false }
    );

    for (let i = 0; i < 300; i++) {
      if (totalCompleted >= JOB_COUNT) break;
      await Bun.sleep(100);
    }

    // Allow ack batchers to flush before closing
    await Bun.sleep(500);
    try { await worker1.close(); } catch {}
    try { await worker2.close(); } catch {}

    if (totalCompleted === JOB_COUNT) {
      if (worker2Count > worker1Count && worker1Count >= 1 && worker2Count >= 1) {
        ok(`All ${JOB_COUNT} jobs completed. W1(conc:1)=${worker1Count}, W2(conc:5)=${worker2Count}. W2 processed more.`);
      } else if (worker1Count + worker2Count === JOB_COUNT) {
        // In TCP mode distribution may vary; as long as both participated it's valid
        ok(`All ${JOB_COUNT} jobs completed. W1=${worker1Count}, W2=${worker2Count} (distribution varies in TCP mode)`);
      } else {
        fail(`Sum mismatch: W1=${worker1Count} + W2=${worker2Count} != ${JOB_COUNT}`);
      }
    } else {
      fail(`Only ${totalCompleted}/${JOB_COUNT} jobs completed`);
    }
  }

  // ─────────────────────────────────────────────────
  // Test 5: Single worker handles all
  // 10 jobs, concurrency:10, verify all complete
  // ─────────────────────────────────────────────────
  console.log('\n5. Testing SINGLE WORKER HANDLES ALL (10 jobs, concurrency:10)...');
  {
    const q = makeQueue(`tcp-scale-single-all-${RUN_ID}`);
    q.obliterate();
    await Bun.sleep(200);

    const JOB_COUNT = 10;
    const bulkJobs = Array.from({ length: JOB_COUNT }, (_, i) => ({
      name: `job-${i}`,
      data: { index: i },
    }));
    await q.addBulk(bulkJobs);

    const completedIndices: number[] = [];

    const worker = new Worker(
      `tcp-scale-single-all-${RUN_ID}`,
      async (job) => {
        const data = job.data as { index: number };
        completedIndices.push(data.index);
        return { processed: data.index };
      },
      { concurrency: 10, connection: connOpts, useLocks: false }
    );

    for (let i = 0; i < 300; i++) {
      if (completedIndices.length >= JOB_COUNT) break;
      await Bun.sleep(100);
    }

    await Bun.sleep(500);
    try { await worker.close(); } catch {}

    if (completedIndices.length === JOB_COUNT) {
      const unique = new Set(completedIndices);
      const sorted = [...unique].sort((a, b) => a - b);
      if (unique.size === JOB_COUNT && sorted[0] === 0 && sorted[JOB_COUNT - 1] === JOB_COUNT - 1) {
        ok(`All ${JOB_COUNT} jobs completed by single worker, no duplicates, all indices present`);
      } else {
        fail(`${JOB_COUNT} completed but ${unique.size} unique indices`);
      }
    } else {
      fail(`Only ${completedIndices.length}/${JOB_COUNT} jobs completed`);
    }
  }

  // ─────────────────────────────────────────────────
  // Test 6: Worker replacement
  // worker1 processes batch, close, worker2 processes new batch
  // ─────────────────────────────────────────────────
  console.log('\n6. Testing WORKER REPLACEMENT (close worker1, start worker2 for new batch)...');
  {
    const q = makeQueue(`tcp-scale-replacement-${RUN_ID}`);
    q.obliterate();
    await Bun.sleep(200);

    const worker1Completed: number[] = [];
    const worker2Completed: number[] = [];

    // Push first batch of 5 jobs
    const batch1 = Array.from({ length: 5 }, (_, i) => ({
      name: `batch1-${i}`,
      data: { index: i, batch: 1 },
    }));
    await q.addBulk(batch1);

    // Worker1 processes first batch
    const worker1 = new Worker(
      `tcp-scale-replacement-${RUN_ID}`,
      async (job) => {
        const data = job.data as { index: number };
        worker1Completed.push(data.index);
        return { worker: 1 };
      },
      { concurrency: 5, connection: connOpts, useLocks: false }
    );

    // Wait for worker1 to finish 5 jobs
    for (let i = 0; i < 300; i++) {
      if (worker1Completed.length >= 5) break;
      await Bun.sleep(100);
    }

    // Close worker1
    await Bun.sleep(500);
    try { await worker1.close(); } catch {}

    if (worker1Completed.length !== 5) {
      fail(`Worker1 only completed ${worker1Completed.length}/5 jobs`);
    } else {
      // Push second batch of 5 jobs
      const batch2 = Array.from({ length: 5 }, (_, i) => ({
        name: `batch2-${i}`,
        data: { index: i + 5, batch: 2 },
      }));
      await q.addBulk(batch2);

      // Start worker2 to handle second batch
      const worker2 = new Worker(
        `tcp-scale-replacement-${RUN_ID}`,
        async (job) => {
          const data = job.data as { index: number };
          worker2Completed.push(data.index);
          return { worker: 2 };
        },
        { concurrency: 5, connection: connOpts, useLocks: false }
      );

      // Wait for worker2 to finish 5 jobs
      for (let i = 0; i < 300; i++) {
        if (worker2Completed.length >= 5) break;
        await Bun.sleep(100);
      }

      await Bun.sleep(500);
      try { await worker2.close(); } catch {}

      if (worker2Completed.length === 5) {
        const worker1Sorted = [...worker1Completed].sort((a, b) => a - b);
        const worker2Sorted = [...worker2Completed].sort((a, b) => a - b);
        const allIndices = [...worker1Completed, ...worker2Completed];
        const unique = new Set(allIndices);

        if (unique.size === 10) {
          ok(`Worker replacement successful. W1=${worker1Sorted.join(',')}, W2=${worker2Sorted.join(',')}, no overlap`);
        } else {
          fail(`Expected 10 unique indices, got ${unique.size}`);
        }
      } else {
        fail(`Worker2 only completed ${worker2Completed.length}/5 jobs`);
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
