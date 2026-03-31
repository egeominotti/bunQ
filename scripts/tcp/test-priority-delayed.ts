#!/usr/bin/env bun
/**
 * Priority & Delayed Jobs Tests (TCP Mode)
 *
 * Tests for priority ordering, high-priority preemption, delayed execution,
 * promote, changePriority, and priority with concurrency over TCP connections.
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
  console.log('=== Priority & Delayed Jobs Tests (TCP) ===\n');

  // ---------------------------------------------------------------
  // Test 1: Priority ordering
  // Push 5 jobs with priorities 1,5,3,2,4, verify processed in
  // descending priority order
  // ---------------------------------------------------------------
  console.log('1. Testing PRIORITY ORDERING...');
  try {
    const q = makeQueue('tcp-prio-ordering');
    q.obliterate();
    await Bun.sleep(200);

    const priorities = [1, 5, 3, 2, 4];
    for (const p of priorities) {
      await q.add(`job-p${p}`, { priority: p }, { priority: p });
    }

    const processedOrder: number[] = [];

    const w = new Worker(
      'tcp-prio-ordering',
      async (job) => {
        const data = job.data as { priority: number };
        processedOrder.push(data.priority);
        return { ok: true };
      },
      { connection: connOpts, useLocks: false, concurrency: 1 },
    );

    for (let i = 0; i < 100; i++) {
      if (processedOrder.length >= 5) break;
      await Bun.sleep(50);
    }

    await w.close();

    if (
      processedOrder.length === 5 &&
      processedOrder[0] === 5 &&
      processedOrder[1] === 4 &&
      processedOrder[2] === 3 &&
      processedOrder[3] === 2 &&
      processedOrder[4] === 1
    ) {
      ok(`Priority ordering correct: [${processedOrder.join(',')}]`);
    } else {
      fail(`Priority ordering wrong: [${processedOrder.join(',')}], expected [5,4,3,2,1]`);
    }
  } catch (e) {
    fail(`Priority ordering test threw: ${e}`);
  }

  // ---------------------------------------------------------------
  // Test 2: High priority preemption
  // Push low-priority jobs, add high-priority while busy, verify
  // high-priority runs before remaining low ones
  // ---------------------------------------------------------------
  console.log('\n2. Testing HIGH PRIORITY PREEMPTION...');
  try {
    const q = makeQueue('tcp-prio-preempt');
    q.obliterate();
    await Bun.sleep(200);

    const processedOrder: string[] = [];
    let processingStarted = false;

    // Push 10 low-priority jobs
    for (let i = 0; i < 10; i++) {
      await q.add(`low-${i}`, { label: `low-${i}` }, { priority: 1 });
    }

    // Start a worker with concurrency 1, each job takes some time
    const w = new Worker(
      'tcp-prio-preempt',
      async (job) => {
        const data = job.data as { label: string };
        processingStarted = true;
        processedOrder.push(data.label);
        await Bun.sleep(100);
        return { ok: true };
      },
      { connection: connOpts, useLocks: false, concurrency: 1 },
    );

    // Wait for worker to start processing the first job
    for (let i = 0; i < 50; i++) {
      if (processingStarted) break;
      await Bun.sleep(20);
    }

    // Now add 1 high-priority job while worker is busy
    await q.add('high-1', { label: 'HIGH' }, { priority: 100 });

    // Wait for all 11 jobs to complete
    for (let i = 0; i < 200; i++) {
      if (processedOrder.length >= 11) break;
      await Bun.sleep(50);
    }

    await w.close();

    if (processedOrder.length === 11) {
      const highIndex = processedOrder.indexOf('HIGH');
      // HIGH should appear within the first few positions (not last)
      if (highIndex >= 0 && highIndex < 5) {
        ok(`High priority preempted: HIGH at position ${highIndex}, order: [${processedOrder.join(',')}]`);
      } else {
        fail(`HIGH at position ${highIndex}, expected < 5. Order: [${processedOrder.join(',')}]`);
      }
    } else {
      fail(`Expected 11 jobs, got ${processedOrder.length}: [${processedOrder.join(',')}]`);
    }
  } catch (e) {
    fail(`High priority preemption test threw: ${e}`);
  }

  // ---------------------------------------------------------------
  // Test 3: Delayed job execution
  // Push a job with delay:500ms, verify it's not processed
  // immediately but is processed after delay
  // ---------------------------------------------------------------
  console.log('\n3. Testing DELAYED JOB EXECUTION...');
  try {
    const q = makeQueue('tcp-prio-delayed-exec');
    q.obliterate();
    await Bun.sleep(200);

    let processedAt = 0;
    const addedAt = Date.now();

    // Push a job with 500ms delay
    await q.add('delayed-job', { value: 42 }, { delay: 500 });

    const w = new Worker(
      'tcp-prio-delayed-exec',
      async (job) => {
        processedAt = Date.now();
        return { ok: true };
      },
      { connection: connOpts, useLocks: false, concurrency: 1 },
    );

    // Check that it is NOT processed immediately (within 200ms)
    await Bun.sleep(200);
    const notProcessedYet = processedAt === 0;

    // Wait for the delay to pass and the job to be processed
    for (let i = 0; i < 100; i++) {
      if (processedAt > 0) break;
      await Bun.sleep(50);
    }

    await w.close();

    const elapsed = processedAt - addedAt;

    if (notProcessedYet && processedAt > 0 && elapsed >= 400) {
      ok(`Delayed execution correct: not processed at 200ms, processed after ${elapsed}ms`);
    } else if (!notProcessedYet) {
      fail(`Job was processed too early (within 200ms)`);
    } else if (processedAt === 0) {
      fail(`Delayed job was never processed`);
    } else {
      fail(`Delay too short: elapsed=${elapsed}ms, expected >= 400ms`);
    }
  } catch (e) {
    fail(`Delayed job execution test threw: ${e}`);
  }

  // ---------------------------------------------------------------
  // Test 4: Promote delayed job
  // Push delayed job (delay:5000ms), promote it, verify it processes
  // quickly
  // ---------------------------------------------------------------
  console.log('\n4. Testing PROMOTE DELAYED JOB...');
  try {
    const q = makeQueue('tcp-prio-promote');
    q.obliterate();
    await Bun.sleep(200);

    let processed = false;

    // Push a job with very long delay (5000ms)
    const job = await q.add('long-delayed', { value: 99 }, { delay: 5000 });

    // Verify it is in delayed state
    const stateBefore = await q.getJobState(job.id);

    // Promote it
    await q.promoteJob(job.id);

    // Verify state changed to waiting
    const stateAfter = await q.getJobState(job.id);

    // Start worker and verify it processes quickly
    const startTime = Date.now();
    const w = new Worker(
      'tcp-prio-promote',
      async () => {
        processed = true;
        return { ok: true };
      },
      { connection: connOpts, useLocks: false, concurrency: 1 },
    );

    // Should be processed quickly (well before the original 5s delay)
    for (let i = 0; i < 100; i++) {
      if (processed) break;
      await Bun.sleep(50);
    }

    await w.close();

    const elapsed = Date.now() - startTime;

    if (stateBefore === 'delayed' && stateAfter === 'waiting' && processed && elapsed < 3000) {
      ok(`Promoted: state ${stateBefore}->${stateAfter}, processed in ${elapsed}ms`);
    } else if (!processed) {
      fail(`Promoted job was never processed`);
    } else if (stateBefore !== 'delayed') {
      fail(`Expected initial state 'delayed', got '${stateBefore}'`);
    } else if (stateAfter !== 'waiting') {
      fail(`Expected promoted state 'waiting', got '${stateAfter}'`);
    } else {
      fail(`Promote took too long: ${elapsed}ms`);
    }
  } catch (e) {
    fail(`Promote delayed job test threw: ${e}`);
  }

  // ---------------------------------------------------------------
  // Test 5: Change job priority
  // Push low priority, change to high before processing, verify it
  // runs before other low-priority
  // ---------------------------------------------------------------
  console.log('\n5. Testing CHANGE JOB PRIORITY...');
  try {
    const q = makeQueue('tcp-prio-change');
    q.obliterate();
    await Bun.sleep(200);

    // Push 3 low-priority jobs
    const job1 = await q.add('first', { label: 'A' }, { priority: 1 });
    await q.add('second', { label: 'B' }, { priority: 1 });
    await q.add('third', { label: 'C' }, { priority: 1 });

    // Change job1's priority to very high
    await q.changeJobPriority(job1.id, { priority: 100 });

    const processedOrder: string[] = [];

    const w = new Worker(
      'tcp-prio-change',
      async (job) => {
        const data = job.data as { label: string };
        processedOrder.push(data.label);
        return { ok: true };
      },
      { connection: connOpts, useLocks: false, concurrency: 1 },
    );

    // Wait for all 3 to complete
    for (let i = 0; i < 100; i++) {
      if (processedOrder.length >= 3) break;
      await Bun.sleep(50);
    }

    await w.close();

    if (processedOrder.length === 3 && processedOrder[0] === 'A') {
      ok(`Priority changed: A (now priority 100) processed first. Order: [${processedOrder.join(',')}]`);
    } else if (processedOrder.length === 3) {
      fail(`Expected A first, got order: [${processedOrder.join(',')}]`);
    } else {
      fail(`Expected 3 jobs, got ${processedOrder.length}`);
    }
  } catch (e) {
    fail(`Change job priority test threw: ${e}`);
  }

  // ---------------------------------------------------------------
  // Test 6: Priority with concurrency
  // Push 20 jobs with varied priorities and concurrency:3, verify
  // higher priority jobs generally complete first
  // ---------------------------------------------------------------
  console.log('\n6. Testing PRIORITY WITH CONCURRENCY...');
  try {
    const q = makeQueue('tcp-prio-concurrent');
    q.obliterate();
    await Bun.sleep(200);

    // Push 20 jobs: 5 high (100), 5 medium (50), 10 low (1)
    const jobs: Array<{ name: string; data: { priority: number; index: number }; opts: { priority: number } }> = [];

    for (let i = 0; i < 5; i++) {
      jobs.push({
        name: `high-${i}`,
        data: { priority: 100, index: i },
        opts: { priority: 100 },
      });
    }
    for (let i = 0; i < 5; i++) {
      jobs.push({
        name: `med-${i}`,
        data: { priority: 50, index: i + 5 },
        opts: { priority: 50 },
      });
    }
    for (let i = 0; i < 10; i++) {
      jobs.push({
        name: `low-${i}`,
        data: { priority: 1, index: i + 10 },
        opts: { priority: 1 },
      });
    }

    await q.addBulk(jobs);

    const completionOrder: Array<{ priority: number; index: number }> = [];

    const w = new Worker(
      'tcp-prio-concurrent',
      async (job) => {
        const data = job.data as { priority: number; index: number };
        completionOrder.push({ priority: data.priority, index: data.index });
        await Bun.sleep(30); // Small delay to simulate work
        return { ok: true };
      },
      { connection: connOpts, useLocks: false, concurrency: 3 },
    );

    // Wait for all 20 to complete
    for (let i = 0; i < 200; i++) {
      if (completionOrder.length >= 20) break;
      await Bun.sleep(50);
    }

    await w.close();

    if (completionOrder.length === 20) {
      // Check that the average position of high-priority jobs is earlier
      // than low-priority jobs
      const highPositions = completionOrder
        .map((item, idx) => ({ ...item, pos: idx }))
        .filter((item) => item.priority === 100)
        .map((item) => item.pos);

      const lowPositions = completionOrder
        .map((item, idx) => ({ ...item, pos: idx }))
        .filter((item) => item.priority === 1)
        .map((item) => item.pos);

      const avgHighPos = highPositions.reduce((a, b) => a + b, 0) / highPositions.length;
      const avgLowPos = lowPositions.reduce((a, b) => a + b, 0) / lowPositions.length;

      // At least 3 of the first 5 completed should be high-priority
      const firstFive = completionOrder.slice(0, 5);
      const highInFirstFive = firstFive.filter((item) => item.priority === 100).length;

      if (avgHighPos < avgLowPos && highInFirstFive >= 3) {
        ok(`Priority with concurrency: avgHigh=${avgHighPos.toFixed(1)} < avgLow=${avgLowPos.toFixed(1)}, ${highInFirstFive}/5 high in first batch`);
      } else {
        fail(`Priority ordering weak: avgHigh=${avgHighPos.toFixed(1)}, avgLow=${avgLowPos.toFixed(1)}, highInFirst5=${highInFirstFive}`);
      }
    } else {
      fail(`Expected 20 jobs, got ${completionOrder.length}`);
    }
  } catch (e) {
    fail(`Priority with concurrency test threw: ${e}`);
  }

  // ---------------------------------------------------------------
  // Cleanup
  // ---------------------------------------------------------------
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
