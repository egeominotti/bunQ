#!/usr/bin/env bun
/**
 * Job Lifecycle & Events Tests (TCP Mode)
 *
 * Tests covering state transitions, event ordering, progress tracking,
 * multiple event listeners, and completed event results over TCP connections.
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

async function main() {
  console.log('=== Job Lifecycle & Events Tests (TCP) ===\n');

  // ---------------------------------------------------------------
  // Test 1: Complete state transitions — waiting -> active -> completed
  // ---------------------------------------------------------------
  console.log('1. Testing COMPLETE STATE TRANSITIONS...');
  try {
    const q = makeQueue('tcp-lifecycle-complete-states');
    q.obliterate();
    await Bun.sleep(200);

    const job = await q.add('task', { value: 1 });
    const jobId = job.id;

    // After push, job should be in waiting state
    const stateAfterPush = await q.getJobState(jobId);

    let sawActive = false;
    let sawCompleted = false;

    const w = makeWorker('tcp-lifecycle-complete-states', async (job) => {
      // While processing, job should be active
      const stateWhileActive = await q.getJobState(job.id);
      if (stateWhileActive === 'active') {
        sawActive = true;
      }
      return { done: true };
    });

    w.on('completed', () => {
      sawCompleted = true;
    });

    // Wait for job to complete
    for (let i = 0; i < 100; i++) {
      if (sawCompleted) break;
      await Bun.sleep(100);
    }

    await w.close();

    // After completion, job should be in completed state
    const stateAfterComplete = await q.getJobState(jobId);

    if (
      stateAfterPush === 'waiting' &&
      sawActive &&
      sawCompleted &&
      stateAfterComplete === 'completed'
    ) {
      ok(`State transitions: waiting -> active -> completed`);
    } else {
      fail(
        `State transitions incorrect: push=${stateAfterPush}, sawActive=${sawActive}, sawCompleted=${sawCompleted}, final=${stateAfterComplete}`,
      );
    }
  } catch (e) {
    fail(`Complete state transitions test failed: ${e}`);
  }

  // ---------------------------------------------------------------
  // Test 2: Failed state transition — waiting -> active -> failed
  // ---------------------------------------------------------------
  console.log('\n2. Testing FAILED STATE TRANSITION...');
  try {
    const q = makeQueue('tcp-lifecycle-failed-states');
    q.obliterate();
    await Bun.sleep(200);

    const job = await q.add('failing-task', { value: 2 }, { attempts: 1 });
    const jobId = job.id;

    // After push, job should be in waiting state
    const stateAfterPush = await q.getJobState(jobId);

    let sawActive = false;
    let sawFailed = false;

    const w = makeWorker('tcp-lifecycle-failed-states', async (job) => {
      const stateWhileActive = await q.getJobState(job.id);
      if (stateWhileActive === 'active') {
        sawActive = true;
      }
      throw new Error('Deliberate failure');
    });

    w.on('failed', () => {
      sawFailed = true;
    });

    // Wait for job to fail
    for (let i = 0; i < 100; i++) {
      if (sawFailed) break;
      await Bun.sleep(100);
    }

    await w.close();

    // After failure with attempts=1, job should be in failed state
    const stateAfterFail = await q.getJobState(jobId);

    if (
      stateAfterPush === 'waiting' &&
      sawActive &&
      sawFailed &&
      stateAfterFail === 'failed'
    ) {
      ok(`State transitions: waiting -> active -> failed`);
    } else {
      fail(
        `State transitions incorrect: push=${stateAfterPush}, sawActive=${sawActive}, sawFailed=${sawFailed}, final=${stateAfterFail}`,
      );
    }
  } catch (e) {
    fail(`Failed state transition test failed: ${e}`);
  }

  // ---------------------------------------------------------------
  // Test 3: Event ordering — active fires before completed
  // ---------------------------------------------------------------
  console.log('\n3. Testing EVENT ORDERING...');
  try {
    const q = makeQueue('tcp-lifecycle-event-order');
    q.obliterate();
    await Bun.sleep(200);

    await q.add('ordered-task', { value: 3 });

    const eventOrder: string[] = [];

    const w = makeWorker('tcp-lifecycle-event-order', async () => {
      return { result: 'ok' };
    });

    w.on('active', () => {
      eventOrder.push('active');
    });

    w.on('completed', () => {
      eventOrder.push('completed');
    });

    // Wait for completion
    for (let i = 0; i < 100; i++) {
      if (eventOrder.includes('completed')) break;
      await Bun.sleep(100);
    }

    await w.close();

    // Active should fire before completed
    const activeIdx = eventOrder.indexOf('active');
    const completedIdx = eventOrder.indexOf('completed');

    if (
      eventOrder.length >= 2 &&
      activeIdx >= 0 &&
      completedIdx >= 0 &&
      activeIdx < completedIdx
    ) {
      ok(`Event ordering correct: [${eventOrder.join(', ')}]`);
    } else {
      fail(`Event ordering incorrect: [${eventOrder.join(', ')}]`);
    }
  } catch (e) {
    fail(`Event ordering test failed: ${e}`);
  }

  // ---------------------------------------------------------------
  // Test 4: Progress tracking — updateProgress values received via event
  // ---------------------------------------------------------------
  console.log('\n4. Testing PROGRESS TRACKING...');
  try {
    const q = makeQueue('tcp-lifecycle-progress');
    q.obliterate();
    await Bun.sleep(200);

    await q.add('progress-task', { value: 4 });

    const progressValues: number[] = [];
    let completed = false;

    const w = makeWorker('tcp-lifecycle-progress', async (job) => {
      await job.updateProgress(25);
      await job.updateProgress(50);
      await job.updateProgress(100);
      return { done: true };
    });

    w.on('progress', (_job: any, progress: number) => {
      progressValues.push(progress);
    });

    w.on('completed', () => {
      completed = true;
    });

    // Wait for completion
    for (let i = 0; i < 100; i++) {
      if (completed) break;
      await Bun.sleep(100);
    }

    await w.close();

    if (
      completed &&
      progressValues.length === 3 &&
      progressValues[0] === 25 &&
      progressValues[1] === 50 &&
      progressValues[2] === 100
    ) {
      ok(`Progress values received: [${progressValues.join(', ')}]`);
    } else {
      fail(
        `Progress tracking incorrect: completed=${completed}, values=[${progressValues.join(', ')}]`,
      );
    }
  } catch (e) {
    fail(`Progress tracking test failed: ${e}`);
  }

  // ---------------------------------------------------------------
  // Test 5: Multiple event listeners — all 3 completed listeners fire
  // ---------------------------------------------------------------
  console.log('\n5. Testing MULTIPLE EVENT LISTENERS...');
  try {
    const q = makeQueue('tcp-lifecycle-multi-listener');
    q.obliterate();
    await Bun.sleep(200);

    await q.add('multi-task', { value: 5 });

    let listener1Fired = false;
    let listener2Fired = false;
    let listener3Fired = false;

    const w = makeWorker('tcp-lifecycle-multi-listener', async () => {
      return { ok: true };
    });

    w.on('completed', () => {
      listener1Fired = true;
    });

    w.on('completed', () => {
      listener2Fired = true;
    });

    w.on('completed', () => {
      listener3Fired = true;
    });

    // Wait for all listeners to fire
    for (let i = 0; i < 100; i++) {
      if (listener1Fired && listener2Fired && listener3Fired) break;
      await Bun.sleep(100);
    }

    await w.close();

    if (listener1Fired && listener2Fired && listener3Fired) {
      ok(`All 3 completed listeners fired`);
    } else {
      fail(
        `Not all listeners fired: l1=${listener1Fired}, l2=${listener2Fired}, l3=${listener3Fired}`,
      );
    }
  } catch (e) {
    fail(`Multiple event listeners test failed: ${e}`);
  }

  // ---------------------------------------------------------------
  // Test 6: Completed event with result — result accessible in event
  // ---------------------------------------------------------------
  console.log('\n6. Testing COMPLETED EVENT WITH RESULT...');
  try {
    const q = makeQueue('tcp-lifecycle-result');
    q.obliterate();
    await Bun.sleep(200);

    await q.add('result-task', { value: 6 });

    let capturedResult: any = null;
    let capturedJobId: string | null = null;
    let completed = false;

    const w = makeWorker('tcp-lifecycle-result', async () => {
      return { answer: 42 };
    });

    w.on('completed', (job: any, result: any) => {
      capturedJobId = job.id;
      capturedResult = result;
      completed = true;
    });

    // Wait for completion
    for (let i = 0; i < 100; i++) {
      if (completed) break;
      await Bun.sleep(100);
    }

    await w.close();

    if (
      completed &&
      capturedJobId &&
      capturedResult &&
      capturedResult.answer === 42
    ) {
      ok(`Result received: { answer: ${capturedResult.answer} } for job ${capturedJobId}`);
    } else {
      fail(
        `Result incorrect: completed=${completed}, jobId=${capturedJobId}, result=${JSON.stringify(capturedResult)}`,
      );
    }
  } catch (e) {
    fail(`Completed event with result test failed: ${e}`);
  }

  // ---------------------------------------------------------------
  // Cleanup
  // ---------------------------------------------------------------
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
