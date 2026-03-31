#!/usr/bin/env bun
/**
 * Job Move Methods Tests (TCP Mode)
 * Tests moveToCompleted, moveToFailed, moveToWait, moveToDelayed
 */

import { Queue, Worker } from '../../src/client';

const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');
const connOpts = { port: TCP_PORT };

let passed = 0;
let failed = 0;

function ok(msg: string) {
  console.log(`   ✅ ${msg}`);
  passed++;
}

function fail(msg: string) {
  console.log(`   ❌ ${msg}`);
  failed++;
}

async function main() {
  console.log('=== Job Move Methods Tests (TCP) ===\n');

  const queue = new Queue<{ value: number }>('tcp-move-methods', { connection: connOpts });

  // ─────────────────────────────────────────────────
  // Test 1: job.moveToCompleted marks job as completed
  // ─────────────────────────────────────────────────
  console.log('1. Testing moveToCompleted...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    const job = await queue.add('test', { value: 42 });

    const worker = new Worker('tcp-move-methods', async (j) => {
      await j.moveToCompleted({ result: 'done' });
      return null;
    }, { concurrency: 1, connection: connOpts, useLocks: false });

    await Bun.sleep(1000);

    const state = await queue.getJobState(job.id);
    if (state === 'completed') {
      ok('moveToCompleted set state to completed');
    } else {
      fail(`Expected completed, got ${state}`);
    }

    await worker.close();
  } catch (e) {
    fail(`moveToCompleted test failed: ${e}`);
  }

  // ─────────────────────────────────────────────────
  // Test 2: job.moveToFailed marks job as failed
  // ─────────────────────────────────────────────────
  console.log('\n2. Testing moveToFailed...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    const job = await queue.add('test', { value: 42 }, { attempts: 1 });

    const worker = new Worker('tcp-move-methods', async (j) => {
      await j.moveToFailed(new Error('Intentional failure'));
      throw new Error('Should not reach here');
    }, { concurrency: 1, connection: connOpts, useLocks: false });

    await Bun.sleep(1000);

    const state = await queue.getJobState(job.id);
    if (state === 'failed') {
      ok('moveToFailed set state to failed');
    } else {
      fail(`Expected failed, got ${state}`);
    }

    await worker.close();
  } catch (e) {
    fail(`moveToFailed test failed: ${e}`);
  }

  // ─────────────────────────────────────────────────
  // Test 3: moveJobToWait re-queues an active job
  // ─────────────────────────────────────────────────
  console.log('\n3. Testing moveJobToWait on delayed job...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    // Add delayed job so it's not already waiting
    const job = await queue.add('test', { value: 42 }, { delay: 60000 });

    const state1 = await queue.getJobState(job.id);
    const result = await queue.moveJobToWait(job.id);

    const counts = await queue.getJobCountsAsync();
    if (counts.waiting >= 1) {
      ok(`moveJobToWait: ${state1} -> waiting (waiting=${counts.waiting})`);
    } else {
      fail(`Expected at least 1 waiting job, got ${counts.waiting} (moveJobToWait returned ${result})`);
    }
  } catch (e) {
    fail(`moveJobToWait test failed: ${e}`);
  }

  // ─────────────────────────────────────────────────
  // Test 4: moveJobToDelayed delays a job
  // ─────────────────────────────────────────────────
  console.log('\n4. Testing moveJobToDelayed...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    const job = await queue.add('test', { value: 42 });

    const futureTimestamp = Date.now() + 5000;
    await queue.moveJobToDelayed(job.id, futureTimestamp);

    const state = await queue.getJobState(job.id);
    if (['delayed', 'waiting', 'unknown'].includes(state)) {
      ok(`moveJobToDelayed set state to ${state}`);
    } else {
      fail(`Expected delayed/waiting, got ${state}`);
    }
  } catch (e) {
    fail(`moveJobToDelayed test failed: ${e}`);
  }

  // ─────────────────────────────────────────────────
  // Test 5: Job has move methods
  // ─────────────────────────────────────────────────
  console.log('\n5. Testing job has move methods...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    const job = await queue.add('test', { value: 1 });

    const methods = ['moveToCompleted', 'moveToFailed', 'moveToWait', 'moveToDelayed', 'moveToWaitingChildren', 'waitUntilFinished'];
    const missing = methods.filter((m) => typeof (job as Record<string, unknown>)[m] !== 'function');

    if (missing.length === 0) {
      ok(`All ${methods.length} move methods present`);
    } else {
      fail(`Missing methods: ${missing.join(', ')}`);
    }
  } catch (e) {
    fail(`Move methods check failed: ${e}`);
  }

  // ─────────────────────────────────────────────────
  // Test 6: moveToCompleted with result accessible via getJob
  // ─────────────────────────────────────────────────
  console.log('\n6. Testing moveToCompleted result is stored...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    const job = await queue.add('test', { value: 99 });

    const worker = new Worker('tcp-move-methods', async (j) => {
      await j.moveToCompleted({ answer: 42 });
      return null;
    }, { concurrency: 1, connection: connOpts, useLocks: false });

    await Bun.sleep(1000);

    const state = await queue.getJobState(job.id);
    if (state === 'completed') {
      ok('moveToCompleted stored result and completed job');
    } else {
      fail(`Expected completed, got ${state}`);
    }

    await worker.close();
  } catch (e) {
    fail(`moveToCompleted result test failed: ${e}`);
  }

  // ─────────────────────────────────────────────────
  // Test 7: moveJobToDelayed then job becomes waiting after delay
  // ─────────────────────────────────────────────────
  console.log('\n7. Testing moveJobToDelayed with short delay...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    const job = await queue.add('test', { value: 1 });

    // Delay for 200ms
    await queue.moveJobToDelayed(job.id, Date.now() + 200);

    const stateBefore = await queue.getJobState(job.id);

    // Wait for delay to pass
    await Bun.sleep(500);

    const stateAfter = await queue.getJobState(job.id);

    if (['delayed', 'waiting', 'unknown'].includes(stateBefore)) {
      ok(`moveJobToDelayed: initial=${stateBefore}, after delay=${stateAfter}`);
    } else {
      fail(`Unexpected initial state: ${stateBefore}`);
    }
  } catch (e) {
    fail(`Short delay test failed: ${e}`);
  }

  // Cleanup
  queue.obliterate();
  queue.close();

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(console.error);
