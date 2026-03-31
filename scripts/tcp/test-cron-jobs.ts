#!/usr/bin/env bun
/**
 * Test Cron/Scheduled Jobs (TCP Mode)
 */

import { Queue, Worker } from '../../src/client';

const QUEUE_NAME = 'tcp-test-cron';
const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');

async function main() {
  console.log('=== Test Cron/Scheduled Jobs (TCP) ===\n');

  const queue = new Queue<{ type: string }>(QUEUE_NAME, {
    connection: { port: TCP_PORT },
  });
  let passed = 0;
  let failed = 0;

  // Clean up
  queue.obliterate();
  await Bun.sleep(100);

  // Test 1: Repeating job with interval
  console.log('1. Testing REPEAT EVERY INTERVAL...');
  try {
    let executions = 0;

    await queue.add('repeat-job', { type: 'interval' }, {
      repeat: { every: 200, limit: 5 }
    });

    const worker = new Worker<{ type: string }>(QUEUE_NAME, async () => {
      executions++;
      return { executed: true };
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(1500);
    await worker.close();

    if (executions >= 3) {
      console.log(`   ✅ Repeated job executed ${executions} times`);
      passed++;
    } else {
      console.log(`   ❌ Only ${executions} executions (expected >=3)`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Repeat interval test failed: ${e}`);
    failed++;
  }

  // Test 2: Repeat with limit
  console.log('\n2. Testing REPEAT WITH LIMIT...');
  try {
    queue.obliterate();
    await Bun.sleep(100);
    let executions = 0;

    await queue.add('limited-repeat', { type: 'limited' }, {
      repeat: { every: 100, limit: 3 }
    });

    const worker = new Worker<{ type: string }>(QUEUE_NAME, async () => {
      executions++;
      return {};
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(1000);
    await worker.close();

    if (executions >= 3 && executions <= 4) {
      console.log(`   ✅ Repeat with limit: ${executions} executions`);
      passed++;
    } else {
      console.log(`   ❌ Executions: ${executions}, expected 3-4`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Repeat limit test failed: ${e}`);
    failed++;
  }

  // Test 3: Multiple jobs with different priorities
  console.log('\n3. Testing JOBS WITH PRIORITY...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    await queue.add('low-priority', { type: 'low' }, { priority: 1 });
    await queue.add('high-priority', { type: 'high' }, { priority: 10 });

    const order: string[] = [];
    const worker = new Worker<{ type: string }>(QUEUE_NAME, async (job) => {
      order.push((job.data as { type: string }).type);
      return {};
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(1000);
    await worker.close();

    if (order[0] === 'high' && order[1] === 'low') {
      console.log('   ✅ High priority job processed first');
      passed++;
    } else {
      console.log(`   ❌ Wrong order: ${order.join(', ')}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Priority test failed: ${e}`);
    failed++;
  }

  // Test 4: Delayed repeating job
  console.log('\n4. Testing DELAYED START...');
  try {
    queue.obliterate();
    await Bun.sleep(100);
    const start = Date.now();
    let firstExecution = 0;

    await queue.add('delayed-repeat', { type: 'delayed' }, {
      delay: 300,
    });

    const worker = new Worker<{ type: string }>(QUEUE_NAME, async () => {
      if (firstExecution === 0) firstExecution = Date.now();
      return {};
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(1000);
    await worker.close();

    const actualDelay = firstExecution - start;
    if (firstExecution > 0 && actualDelay >= 280) {
      console.log(`   ✅ Job started after delay: ~${actualDelay}ms`);
      passed++;
    } else {
      console.log(`   ❌ Job started too early: ${actualDelay}ms`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Delayed start test failed: ${e}`);
    failed++;
  }

  // Test 5: Queue drain stops processing
  console.log('\n5. Testing QUEUE DRAIN...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    await queue.addBulk([
      { name: 'drain-1', data: { type: 'drain' } },
      { name: 'drain-2', data: { type: 'drain' } },
      { name: 'drain-3', data: { type: 'drain' } },
    ]);

    // Drain the queue before worker starts
    queue.drain();
    await Bun.sleep(100);

    let processed = 0;
    const worker = new Worker<{ type: string }>(QUEUE_NAME, async () => {
      processed++;
      return {};
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(500);
    await worker.close();

    if (processed === 0) {
      console.log('   ✅ Queue drained, no jobs processed');
      passed++;
    } else {
      console.log(`   ❌ ${processed} jobs processed after drain`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Queue drain test failed: ${e}`);
    failed++;
  }

  // Test 6: Bulk add performance
  console.log('\n6. Testing BULK ADD...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    const jobs = Array.from({ length: 100 }, (_, i) => ({
      name: `bulk-${i}`,
      data: { type: 'bulk' },
    }));

    const start = Date.now();
    const addedJobs = await queue.addBulk(jobs);
    const duration = Date.now() - start;

    if (addedJobs.length === 100) {
      console.log(`   ✅ Bulk added 100 jobs in ${duration}ms`);
      passed++;
    } else {
      console.log(`   ❌ Only ${addedJobs.length} jobs added`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Bulk add test failed: ${e}`);
    failed++;
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
