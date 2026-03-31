#!/usr/bin/env bun
/**
 * Test Batch Operations (TCP Mode): addBulk, batch processing
 */

import { Queue, Worker } from '../../src/client';

const QUEUE_NAME = 'tcp-test-batch';
const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');

async function main() {
  console.log('=== Test Batch Operations (TCP) ===\n');

  const queue = new Queue<{ index: number }>(QUEUE_NAME, {
    connection: { port: TCP_PORT },
  });
  let passed = 0;
  let failed = 0;

  // Clean state
  queue.obliterate();
  await Bun.sleep(100);

  // Test 1: Bulk add jobs
  console.log('1. Testing BULK ADD...');
  try {
    const jobs = await queue.addBulk([
      { name: 'batch-1', data: { index: 1 } },
      { name: 'batch-2', data: { index: 2 } },
      { name: 'batch-3', data: { index: 3 } },
    ]);

    if (jobs.length === 3) {
      console.log(`   ✅ Bulk added ${jobs.length} jobs`);
      passed++;
    } else {
      console.log(`   ❌ Expected 3 jobs, got ${jobs.length}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Bulk add failed: ${e}`);
    failed++;
  }

  // Test 2: Process bulk jobs
  console.log('\n2. Testing BULK PROCESSING...');
  try {
    const processed: number[] = [];
    const worker = new Worker<{ index: number }>(QUEUE_NAME, async (job) => {
      processed.push((job.data as { index: number }).index);
      return {};
    }, { concurrency: 5, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(1000);
    await worker.close();

    if (processed.length === 3) {
      console.log(`   ✅ All ${processed.length} jobs processed`);
      passed++;
    } else {
      console.log(`   ❌ Only ${processed.length} jobs processed`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Bulk processing failed: ${e}`);
    failed++;
  }

  // Test 3: Large batch add
  console.log('\n3. Testing LARGE BATCH (100 jobs)...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    const jobs = Array.from({ length: 100 }, (_, i) => ({
      name: `job-${i}`,
      data: { index: i },
    }));

    const start = Date.now();
    const added = await queue.addBulk(jobs);
    const duration = Date.now() - start;

    if (added.length === 100) {
      console.log(`   ✅ Added 100 jobs in ${duration}ms`);
      passed++;
    } else {
      console.log(`   ❌ Expected 100 jobs, got ${added.length}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Large batch failed: ${e}`);
    failed++;
  }

  // Test 4: Process large batch
  console.log('\n4. Testing LARGE BATCH PROCESSING...');
  try {
    let processedCount = 0;
    const worker = new Worker<{ index: number }>(QUEUE_NAME, async () => {
      processedCount++;
      return {};
    }, { concurrency: 20, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(3000);
    await worker.close();

    if (processedCount === 100) {
      console.log(`   ✅ Processed all ${processedCount} jobs`);
      passed++;
    } else {
      console.log(`   ❌ Processed ${processedCount}/100 jobs`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Large batch processing failed: ${e}`);
    failed++;
  }

  // Test 5: Batch with different priorities
  console.log('\n5. Testing BATCH WITH PRIORITIES...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    await queue.addBulk([
      { name: 'low', data: { index: 1 }, opts: { priority: 1 } },
      { name: 'high', data: { index: 2 }, opts: { priority: 10 } },
      { name: 'medium', data: { index: 3 }, opts: { priority: 5 } },
    ]);

    const order: number[] = [];
    const worker = new Worker<{ index: number }>(QUEUE_NAME, async (job) => {
      order.push((job.data as { index: number }).index);
      return {};
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(1000);
    await worker.close();

    // High (2) should come first, then medium (3), then low (1)
    if (order[0] === 2 && order[1] === 3 && order[2] === 1) {
      console.log('   ✅ Priority order correct');
      passed++;
    } else {
      console.log(`   ❌ Wrong order: ${order.join(', ')}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Batch priority test failed: ${e}`);
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
