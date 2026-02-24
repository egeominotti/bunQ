#!/usr/bin/env bun
/**
 * Test Deduplication via jobId in TCP Mode
 *
 * Reproduces bug: deduplication via jobId only works in embedded mode.
 * In TCP mode, the auto-batcher routes add() through addBulk() → PUSHB,
 * which sends `jobId` field instead of `customId` in batch jobs.
 * The server's PUSHB handler passes jobs directly to pushBatch() without
 * mapping jobId → customId, so handleCustomId() sees customId=undefined
 * and creates duplicate jobs.
 */

import { Queue } from '../../src/client';

const QUEUE_NAME = 'tcp-test-dedup-bug';
const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');

async function main() {
  console.log('=== Test Deduplication via jobId (TCP Mode) ===\n');

  const queue = new Queue<{ ts: number }>(QUEUE_NAME, {
    connection: { port: TCP_PORT },
  });
  let passed = 0;
  let failed = 0;

  // Clean up
  queue.obliterate();
  await Bun.sleep(200);

  // Test 1: Sequential adds with same jobId should deduplicate
  console.log('1. Testing SEQUENTIAL DEDUP with same jobId (no worker)...');
  try {
    const ids: string[] = [];
    for (let i = 0; i < 10; i++) {
      const job = await queue.add(
        'dedup-task',
        { ts: Date.now() },
        { jobId: 'test-dedup-key' }
      );
      ids.push(job.id);
    }

    const uniqueIds = new Set(ids);
    if (uniqueIds.size === 1) {
      console.log(`   ✅ All 10 adds returned same job ID: ${ids[0]}`);
      passed++;
    } else {
      console.log(`   ❌ Expected 1 unique ID, got ${uniqueIds.size} unique IDs`);
      console.log(`   IDs: ${ids.join(', ')}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Test failed: ${e}`);
    failed++;
  }

  // Test 2: Check queue count - should be 1 job
  console.log('\n2. Testing QUEUE COUNT after dedup...');
  try {
    const counts = await queue.getJobCountsAsync();
    const waitingCount = counts?.waiting ?? -1;
    if (waitingCount === 1) {
      console.log(`   ✅ Queue has exactly 1 waiting job`);
      passed++;
    } else {
      console.log(`   ❌ Expected 1 waiting job, got ${waitingCount}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Test failed: ${e}`);
    failed++;
  }

  // Test 3: Two adds with same jobId return same ID (BullMQ-style idempotency)
  console.log('\n3. Testing TWO ADDS return same ID...');
  try {
    queue.obliterate();
    await Bun.sleep(200);

    const job1 = await queue.add('task-a', { ts: 1 }, { jobId: 'idempotent-key' });
    const job2 = await queue.add('task-b', { ts: 2 }, { jobId: 'idempotent-key' });

    if (job1.id === job2.id) {
      console.log(`   ✅ Both adds returned same job ID: ${job1.id}`);
      passed++;
    } else {
      console.log(`   ❌ Got different IDs: job1=${job1.id}, job2=${job2.id}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Test failed: ${e}`);
    failed++;
  }

  // Test 4: Different jobIds create different jobs
  console.log('\n4. Testing DIFFERENT jobIds create separate jobs...');
  try {
    queue.obliterate();
    await Bun.sleep(200);

    const job1 = await queue.add('task-1', { ts: 1 }, { jobId: 'key-alpha' });
    const job2 = await queue.add('task-2', { ts: 2 }, { jobId: 'key-beta' });
    const job3 = await queue.add('task-3', { ts: 3 }, { jobId: 'key-gamma' });

    if (job1.id !== job2.id && job2.id !== job3.id && job1.id !== job3.id) {
      console.log('   ✅ Different jobIds created different jobs');
      passed++;
    } else {
      console.log(`   ❌ Jobs not properly differentiated: ${job1.id}, ${job2.id}, ${job3.id}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Test failed: ${e}`);
    failed++;
  }

  // Test 5: addBulk with duplicate jobIds should deduplicate within batch
  console.log('\n5. Testing ADDBULK dedup within batch...');
  try {
    queue.obliterate();
    await Bun.sleep(200);

    const jobs = await queue.addBulk([
      { name: 'bulk-1', data: { ts: 1 }, opts: { jobId: 'bulk-dedup-key' } },
      { name: 'bulk-2', data: { ts: 2 }, opts: { jobId: 'bulk-dedup-key' } },
      { name: 'bulk-3', data: { ts: 3 }, opts: { jobId: 'bulk-dedup-key' } },
    ]);

    const uniqueIds = new Set(jobs.map((j) => j.id));
    if (uniqueIds.size === 1) {
      console.log(`   ✅ Bulk add deduplicated to 1 job`);
      passed++;
    } else {
      console.log(`   ❌ Expected 1 unique ID, got ${uniqueIds.size}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Test failed: ${e}`);
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
