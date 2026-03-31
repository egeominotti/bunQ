#!/usr/bin/env bun
/**
 * Test Unique Jobs / Deduplication
 */

// Force embedded mode BEFORE imports
process.env.BUNQUEUE_EMBEDDED = '1';

import { Queue, Worker } from '../../src/client';

const QUEUE_NAME = 'test-unique';

async function main() {
  console.log('=== Test Unique Jobs / Deduplication ===\n');

  const queue = new Queue<{ value: number }>(QUEUE_NAME, { embedded: true });
  let passed = 0;
  let failed = 0;

  // Clean up
  queue.obliterate();

  // Test 1: Duplicate returns same job (BullMQ-style idempotency)
  console.log('1. Testing DUPLICATE RETURNS SAME JOB...');
  try {
    const job1 = await queue.add('unique-1', { value: 1 }, { jobId: 'unique-key-1' });

    // Try to add another with same jobId - BullMQ returns existing job (idempotent)
    const job2 = await queue.add('unique-1', { value: 2 }, { jobId: 'unique-key-1' });

    // Both should return the same job ID (idempotent behavior)
    if (job1.id && job2.id && job1.id === job2.id) {
      console.log('   ✅ Duplicate returns same job (idempotent)');
      passed++;
    } else {
      console.log(`   ❌ Expected same job ID, got job1=${job1.id}, job2=${job2.id}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Unique key test failed: ${e}`);
    failed++;
  }

  // Test 2: Different unique keys allowed
  console.log('\n2. Testing DIFFERENT KEYS...');
  try {
    queue.obliterate();

    const job1 = await queue.add('key-1', { value: 1 }, { jobId: 'key-a' });
    const job2 = await queue.add('key-2', { value: 2 }, { jobId: 'key-b' });
    const job3 = await queue.add('key-3', { value: 3 }, { jobId: 'key-c' });

    if (job1.id && job2.id && job3.id && job1.id !== job2.id && job2.id !== job3.id) {
      console.log('   ✅ Different keys allowed');
      passed++;
    } else {
      console.log('   ❌ Jobs not created correctly');
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Different keys test failed: ${e}`);
    failed++;
  }

  // Test 3: Job processed with unique key
  console.log('\n3. Testing JOB WITH UNIQUE KEY PROCESSED...');
  try {
    queue.obliterate();

    await queue.add('process-test', { value: 1 }, { jobId: 'process-key' });

    // Process the job
    let processed = false;
    const worker = new Worker<{ value: number }>(QUEUE_NAME, async () => {
      processed = true;
      return { done: true };
    }, { concurrency: 1, embedded: true });

    await Bun.sleep(500);
    await worker.close();

    if (processed) {
      console.log('   ✅ Job with unique key processed');
      passed++;
    } else {
      console.log('   ❌ Job not processed');
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Process test failed: ${e}`);
    failed++;
  }

  // Test 4: Custom job ID
  console.log('\n4. Testing CUSTOM ID...');
  try {
    queue.obliterate();

    const customId = 'my-custom-id-123';
    const job = await queue.add('custom-id-job', { value: 42 }, { jobId: customId });

    // The job should be retrievable by the returned ID
    const retrieved = await queue.getJob(job.id);

    if (retrieved && retrieved.id === job.id) {
      console.log('   ✅ Custom ID job created and retrievable');
      passed++;
    } else {
      console.log('   ❌ Could not retrieve job by ID');
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Custom ID test failed: ${e}`);
    failed++;
  }

  // Test 5: Processing unique jobs
  console.log('\n5. Testing PROCESSING UNIQUE JOBS...');
  try {
    queue.obliterate();

    await queue.add('process-unique', { value: 1 }, { jobId: 'process-key-5' });

    let processed = false;
    const worker = new Worker<{ value: number }>(QUEUE_NAME, async (job) => {
      processed = true;
      return { value: (job.data as { value: number }).value * 2 };
    }, { concurrency: 1, embedded: true });

    await Bun.sleep(500);
    await worker.close();

    if (processed) {
      console.log('   ✅ Unique job processed correctly');
      passed++;
    } else {
      console.log('   ❌ Job not processed');
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Processing test failed: ${e}`);
    failed++;
  }

  // Test 6: Bulk add with unique keys
  console.log('\n6. Testing BULK ADD WITH UNIQUE KEYS...');
  try {
    queue.obliterate();

    const jobs = await queue.addBulk([
      { name: 'bulk-1', data: { value: 1 }, opts: { jobId: 'bulk-a' } },
      { name: 'bulk-2', data: { value: 2 }, opts: { jobId: 'bulk-b' } },
      { name: 'bulk-3', data: { value: 3 }, opts: { jobId: 'bulk-c' } },
    ]);

    if (jobs.length === 3) {
      console.log('   ✅ Bulk add with unique keys succeeded');
      passed++;
    } else {
      console.log(`   ❌ Expected 3 jobs, got ${jobs.length}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Bulk unique test failed: ${e}`);
    failed++;
  }

  // Cleanup
  queue.obliterate();

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(console.error);
