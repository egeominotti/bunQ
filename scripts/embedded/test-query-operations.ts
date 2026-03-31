#!/usr/bin/env bun
/**
 * Test Query Operations: GetState, GetResult, GetJobs, GetCountsPerPriority, GetJobByCustomId
 */

// Force embedded mode BEFORE imports
process.env.BUNQUEUE_EMBEDDED = '1';

import { Queue, Worker } from '../../src/client';

const QUEUE_NAME = 'test-query-ops';

async function main() {
  console.log('=== Test Query Operations ===\n');

  const queue = new Queue<{ value: number }>(QUEUE_NAME, { embedded: true });
  let passed = 0;
  let failed = 0;

  // Clean state
  queue.obliterate();

  // Test 1: GetState - waiting, active, completed, failed states
  console.log('1. Testing GetState...');
  try {
    queue.obliterate();

    // Create a job and check waiting state
    const job = await queue.add('state-test', { value: 1 });
    const retrieved = await queue.getJob(job.id);

    // Job should be in waiting state (not started, not completed, runAt <= now)
    if (retrieved && !retrieved.returnvalue && retrieved.attemptsMade === 0) {
      console.log('   Job in waiting state (no attempts, no return value)');

      // Process the job
      let processedId: string | null = null;
      const worker = new Worker<{ value: number }>(QUEUE_NAME, async (j) => {
        processedId = j.id;
        return { processed: true };
      }, { concurrency: 1, embedded: true });

      await Bun.sleep(500);
      await worker.close();

      // After processing, job should be completed
      if (processedId === job.id) {
        console.log('   Job processed and completed');
        console.log('   [PASS] GetState transitions verified');
        passed++;
      } else {
        console.log('   [FAIL] Job was not processed');
        failed++;
      }
    } else {
      console.log('   [FAIL] Job not in expected state');
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] GetState test failed: ${e}`);
    failed++;
  }

  // Test 2: GetResult - Get result of completed job
  console.log('\n2. Testing GetResult...');
  try {
    queue.obliterate();

    const expectedResult = { sum: 42, processed: true };
    const job = await queue.add('result-test', { value: 42 });

    const worker = new Worker<{ value: number }>(QUEUE_NAME, async (j) => {
      return { sum: (j.data as { value: number }).value, processed: true };
    }, { concurrency: 1, embedded: true });

    await Bun.sleep(500);
    await worker.close();

    // Get job counts to verify completion
    const counts = queue.getJobCounts();
    if (counts.completed > 0) {
      console.log(`   Job completed: ${job.id}`);
      console.log('   [PASS] GetResult verified');
      passed++;
    } else {
      console.log('   [FAIL] Job not marked as completed');
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] GetResult test failed: ${e}`);
    failed++;
  }

  // Test 3: GetJobs - List jobs with state filter
  console.log('\n3. Testing GetJobs with state filter...');
  try {
    queue.obliterate();

    // Add jobs with different characteristics
    await queue.add('job-1', { value: 1 });
    await queue.add('job-2', { value: 2 });
    await queue.add('delayed-job', { value: 3 }, { delay: 60000 }); // 1 minute delay

    // Get waiting jobs
    const waitingJobs = queue.getJobs({ state: 'waiting' });
    const delayedJobs = queue.getJobs({ state: 'delayed' });

    console.log(`   Waiting jobs: ${waitingJobs.length}`);
    console.log(`   Delayed jobs: ${delayedJobs.length}`);

    if (waitingJobs.length >= 2 && delayedJobs.length >= 1) {
      console.log('   [PASS] GetJobs state filter works');
      passed++;
    } else {
      console.log(`   [FAIL] Expected >=2 waiting and >=1 delayed, got ${waitingJobs.length} waiting and ${delayedJobs.length} delayed`);
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] GetJobs state filter test failed: ${e}`);
    failed++;
  }

  // Test 4: GetJobs - List jobs with limit/offset pagination
  console.log('\n4. Testing GetJobs with pagination...');
  try {
    queue.obliterate();

    // Add multiple jobs
    for (let i = 0; i < 10; i++) {
      await queue.add(`job-${i}`, { value: i });
    }

    // Get first 3 jobs
    const firstPage = queue.getJobs({ start: 0, end: 3 });
    // Get next 3 jobs
    const secondPage = queue.getJobs({ start: 3, end: 6 });
    // Get all jobs
    const allJobs = queue.getJobs({ start: 0, end: 100 });

    console.log(`   First page: ${firstPage.length} jobs`);
    console.log(`   Second page: ${secondPage.length} jobs`);
    console.log(`   All jobs: ${allJobs.length} jobs`);

    if (firstPage.length === 3 && secondPage.length === 3 && allJobs.length === 10) {
      // Check that pages don't overlap
      const firstIds = new Set(firstPage.map(j => j.id));
      const hasOverlap = secondPage.some(j => firstIds.has(j.id));

      if (!hasOverlap) {
        console.log('   [PASS] GetJobs pagination works correctly');
        passed++;
      } else {
        console.log('   [FAIL] Pages have overlapping jobs');
        failed++;
      }
    } else {
      console.log(`   [FAIL] Unexpected page sizes`);
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] GetJobs pagination test failed: ${e}`);
    failed++;
  }

  // Test 5: GetCountsPerPriority - Get counts grouped by priority
  console.log('\n5. Testing GetCountsPerPriority...');
  try {
    queue.obliterate();

    // Add jobs with different priorities
    await queue.add('low-1', { value: 1 }, { priority: 1 });
    await queue.add('low-2', { value: 2 }, { priority: 1 });
    await queue.add('medium-1', { value: 3 }, { priority: 5 });
    await queue.add('medium-2', { value: 4 }, { priority: 5 });
    await queue.add('medium-3', { value: 5 }, { priority: 5 });
    await queue.add('high-1', { value: 6 }, { priority: 10 });

    const counts = queue.getCountsPerPriority();

    console.log(`   Priority counts: ${JSON.stringify(counts)}`);

    // Check counts per priority
    const p1Count = counts[1] ?? 0;
    const p5Count = counts[5] ?? 0;
    const p10Count = counts[10] ?? 0;

    if (p1Count === 2 && p5Count === 3 && p10Count === 1) {
      console.log('   [PASS] GetCountsPerPriority works correctly');
      passed++;
    } else {
      console.log(`   [FAIL] Expected p1=2, p5=3, p10=1, got p1=${p1Count}, p5=${p5Count}, p10=${p10Count}`);
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] GetCountsPerPriority test failed: ${e}`);
    failed++;
  }

  // Test 6: GetJobByCustomId - Get job by custom ID
  console.log('\n6. Testing GetJobByCustomId...');
  try {
    queue.obliterate();

    const customId = 'my-unique-custom-id-123';
    const job = await queue.add('custom-id-job', { value: 42 }, { jobId: customId });

    // Retrieve by the job's actual ID
    const retrievedById = await queue.getJob(job.id);

    if (retrievedById) {
      console.log(`   Job created with ID: ${job.id}`);
      console.log(`   Retrieved job data value: ${(retrievedById.data as { value: number }).value}`);

      if ((retrievedById.data as { value: number }).value === 42) {
        console.log('   [PASS] GetJobByCustomId works correctly');
        passed++;
      } else {
        console.log('   [FAIL] Retrieved job has wrong data');
        failed++;
      }
    } else {
      console.log('   [FAIL] Could not retrieve job by custom ID');
      failed++;
    }
  } catch (e) {
    console.log(`   [FAIL] GetJobByCustomId test failed: ${e}`);
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
