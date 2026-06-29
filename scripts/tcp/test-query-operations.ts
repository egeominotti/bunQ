#!/usr/bin/env bun
/**
 * Test Query Operations (TCP Mode): GetState, GetResult, GetJobs, GetCountsPerPriority, GetJobByCustomId
 *
 * Each test uses its own queue name for isolation, but all queues share
 * the same TCP pool (via a single connection config). Queues are only
 * obliterated (not closed) between tests to keep the pool alive.
 */

import { Queue, Worker } from '../../src/client';

const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');
const connOpts = { port: TCP_PORT };

const queues: Queue[] = [];

function makeQueue(name: string): Queue {
  const q = new Queue(name, { connection: connOpts });
  queues.push(q);
  return q;
}

async function main() {
  console.log('=== Test Query Operations (TCP) ===\n');

  let passed = 0;
  let failed = 0;

  // Test 1: GetState - waiting, active, completed, failed states
  console.log('1. Testing GetState...');
  try {
    const queue = makeQueue('tcp-qops-getstate');
    queue.obliterate();
    await Bun.sleep(200);

    const job = await queue.add('state-test', { value: 1 });
    const retrieved = await queue.getJob(job.id);

    if (retrieved && !retrieved.returnvalue && retrieved.attemptsMade === 0) {
      console.log('   Job in waiting state (no attempts, no return value)');

      let processedId: string | null = null;
      const worker = new Worker('tcp-qops-getstate', async (j) => {
        processedId = j.id;
        return { processed: true };
      }, { concurrency: 1, connection: connOpts, useLocks: false });

      await Bun.sleep(1000);
      await worker.close();

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
    const queue = makeQueue('tcp-qops-getresult');
    queue.obliterate();
    await Bun.sleep(200);

    const job = await queue.add('result-test', { value: 42 });

    const worker = new Worker('tcp-qops-getresult', async (j) => {
      return { sum: (j.data as { value: number }).value, processed: true };
    }, { concurrency: 1, connection: connOpts, useLocks: false });

    await Bun.sleep(1000);
    await worker.close();

    const counts = await queue.getJobCountsAsync();
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
    const queue = makeQueue('tcp-qops-filter');
    queue.obliterate();
    await Bun.sleep(200);

    await queue.add('job-1', { value: 1 });
    await queue.add('job-2', { value: 2 });
    await queue.add('delayed-job', { value: 3 }, { delay: 60000 });

    const waitingJobs = await queue.getJobsAsync({ state: 'waiting' });
    const delayedJobs = await queue.getJobsAsync({ state: 'delayed' });

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
    const queue = makeQueue('tcp-qops-pagination');
    queue.obliterate();
    await Bun.sleep(200);

    for (let i = 0; i < 10; i++) {
      await queue.add(`job-${i}`, { value: i });
    }

    const firstPage = await queue.getJobsAsync({ start: 0, end: 3 });
    const secondPage = await queue.getJobsAsync({ start: 3, end: 6 });
    const allJobs = await queue.getJobsAsync({ start: 0, end: 100 });

    console.log(`   First page: ${firstPage.length} jobs`);
    console.log(`   Second page: ${secondPage.length} jobs`);
    console.log(`   All jobs: ${allJobs.length} jobs`);

    if (firstPage.length === 3 && secondPage.length === 3 && allJobs.length === 10) {
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

  // Test 5: GetCountsPerPriority
  console.log('\n5. Testing GetCountsPerPriority...');
  try {
    const queue = makeQueue('tcp-qops-priority');
    queue.obliterate();
    await Bun.sleep(200);

    await queue.add('low-1', { value: 1 }, { priority: 1 });
    await queue.add('low-2', { value: 2 }, { priority: 1 });
    await queue.add('medium-1', { value: 3 }, { priority: 5 });
    await queue.add('medium-2', { value: 4 }, { priority: 5 });
    await queue.add('medium-3', { value: 5 }, { priority: 5 });
    await queue.add('high-1', { value: 6 }, { priority: 10 });

    const counts = await queue.getCountsPerPriorityAsync();
    console.log(`   Priority counts: ${JSON.stringify(counts)}`);

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

  // Test 6: GetJobByCustomId
  console.log('\n6. Testing GetJobByCustomId...');
  try {
    const queue = makeQueue('tcp-qops-customid');
    queue.obliterate();
    await Bun.sleep(200);

    const customId = 'my-unique-custom-id-123';
    const job = await queue.add('custom-id-job', { value: 42 }, { jobId: customId });

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

  // Test 7: finishedOn / processedOn on completed jobs (issue #104, TCP path)
  console.log('\n7. Testing finishedOn/processedOn over TCP (issue #104)...');
  try {
    const queue = makeQueue('tcp-qops-finishedon');
    queue.obliterate();
    await Bun.sleep(200);

    const job = await queue.add('finishedon-test', { value: 7 });

    const worker = new Worker('tcp-qops-finishedon', async () => ({ ok: true }), {
      concurrency: 1,
      connection: connOpts,
      useLocks: false,
    });

    // Poll until completed (TCP, no shared in-process state)
    let state = 'unknown';
    for (let i = 0; i < 100; i++) {
      state = await queue.getJobState(job.id);
      if (state === 'completed') break;
      await Bun.sleep(50);
    }
    await worker.close();

    if (state !== 'completed') {
      console.log(`   [FAIL] job never completed (state=${state})`);
      failed++;
    } else {
      // List path (getJobsAsync TCP branch) — concern this test exists to cover
      const fromList = (await queue.getJobsAsync({ state: 'completed', start: 0, end: 50 })).find(
        (j) => j.id === job.id
      );
      // Single-fetch path (getJob TCP) — must agree (parity)
      const single = await queue.getJob(job.id);

      const listFinished = fromList?.finishedOn;
      const listProcessed = fromList?.processedOn;
      const singleFinished = single?.finishedOn;
      const singleProcessed = single?.processedOn;

      console.log(`   getJobs:  finishedOn=${listFinished} processedOn=${listProcessed}`);
      console.log(`   getJob:   finishedOn=${singleFinished} processedOn=${singleProcessed}`);

      const ok =
        typeof listFinished === 'number' &&
        listFinished > 0 &&
        typeof listProcessed === 'number' &&
        listProcessed > 0 &&
        typeof singleFinished === 'number' &&
        typeof singleProcessed === 'number' &&
        listFinished === singleFinished &&
        listProcessed === singleProcessed;

      if (ok) {
        console.log('   [PASS] finishedOn/processedOn populated and consistent over TCP');
        passed++;
      } else {
        console.log('   [FAIL] finishedOn/processedOn missing or inconsistent over TCP');
        failed++;
      }
    }
  } catch (e) {
    console.log(`   [FAIL] finishedOn/processedOn TCP test failed: ${e}`);
    failed++;
  }

  // Cleanup: obliterate all queues, then close
  for (const q of queues) {
    q.obliterate();
  }
  await Bun.sleep(100);
  for (const q of queues) {
    q.close();
  }

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(console.error);
