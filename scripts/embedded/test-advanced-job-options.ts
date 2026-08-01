#!/usr/bin/env bun
/**
 * Test Advanced Job Options: timeout, ttl, tags, groupId, lifo, removeOnComplete + removeOnFail
 */

// Force embedded mode BEFORE imports
process.env.BUNQUEUE_EMBEDDED = '1';

import { Queue, Worker } from '../../src/client';
import { getSharedManager } from '../../src/client/manager';

const QUEUE_NAME = 'test-advanced-opts';

async function main() {
  console.log('=== Test Advanced Job Options ===\n');

  const queue = new Queue<{ value: number }>(QUEUE_NAME, { embedded: true });
  let passed = 0;
  let failed = 0;

  // Clean state
  queue.obliterate();
  queue.purgeDlq();

  // Test 1: Timeout - Job fails if exceeds timeout
  console.log('1. Testing TIMEOUT...');
  try {
    queue.obliterate();
    queue.purgeDlq();

    // Keep the processor active until the periodic timeout sweep runs.
    const job = await queue.add('slow-job', { value: 1 }, { timeout: 100, attempts: 1 });

    let processingStarted = false;

    const worker = new Worker<{ value: number }>(
      QUEUE_NAME,
      async () => {
        processingStarted = true;
        await new Promise<never>(() => undefined);
      },
      { concurrency: 1, embedded: true }
    );
    worker.on('error', () => undefined);

    let dlq = await queue.getDlqAsync({ reason: 'timeout' });
    const deadline = Date.now() + 12_000;
    while (dlq.length !== 1 && Date.now() < deadline) {
      await Bun.sleep(50);
      dlq = await queue.getDlqAsync({ reason: 'timeout' });
    }
    await worker.close(true);
    const state = await queue.getJobState(job.id);
    const entry = dlq[0];

    if (
      processingStarted &&
      state === 'failed' &&
      dlq.length === 1 &&
      entry.reason === 'timeout' &&
      entry.attempts.length === 1 &&
      entry.attempts[0]?.reason === 'timeout'
    ) {
      console.log('   ✅ Timeout produced one correctly classified terminal DLQ entry');
      passed++;
    } else {
      console.log(
        `   ❌ Timeout contract failed: started=${processingStarted}, state=${state}, dlq=${JSON.stringify(dlq)}`
      );
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Timeout test failed: ${e}`);
    failed++;
  }

  // Test 2: TTL - Job expires if not processed within TTL
  // NOTE: TTL is checked when job is pulled, not continuously.
  // A job with TTL that hasn't been processed within TTL should be skipped or expired.
  console.log('\n2. Testing TTL (Time-To-Live)...');
  try {
    queue.obliterate();
    queue.purgeDlq();

    // Use manager directly to test TTL since it's an advanced feature
    const manager = getSharedManager();

    // Create a job that has a TTL (this verifies TTL field is stored)
    const ttlJob = await manager.push(QUEUE_NAME, {
      data: { name: 'ttl-test', value: 999 },
      ttl: 100, // TTL 100ms - job must be processed within 100ms of creation
    });

    // Verify TTL is stored on the job
    const retrievedJob = await manager.getJob(ttlJob.id);
    const hasTtl = retrievedJob?.ttl === 100;

    // Also create a job without TTL for comparison
    const normalJob = await manager.push(QUEUE_NAME, {
      data: { name: 'normal-job', value: 888 },
    });
    const retrievedNormal = await manager.getJob(normalJob.id);
    const normalHasNoTtl = retrievedNormal?.ttl === null || retrievedNormal?.ttl === undefined;

    if (hasTtl && normalHasNoTtl) {
      console.log(
        '   ✅ TTL is stored correctly on jobs (ttl job has ttl=100, normal job has no ttl)'
      );
      passed++;
    } else {
      console.log(
        `   ❌ TTL storage issue: ttlJob.ttl=${retrievedJob?.ttl}, normalJob.ttl=${retrievedNormal?.ttl}`
      );
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ TTL test failed: ${e}`);
    failed++;
  }

  // Test 3: Tags - Jobs can have tags (metadata)
  console.log('\n3. Testing TAGS...');
  try {
    queue.obliterate();

    const manager = getSharedManager();

    // Add jobs with different tags
    const job1 = await manager.push(QUEUE_NAME, {
      data: { name: 'tagged-1', value: 10 },
      tags: ['important', 'email'],
    });
    const job2 = await manager.push(QUEUE_NAME, {
      data: { name: 'tagged-2', value: 20 },
      tags: ['low-priority'],
    });
    const job3 = await manager.push(QUEUE_NAME, {
      data: { name: 'tagged-3', value: 30 },
      tags: ['important', 'sms'],
    });

    // Get jobs and verify tags are stored
    const retrievedJob1 = await manager.getJob(job1.id);
    const retrievedJob2 = await manager.getJob(job2.id);
    const retrievedJob3 = await manager.getJob(job3.id);

    const hasCorrectTags =
      retrievedJob1?.tags.includes('important') &&
      retrievedJob1?.tags.includes('email') &&
      retrievedJob2?.tags.includes('low-priority') &&
      retrievedJob3?.tags.includes('important') &&
      retrievedJob3?.tags.includes('sms');

    if (hasCorrectTags) {
      console.log(
        `   ✅ Jobs have correct tags: job1=[${retrievedJob1?.tags}], job2=[${retrievedJob2?.tags}], job3=[${retrievedJob3?.tags}]`
      );
      passed++;
    } else {
      console.log('   ❌ Tags not stored correctly');
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Tags test failed: ${e}`);
    failed++;
  }

  // Test 4: GroupId - Jobs can be grouped
  console.log('\n4. Testing GROUPID...');
  try {
    queue.obliterate();

    const manager = getSharedManager();

    // Add jobs with different group IDs
    const jobA = await manager.push(QUEUE_NAME, {
      data: { name: 'group-a-1', value: 100 },
      groupId: 'group-A',
    });
    const jobB = await manager.push(QUEUE_NAME, {
      data: { name: 'group-b-1', value: 200 },
      groupId: 'group-B',
    });
    const jobA2 = await manager.push(QUEUE_NAME, {
      data: { name: 'group-a-2', value: 101 },
      groupId: 'group-A',
    });

    // Retrieve and verify groupIds
    const retrievedJobA = await manager.getJob(jobA.id);
    const retrievedJobB = await manager.getJob(jobB.id);
    const retrievedJobA2 = await manager.getJob(jobA2.id);

    const groupsCorrect =
      retrievedJobA?.groupId === 'group-A' &&
      retrievedJobB?.groupId === 'group-B' &&
      retrievedJobA2?.groupId === 'group-A';

    if (groupsCorrect) {
      console.log(
        `   ✅ Jobs have correct groupIds: A=${retrievedJobA?.groupId}, B=${retrievedJobB?.groupId}, A2=${retrievedJobA2?.groupId}`
      );
      passed++;
    } else {
      console.log('   ❌ GroupIds not stored correctly');
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ GroupId test failed: ${e}`);
    failed++;
  }

  // Test 5: LIFO - Last-in-first-out processing order
  console.log('\n5. Testing LIFO (Last-In-First-Out)...');
  try {
    queue.obliterate();

    const manager = getSharedManager();

    // Add jobs in FIFO mode first (default)
    await manager.push(QUEUE_NAME, { data: { name: 'fifo-1', value: 1 } });
    await manager.push(QUEUE_NAME, { data: { name: 'fifo-2', value: 2 } });
    await manager.push(QUEUE_NAME, { data: { name: 'fifo-3', value: 3 } });

    // Pull them - should come out in order 1, 2, 3
    const fifo1 = await manager.pull(QUEUE_NAME, 0);
    const fifo2 = await manager.pull(QUEUE_NAME, 0);
    const fifo3 = await manager.pull(QUEUE_NAME, 0);

    const fifoData1 = (fifo1?.data as { value: number })?.value;
    const fifoData2 = (fifo2?.data as { value: number })?.value;
    const fifoData3 = (fifo3?.data as { value: number })?.value;

    const fifoCorrect = fifoData1 === 1 && fifoData2 === 2 && fifoData3 === 3;

    // Ack FIFO jobs
    if (fifo1) await manager.ack(fifo1.id);
    if (fifo2) await manager.ack(fifo2.id);
    if (fifo3) await manager.ack(fifo3.id);

    // Now add jobs in LIFO mode
    queue.obliterate();
    await manager.push(QUEUE_NAME, { data: { name: 'lifo-1', value: 10 }, lifo: true });
    await manager.push(QUEUE_NAME, { data: { name: 'lifo-2', value: 20 }, lifo: true });
    await manager.push(QUEUE_NAME, { data: { name: 'lifo-3', value: 30 }, lifo: true });

    // Pull them - should come out in reverse order 30, 20, 10 (last-in-first-out)
    const lifo1 = await manager.pull(QUEUE_NAME, 0);
    const lifo2 = await manager.pull(QUEUE_NAME, 0);
    const lifo3 = await manager.pull(QUEUE_NAME, 0);

    const lifoData1 = (lifo1?.data as { value: number })?.value;
    const lifoData2 = (lifo2?.data as { value: number })?.value;
    const lifoData3 = (lifo3?.data as { value: number })?.value;

    // LIFO should return 30, 20, 10
    const lifoCorrect = lifoData1 === 30 && lifoData2 === 20 && lifoData3 === 10;

    if (fifoCorrect && lifoCorrect) {
      console.log(
        `   ✅ FIFO order: [${fifoData1}, ${fifoData2}, ${fifoData3}], LIFO order: [${lifoData1}, ${lifoData2}, ${lifoData3}]`
      );
      passed++;
    } else if (!fifoCorrect) {
      console.log(
        `   ❌ FIFO order wrong: [${fifoData1}, ${fifoData2}, ${fifoData3}] expected [1, 2, 3]`
      );
      failed++;
    } else {
      console.log(
        `   ❌ LIFO order wrong: [${lifoData1}, ${lifoData2}, ${lifoData3}] expected [30, 20, 10]`
      );
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ LIFO test failed: ${e}`);
    failed++;
  }

  // Test 6: removeOnComplete + removeOnFail combined behavior
  // NOTE: After a job completes, it may be removed from the job index depending on settings.
  // We test that the flags are honored during processing.
  console.log('\n6. Testing REMOVE_ON_COMPLETE + REMOVE_ON_FAIL combined...');
  try {
    queue.obliterate();
    queue.purgeDlq();

    const manager = getSharedManager();

    // Add job that will succeed with removeOnComplete=true
    const successJob = await manager.push(QUEUE_NAME, {
      data: { name: 'success-remove', value: 111 },
      removeOnComplete: true,
    });

    // Add job that will fail with removeOnFail=true
    const failJob = await manager.push(QUEUE_NAME, {
      data: { name: 'fail-remove', value: 222 },
      removeOnFail: true,
      maxAttempts: 1,
    });

    // Verify the options are stored
    const successBefore = await manager.getJob(successJob.id);
    const failBefore = await manager.getJob(failJob.id);

    const successHasFlag = successBefore?.removeOnComplete === true;
    const failHasFlag = failBefore?.removeOnFail === true;

    // Process jobs
    const processedIds: string[] = [];
    const worker = new Worker<{ value: number }>(
      QUEUE_NAME,
      (job) => {
        processedIds.push(job.id);
        if ((job.data as { value: number }).value === 222) {
          throw new Error('Intentional failure');
        }
        return { processed: true };
      },
      { concurrency: 1, embedded: true }
    );

    await Bun.sleep(800);
    await worker.close();

    // Check job states after processing
    const successJobAfter = await manager.getJob(successJob.id);
    const failJobAfter = await manager.getJob(failJob.id);

    // Success job with removeOnComplete should be gone
    const successRemoved = successJobAfter === null || successJobAfter === undefined;

    // Fail job with removeOnFail should be gone (not in DLQ)
    const failRemoved = failJobAfter === null || failJobAfter === undefined;
    const dlqEntries = queue.getDlq();
    const notInDlq = !dlqEntries.some((e) => e.job.id === String(failJob.id));

    // Check that jobs were processed
    const allProcessed = processedIds.length === 2;

    if (
      successHasFlag &&
      failHasFlag &&
      allProcessed &&
      successRemoved &&
      (failRemoved || notInDlq)
    ) {
      console.log('   ✅ removeOnComplete and removeOnFail flags are honored');
      passed++;
    } else {
      console.log(
        `   ❌ successHasFlag=${successHasFlag}, failHasFlag=${failHasFlag}, processed=${processedIds.length}, successRemoved=${successRemoved}, failRemoved=${failRemoved}, notInDlq=${notInDlq}`
      );
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ removeOnComplete/removeOnFail test failed: ${e}`);
    failed++;
  }

  // Cleanup
  queue.purgeDlq();
  queue.obliterate();

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(console.error);
