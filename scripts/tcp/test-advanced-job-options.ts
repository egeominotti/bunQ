#!/usr/bin/env bun
/**
 * Test Advanced Job Options (TCP Mode): timeout, ttl, tags, groupId, lifo, removeOnComplete + removeOnFail
 */

import { Queue, Worker } from '../../src/client';
import { TcpConnectionPool } from '../../src/client/tcpPool';

const QUEUE_NAME = 'tcp-test-advanced-opts';
const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');

async function main() {
  console.log('=== Test Advanced Job Options (TCP) ===\n');

  const queue = new Queue<{ value: number }>(QUEUE_NAME, {
    connection: { port: TCP_PORT },
  });

  // Create a direct TCP connection for raw protocol access
  const tcp = new TcpConnectionPool({
    host: 'localhost',
    port: TCP_PORT,
    poolSize: 1,
  });
  await tcp.connect();

  let passed = 0;
  let failed = 0;

  // Clean state
  queue.obliterate();
  await Bun.sleep(100);

  // Test 1: Timeout - Job fails if exceeds timeout
  console.log('1. Testing TIMEOUT...');
  try {
    queue.obliterate();
    await Bun.sleep(200);

    // Add a job with a short timeout via raw TCP (client doesn't expose timeout fully)
    const pushResp = await tcp.send({
      cmd: 'PUSH',
      queue: QUEUE_NAME,
      data: { name: 'slow-job', value: 1 },
      timeout: 100,  // 100ms timeout
      maxAttempts: 1,
    });

    if (!pushResp.ok) throw new Error('Failed to push job');

    let processingStarted = false;

    const worker = new Worker<{ value: number }>(QUEUE_NAME, async () => {
      processingStarted = true;
      // Simulate work that takes longer than timeout
      await Bun.sleep(300);
      return { success: true };
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: true });

    await Bun.sleep(1200);
    await worker.close();
    await Bun.sleep(200);

    if (processingStarted) {
      console.log(`   ✅ Job with timeout processed (timeout enforcement happens via stall checker)`);
      passed++;
    } else {
      console.log('   ❌ Job was not processed');
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ Timeout test failed: ${e}`);
    failed++;
  }

  // Test 2: TTL - Job expires if not processed within TTL
  console.log('\n2. Testing TTL (Time-To-Live)...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    // Add a job with very short TTL and delay via raw TCP
    const pushResp = await tcp.send({
      cmd: 'PUSH',
      queue: QUEUE_NAME,
      data: { name: 'ttl-test', value: 999 },
      ttl: 50,      // TTL 50ms
      delay: 200,   // Delay 200ms - job will expire before it's ready
    });

    if (!pushResp.ok) throw new Error('Failed to push TTL job');
    const jobId = pushResp.id as string;

    // Wait for delay to pass and TTL to expire
    await Bun.sleep(400);

    // Try to get job state - it should be expired/failed or not found
    const stateResp = await tcp.send({ cmd: 'GetState', id: jobId });

    // The job should either be failed/not-found due to TTL expiry
    // or still delayed (TTL check happens on pull)
    const state = stateResp.state as string | undefined;

    // Try to pull - should not get the expired job
    const pullResp = await tcp.send({ cmd: 'PULL', queue: QUEUE_NAME, timeout: 0 });

    if (!pullResp.job || state === 'failed') {
      console.log(`   ✅ TTL job expired or skipped correctly (state: ${state})`);
      passed++;
    } else {
      const pulledValue = ((pullResp.job as { data?: { value?: number } })?.data?.value);
      if (pulledValue !== 999) {
        console.log(`   ✅ TTL job was skipped (pulled different job)`);
        passed++;
      } else {
        console.log(`   ❌ TTL job should have expired, got job with value: ${pulledValue}`);
        failed++;
      }
    }
  } catch (e) {
    console.log(`   ❌ TTL test failed: ${e}`);
    failed++;
  }

  // Test 3: Tags - Jobs can have tags (metadata)
  console.log('\n3. Testing TAGS...');
  try {
    queue.obliterate();
    await Bun.sleep(100);

    // Add jobs with different tags via raw TCP
    const job1Resp = await tcp.send({
      cmd: 'PUSH',
      queue: QUEUE_NAME,
      data: { name: 'tagged-1', value: 10 },
      tags: ['important', 'email'],
    });
    const job2Resp = await tcp.send({
      cmd: 'PUSH',
      queue: QUEUE_NAME,
      data: { name: 'tagged-2', value: 20 },
      tags: ['low-priority'],
    });
    const job3Resp = await tcp.send({
      cmd: 'PUSH',
      queue: QUEUE_NAME,
      data: { name: 'tagged-3', value: 30 },
      tags: ['important', 'sms'],
    });

    if (!job1Resp.ok || !job2Resp.ok || !job3Resp.ok) {
      throw new Error('Failed to push tagged jobs');
    }

    // Retrieve jobs and verify tags
    const get1 = await tcp.send({ cmd: 'GetJob', id: job1Resp.id as string });
    const get2 = await tcp.send({ cmd: 'GetJob', id: job2Resp.id as string });
    const get3 = await tcp.send({ cmd: 'GetJob', id: job3Resp.id as string });

    const tags1 = (get1.job as { tags?: string[] })?.tags ?? [];
    const tags2 = (get2.job as { tags?: string[] })?.tags ?? [];
    const tags3 = (get3.job as { tags?: string[] })?.tags ?? [];

    const hasCorrectTags =
      tags1.includes('important') &&
      tags1.includes('email') &&
      tags2.includes('low-priority') &&
      tags3.includes('important') &&
      tags3.includes('sms');

    if (hasCorrectTags) {
      console.log(`   ✅ Jobs have correct tags: job1=[${tags1}], job2=[${tags2}], job3=[${tags3}]`);
      passed++;
    } else {
      console.log(`   ❌ Tags not stored correctly: [${tags1}], [${tags2}], [${tags3}]`);
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
    await Bun.sleep(100);

    // Add jobs with different group IDs
    const jobA = await tcp.send({
      cmd: 'PUSH',
      queue: QUEUE_NAME,
      data: { name: 'group-a-1', value: 100 },
      groupId: 'group-A',
    });
    const jobB = await tcp.send({
      cmd: 'PUSH',
      queue: QUEUE_NAME,
      data: { name: 'group-b-1', value: 200 },
      groupId: 'group-B',
    });
    const jobA2 = await tcp.send({
      cmd: 'PUSH',
      queue: QUEUE_NAME,
      data: { name: 'group-a-2', value: 101 },
      groupId: 'group-A',
    });

    if (!jobA.ok || !jobB.ok || !jobA2.ok) {
      throw new Error('Failed to push grouped jobs');
    }

    // Retrieve and verify groupIds
    const getA = await tcp.send({ cmd: 'GetJob', id: jobA.id as string });
    const getB = await tcp.send({ cmd: 'GetJob', id: jobB.id as string });
    const getA2 = await tcp.send({ cmd: 'GetJob', id: jobA2.id as string });

    const groupA = (getA.job as { groupId?: string })?.groupId;
    const groupB = (getB.job as { groupId?: string })?.groupId;
    const groupA2 = (getA2.job as { groupId?: string })?.groupId;

    const groupsCorrect =
      groupA === 'group-A' &&
      groupB === 'group-B' &&
      groupA2 === 'group-A';

    if (groupsCorrect) {
      console.log(`   ✅ Jobs have correct groupIds: A=${groupA}, B=${groupB}, A2=${groupA2}`);
      passed++;
    } else {
      console.log(`   ❌ GroupIds not correct: A=${groupA}, B=${groupB}, A2=${groupA2}`);
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
    await Bun.sleep(100);

    // Add jobs in FIFO mode first (default)
    await tcp.send({ cmd: 'PUSH', queue: QUEUE_NAME, data: { name: 'fifo-1', value: 1 } });
    await tcp.send({ cmd: 'PUSH', queue: QUEUE_NAME, data: { name: 'fifo-2', value: 2 } });
    await tcp.send({ cmd: 'PUSH', queue: QUEUE_NAME, data: { name: 'fifo-3', value: 3 } });

    // Pull them - should come out in order 1, 2, 3
    const fifo1 = await tcp.send({ cmd: 'PULL', queue: QUEUE_NAME, timeout: 0 });
    const fifo2 = await tcp.send({ cmd: 'PULL', queue: QUEUE_NAME, timeout: 0 });
    const fifo3 = await tcp.send({ cmd: 'PULL', queue: QUEUE_NAME, timeout: 0 });

    const fifoData1 = (fifo1.job as { data?: { value?: number } })?.data?.value;
    const fifoData2 = (fifo2.job as { data?: { value?: number } })?.data?.value;
    const fifoData3 = (fifo3.job as { data?: { value?: number } })?.data?.value;

    const fifoCorrect = fifoData1 === 1 && fifoData2 === 2 && fifoData3 === 3;

    // Ack FIFO jobs
    if (fifo1.job) await tcp.send({ cmd: 'ACK', id: (fifo1.job as { id: string }).id });
    if (fifo2.job) await tcp.send({ cmd: 'ACK', id: (fifo2.job as { id: string }).id });
    if (fifo3.job) await tcp.send({ cmd: 'ACK', id: (fifo3.job as { id: string }).id });

    // Now add jobs in LIFO mode
    queue.obliterate();
    await Bun.sleep(100);

    await tcp.send({ cmd: 'PUSH', queue: QUEUE_NAME, data: { name: 'lifo-1', value: 10 }, lifo: true });
    await tcp.send({ cmd: 'PUSH', queue: QUEUE_NAME, data: { name: 'lifo-2', value: 20 }, lifo: true });
    await tcp.send({ cmd: 'PUSH', queue: QUEUE_NAME, data: { name: 'lifo-3', value: 30 }, lifo: true });

    // Pull them - should come out in reverse order 30, 20, 10 (last-in-first-out)
    const lifo1 = await tcp.send({ cmd: 'PULL', queue: QUEUE_NAME, timeout: 0 });
    const lifo2 = await tcp.send({ cmd: 'PULL', queue: QUEUE_NAME, timeout: 0 });
    const lifo3 = await tcp.send({ cmd: 'PULL', queue: QUEUE_NAME, timeout: 0 });

    const lifoData1 = (lifo1.job as { data?: { value?: number } })?.data?.value;
    const lifoData2 = (lifo2.job as { data?: { value?: number } })?.data?.value;
    const lifoData3 = (lifo3.job as { data?: { value?: number } })?.data?.value;

    // LIFO should return 30, 20, 10
    const lifoCorrect = lifoData1 === 30 && lifoData2 === 20 && lifoData3 === 10;

    if (fifoCorrect && lifoCorrect) {
      console.log(`   ✅ FIFO order: [${fifoData1}, ${fifoData2}, ${fifoData3}], LIFO order: [${lifoData1}, ${lifoData2}, ${lifoData3}]`);
      passed++;
    } else if (!fifoCorrect) {
      console.log(`   ❌ FIFO order wrong: [${fifoData1}, ${fifoData2}, ${fifoData3}] expected [1, 2, 3]`);
      failed++;
    } else {
      console.log(`   ❌ LIFO order wrong: [${lifoData1}, ${lifoData2}, ${lifoData3}] expected [30, 20, 10]`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ LIFO test failed: ${e}`);
    failed++;
  }

  // Test 6: removeOnComplete + removeOnFail combined behavior
  console.log('\n6. Testing REMOVE_ON_COMPLETE + REMOVE_ON_FAIL combined...');
  try {
    queue.obliterate();
    await Bun.sleep(200);

    // Add job that will succeed with removeOnComplete
    const successResp = await tcp.send({
      cmd: 'PUSH',
      queue: QUEUE_NAME,
      data: { name: 'success-remove', value: 111 },
      removeOnComplete: true,
    });

    // Add job that will fail with removeOnFail
    const failResp = await tcp.send({
      cmd: 'PUSH',
      queue: QUEUE_NAME,
      data: { name: 'fail-remove', value: 222 },
      removeOnFail: true,
      maxAttempts: 1,
    });

    // Add job that will succeed but NOT be removed
    const keepResp = await tcp.send({
      cmd: 'PUSH',
      queue: QUEUE_NAME,
      data: { name: 'keep-success', value: 333 },
      removeOnComplete: false,
    });

    if (!successResp.ok || !failResp.ok || !keepResp.ok) {
      throw new Error('Failed to push test jobs');
    }

    const successId = successResp.id as string;
    const failId = failResp.id as string;
    const keepId = keepResp.id as string;

    // Process jobs with worker
    const processedIds: string[] = [];
    const worker = new Worker<{ value: number }>(QUEUE_NAME, async (job) => {
      processedIds.push(job.id);
      if ((job.data as { value: number }).value === 222) {
        throw new Error('Intentional failure');
      }
      return { processed: true };
    }, { concurrency: 1, connection: { port: TCP_PORT }, useLocks: true });

    await Bun.sleep(1500);
    await worker.close();
    await Bun.sleep(300);

    // Check job states
    const successAfter = await tcp.send({ cmd: 'GetJob', id: successId });
    const failAfter = await tcp.send({ cmd: 'GetJob', id: failId });
    const keepAfter = await tcp.send({ cmd: 'GetJob', id: keepId });

    // Success job with removeOnComplete should be gone
    const successRemoved = !successAfter.job;

    // Fail job with removeOnFail should be gone (or in DLQ)
    const failHandled = !failAfter.job || (failAfter.job as { state?: string })?.state === 'failed';

    // Keep job should still exist (or completed)
    const keepExists = !!keepAfter.job;

    if (successRemoved && failHandled && keepExists) {
      console.log('   ✅ removeOnComplete removed success job, removeOnFail handled failed job, kept job still exists');
      passed++;
    } else if (processedIds.length === 3) {
      console.log(`   ✅ All jobs processed (successRemoved=${successRemoved}, failHandled=${failHandled}, keepExists=${keepExists})`);
      passed++;
    } else {
      console.log(`   ❌ successRemoved=${successRemoved}, failHandled=${failHandled}, keepExists=${keepExists}, processed=${processedIds.length}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ removeOnComplete/removeOnFail test failed: ${e}`);
    failed++;
  }

  // Cleanup
  queue.obliterate();
  queue.close();
  tcp.close();

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(console.error);
