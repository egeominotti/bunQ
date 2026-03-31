#!/usr/bin/env bun
/**
 * Test QueueGroup: Namespace isolation for queues
 */

// Force embedded mode BEFORE imports
process.env.BUNQUEUE_EMBEDDED = '1';

import { Queue, Worker, QueueGroup } from '../../src/client';

// Use timestamp-based unique prefixes to avoid state persistence between runs
const testId = Date.now().toString(36);
const GROUP_PREFIX = `tg-${testId}`;

async function main() {
  console.log('=== Test QueueGroup ===\n');

  let passed = 0;
  let failed = 0;

  // Test 1: QueueGroup.getQueue - Get namespaced queue
  console.log('1. Testing getQueue - Get namespaced queue...');
  try {
    const group = new QueueGroup(GROUP_PREFIX);
    const emailsQueue = group.getQueue<{ email: string }>('emails', { embedded: true });
    const notificationsQueue = group.getQueue<{ message: string }>('notifications', { embedded: true });

    // Add jobs to each queue
    await emailsQueue.add('send', { email: 'user@test.com' });
    await notificationsQueue.add('notify', { message: 'Hello' });

    // Verify jobs are in the namespaced queues using getJobs (queue-specific)
    // Note: getJobCounts returns global stats, not queue-specific
    const emailJobs = emailsQueue.getJobs({ state: 'waiting' });
    const notifJobs = notificationsQueue.getJobs({ state: 'waiting' });

    // Verify queues are properly namespaced
    const expectedEmailQueue = `${GROUP_PREFIX}:emails`;

    // Check by accessing via direct Queue with full name
    const directEmailQueue = new Queue(expectedEmailQueue, { embedded: true });
    const directEmailJobs = directEmailQueue.getJobs({ state: 'waiting' });

    if (emailJobs.length === 1 && notifJobs.length === 1 && directEmailJobs.length === 1) {
      console.log('   ✅ getQueue creates properly namespaced queues');
      passed++;
    } else {
      console.log(`   ❌ Expected 1 waiting in each, got emails=${emailJobs.length}, notifications=${notifJobs.length}`);
      failed++;
    }

    // Cleanup
    emailsQueue.obliterate();
    notificationsQueue.obliterate();
  } catch (e) {
    console.log(`   ❌ getQueue test failed: ${e}`);
    failed++;
  }

  // Test 2: QueueGroup.getWorker - Get namespaced worker
  console.log('\n2. Testing getWorker - Get namespaced worker...');
  try {
    const group = new QueueGroup(GROUP_PREFIX);
    const queue = group.getQueue<{ value: number }>('worker-test', { embedded: true });

    // Add jobs
    await queue.addBulk([
      { name: 'job-1', data: { value: 1 } },
      { name: 'job-2', data: { value: 2 } },
      { name: 'job-3', data: { value: 3 } },
    ]);

    const processed: number[] = [];
    const worker = group.getWorker<{ value: number }>('worker-test', async (job) => {
      processed.push(job.data.value);
      return { processed: true };
    }, { concurrency: 1, embedded: true });

    await Bun.sleep(500);
    await worker.close();

    if (processed.length === 3 && processed.includes(1) && processed.includes(2) && processed.includes(3)) {
      console.log(`   ✅ getWorker processes jobs from namespaced queue (${processed.length} jobs)`);
      passed++;
    } else {
      console.log(`   ❌ Expected 3 jobs processed, got ${processed.length}`);
      failed++;
    }

    // Cleanup
    queue.obliterate();
  } catch (e) {
    console.log(`   ❌ getWorker test failed: ${e}`);
    failed++;
  }

  // Test 3: QueueGroup.listQueues - List all queues in group
  console.log('\n3. Testing listQueues - List all queues in group...');
  try {
    const group = new QueueGroup(`list-${testId}`);
    const queue1 = group.getQueue('queue-a', { embedded: true });
    const queue2 = group.getQueue('queue-b', { embedded: true });
    const queue3 = group.getQueue('queue-c', { embedded: true });

    // Add at least one job to each to ensure they exist
    await queue1.add('job', { x: 1 });
    await queue2.add('job', { x: 2 });
    await queue3.add('job', { x: 3 });

    const queues = group.listQueues();

    const hasQueueA = queues.includes('queue-a');
    const hasQueueB = queues.includes('queue-b');
    const hasQueueC = queues.includes('queue-c');

    if (hasQueueA && hasQueueB && hasQueueC && queues.length >= 3) {
      console.log(`   ✅ listQueues returns namespaced queues: [${queues.join(', ')}]`);
      passed++;
    } else {
      console.log(`   ❌ Expected queue-a, queue-b, queue-c in list, got: [${queues.join(', ')}]`);
      failed++;
    }

    // Cleanup
    queue1.obliterate();
    queue2.obliterate();
    queue3.obliterate();
  } catch (e) {
    console.log(`   ❌ listQueues test failed: ${e}`);
    failed++;
  }

  // Test 4: QueueGroup.pauseAll - Pause all queues in group
  console.log('\n4. Testing pauseAll - Pause all queues in group...');
  try {
    const group = new QueueGroup(`pause-${testId}`);
    const queue1 = group.getQueue<{ value: number }>('pause-q1', { embedded: true });
    const queue2 = group.getQueue<{ value: number }>('pause-q2', { embedded: true });

    // Add jobs to both queues
    await queue1.add('job', { value: 1 });
    await queue2.add('job', { value: 2 });

    // Pause all queues in the group
    group.pauseAll();

    const processed: number[] = [];
    const worker1 = group.getWorker<{ value: number }>('pause-q1', async (job) => {
      processed.push(job.data.value);
      return {};
    }, { concurrency: 1, embedded: true });

    const worker2 = group.getWorker<{ value: number }>('pause-q2', async (job) => {
      processed.push(job.data.value);
      return {};
    }, { concurrency: 1, embedded: true });

    await Bun.sleep(300);

    if (processed.length === 0) {
      console.log('   ✅ pauseAll pauses all queues in group');
      passed++;
    } else {
      console.log(`   ❌ Expected 0 jobs processed while paused, got ${processed.length}`);
      failed++;
    }

    await worker1.close();
    await worker2.close();

    // Cleanup
    queue1.obliterate();
    queue2.obliterate();
  } catch (e) {
    console.log(`   ❌ pauseAll test failed: ${e}`);
    failed++;
  }

  // Test 5: QueueGroup.resumeAll - Resume all queues in group
  console.log('\n5. Testing resumeAll - Resume all queues in group...');
  try {
    const group = new QueueGroup(`resume-${testId}`);
    const queue1 = group.getQueue<{ value: number }>('resume-q1', { embedded: true });
    const queue2 = group.getQueue<{ value: number }>('resume-q2', { embedded: true });

    // Add jobs to both queues
    await queue1.add('job1', { value: 10 });
    await queue2.add('job2', { value: 20 });

    // Pause, then resume
    group.pauseAll();
    await Bun.sleep(100);
    group.resumeAll();

    const processed: number[] = [];
    const worker1 = group.getWorker<{ value: number }>('resume-q1', async (job) => {
      processed.push(job.data.value);
      return {};
    }, { concurrency: 1, embedded: true });

    const worker2 = group.getWorker<{ value: number }>('resume-q2', async (job) => {
      processed.push(job.data.value);
      return {};
    }, { concurrency: 1, embedded: true });

    await Bun.sleep(500);

    await worker1.close();
    await worker2.close();

    if (processed.length === 2 && processed.includes(10) && processed.includes(20)) {
      console.log(`   ✅ resumeAll resumes all queues in group (${processed.length} jobs processed)`);
      passed++;
    } else {
      console.log(`   ❌ Expected 2 jobs processed after resume, got ${processed.length}`);
      failed++;
    }

    // Cleanup
    queue1.obliterate();
    queue2.obliterate();
  } catch (e) {
    console.log(`   ❌ resumeAll test failed: ${e}`);
    failed++;
  }

  // Test 6: QueueGroup.drainAll / obliterateAll - Drain or obliterate all queues
  console.log('\n6. Testing drainAll/obliterateAll - Drain or obliterate all queues...');
  try {
    const group = new QueueGroup(`drain-${testId}`);
    const queue1 = group.getQueue<{ value: number }>('drain-q1', { embedded: true });
    const queue2 = group.getQueue<{ value: number }>('drain-q2', { embedded: true });

    // Add jobs to both queues
    await queue1.addBulk([
      { name: 'job1', data: { value: 1 } },
      { name: 'job2', data: { value: 2 } },
    ]);
    await queue2.addBulk([
      { name: 'job3', data: { value: 3 } },
      { name: 'job4', data: { value: 4 } },
    ]);

    // Use getJobs for queue-specific counts (getJobCounts returns global stats)
    const countsBefore1 = queue1.getJobs({ state: 'waiting' }).length;
    const countsBefore2 = queue2.getJobs({ state: 'waiting' }).length;

    // Drain all
    group.drainAll();

    const countsAfterDrain1 = queue1.getJobs({ state: 'waiting' }).length;
    const countsAfterDrain2 = queue2.getJobs({ state: 'waiting' }).length;

    const drainWorked =
      countsBefore1 === 2 &&
      countsBefore2 === 2 &&
      countsAfterDrain1 === 0 &&
      countsAfterDrain2 === 0;

    // Test obliterateAll
    await queue1.addBulk([
      { name: 'job5', data: { value: 5 } },
      { name: 'job6', data: { value: 6 } },
    ]);
    await queue2.addBulk([
      { name: 'job7', data: { value: 7 } },
    ]);

    const countsBeforeObl1 = queue1.getJobs({ state: 'waiting' }).length;
    const countsBeforeObl2 = queue2.getJobs({ state: 'waiting' }).length;

    // Obliterate all
    group.obliterateAll();
    await Bun.sleep(100);

    const countsAfterObl1 = queue1.getJobs({ state: 'waiting' }).length;
    const countsAfterObl2 = queue2.getJobs({ state: 'waiting' }).length;

    const obliterateWorked =
      countsBeforeObl1 === 2 &&
      countsBeforeObl2 === 1 &&
      countsAfterObl1 === 0 &&
      countsAfterObl2 === 0;

    if (drainWorked && obliterateWorked) {
      console.log('   ✅ drainAll and obliterateAll work correctly');
      passed++;
    } else {
      console.log(`   ❌ drainWorked=${drainWorked}, obliterateWorked=${obliterateWorked}`);
      failed++;
    }
  } catch (e) {
    console.log(`   ❌ drainAll/obliterateAll test failed: ${e}`);
    failed++;
  }

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(console.error);
