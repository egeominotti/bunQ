import { QueueManager } from '../../application/queueManager';
import { EventType } from '../../domain/types/queue';
import { formatCount as fmt, heapMegabytes as memMB, megabytesPerSecond } from './common';

export async function testHighVolume(jobs: number): Promise<void> {
  console.log(`\n${'═'.repeat(60)}`);
  console.log(`🔥 TEST 1: High Volume - ${fmt(jobs)} jobs`);
  console.log('═'.repeat(60));

  const queueManager = new QueueManager();
  const queue = 'stress-high-volume';
  let completed = 0;
  queueManager.subscribe((event) => {
    if (event.eventType === EventType.Completed) completed++;
  });

  const startMemory = memMB();
  const start = performance.now();
  console.log('📤 Pushing...');
  const pushStart = performance.now();
  for (let index = 0; index < jobs; index++) {
    await queueManager.push(queue, { data: { i: index, payload: `job-${index}` } });
  }
  const pushTime = performance.now() - pushStart;
  console.log(
    `   ✓ ${fmt(jobs)} pushed in ${fmt(pushTime)}ms (${fmt((jobs / pushTime) * 1000)}/sec)`
  );

  console.log('🔄 Processing...');
  const processingStart = performance.now();
  for (let index = 0; index < jobs; index++) {
    const job = await queueManager.pull(queue, 0);
    if (job) await queueManager.ack(job.id);
  }
  const processingTime = performance.now() - processingStart;
  console.log(
    `   ✓ ${fmt(jobs)} processed in ${fmt(processingTime)}ms (${fmt((jobs / processingTime) * 1000)}/sec)`
  );

  await Bun.sleep(100);
  const totalTime = performance.now() - start;
  const endMemory = memMB();
  console.log('\n📊 Results:');
  console.log(`   Total time:     ${fmt(totalTime)}ms`);
  console.log(`   Throughput:     ${fmt((jobs / totalTime) * 1000)} jobs/sec`);
  console.log(`   Events:         ${fmt(completed)}/${fmt(jobs)}`);
  console.log(
    `   Memory:         ${startMemory.toFixed(1)}MB → ${endMemory.toFixed(1)}MB (+${(endMemory - startMemory).toFixed(1)}MB)`
  );
  console.log(`   Status:         ${completed === jobs ? '✅ PASS' : '❌ FAIL'}`);
  queueManager.shutdown();
}

export async function testConcurrentQueues(queues: number, jobsPerQueue: number): Promise<void> {
  console.log(`\n${'═'.repeat(60)}`);
  console.log(`🔥 TEST 2: Concurrent Queues - ${queues} queues × ${fmt(jobsPerQueue)} jobs`);
  console.log('═'.repeat(60));

  const queueManager = new QueueManager();
  const totalJobs = queues * jobsPerQueue;
  let completed = 0;
  queueManager.subscribe((event) => {
    if (event.eventType === EventType.Completed) completed++;
  });
  const start = performance.now();

  console.log('📤 Pushing to all queues...');
  const pushPromises = [];
  for (let queueIndex = 0; queueIndex < queues; queueIndex++) {
    const queueName = `concurrent-q-${queueIndex}`;
    pushPromises.push(
      (async () => {
        for (let index = 0; index < jobsPerQueue; index++) {
          await queueManager.push(queueName, { data: { q: queueIndex, i: index } });
        }
      })()
    );
  }
  await Promise.all(pushPromises);
  const afterPush = performance.now();
  console.log(`   ✓ ${fmt(totalJobs)} pushed in ${fmt(afterPush - start)}ms`);

  console.log('🔄 Processing all queues...');
  const processingPromises = [];
  for (let queueIndex = 0; queueIndex < queues; queueIndex++) {
    const queueName = `concurrent-q-${queueIndex}`;
    processingPromises.push(
      (async () => {
        for (let index = 0; index < jobsPerQueue; index++) {
          const job = await queueManager.pull(queueName, 0);
          if (job) await queueManager.ack(job.id);
        }
      })()
    );
  }
  await Promise.all(processingPromises);
  await Bun.sleep(100);
  const totalTime = performance.now() - start;
  console.log('\n📊 Results:');
  console.log(`   Total jobs:     ${fmt(totalJobs)}`);
  console.log(`   Total time:     ${fmt(totalTime)}ms`);
  console.log(`   Throughput:     ${fmt((totalJobs / totalTime) * 1000)} jobs/sec`);
  console.log(`   Events:         ${fmt(completed)}/${fmt(totalJobs)}`);
  console.log(`   Status:         ${completed === totalJobs ? '✅ PASS' : '❌ FAIL'}`);
  queueManager.shutdown();
}

export async function testLargePayloads(jobs: number, payloadKB: number): Promise<void> {
  console.log(`\n${'═'.repeat(60)}`);
  console.log(`🔥 TEST 3: Large Payloads - ${fmt(jobs)} jobs × ${payloadKB}KB`);
  console.log('═'.repeat(60));
  const queueManager = new QueueManager();
  const queue = 'stress-large-payload';
  const payload = { data: 'x'.repeat(payloadKB * 1024), timestamp: Date.now() };
  let completed = 0;
  queueManager.subscribe((event) => {
    if (event.eventType === EventType.Completed) completed++;
  });

  const startMemory = memMB();
  const start = performance.now();
  console.log('📤 Pushing large payloads...');
  for (let index = 0; index < jobs; index++) {
    await queueManager.push(queue, { data: { ...payload, i: index } });
  }
  const afterPush = performance.now();
  console.log(`   ✓ ${fmt(jobs)} pushed in ${fmt(afterPush - start)}ms`);
  console.log('🔄 Processing...');
  for (let index = 0; index < jobs; index++) {
    const job = await queueManager.pull(queue, 0);
    if (job) await queueManager.ack(job.id);
  }
  await Bun.sleep(100);
  const totalTime = performance.now() - start;
  const endMemory = memMB();
  const totalDataMB = (jobs * payloadKB) / 1024;
  console.log('\n📊 Results:');
  console.log(`   Data processed: ${totalDataMB.toFixed(1)}MB`);
  console.log(`   Total time:     ${fmt(totalTime)}ms`);
  console.log(`   Throughput:     ${fmt(megabytesPerSecond(totalDataMB, totalTime))} MB/sec`);
  console.log(`   Events:         ${fmt(completed)}/${fmt(jobs)}`);
  console.log(`   Memory:         ${startMemory.toFixed(1)}MB → ${endMemory.toFixed(1)}MB`);
  console.log(`   Status:         ${completed === jobs ? '✅ PASS' : '❌ FAIL'}`);
  queueManager.shutdown();
}
