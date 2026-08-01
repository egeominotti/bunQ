import { QueueManager } from '../../application/queueManager';
import { formatCount as fmt, heapMegabytes as memMB } from './common';

export async function testMemoryStability(iterations: number, batchSize: number): Promise<void> {
  console.log(`\n${'═'.repeat(60)}`);
  console.log(`🔥 TEST 6: Memory Stability - ${iterations} iterations × ${fmt(batchSize)} jobs`);
  console.log('═'.repeat(60));
  const queueManager = new QueueManager();
  const queue = 'stress-memory';
  const memorySamples: number[] = [];
  const totalJobs = iterations * batchSize;
  const startMemory = memMB();
  const start = performance.now();

  for (let iteration = 0; iteration < iterations; iteration++) {
    for (let index = 0; index < batchSize; index++) {
      await queueManager.push(queue, {
        data: { iter: iteration, i: index, payload: 'x'.repeat(100) },
      });
    }
    for (let index = 0; index < batchSize; index++) {
      const job = await queueManager.pull(queue, 0);
      if (job) await queueManager.ack(job.id);
    }
    memorySamples.push(memMB());
    if ((iteration + 1) % 10 === 0) {
      process.stdout.write(`\r   Progress: ${iteration + 1}/${iterations} iterations`);
    }
  }
  console.log('');

  const totalTime = performance.now() - start;
  const endMemory = memMB();
  const memoryGrowth = endMemory - startMemory;
  const maxMemory = Math.max(...memorySamples);
  const minMemory = Math.min(...memorySamples);
  console.log('\n📊 Results:');
  console.log(`   Total jobs:     ${fmt(totalJobs)}`);
  console.log(`   Total time:     ${fmt(totalTime)}ms`);
  console.log(`   Throughput:     ${fmt((totalJobs / totalTime) * 1000)} jobs/sec`);
  console.log(`   Memory start:   ${startMemory.toFixed(1)}MB`);
  console.log(`   Memory end:     ${endMemory.toFixed(1)}MB`);
  console.log(`   Memory growth:  ${memoryGrowth.toFixed(1)}MB`);
  console.log(`   Memory range:   ${minMemory.toFixed(1)}MB - ${maxMemory.toFixed(1)}MB`);
  console.log(`   Status:         ${memoryGrowth < 50 ? '✅ PASS' : '⚠️ MEMORY GROWTH'}`);
  queueManager.shutdown();
}

export async function testBatchOperations(totalJobs: number, batchSize: number): Promise<void> {
  console.log(`\n${'═'.repeat(60)}`);
  console.log(`🔥 TEST 7: Batch Operations - ${fmt(totalJobs)} jobs, batch=${batchSize}`);
  console.log('═'.repeat(60));
  const queueManager = new QueueManager();
  const queue = 'stress-batch';
  const batches = Math.ceil(totalJobs / batchSize);
  const jobs = Array.from({ length: batchSize }, (_, index) => ({ data: { i: index } }));
  const start = performance.now();

  console.log('📤 Batch pushing...');
  const pushStart = performance.now();
  for (let batch = 0; batch < batches; batch++) await queueManager.pushBatch(queue, jobs);
  const pushTime = performance.now() - pushStart;
  const actualJobs = batches * batchSize;
  console.log(
    `   ✓ ${fmt(actualJobs)} pushed in ${fmt(pushTime)}ms (${fmt((actualJobs / pushTime) * 1000)}/sec)`
  );

  console.log('🔄 Processing...');
  const processingStart = performance.now();
  let processed = 0;
  for (let index = 0; index < actualJobs; index++) {
    const job = await queueManager.pull(queue, 0);
    if (job) {
      await queueManager.ack(job.id);
      processed++;
    }
  }
  const processingTime = performance.now() - processingStart;
  console.log(
    `   ✓ ${fmt(processed)} processed in ${fmt(processingTime)}ms (${fmt((processed / processingTime) * 1000)}/sec)`
  );
  const totalTime = performance.now() - start;
  console.log('\n📊 Results:');
  console.log(`   Total time:     ${fmt(totalTime)}ms`);
  console.log(`   Batch push:     ${fmt((actualJobs / pushTime) * 1000)} jobs/sec`);
  console.log(`   Processing:     ${fmt((processed / processingTime) * 1000)} jobs/sec`);
  console.log(`   Status:         ${processed === actualJobs ? '✅ PASS' : '❌ FAIL'}`);
  queueManager.shutdown();
}
