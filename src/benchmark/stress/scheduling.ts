import { QueueManager } from '../../application/queueManager';
import { formatCount as fmt } from './common';

export async function testPriorityStress(jobs: number): Promise<void> {
  console.log(`\n${'═'.repeat(60)}`);
  console.log(`🔥 TEST 4: Priority Queue Stress - ${fmt(jobs)} jobs`);
  console.log('═'.repeat(60));
  const queueManager = new QueueManager();
  const queue = 'stress-priority';
  const priorities = [0, 1, 5, 10, 50, 100];
  const start = performance.now();

  console.log('📤 Pushing with random priorities...');
  for (let index = 0; index < jobs; index++) {
    const priority = priorities[Math.floor(Math.random() * priorities.length)];
    await queueManager.push(queue, { data: { i: index }, priority });
  }
  const afterPush = performance.now();
  console.log(`   ✓ ${fmt(jobs)} pushed in ${fmt(afterPush - start)}ms`);

  console.log('🔄 Processing and verifying order...');
  let lastPriority = Infinity;
  let orderViolations = 0;
  let processed = 0;
  for (let index = 0; index < jobs; index++) {
    const job = await queueManager.pull(queue, 0);
    if (job) {
      if (job.priority > lastPriority) orderViolations++;
      lastPriority = job.priority;
      await queueManager.ack(job.id);
      processed++;
    }
  }
  const totalTime = performance.now() - start;
  console.log('\n📊 Results:');
  console.log(`   Processed:      ${fmt(processed)}/${fmt(jobs)}`);
  console.log(`   Order errors:   ${orderViolations}`);
  console.log(`   Total time:     ${fmt(totalTime)}ms`);
  console.log(`   Throughput:     ${fmt((jobs / totalTime) * 1000)} jobs/sec`);
  console.log(
    `   Status:         ${processed === jobs && orderViolations === 0 ? '✅ PASS' : '❌ FAIL'}`
  );
  queueManager.shutdown();
}

export async function testRetryStorm(jobs: number, failRate: number): Promise<void> {
  console.log(`\n${'═'.repeat(60)}`);
  console.log(
    `🔥 TEST 5: Retry Storm - ${fmt(jobs)} jobs, ${(failRate * 100).toFixed(0)}% fail rate`
  );
  console.log('═'.repeat(60));
  const queueManager = new QueueManager();
  const queue = 'stress-retry';
  let totalAttempts = 0;
  let completed = 0;
  let failed = 0;
  const start = performance.now();

  console.log('📤 Pushing jobs with retries...');
  for (let index = 0; index < jobs; index++) {
    await queueManager.push(queue, { data: { i: index }, maxAttempts: 3, backoff: 1 });
  }
  console.log('🔄 Processing with failures...');
  while (completed + failed < jobs) {
    const job = await queueManager.pull(queue, 100);
    if (!job) continue;
    totalAttempts++;
    if (Math.random() < failRate && job.attempts < job.maxAttempts - 1) {
      await queueManager.fail(job.id, 'Simulated failure');
    } else {
      await queueManager.ack(job.id);
      completed++;
    }
  }

  failed = Number(queueManager.getStats().totalFailed);
  const totalTime = performance.now() - start;
  console.log('\n📊 Results:');
  console.log(`   Completed:      ${fmt(completed)}`);
  console.log(`   Failed (DLQ):   ${fmt(failed)}`);
  console.log(`   Total attempts: ${fmt(totalAttempts)}`);
  console.log(`   Retry ratio:    ${((totalAttempts / jobs - 1) * 100).toFixed(1)}%`);
  console.log(`   Total time:     ${fmt(totalTime)}ms`);
  console.log(`   Status:         ${completed + failed === jobs ? '✅ PASS' : '❌ FAIL'}`);
  queueManager.shutdown();
}
