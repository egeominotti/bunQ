/**
 * Test: 10,000 jobs with COMPLETED events verification
 */

import { QueueManager } from '../application/queueManager';
import { EventType } from '../domain/types/queue';

const JOBS = 10_000;
const QUEUE = 'completed-test';

function fmt(n: number): string {
  return n.toLocaleString('en-US', { maximumFractionDigits: 0 });
}

async function main(): Promise<void> {
  console.log(`\n🧪 Test: ${fmt(JOBS)} jobs with COMPLETED events\n`);

  const qm = new QueueManager();
  let completedEvents = 0;
  const receivedJobIds = new Set<string>();

  // Subscribe to completed events
  qm.subscribe((event) => {
    if (event.eventType === EventType.Completed) {
      completedEvents++;
      receivedJobIds.add(event.jobId);
    }
  });

  const start = performance.now();

  // Push all jobs
  console.log('📤 Pushing jobs...');
  const pushStart = performance.now();
  for (let i = 0; i < JOBS; i++) {
    await qm.push(QUEUE, { data: { index: i } });
  }
  const pushTime = performance.now() - pushStart;
  console.log(`   ✓ ${fmt(JOBS)} jobs pushed in ${pushTime.toFixed(0)}ms`);

  // Pull and ack all jobs
  console.log('🔄 Processing jobs (pull + ack)...');
  const processStart = performance.now();
  let pulled = 0;
  let acked = 0;
  for (let i = 0; i < JOBS; i++) {
    const job = await qm.pull(QUEUE, 100); // timeout 100ms
    if (job) {
      pulled++;
      await qm.ack(job.id, { result: 'success', index: i });
      acked++;
    }
  }
  const processTime = performance.now() - processStart;
  console.log(`   ✓ Pulled: ${fmt(pulled)}, Acked: ${fmt(acked)} in ${processTime.toFixed(0)}ms`);

  // Wait for events to propagate
  await Bun.sleep(100);

  const totalTime = performance.now() - start;

  // Results
  console.log('\n' + '═'.repeat(50));
  console.log('📊 RESULTS');
  console.log('═'.repeat(50));
  console.log(`Total jobs:         ${fmt(JOBS)}`);
  console.log(`COMPLETED events:   ${fmt(completedEvents)}`);
  console.log(`Unique job IDs:     ${fmt(receivedJobIds.size)}`);
  console.log('─'.repeat(50));
  console.log(
    `Push time:          ${pushTime.toFixed(0)}ms (${fmt((JOBS / pushTime) * 1000)} jobs/sec)`
  );
  console.log(
    `Processing time:    ${processTime.toFixed(0)}ms (${fmt((JOBS / processTime) * 1000)} jobs/sec)`
  );
  console.log(
    `Total time:         ${totalTime.toFixed(0)}ms (${fmt((JOBS / totalTime) * 1000)} jobs/sec)`
  );
  console.log('─'.repeat(50));

  if (completedEvents === JOBS && receivedJobIds.size === JOBS) {
    console.log('✅ TEST PASSED: all COMPLETED events received!');
  } else {
    console.log(`❌ TEST FAILED: expected ${JOBS}, received ${completedEvents}`);
  }

  // Stats
  const stats = qm.getStats();
  console.log('\n📈 Final stats:');
  console.log(`   Completed in memory: ${fmt(stats.completed)}`);
  console.log(`   Total pushed:        ${fmt(Number(stats.totalPushed))}`);
  console.log(`   Total completed:     ${fmt(Number(stats.totalCompleted))}`);

  qm.shutdown();
  console.log('\n');
}

main().catch(console.error);
