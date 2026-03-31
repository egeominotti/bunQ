/**
 * Demo: Batch notify fix
 *
 * Simulates a producer pushing 100 jobs in batch
 * while 10 workers are waiting with long-poll.
 * Shows how quickly each worker wakes up.
 */

import { QueueManager } from '../src/application/queueManager';

const QUEUE = 'demo-batch';
const WORKER_COUNT = 10;
const JOB_COUNT = 100;
const POLL_TIMEOUT = 5000; // 5s long-poll

const qm = new QueueManager();

console.log('=== Batch Notify Demo ===\n');
console.log(`Workers: ${WORKER_COUNT} (long-poll ${POLL_TIMEOUT}ms)`);
console.log(`Jobs:    ${JOB_COUNT} (batch push)\n`);

// Start workers FIRST — they block in waitForJob
console.log('Starting workers...');
const workerStart = Date.now();

const workers = Array.from({ length: WORKER_COUNT }, async (_, i) => {
  const start = Date.now();
  const job = await qm.pull(QUEUE, POLL_TIMEOUT);
  const wakeup = Date.now() - start;
  return { id: i, wakeup, got: job !== null };
});

// Let workers register in waiterManager
await Bun.sleep(30);

// Batch push
console.log(`Pushing ${JOB_COUNT} jobs in batch...\n`);
const pushStart = Date.now();
await qm.pushBatch(
  QUEUE,
  Array.from({ length: JOB_COUNT }, (_, i) => ({ data: { i } }))
);
const pushTime = Date.now() - pushStart;

// Wait for all workers
const results = await Promise.all(workers);
const totalTime = Date.now() - workerStart;

// Print results
console.log('Worker  | Wakeup   | Got Job');
console.log('--------|----------|--------');
for (const r of results.sort((a, b) => a.id - b.id)) {
  const bar = '█'.repeat(Math.min(Math.round(r.wakeup / 50), 40));
  const status = r.got ? '✓' : '✗';
  console.log(
    `  W-${String(r.id).padStart(2, '0')}  | ${String(r.wakeup).padStart(5)}ms  |   ${status}  ${bar}`
  );
}

const fast = results.filter((r) => r.wakeup < 100).length;
const slow = results.filter((r) => r.wakeup >= 100).length;

console.log('\n--- Summary ---');
console.log(`Push time:      ${pushTime}ms`);
console.log(`Total time:     ${totalTime}ms`);
console.log(`Fast wakeup:    ${fast}/${WORKER_COUNT} (< 100ms)`);
if (slow > 0) {
  console.log(`Slow wakeup:    ${slow}/${WORKER_COUNT} (waited for timeout)`);
}
console.log(
  fast === WORKER_COUNT
    ? '\n✓ All workers woke up immediately!'
    : `\n✗ ${slow} workers waited for timeout — bug still present`
);

// Drain remaining jobs
const remaining = await qm.pullBatch(QUEUE, JOB_COUNT, 0);
console.log(`\nRemaining jobs in queue: ${remaining.length}`);

qm.shutdown();
