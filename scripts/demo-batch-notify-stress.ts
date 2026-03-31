/**
 * Stress test: Batch notify under heavy load
 *
 * Tests multiple scenarios with increasing pressure:
 * 1. Many workers, single batch push
 * 2. Many workers, rapid-fire single pushes
 * 3. Workers and producers racing concurrently
 * 4. Repeated burst pattern (push-drain cycles)
 * 5. Extreme: 100 workers, 10k jobs
 */

import { QueueManager } from '../src/application/queueManager';

let qm: QueueManager;

function reset() {
  qm?.shutdown();
  qm = new QueueManager();
}

interface Result {
  id: number;
  wakeup: number;
  got: boolean;
}

async function runWorkers(queue: string, count: number, timeout: number): Promise<Result[]> {
  const workers = Array.from({ length: count }, async (_, i) => {
    const start = Date.now();
    const job = await qm.pull(queue, timeout);
    return { id: i, wakeup: Date.now() - start, got: job !== null };
  });
  await Bun.sleep(20);
  return workers;
}

function report(name: string, results: Result[], maxAcceptable: number) {
  const fast = results.filter((r) => r.wakeup < maxAcceptable);
  const gotJob = results.filter((r) => r.got);
  const maxWakeup = Math.max(...results.map((r) => r.wakeup));
  const avgWakeup = Math.round(results.reduce((s, r) => s + r.wakeup, 0) / results.length);
  const p99 = results.sort((a, b) => a.wakeup - b.wakeup)[Math.floor(results.length * 0.99)]?.wakeup ?? 0;

  const pass = fast.length === results.length;
  const icon = pass ? '✓' : '✗';

  console.log(`  ${icon} ${name}`);
  console.log(`    Workers: ${results.length} | Got job: ${gotJob.length} | Avg: ${avgWakeup}ms | P99: ${p99}ms | Max: ${maxWakeup}ms`);
  if (!pass) {
    const slow = results.filter((r) => r.wakeup >= maxAcceptable);
    console.log(`    SLOW: ${slow.length} workers waited >= ${maxAcceptable}ms`);
  }
  console.log();
  return pass;
}

async function test1() {
  reset();
  const queue = 'stress-1';
  const workerPromises = await runWorkers(queue, 50, 3000);

  await qm.pushBatch(queue, Array.from({ length: 200 }, (_, i) => ({ data: { i } })));

  const results = await Promise.all(workerPromises);
  return report('50 workers, 200 jobs batch push (timeout 3s)', results, 200);
}

async function test2() {
  reset();
  const queue = 'stress-2';
  const workerPromises = await runWorkers(queue, 20, 3000);

  // Rapid single pushes
  for (let i = 0; i < 20; i++) {
    await qm.push(queue, { data: { i } });
  }

  const results = await Promise.all(workerPromises);
  return report('20 workers, 20 rapid single pushes', results, 200);
}

async function test3() {
  reset();
  const queue = 'stress-3';

  // Workers and producers start at the same time
  const workerPromises = Array.from({ length: 30 }, async (_, i) => {
    const start = Date.now();
    const job = await qm.pull(queue, 3000);
    return { id: i, wakeup: Date.now() - start, got: job !== null };
  });

  // Producers fire batches concurrently
  const producerPromises = Array.from({ length: 5 }, (_, batch) =>
    qm.pushBatch(queue, Array.from({ length: 20 }, (_, j) => ({ data: { batch, j } })))
  );

  await Promise.all(producerPromises);
  const results = await Promise.all(workerPromises);
  return report('30 workers vs 5 concurrent batch pushes (100 jobs)', results, 300);
}

async function test4() {
  reset();
  const queue = 'stress-4';
  let allResults: Result[] = [];

  // 5 burst cycles
  for (let cycle = 0; cycle < 5; cycle++) {
    const workerPromises = await runWorkers(queue, 10, 2000);

    await qm.pushBatch(queue, Array.from({ length: 10 }, (_, i) => ({ data: { cycle, i } })));

    const results = await Promise.all(workerPromises);
    allResults = allResults.concat(results.map((r, i) => ({ ...r, id: cycle * 10 + i })));

    // Drain leftovers
    await qm.pullBatch(queue, 100, 0);
  }

  return report('5 burst cycles (10 workers × 10 jobs each)', allResults, 200);
}

async function test5() {
  reset();
  const queue = 'stress-5';
  const WORKERS = 100;
  const JOBS = 10000;

  const workerPromises = await runWorkers(queue, WORKERS, 5000);

  // Push 10k jobs in batches of 500
  for (let offset = 0; offset < JOBS; offset += 500) {
    const batch = Array.from({ length: 500 }, (_, i) => ({ data: { i: offset + i } }));
    await qm.pushBatch(queue, batch);
  }

  const results = await Promise.all(workerPromises);
  return report(`100 workers, 10k jobs (20 × 500 batch pushes)`, results, 300);
}

async function test6() {
  reset();
  const queue = 'stress-6';

  // Push FIRST with no workers → tests pendingNotifications counter
  for (let i = 0; i < 50; i++) {
    await qm.push(queue, { data: { i } });
  }

  // Now start workers — they should all find jobs immediately
  const results: Result[] = [];
  const promises = Array.from({ length: 50 }, async (_, i) => {
    const start = Date.now();
    const job = await qm.pull(queue, 2000);
    const r = { id: i, wakeup: Date.now() - start, got: job !== null };
    results.push(r);
    return r;
  });

  await Promise.all(promises);
  return report('50 pushes before 50 workers (pendingNotifications counter)', results, 100);
}

// Run all tests
console.log('=== Batch Notify Stress Test ===\n');

const startAll = Date.now();
const passed: boolean[] = [];

passed.push(await test1());
passed.push(await test2());
passed.push(await test3());
passed.push(await test4());
passed.push(await test5());
passed.push(await test6());

const total = passed.length;
const ok = passed.filter(Boolean).length;
const elapsed = Date.now() - startAll;

console.log('━'.repeat(50));
console.log(`\n${ok}/${total} passed in ${elapsed}ms`);
if (ok === total) {
  console.log('\n✓ All stress tests passed!');
} else {
  console.log(`\n✗ ${total - ok} tests failed`);
}

qm.shutdown();
process.exit(ok === total ? 0 : 1);
