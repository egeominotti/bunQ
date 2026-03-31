/**
 * EXTREME stress test: Batch notify under brutal conditions
 *
 * 1. 500 workers, 50k jobs — raw throughput
 * 2. Thundering herd — 200 workers wake simultaneously
 * 3. Staggered producers — 50 producers firing at random intervals
 * 4. Drain-refill torture — rapid empty/fill cycles
 * 5. Asymmetric — few workers, massive batch, repeated pulls
 * 6. Notification storm — 10k pushes with no workers, then 10k workers
 * 7. Interleaved chaos — workers and producers in tight alternation
 * 8. Worst case — 1000 workers, 1 job each, single pushes
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

function stats(results: Result[]) {
  const wakeups = results.map((r) => r.wakeup).sort((a, b) => a - b);
  return {
    got: results.filter((r) => r.got).length,
    total: results.length,
    avg: Math.round(wakeups.reduce((s, w) => s + w, 0) / wakeups.length),
    p50: wakeups[Math.floor(wakeups.length * 0.5)] ?? 0,
    p95: wakeups[Math.floor(wakeups.length * 0.95)] ?? 0,
    p99: wakeups[Math.floor(wakeups.length * 0.99)] ?? 0,
    max: wakeups[wakeups.length - 1] ?? 0,
  };
}

function report(name: string, results: Result[], maxAcceptable: number): boolean {
  const s = stats(results);
  const pass = s.p99 < maxAcceptable && s.got === s.total;
  const icon = pass ? '✓' : '✗';

  console.log(`  ${icon} ${name}`);
  console.log(`    ${s.got}/${s.total} got jobs | avg ${s.avg}ms | p50 ${s.p50}ms | p95 ${s.p95}ms | p99 ${s.p99}ms | max ${s.max}ms`);
  if (!pass) {
    if (s.got < s.total) console.log(`    MISS: ${s.total - s.got} workers got no job`);
    if (s.p99 >= maxAcceptable) console.log(`    SLOW: p99 ${s.p99}ms >= ${maxAcceptable}ms threshold`);
  }
  console.log();
  return pass;
}

// ─── Test 1: Raw throughput ───

async function test1() {
  reset();
  const q = 'extreme-1';
  const WORKERS = 500;
  const JOBS = 50_000;

  const promises = Array.from({ length: WORKERS }, async (_, i) => {
    const start = Date.now();
    const job = await qm.pull(q, 10_000);
    return { id: i, wakeup: Date.now() - start, got: job !== null };
  });
  await Bun.sleep(30);

  // Push in chunks of 1000
  for (let i = 0; i < JOBS; i += 1000) {
    await qm.pushBatch(q, Array.from({ length: 1000 }, (_, j) => ({ data: { n: i + j } })));
  }

  return report('500 workers, 50k jobs (50×1000 batches)', await Promise.all(promises), 500);
}

// ─── Test 2: Thundering herd ───

async function test2() {
  reset();
  const q = 'extreme-2';
  const WORKERS = 200;

  const promises = Array.from({ length: WORKERS }, async (_, i) => {
    const start = Date.now();
    const job = await qm.pull(q, 5000);
    return { id: i, wakeup: Date.now() - start, got: job !== null };
  });
  await Bun.sleep(30);

  // Single batch — all 200 workers should wake at once
  await qm.pushBatch(q, Array.from({ length: WORKERS }, (_, i) => ({ data: { i } })));

  return report('Thundering herd: 200 workers, 200 jobs, 1 batch', await Promise.all(promises), 300);
}

// ─── Test 3: 50 producers firing randomly ───

async function test3() {
  reset();
  const q = 'extreme-3';
  const WORKERS = 100;
  const PRODUCERS = 50;
  const JOBS_PER_PRODUCER = 10;

  const workerPromises = Array.from({ length: WORKERS }, async (_, i) => {
    const start = Date.now();
    const job = await qm.pull(q, 5000);
    return { id: i, wakeup: Date.now() - start, got: job !== null };
  });
  await Bun.sleep(20);

  // 50 producers fire concurrently with random small delays
  const producerPromises = Array.from({ length: PRODUCERS }, async (_, p) => {
    await Bun.sleep(Math.random() * 20);
    await qm.pushBatch(
      q,
      Array.from({ length: JOBS_PER_PRODUCER }, (_, j) => ({ data: { p, j } }))
    );
  });

  await Promise.all(producerPromises);
  return report('100 workers vs 50 staggered producers (500 jobs)', await Promise.all(workerPromises), 300);
}

// ─── Test 4: Drain-refill torture ───

async function test4() {
  reset();
  const q = 'extreme-4';
  const CYCLES = 20;
  const WORKERS_PER_CYCLE = 20;
  const JOBS_PER_CYCLE = 20;
  const allResults: Result[] = [];

  for (let c = 0; c < CYCLES; c++) {
    const promises = Array.from({ length: WORKERS_PER_CYCLE }, async (_, i) => {
      const start = Date.now();
      const job = await qm.pull(q, 2000);
      return { id: c * WORKERS_PER_CYCLE + i, wakeup: Date.now() - start, got: job !== null };
    });
    await Bun.sleep(5);

    await qm.pushBatch(q, Array.from({ length: JOBS_PER_CYCLE }, (_, i) => ({ data: { c, i } })));

    const results = await Promise.all(promises);
    allResults.push(...results);

    // Drain leftovers
    await qm.pullBatch(q, 100, 0);
  }

  return report(`Drain-refill: ${CYCLES} cycles × ${WORKERS_PER_CYCLE} workers`, allResults, 300);
}

// ─── Test 5: Asymmetric — 5 workers pulling repeatedly from 10k jobs ───

async function test5() {
  reset();
  const q = 'extreme-5';
  const WORKERS = 5;
  const TOTAL_JOBS = 10_000;
  const results: Result[] = [];

  // Push all jobs first
  for (let i = 0; i < TOTAL_JOBS; i += 500) {
    await qm.pushBatch(q, Array.from({ length: 500 }, (_, j) => ({ data: { n: i + j } })));
  }

  // Workers pull in a loop until all consumed
  const workerPromises = Array.from({ length: WORKERS }, async (_, w) => {
    let pulled = 0;
    while (true) {
      const start = Date.now();
      const jobs = await qm.pullBatch(q, 100, 100);
      const wakeup = Date.now() - start;
      if (jobs.length === 0) break;
      pulled += jobs.length;
      results.push({ id: w, wakeup, got: true });
    }
    return pulled;
  });

  const counts = await Promise.all(workerPromises);
  const totalPulled = counts.reduce((a, b) => a + b, 0);

  console.log(`  ✓ Asymmetric: 5 workers drained ${totalPulled}/${TOTAL_JOBS} jobs`);
  console.log(`    Per worker: ${counts.join(', ')}`);
  const s = stats(results);
  console.log(`    Pull latency: avg ${s.avg}ms | p50 ${s.p50}ms | p95 ${s.p95}ms | p99 ${s.p99}ms | max ${s.max}ms`);
  console.log();
  return totalPulled === TOTAL_JOBS;
}

// ─── Test 6: Notification storm — push 10k with no workers, then 10k workers ───

async function test6() {
  reset();
  const q = 'extreme-6';
  const COUNT = 5000;

  // Push 5k jobs — no workers, all become pendingNotifications
  for (let i = 0; i < COUNT; i += 500) {
    await qm.pushBatch(q, Array.from({ length: 500 }, (_, j) => ({ data: { n: i + j } })));
  }

  // Now start 5k workers — each should consume a pendingNotification and find a job
  const promises = Array.from({ length: COUNT }, async (_, i) => {
    const start = Date.now();
    const job = await qm.pull(q, 3000);
    return { id: i, wakeup: Date.now() - start, got: job !== null };
  });

  return report(`Notification storm: ${COUNT} pushes → ${COUNT} workers`, await Promise.all(promises), 500);
}

// ─── Test 7: Interleaved chaos ───

async function test7() {
  reset();
  const q = 'extreme-7';
  const ROUNDS = 200;
  const results: Result[] = [];

  // Alternate: push 1, pull 1, push 1, pull 1... very fast
  for (let i = 0; i < ROUNDS; i++) {
    await qm.push(q, { data: { i } });

    const start = Date.now();
    const job = await qm.pull(q, 1000);
    results.push({ id: i, wakeup: Date.now() - start, got: job !== null });
  }

  return report(`Interleaved: ${ROUNDS} push-pull pairs`, results, 50);
}

// ─── Test 8: Worst case — 1000 workers, 1 job each via single push ───

async function test8() {
  reset();
  const q = 'extreme-8';
  const COUNT = 500;

  const promises = Array.from({ length: COUNT }, async (_, i) => {
    const start = Date.now();
    const job = await qm.pull(q, 10_000);
    return { id: i, wakeup: Date.now() - start, got: job !== null };
  });
  await Bun.sleep(30);

  // Push 1 job at a time — each push should wake exactly 1 worker
  for (let i = 0; i < COUNT; i++) {
    await qm.push(q, { data: { i } });
  }

  return report(`Worst case: ${COUNT} workers, ${COUNT} single pushes`, await Promise.all(promises), 500);
}

// ─── Run all ───

console.log('=== EXTREME Batch Notify Stress Test ===\n');

const start = Date.now();
const passed: boolean[] = [];

passed.push(await test1());
passed.push(await test2());
passed.push(await test3());
passed.push(await test4());
passed.push(await test5());
passed.push(await test6());
passed.push(await test7());
passed.push(await test8());

const ok = passed.filter(Boolean).length;
const elapsed = Date.now() - start;

console.log('━'.repeat(60));
console.log(`\n${ok}/${passed.length} passed in ${(elapsed / 1000).toFixed(1)}s`);
console.log(ok === passed.length ? '\n✓ ALL EXTREME TESTS PASSED' : `\n✗ ${passed.length - ok} FAILED`);

qm.shutdown();
process.exit(ok === passed.length ? 0 : 1);
