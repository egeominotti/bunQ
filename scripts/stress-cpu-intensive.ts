#!/usr/bin/env bun
/**
 * Stress test: CPU-intensive jobs with slow workers (TCP mode)
 *
 * Simulates a realistic heavy workload:
 * - 500 jobs with CPU-heavy payloads (matrix math, hashing, prime search)
 * - Workers with random delays (500ms-3s) to simulate slow processing
 * - Concurrency 3 to create backpressure
 * - CPU work yields periodically so heartbeats can fire
 *
 * Usage:
 *   1. Start the server:  bun run src/main.ts
 *   2. Run this script:   bun scripts/stress-cpu-intensive.ts
 */

import { Queue, Worker } from '../src/client';

const TCP_PORT = parseInt(process.env.TCP_PORT ?? '6789');
const QUEUE_NAME = 'stress-cpu-heavy';
const TOTAL_JOBS = 500;
const WORKER_CONCURRENCY = 3;

const TCP_OPTS = {
  port: TCP_PORT,
  // Disable ping health check — under CPU load the event loop can't
  // respond in time, causing 3 consecutive ping failures after ~90s
  // which triggers forceReconnect → socket close → releaseClientJobs.
  pingInterval: 0,
  commandTimeout: 60000,
};

// ---------- Async CPU-intensive functions (yield every N iterations) ----------

const YIELD_EVERY = 500;

async function fibonacci(n: number): Promise<number> {
  if (n <= 1) return n;
  let a = 0, b = 1;
  for (let i = 2; i <= n; i++) {
    const t = a + b;
    a = b;
    b = t;
  }
  return b;
}

function isPrime(n: number): boolean {
  if (n < 2) return false;
  for (let i = 2; i * i <= n; i++) {
    if (n % i === 0) return false;
  }
  return true;
}

async function findNthPrime(nth: number): Promise<number> {
  let count = 0;
  let candidate = 1;
  let ops = 0;
  while (count < nth) {
    candidate++;
    if (isPrime(candidate)) count++;
    if (++ops % YIELD_EVERY === 0) await Bun.sleep(0);
  }
  return candidate;
}

async function matrixMultiply(size: number): Promise<number> {
  const a: number[][] = [];
  const b: number[][] = [];
  const c: number[][] = [];
  for (let i = 0; i < size; i++) {
    a[i] = [];
    b[i] = [];
    c[i] = [];
    for (let j = 0; j < size; j++) {
      a[i][j] = Math.random();
      b[i][j] = Math.random();
      c[i][j] = 0;
    }
  }
  let ops = 0;
  for (let i = 0; i < size; i++) {
    for (let k = 0; k < size; k++) {
      for (let j = 0; j < size; j++) {
        c[i][j] += a[i][k] * b[k][j];
      }
      if (++ops % 50 === 0) await Bun.sleep(0);
    }
  }
  return c.length;
}

async function heavySha256(iterations: number, seed: string): Promise<string> {
  let data = seed;
  for (let i = 0; i < iterations; i++) {
    const hasher = new Bun.CryptoHasher('sha256');
    hasher.update(data);
    data = hasher.digest('hex');
    if (i % YIELD_EVERY === 0) await Bun.sleep(0);
  }
  return data;
}

// ---------- Job types ----------

type TaskType = 'fibonacci' | 'prime' | 'matrix' | 'hash' | 'mixed';

interface CpuJobData {
  task: TaskType;
  params: Record<string, number | string>;
  createdAt: number;
}

// ---------- Main ----------

async function main() {
  console.log('╔══════════════════════════════════════════════════╗');
  console.log('║   CPU-INTENSIVE STRESS TEST (TCP)               ║');
  console.log('║   500 heavy jobs • concurrency 3 • slow workers ║');
  console.log('╚══════════════════════════════════════════════════╝\n');

  const queue = new Queue<CpuJobData>(QUEUE_NAME, {
    connection: TCP_OPTS,
  });

  // Clean previous run
  queue.obliterate();
  await Bun.sleep(300);

  // ---------- Enqueue jobs ----------
  console.log(`⏳ Pushing ${TOTAL_JOBS} CPU-heavy jobs...\n`);
  const pushStart = Date.now();

  const tasks: TaskType[] = ['fibonacci', 'prime', 'matrix', 'hash', 'mixed'];
  const jobs: { name: string; data: CpuJobData; opts?: { priority?: number } }[] = [];

  for (let i = 0; i < TOTAL_JOBS; i++) {
    const task = tasks[i % tasks.length];
    let params: Record<string, number | string> = {};

    switch (task) {
      case 'fibonacci':
        params = { n: 40 + Math.floor(Math.random() * 20) };
        break;
      case 'prime':
        params = { nth: 5000 + Math.floor(Math.random() * 10000) };
        break;
      case 'matrix':
        params = { size: 80 + Math.floor(Math.random() * 70) };
        break;
      case 'hash':
        params = { iterations: 5000 + Math.floor(Math.random() * 10000), seed: `job-${i}` };
        break;
      case 'mixed':
        params = { n: 35, nth: 3000, size: 60, iterations: 3000, seed: `mix-${i}` };
        break;
    }

    jobs.push({
      name: `cpu-${task}-${i}`,
      data: { task, params, createdAt: Date.now() },
      opts: { priority: Math.floor(Math.random() * 10) },
    });
  }

  await queue.addBulk(jobs);
  const pushDuration = Date.now() - pushStart;
  console.log(`✅ Pushed ${TOTAL_JOBS} jobs in ${pushDuration}ms (${Math.round(TOTAL_JOBS / (pushDuration / 1000))} jobs/s)\n`);

  // ---------- Worker ----------
  let completed = 0;
  let failed = 0;
  let totalCpuMs = 0;
  let totalDelayMs = 0;
  const startTime = Date.now();

  const worker = new Worker<CpuJobData>(
    QUEUE_NAME,
    async (job) => {
      const data = job.data as CpuJobData;
      const cpuStart = Date.now();

      // CPU-intensive work (yields periodically so heartbeats can fire)
      let result: unknown;
      switch (data.task) {
        case 'fibonacci':
          result = await fibonacci(data.params.n as number);
          break;
        case 'prime':
          result = await findNthPrime(data.params.nth as number);
          break;
        case 'matrix':
          result = await matrixMultiply(data.params.size as number);
          break;
        case 'hash':
          result = await heavySha256(data.params.iterations as number, data.params.seed as string);
          break;
        case 'mixed':
          await fibonacci(data.params.n as number);
          await findNthPrime(data.params.nth as number);
          await matrixMultiply(data.params.size as number);
          result = await heavySha256(data.params.iterations as number, data.params.seed as string);
          break;
      }

      const cpuTime = Date.now() - cpuStart;
      totalCpuMs += cpuTime;

      // Simulate I/O delay (database, network, external API, etc.)
      const delay = 500 + Math.floor(Math.random() * 2500);
      totalDelayMs += delay;
      await Bun.sleep(delay);

      return { task: data.task, cpuTime, delay, result };
    },
    {
      concurrency: WORKER_CONCURRENCY,
      connection: TCP_OPTS,
      useLocks: false,
      heartbeatInterval: 0,
    },
  );

  worker.on('completed', () => {
    completed++;
    if (completed % 25 === 0 || completed === TOTAL_JOBS) {
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
      const rate = (completed / ((Date.now() - startTime) / 1000)).toFixed(1);
      const avgCpu = (totalCpuMs / completed).toFixed(0);
      const avgDelay = (totalDelayMs / completed).toFixed(0);
      const pct = ((completed / TOTAL_JOBS) * 100).toFixed(0);
      const bar = '█'.repeat(Math.floor(completed / TOTAL_JOBS * 30)) + '░'.repeat(30 - Math.floor(completed / TOTAL_JOBS * 30));
      console.log(`  [${bar}] ${pct}% | ${completed}/${TOTAL_JOBS} | ${elapsed}s | ${rate} jobs/s | cpu ${avgCpu}ms | delay ${avgDelay}ms`);
    }
  });

  worker.on('failed', (job, err) => {
    failed++;
    console.log(`  ❌ Job ${job?.id} failed: ${err}`);
  });

  // ---------- Wait for completion ----------
  console.log(`🔥 Processing with concurrency=${WORKER_CONCURRENCY}...\n`);

  while (completed + failed < TOTAL_JOBS) {
    await Bun.sleep(500);
  }

  const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
  await worker.close();

  // ---------- Results ----------
  console.log('\n╔══════════════════════════════════════════════════╗');
  console.log('║   RESULTS                                       ║');
  console.log('╠══════════════════════════════════════════════════╣');
  console.log(`║  Total jobs:       ${TOTAL_JOBS.toString().padEnd(29)}║`);
  console.log(`║  Completed:        ${completed.toString().padEnd(29)}║`);
  console.log(`║  Failed:           ${failed.toString().padEnd(29)}║`);
  console.log(`║  Total time:       ${(totalTime + 's').padEnd(29)}║`);
  console.log(`║  Throughput:       ${(completed / parseFloat(totalTime)).toFixed(1).padEnd(25) + ' j/s '}║`);
  console.log(`║  Avg CPU/job:      ${((totalCpuMs / completed).toFixed(0) + 'ms').padEnd(29)}║`);
  console.log(`║  Avg delay/job:    ${((totalDelayMs / completed).toFixed(0) + 'ms').padEnd(29)}║`);
  console.log(`║  Concurrency:      ${WORKER_CONCURRENCY.toString().padEnd(29)}║`);
  console.log('╚══════════════════════════════════════════════════╝');

  queue.obliterate();
  await Bun.sleep(100);
  process.exit(0);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
