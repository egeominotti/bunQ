/**
 * bunqueue vs BullMQ Comparison Benchmark
 *
 * Compares bunqueue (TCP mode) against BullMQ (Redis)
 * Run: bun run bench/comparison/run.ts
 */

import { Queue as BunQueue, Worker as BunWorker } from '../../src/client';
import { Queue as BullQueue, Worker as BullWorker } from 'bullmq';
import IORedis from 'ioredis';

const ITERATIONS = 10000;
const BULK_SIZE = 100;
const CONCURRENCY = 50;  // High concurrency test
const BATCH_PULL = 20;   // Worker batch size
const PAYLOAD = { data: 'x'.repeat(100) };

// Redis connection for BullMQ
const redis = new IORedis({ maxRetriesPerRequest: null });

interface BenchResult {
  name: string;
  opsPerSec: number;
  totalMs: number;
  p99Ms?: number;
}

async function benchmark(name: string, fn: () => Promise<void>, iterations: number): Promise<BenchResult> {
  const latencies: number[] = [];

  // Warmup
  for (let i = 0; i < Math.min(100, iterations / 10); i++) {
    await fn();
  }

  const start = performance.now();
  for (let i = 0; i < iterations; i++) {
    const opStart = performance.now();
    await fn();
    latencies.push(performance.now() - opStart);
  }
  const totalMs = performance.now() - start;

  latencies.sort((a, b) => a - b);
  const p99Ms = latencies[Math.floor(latencies.length * 0.99)];

  return {
    name,
    opsPerSec: Math.round((iterations / totalMs) * 1000),
    totalMs: Math.round(totalMs),
    p99Ms: Math.round(p99Ms * 100) / 100,
  };
}

async function benchmarkParallel(name: string, fn: () => Promise<void>, iterations: number, concurrency: number): Promise<BenchResult> {
  // Warmup
  for (let i = 0; i < Math.min(100, iterations / 10); i++) {
    await fn();
  }

  const start = performance.now();
  let completed = 0;
  const workers: Promise<void>[] = [];

  for (let w = 0; w < concurrency; w++) {
    workers.push((async () => {
      while (completed < iterations) {
        completed++;
        await fn();
      }
    })());
  }

  await Promise.all(workers);
  const totalMs = performance.now() - start;

  return {
    name,
    opsPerSec: Math.round((iterations / totalMs) * 1000),
    totalMs: Math.round(totalMs),
  };
}

async function runBunqueueBenchmarks(): Promise<BenchResult[]> {
  console.log('\n📦 bunqueue (TCP mode) benchmarks...\n');

  const queue = new BunQueue('bench-bunqueue', {
    connection: { host: 'localhost', port: 6789, poolSize: 32 },
  });

  // Wait for connection
  await new Promise((r) => setTimeout(r, 500));

  const results: BenchResult[] = [];

  // 1. Parallel Push (realistic workload with 32 concurrent clients)
  const pushResult = await benchmarkParallel(
    'Push',
    async () => {
      await queue.add('job', PAYLOAD);
    },
    ITERATIONS,
    32
  );
  results.push(pushResult);
  console.log(`  Push: ${pushResult.opsPerSec.toLocaleString()} ops/sec`);

  // 2. Bulk Push
  const jobs = Array.from({ length: BULK_SIZE }, (_, i) => ({
    name: 'bulk-job',
    data: { ...PAYLOAD, i },
  }));
  const bulkResult = await benchmark(
    'Bulk Push',
    async () => {
      await queue.addBulk(jobs);
    },
    ITERATIONS / 10
  );
  // Normalize to per-job ops/sec
  bulkResult.opsPerSec = Math.round(bulkResult.opsPerSec * BULK_SIZE);
  results.push(bulkResult);
  console.log(`  Bulk Push: ${bulkResult.opsPerSec.toLocaleString()} ops/sec (p99: ${bulkResult.p99Ms}ms)`);

  // Clean up jobs from Push and Bulk Push tests before Process test
  await queue.obliterate();
  await new Promise((r) => setTimeout(r, 200));

  // 3. Push + Process
  let processed = 0;
  const worker = new BunWorker(
    'bench-bunqueue-process',
    async () => {
      processed++;
      return { ok: true };
    },
    {
      connection: {
        host: 'localhost',
        port: 6789,
        poolSize: 32,
        pingInterval: 0, // Disable ping during benchmark (prevents timeout under load)
        commandTimeout: 60000, // Increase timeout for heavy load
      },
      concurrency: CONCURRENCY,
      heartbeatInterval: 0, // Disable job heartbeat during benchmark
      batchSize: BATCH_PULL,
    }
  );

  const processQueue = new BunQueue('bench-bunqueue-process', {
    connection: {
      host: 'localhost',
      port: 6789,
      poolSize: 32,
      pingInterval: 0, // Disable ping during benchmark
      commandTimeout: 60000, // Increase timeout for heavy load
    },
  });

  await new Promise((r) => setTimeout(r, 500));

  const processStart = performance.now();
  const processIterations = ITERATIONS;

  // Push all jobs using parallel batches for better throughput
  const BATCH_SIZE = 1000;
  for (let i = 0; i < processIterations; i += BATCH_SIZE) {
    const batch = Math.min(BATCH_SIZE, processIterations - i);
    const promises = [];
    for (let j = 0; j < batch; j++) {
      promises.push(processQueue.add('process-job', PAYLOAD));
    }
    await Promise.all(promises);
  }

  // Wait for all to process
  while (processed < processIterations) {
    await new Promise((r) => setTimeout(r, 10));
  }

  const processMs = performance.now() - processStart;
  const processResult: BenchResult = {
    name: 'Process',
    opsPerSec: Math.round((processIterations / processMs) * 1000),
    totalMs: Math.round(processMs),
  };
  results.push(processResult);
  console.log(`  Process: ${processResult.opsPerSec.toLocaleString()} ops/sec`);

  await worker.close();
  await queue.close();
  await processQueue.close();

  return results;
}

async function runBullMQBenchmarks(): Promise<BenchResult[]> {
  console.log('\n🐂 BullMQ (Redis) benchmarks...\n');

  const queue = new BullQueue('bench-bullmq', { connection: redis });

  const results: BenchResult[] = [];

  // 1. Parallel Push (realistic workload with 32 concurrent clients)
  const pushResult = await benchmarkParallel(
    'Push',
    async () => {
      await queue.add('job', PAYLOAD);
    },
    ITERATIONS,
    32
  );
  results.push(pushResult);
  console.log(`  Push: ${pushResult.opsPerSec.toLocaleString()} ops/sec`);

  // 2. Bulk Push
  const jobs = Array.from({ length: BULK_SIZE }, (_, i) => ({
    name: 'bulk-job',
    data: { ...PAYLOAD, i },
  }));
  const bulkResult = await benchmark(
    'Bulk Push',
    async () => {
      await queue.addBulk(jobs);
    },
    ITERATIONS / 10
  );
  bulkResult.opsPerSec = Math.round(bulkResult.opsPerSec * BULK_SIZE);
  results.push(bulkResult);
  console.log(`  Bulk Push: ${bulkResult.opsPerSec.toLocaleString()} ops/sec (p99: ${bulkResult.p99Ms}ms)`);

  // Clean up jobs from Push and Bulk Push tests before Process test
  await queue.obliterate();
  await new Promise((r) => setTimeout(r, 200));

  // 3. Push + Process
  let processed = 0;
  const processQueue = new BullQueue('bench-bullmq-process', { connection: redis });

  const worker = new BullWorker(
    'bench-bullmq-process',
    async () => {
      processed++;
      return { ok: true };
    },
    {
      connection: redis,
      concurrency: CONCURRENCY,
    }
  );

  await new Promise((r) => setTimeout(r, 500));

  const processStart = performance.now();
  const processIterations = ITERATIONS;

  // Push all jobs using parallel batches (same as bunqueue for fair comparison)
  const BATCH_SIZE = 1000;
  for (let i = 0; i < processIterations; i += BATCH_SIZE) {
    const batch = Math.min(BATCH_SIZE, processIterations - i);
    const promises = [];
    for (let j = 0; j < batch; j++) {
      promises.push(processQueue.add('process-job', PAYLOAD));
    }
    await Promise.all(promises);
  }

  while (processed < processIterations) {
    await new Promise((r) => setTimeout(r, 10));
  }

  const processMs = performance.now() - processStart;
  const processResult: BenchResult = {
    name: 'Process',
    opsPerSec: Math.round((processIterations / processMs) * 1000),
    totalMs: Math.round(processMs),
  };
  results.push(processResult);
  console.log(`  Process: ${processResult.opsPerSec.toLocaleString()} ops/sec`);

  await worker.close();
  await queue.close();
  await processQueue.close();

  return results;
}

async function main() {
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('           bunqueue vs BullMQ Comparison Benchmark');
  console.log('═══════════════════════════════════════════════════════════════');
  console.log(`\nIterations: ${ITERATIONS.toLocaleString()}`);
  console.log(`Bulk size: ${BULK_SIZE}`);
  console.log(`Concurrency: ${CONCURRENCY}`);
  console.log(`Payload: ${JSON.stringify(PAYLOAD).length} bytes`);

  try {
    // Check Redis
    await redis.ping();
    console.log('\n✓ Redis connected');
  } catch {
    console.error('\n✗ Redis not available. Start with: redis-server');
    process.exit(1);
  }

  // Check bunqueue server
  try {
    const testQueue = new BunQueue('test', {
      connection: { host: 'localhost', port: 6789 },
    });
    await new Promise((r) => setTimeout(r, 500));
    await testQueue.close();
    console.log('✓ bunqueue server connected (port 6789)');
  } catch {
    console.error('\n✗ bunqueue server not available. Start with: bunqueue start');
    process.exit(1);
  }

  const bunResults = await runBunqueueBenchmarks();
  const bullResults = await runBullMQBenchmarks();

  // Summary
  console.log('\n═══════════════════════════════════════════════════════════════');
  console.log('                         RESULTS');
  console.log('═══════════════════════════════════════════════════════════════\n');

  console.log('┌─────────────┬──────────────────┬──────────────────┬──────────┐');
  console.log('│ Operation   │ bunqueue         │ BullMQ           │ Speedup  │');
  console.log('├─────────────┼──────────────────┼──────────────────┼──────────┤');

  for (let i = 0; i < bunResults.length; i++) {
    const bun = bunResults[i];
    const bull = bullResults[i];
    const speedup = (bun.opsPerSec / bull.opsPerSec).toFixed(1);

    const bunStr = `${bun.opsPerSec.toLocaleString()} ops/s`.padEnd(16);
    const bullStr = `${bull.opsPerSec.toLocaleString()} ops/s`.padEnd(16);
    const nameStr = bun.name.padEnd(11);
    const speedupStr = `${speedup}x`.padEnd(8);

    console.log(`│ ${nameStr} │ ${bunStr} │ ${bullStr} │ ${speedupStr} │`);
  }

  console.log('└─────────────┴──────────────────┴──────────────────┴──────────┘');

  // Latency comparison
  console.log('\n┌─────────────┬──────────────────┬──────────────────┬──────────┐');
  console.log('│ Operation   │ bunqueue p99     │ BullMQ p99       │ Improve  │');
  console.log('├─────────────┼──────────────────┼──────────────────┼──────────┤');

  for (let i = 0; i < bunResults.length; i++) {
    const bun = bunResults[i];
    const bull = bullResults[i];
    if (bun.p99Ms && bull.p99Ms) {
      const improvement = (bull.p99Ms / bun.p99Ms).toFixed(1);

      const bunStr = `${bun.p99Ms}ms`.padEnd(16);
      const bullStr = `${bull.p99Ms}ms`.padEnd(16);
      const nameStr = bun.name.padEnd(11);
      const improvStr = `${improvement}x`.padEnd(8);

      console.log(`│ ${nameStr} │ ${bunStr} │ ${bullStr} │ ${improvStr} │`);
    }
  }

  console.log('└─────────────┴──────────────────┴──────────────────┴──────────┘');

  await redis.quit();
  process.exit(0);
}

main().catch(console.error);
