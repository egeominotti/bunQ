/**
 * TCP Benchmark: Batch notify impact
 *
 * Measures real-world TCP performance with multiple workers
 * consuming jobs from batch pushes. Tracks:
 * - Push throughput (ops/s)
 * - Worker wakeup latency (p50, p95, p99, max)
 * - End-to-end processing throughput (jobs/s)
 * - Time to drain (all jobs processed)
 * - Per-worker distribution (fairness)
 *
 * Run: bun scripts/bench-tcp-batch-notify.ts
 * Requires: bunqueue server running on port 6789
 *   Start with: TCP_PORT=6789 bun src/main.ts
 */

import { Queue, Worker } from '../src/client';

const TCP_PORT = 16789;
const TCP_HOST = 'localhost';

// ─── Server setup ───

async function startServer(): Promise<{ stop: () => void }> {
  const { QueueManager } = await import('../src/application/queueManager');
  const { createTcpServer } = await import('../src/infrastructure/server/tcp');

  const qm = new QueueManager();
  const tcp = createTcpServer(qm, { port: TCP_PORT, hostname: '0.0.0.0' });

  return {
    stop() {
      tcp.stop();
      qm.shutdown();
    },
  };
}

function connOpts(poolSize = 4) {
  return {
    host: TCP_HOST,
    port: TCP_PORT,
    poolSize,
    pingInterval: 0,
    commandTimeout: 60000,
  };
}

function percentile(sorted: number[], p: number): number {
  const idx = Math.floor(sorted.length * p);
  return sorted[Math.min(idx, sorted.length - 1)] ?? 0;
}

function formatOps(n: number): string {
  return n >= 1000 ? `${(n / 1000).toFixed(1)}k` : String(n);
}

// ─── Scenarios ───

interface ScenarioResult {
  name: string;
  jobs: number;
  workers: number;
  concurrency: number;
  pushTimeMs: number;
  pushOps: number;
  drainTimeMs: number;
  drainOps: number;
  totalTimeMs: number;
  totalOps: number;
  perWorker: number[];
  wakeupP50: number;
  wakeupP95: number;
  wakeupP99: number;
  wakeupMax: number;
}

async function runScenario(opts: {
  name: string;
  queueName: string;
  jobCount: number;
  batchSize: number;
  workerCount: number;
  concurrency: number;
  payloadBytes: number;
  pollTimeout: number;
}): Promise<ScenarioResult> {
  const { name, queueName, jobCount, batchSize, workerCount, concurrency, payloadBytes, pollTimeout } = opts;
  const payload = { d: 'x'.repeat(payloadBytes) };

  // Track per-worker stats
  const workerCounts = new Array(workerCount).fill(0);
  const wakeupTimes: number[] = [];
  let totalProcessed = 0;

  // Create workers
  const workers: Worker[] = [];
  for (let w = 0; w < workerCount; w++) {
    const idx = w;
    const worker = new Worker(
      queueName,
      async () => {
        workerCounts[idx]++;
        totalProcessed++;
        return { ok: true };
      },
      {
        connection: connOpts(4),
        concurrency,
        heartbeatInterval: 0,
        batchSize: Math.min(50, concurrency * 2),
        pollTimeout,
      }
    );

    // Track wakeup latency per pull cycle
    const origEmit = worker.emit.bind(worker);
    let lastPullStart = Date.now();
    worker.on('active', () => {
      wakeupTimes.push(Date.now() - lastPullStart);
      lastPullStart = Date.now();
    });

    workers.push(worker);
  }

  // Let workers connect and start polling
  await Bun.sleep(500);

  // Push phase
  const pushQueue = new Queue(queueName, {
    connection: connOpts(8),
    autoBatch: { enabled: true, maxSize: 100, maxDelayMs: 2 },
  });
  await Bun.sleep(200);

  const totalStart = performance.now();
  const pushStart = performance.now();

  // Push in batches
  for (let i = 0; i < jobCount; i += batchSize) {
    const size = Math.min(batchSize, jobCount - i);
    const jobs = Array.from({ length: size }, (_, j) => ({
      name: 'bench',
      data: payload,
    }));
    await pushQueue.addBulk(jobs);
  }

  const pushTimeMs = performance.now() - pushStart;

  // Wait for all jobs to be processed
  const drainStart = performance.now();
  const maxWait = 60_000;
  const deadline = Date.now() + maxWait;

  while (totalProcessed < jobCount && Date.now() < deadline) {
    await Bun.sleep(10);
  }

  const drainTimeMs = performance.now() - drainStart;
  const totalTimeMs = performance.now() - totalStart;

  // Cleanup
  for (const w of workers) {
    await w.close();
  }
  await Bun.sleep(100);
  await pushQueue.close();

  // Compute stats
  const sortedWakeups = wakeupTimes.sort((a, b) => a - b);

  return {
    name,
    jobs: jobCount,
    workers: workerCount,
    concurrency,
    pushTimeMs: Math.round(pushTimeMs),
    pushOps: Math.round((jobCount / pushTimeMs) * 1000),
    drainTimeMs: Math.round(drainTimeMs),
    drainOps: Math.round((jobCount / drainTimeMs) * 1000),
    totalTimeMs: Math.round(totalTimeMs),
    totalOps: Math.round((jobCount / totalTimeMs) * 1000),
    perWorker: workerCounts,
    wakeupP50: percentile(sortedWakeups, 0.5),
    wakeupP95: percentile(sortedWakeups, 0.95),
    wakeupP99: percentile(sortedWakeups, 0.99),
    wakeupMax: sortedWakeups[sortedWakeups.length - 1] ?? 0,
  };
}

// ─── Main ───

async function main() {
  console.log('Starting embedded TCP server...');
  const server = await startServer();
  await Bun.sleep(300);

  console.log('\n══════════════════════════════════════════════════════════════');
  console.log('        bunqueue TCP Benchmark — Batch Notify');
  console.log('══════════════════════════════════════════════════════════════\n');

  const results: ScenarioResult[] = [];

  // Scenario 1: Baseline — 10k jobs, 10 workers, concurrency 5
  results.push(
    await runScenario({
      name: '10k jobs, 10 workers ×5 concurrency',
      queueName: `bench-1-${Date.now()}`,
      jobCount: 10_000,
      batchSize: 500,
      workerCount: 10,
      concurrency: 5,
      payloadBytes: 100,
      pollTimeout: 1000,
    })
  );

  // Scenario 2: Many workers — 10k jobs, 50 workers, concurrency 1
  results.push(
    await runScenario({
      name: '10k jobs, 50 workers ×1 concurrency',
      queueName: `bench-2-${Date.now()}`,
      jobCount: 10_000,
      batchSize: 200,
      workerCount: 50,
      concurrency: 1,
      payloadBytes: 100,
      pollTimeout: 2000,
    })
  );

  // Scenario 3: Heavy — 50k jobs, 20 workers, concurrency 10
  results.push(
    await runScenario({
      name: '50k jobs, 20 workers ×10 concurrency',
      queueName: `bench-3-${Date.now()}`,
      jobCount: 50_000,
      batchSize: 1000,
      workerCount: 20,
      concurrency: 10,
      payloadBytes: 200,
      pollTimeout: 1000,
    })
  );

  // Scenario 4: Small batches, many workers — simulates real API webhooks
  results.push(
    await runScenario({
      name: '10k jobs (batch 10), 30 workers ×3',
      queueName: `bench-4-${Date.now()}`,
      jobCount: 10_000,
      batchSize: 10,
      workerCount: 30,
      concurrency: 3,
      payloadBytes: 50,
      pollTimeout: 1000,
    })
  );

  // Scenario 5: Extreme — 100k jobs, 10 workers, high concurrency
  results.push(
    await runScenario({
      name: '100k jobs, 10 workers ×20 concurrency',
      queueName: `bench-5-${Date.now()}`,
      jobCount: 100_000,
      batchSize: 1000,
      workerCount: 10,
      concurrency: 20,
      payloadBytes: 100,
      pollTimeout: 500,
    })
  );

  // Print results
  console.log('\n══════════════════════════════════════════════════════════════');
  console.log('                        RESULTS');
  console.log('══════════════════════════════════════════════════════════════\n');

  for (const r of results) {
    const processed = r.perWorker.reduce((a, b) => a + b, 0);
    const minW = Math.min(...r.perWorker);
    const maxW = Math.max(...r.perWorker);
    const fairness = minW > 0 ? (minW / maxW).toFixed(2) : '0.00';

    console.log(`─── ${r.name} ───`);
    console.log();
    console.log(`  Throughput`);
    console.log(`    Push:      ${formatOps(r.pushOps)} ops/s  (${r.pushTimeMs}ms)`);
    console.log(`    Drain:     ${formatOps(r.drainOps)} ops/s  (${r.drainTimeMs}ms)`);
    console.log(`    Total:     ${formatOps(r.totalOps)} ops/s  (${r.totalTimeMs}ms)`);
    console.log();
    console.log(`  Wakeup latency`);
    console.log(`    p50: ${r.wakeupP50}ms | p95: ${r.wakeupP95}ms | p99: ${r.wakeupP99}ms | max: ${r.wakeupMax}ms`);
    console.log();
    console.log(`  Distribution (${r.workers} workers)`);
    console.log(`    Processed: ${processed}/${r.jobs}`);
    console.log(`    Per worker: min=${minW} max=${maxW} fairness=${fairness}`);
    console.log(`    Breakdown:  [${r.perWorker.join(', ')}]`);
    console.log();
  }

  // Summary table
  console.log('══════════════════════════════════════════════════════════════');
  console.log('                      SUMMARY TABLE');
  console.log('══════════════════════════════════════════════════════════════\n');
  console.log('Scenario                              | Push    | Total   | p99    | Fairness');
  console.log('--------------------------------------|---------|---------|--------|--------');
  for (const r of results) {
    const minW = Math.min(...r.perWorker);
    const maxW = Math.max(...r.perWorker);
    const fairness = minW > 0 ? (minW / maxW).toFixed(2) : '0.00';
    const name = r.name.padEnd(37).slice(0, 37);
    console.log(
      `${name} | ${formatOps(r.pushOps).padStart(7)} | ${formatOps(r.totalOps).padStart(7)} | ${String(r.wakeupP99 + 'ms').padStart(6)} | ${fairness}`
    );
  }

  console.log();
  server.stop();
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
