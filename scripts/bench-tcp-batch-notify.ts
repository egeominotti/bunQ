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
 * The runner owns its local TCP server and tears it down before returning.
 */

import { Queue, Worker } from '../src/client';
import {
  assertExactCompletion,
  assertExactDeliveries,
  closeAll,
  waitForAuthoritativeCompletion,
} from '../bench/native-benchmark-integrity';
import { type ScenarioResult, printScenarioResults } from './bench-tcp-batch-notify-report';

const TCP_HOST = '127.0.0.1';
const MAX_SCENARIO_JOBS = 100_000;

// ─── Server setup ───

async function startServer(): Promise<{ port: number; stop: () => void }> {
  const { QueueManager } = await import('../src/application/queueManager');
  const { createTcpServer } = await import('../src/infrastructure/server/tcp');

  const qm = new QueueManager({ maxCompletedJobs: MAX_SCENARIO_JOBS });
  const tcp = createTcpServer(qm, { port: 0, hostname: '127.0.0.1' });

  return {
    port: tcp.server.port,
    stop() {
      try {
        tcp.stop();
      } finally {
        qm.shutdown();
      }
    },
  };
}

function connOpts(port: number, poolSize = 4) {
  return {
    host: TCP_HOST,
    port,
    poolSize,
    pingInterval: 0,
    commandTimeout: 60000,
  };
}

function percentile(sorted: number[], p: number): number {
  const idx = Math.floor(sorted.length * p);
  return sorted[Math.min(idx, sorted.length - 1)] ?? 0;
}

// ─── Scenarios ───

async function runScenario(opts: {
  name: string;
  queueName: string;
  jobCount: number;
  batchSize: number;
  workerCount: number;
  concurrency: number;
  payloadBytes: number;
  pollTimeout: number;
  port: number;
}): Promise<ScenarioResult> {
  const {
    name,
    queueName,
    jobCount,
    batchSize,
    workerCount,
    concurrency,
    payloadBytes,
    pollTimeout,
    port,
  } = opts;
  const payload = { d: 'x'.repeat(payloadBytes) };
  const workerCounts = new Array(workerCount).fill(0);
  const wakeupTimes: number[] = [];
  const acceptedIds = new Set<string>();
  const invokedIds = new Set<string>();
  let invocations = 0;
  let workerError: unknown;
  const workers: Worker[] = [];
  let pushQueue: Queue | undefined;

  try {
    for (let index = 0; index < workerCount; index++) {
      const workerIndex = index;
      const worker = new Worker(
        queueName,
        (job) => {
          workerCounts[workerIndex]++;
          invocations++;
          invokedIds.add(job.id);
          return { ok: true };
        },
        {
          embedded: false,
          connection: connOpts(port, 4),
          concurrency,
          heartbeatInterval: 5000,
          batchSize: Math.min(50, concurrency * 2),
          pollTimeout,
        }
      );
      let lastPullStart = Date.now();
      worker.on('active', () => {
        wakeupTimes.push(Date.now() - lastPullStart);
        lastPullStart = Date.now();
      });
      worker.on('error', (error) => {
        workerError = error;
      });
      workers.push(worker);
    }

    await Bun.sleep(500);
    pushQueue = new Queue(queueName, {
      embedded: false,
      connection: connOpts(port, 8),
      autoBatch: { enabled: true, maxSize: 100, maxDelayMs: 2 },
    });
    await Bun.sleep(200);

    const totalStart = performance.now();
    const pushStart = performance.now();
    for (let index = 0; index < jobCount; index += batchSize) {
      const size = Math.min(batchSize, jobCount - index);
      const jobs = Array.from({ length: size }, () => ({ name: 'bench', data: payload }));
      const added = await pushQueue.addBulk(jobs);
      for (const job of added) acceptedIds.add(job.id);
    }
    const pushTimeMs = performance.now() - pushStart;
    const drainStart = performance.now();
    const deadline = Date.now() + 60_000;
    assertExactCompletion(`${name} accepted`, jobCount, acceptedIds.size, deadline);
    await waitForAuthoritativeCompletion({
      label: name,
      expected: jobCount,
      deadline,
      getJobCounts: () => pushQueue.getJobCounts(),
      getWorkerError: () => workerError,
    });
    assertExactCompletion(`${name} invocations`, jobCount, invocations, deadline);
    assertExactDeliveries(name, acceptedIds, invokedIds, invocations, jobCount);
    const drainTimeMs = performance.now() - drainStart;
    const totalTimeMs = performance.now() - totalStart;
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
  } finally {
    await closeAll([...workers, pushQueue]);
  }
}

// ─── Main ───

async function main() {
  console.log('Starting embedded TCP server...');
  const server = await startServer();
  try {
    await Bun.sleep(300);
    console.log('\n══════════════════════════════════════════════════════════════');
    console.log('        bunqueue TCP Benchmark — Batch Notify');
    console.log('══════════════════════════════════════════════════════════════\n');
    const scenarios = [
      {
        name: '10k jobs, 10 workers ×5 concurrency',
        queueName: `bench-1-${Date.now()}`,
        jobCount: 10_000,
        batchSize: 500,
        workerCount: 10,
        concurrency: 5,
        payloadBytes: 100,
        pollTimeout: 1000,
      },
      {
        name: '10k jobs, 50 workers ×1 concurrency',
        queueName: `bench-2-${Date.now()}`,
        jobCount: 10_000,
        batchSize: 200,
        workerCount: 50,
        concurrency: 1,
        payloadBytes: 100,
        pollTimeout: 2000,
      },
      {
        name: '50k jobs, 20 workers ×10 concurrency',
        queueName: `bench-3-${Date.now()}`,
        jobCount: 50_000,
        batchSize: 1000,
        workerCount: 20,
        concurrency: 10,
        payloadBytes: 200,
        pollTimeout: 1000,
      },
      {
        name: '10k jobs (batch 10), 30 workers ×3',
        queueName: `bench-4-${Date.now()}`,
        jobCount: 10_000,
        batchSize: 10,
        workerCount: 30,
        concurrency: 3,
        payloadBytes: 50,
        pollTimeout: 1000,
      },
      {
        name: '100k jobs, 10 workers ×20 concurrency',
        queueName: `bench-5-${Date.now()}`,
        jobCount: MAX_SCENARIO_JOBS,
        batchSize: 1000,
        workerCount: 10,
        concurrency: 20,
        payloadBytes: 100,
        pollTimeout: 500,
      },
    ];
    const results: ScenarioResult[] = [];
    for (const scenario of scenarios) {
      results.push(await runScenario({ ...scenario, port: server.port }));
    }
    printScenarioResults(results);
  } finally {
    server.stop();
  }
}

if (import.meta.main) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
