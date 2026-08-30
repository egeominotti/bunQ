/**
 * Native before/after benchmark for SQLite lifecycle batching.
 *
 * Each measured run uses a fresh database and exercises the public manager
 * path: one durable pushBatch, one pullBatch, and one ackBatch. Results are
 * diagnostic host evidence and must not be published as release benchmarks.
 *
 * Run: bun run bench/sqlite-lifecycle-batching.ts
 */
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { positiveInteger } from './native-benchmark-integrity';

interface Timings {
  pushMs: number;
  pullMs: number;
  ackMs: number;
  totalMs: number;
}

const JOBS = positiveInteger('BENCH_JOBS', 5_000);
const RUNS = positiveInteger('BENCH_RUNS', 5);
const WARMUP_JOBS = Math.min(JOBS, positiveInteger('BENCH_WARMUP_JOBS', 250));

function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)];
}

function round(value: number): number {
  return Math.round(value * 100) / 100;
}

async function runLifecycle(jobCount: number, measured: boolean): Promise<Timings> {
  const directory = mkdtempSync(join(tmpdir(), 'bunqueue-sqlite-lifecycle-bench-'));
  const manager = new QueueManager({
    dataPath: join(directory, 'queue.db'),
    maxCompletedJobs: Math.max(50_000, jobCount + 1),
  });
  const queue = `sqlite-lifecycle-${process.pid}-${Date.now()}`;

  try {
    const inputs = Array.from({ length: jobCount }, (_, index) => ({
      data: { index, payload: 'x'.repeat(64) },
      durable: true,
    }));

    const pushStart = performance.now();
    const ids = await manager.pushBatch(queue, inputs);
    const pushEnd = performance.now();
    if (ids.length !== jobCount) {
      throw new Error(`pushBatch accepted ${ids.length}/${jobCount} jobs`);
    }

    const pulled = await manager.pullBatch(queue, jobCount);
    const pullEnd = performance.now();
    if (pulled.length !== jobCount) {
      throw new Error(`pullBatch delivered ${pulled.length}/${jobCount} jobs`);
    }

    await manager.ackBatch(pulled.map((job) => job.id));
    const ackEnd = performance.now();

    const counts = manager.getQueueJobCounts(queue);
    if (
      counts.waiting !== 0 ||
      counts.prioritized !== 0 ||
      counts.delayed !== 0 ||
      counts.active !== 0 ||
      counts.completed !== jobCount ||
      counts.failed !== 0
    ) {
      throw new Error(`unexpected final counts: ${JSON.stringify(counts)}`);
    }
    const metrics = manager.getQueueMetrics(queue, 'completed');
    if (metrics.meta.count !== jobCount) {
      throw new Error(`completed metrics recorded ${metrics.meta.count}/${jobCount} jobs`);
    }

    const timings = {
      pushMs: pushEnd - pushStart,
      pullMs: pullEnd - pushEnd,
      ackMs: ackEnd - pullEnd,
      totalMs: ackEnd - pushStart,
    };
    if (measured) {
      console.log(
        JSON.stringify({
          type: 'run',
          jobs: jobCount,
          ...Object.fromEntries(Object.entries(timings).map(([key, value]) => [key, round(value)])),
        })
      );
    }
    return timings;
  } finally {
    manager.shutdown();
    rmSync(directory, { recursive: true, force: true });
  }
}

async function main(): Promise<void> {
  console.log(
    JSON.stringify({
      type: 'environment',
      platform: process.platform,
      arch: process.arch,
      bun: Bun.version,
      jobs: JOBS,
      runs: RUNS,
      warmupJobs: WARMUP_JOBS,
    })
  );

  await runLifecycle(WARMUP_JOBS, false);
  const runs: Timings[] = [];
  for (let index = 0; index < RUNS; index++) {
    Bun.gc(true);
    runs.push(await runLifecycle(JOBS, true));
  }

  const summary = {
    pushMs: median(runs.map((run) => run.pushMs)),
    pullMs: median(runs.map((run) => run.pullMs)),
    ackMs: median(runs.map((run) => run.ackMs)),
    totalMs: median(runs.map((run) => run.totalMs)),
  };
  console.log(
    JSON.stringify({
      type: 'median',
      jobs: JOBS,
      ...Object.fromEntries(Object.entries(summary).map(([key, value]) => [key, round(value)])),
      jobsPerSecond: round((JOBS / summary.totalMs) * 1_000),
    })
  );
}

if (import.meta.main) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
