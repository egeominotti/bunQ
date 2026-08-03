import { Queue, Worker } from 'bullmq';
import type IORedis from 'ioredis';
import type { BenchResult } from './config';
import { BULK_SIZE, CONCURRENCY, ITERATIONS, PAYLOAD, TIMEOUT_MS, queueName } from './config';
import { assertExactDeliveries, benchmark, benchmarkParallel, waitFor } from './harness';

export async function runBullMqBenchmarks(redis: IORedis): Promise<BenchResult[]> {
  console.log('\n🐂 BullMQ (Redis) benchmarks...\n');
  const queue = new Queue(queueName('bench-bullmq'), { connection: redis });
  const results: BenchResult[] = [];

  try {
    const push = await benchmarkParallel(
      'Push',
      async () => {
        await queue.add('job', PAYLOAD);
      },
      ITERATIONS,
      32
    );
    results.push(push);
    console.log(`  Push: ${push.opsPerSec.toLocaleString()} ops/sec`);

    const jobs = Array.from({ length: BULK_SIZE }, (_, index) => ({
      name: 'bulk-job',
      data: { ...PAYLOAD, index },
    }));
    const bulkCalls = Math.max(1, Math.floor(ITERATIONS / 10));
    const bulk = await benchmark(
      'Bulk Push',
      async () => void (await queue.addBulk(jobs)),
      bulkCalls
    );
    bulk.opsPerSec *= BULK_SIZE;
    results.push(bulk);
    console.log(`  Bulk Push: ${bulk.opsPerSec.toLocaleString()} ops/sec (p99: ${bulk.p99Ms}ms)`);
  } finally {
    await queue.obliterate({ force: true }).catch(() => undefined);
    await queue.close();
  }

  results.push(await runBullMqProcessing(redis));
  return results;
}

async function runBullMqProcessing(redis: IORedis): Promise<BenchResult> {
  const name = queueName('bench-bullmq-process');
  const queue = new Queue(name, { connection: redis });
  const accepted = new Set<string>();
  const invoked = new Set<string>();
  let invocations = 0;
  let workerError: Error | undefined;
  let lastCounts: Record<string, number> | undefined;
  const worker = new Worker(
    name,
    (job) => {
      invocations++;
      invoked.add(String(job.id));
      return { ok: true };
    },
    { connection: redis, concurrency: CONCURRENCY }
  );
  worker.on('error', (error) => {
    workerError ??= error;
  });

  try {
    await worker.waitUntilReady();
    const startedAt = performance.now();
    for (let offset = 0; offset < ITERATIONS; offset += 1_000) {
      const count = Math.min(1_000, ITERATIONS - offset);
      const jobs = await Promise.all(
        Array.from({ length: count }, () => queue.add('process-job', PAYLOAD))
      );
      for (const job of jobs) accepted.add(String(job.id));
    }

    await waitFor(
      async () => {
        if (workerError) throw workerError;
        lastCounts = await queue.getJobCounts(
          'wait',
          'active',
          'completed',
          'failed',
          'delayed',
          'paused',
          'prioritized',
          'waiting-children'
        );
        return (
          lastCounts.completed === ITERATIONS &&
          Object.entries(lastCounts).every(([state, count]) => state === 'completed' || count === 0)
        );
      },
      TIMEOUT_MS,
      () => `BullMQ processing timed out: ${JSON.stringify({ invocations, lastCounts })}`
    );
    if (workerError) throw workerError;
    assertExactDeliveries(accepted, invoked, invocations, ITERATIONS);
    const totalMs = performance.now() - startedAt;
    const result = {
      name: 'Process',
      opsPerSec: Math.round((ITERATIONS / totalMs) * 1000),
      totalMs: Math.round(totalMs),
    };
    console.log(`  Process: ${result.opsPerSec.toLocaleString()} ops/sec (exact ${ITERATIONS})`);
    return result;
  } finally {
    await worker.close().catch(() => undefined);
    await queue.obliterate({ force: true }).catch(() => undefined);
    await queue.close();
  }
}
