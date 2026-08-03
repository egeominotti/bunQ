import { Queue, Worker } from '../../src/client';
import type { BenchResult } from './config';
import {
  BATCH_PULL,
  BULK_SIZE,
  CONCURRENCY,
  ITERATIONS,
  PAYLOAD,
  TIMEOUT_MS,
  bunqueueConnection,
  queueName,
} from './config';
import { assertExactDeliveries, benchmark, benchmarkParallel, waitFor } from './harness';

export async function runBunqueueBenchmarks(): Promise<BenchResult[]> {
  console.log('\n📦 bunqueue (TCP + SQLite) benchmarks...\n');
  const queue = new Queue(queueName('bench-bunqueue'), {
    embedded: false,
    connection: bunqueueConnection,
  });
  const results: BenchResult[] = [];

  try {
    await queue.waitUntilReady();
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
    await queue.obliterateAsync().catch(() => undefined);
    await queue.close();
  }

  results.push(await runBunqueueProcessing());
  return results;
}

async function runBunqueueProcessing(): Promise<BenchResult> {
  const name = queueName('bench-bunqueue-process');
  const accepted = new Set<string>();
  const invoked = new Set<string>();
  let invocations = 0;
  let workerError: Error | undefined;
  let lastCounts: Awaited<ReturnType<Queue['getJobCountsAsync']>> | undefined;
  const worker = new Worker(
    name,
    (job) => {
      invocations++;
      invoked.add(job.id);
      return { ok: true };
    },
    {
      embedded: false,
      connection: bunqueueConnection,
      concurrency: CONCURRENCY,
      heartbeatInterval: 5_000,
      batchSize: BATCH_PULL,
    }
  );
  worker.on('error', (error) => {
    workerError ??= error instanceof Error ? error : new Error(String(error));
  });
  const queue = new Queue(name, { embedded: false, connection: bunqueueConnection });

  try {
    await worker.waitUntilReady();
    const startedAt = performance.now();
    for (let offset = 0; offset < ITERATIONS; offset += 1_000) {
      const count = Math.min(1_000, ITERATIONS - offset);
      const jobs = await Promise.all(
        Array.from({ length: count }, () => queue.add('process-job', PAYLOAD))
      );
      for (const job of jobs) accepted.add(job.id);
    }

    await waitFor(
      async () => {
        if (workerError) throw workerError;
        lastCounts = await queue.getJobCountsAsync();
        return (
          lastCounts.completed === ITERATIONS &&
          lastCounts.waiting === 0 &&
          lastCounts.prioritized === 0 &&
          lastCounts.active === 0 &&
          lastCounts.failed === 0 &&
          lastCounts.delayed === 0 &&
          lastCounts.paused === 0 &&
          lastCounts['waiting-children'] === 0
        );
      },
      TIMEOUT_MS,
      () => `bunqueue processing timed out: ${JSON.stringify({ invocations, lastCounts })}`
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
    await worker.close(true).catch(() => undefined);
    await queue.obliterateAsync().catch(() => undefined);
    await queue.close();
  }
}
