/**
 * TCP enqueue-and-process throughput across Worker concurrency levels.
 *
 * The measured interval ends only after the broker reports every accepted job
 * completed. Processor calls are tracked separately so a duplicate delivery or
 * expired ACK invalidates the sample instead of inflating its throughput.
 */
import { randomUUID } from 'node:crypto';
import { Queue, Worker } from '../src/client';
import type { JobCounts } from '../src/client/queue/operations/counts';

const PAYLOAD = { data: 'x'.repeat(100) };
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

export interface TcpProcessSweepCase {
  concurrency: number;
  batchSize: number;
}

export interface TcpProcessSweepOptions extends TcpProcessSweepCase {
  scale: number;
  host?: string;
  port: number;
  heartbeatInterval?: number;
  timeoutMs?: number;
}

export interface TcpProcessSweepResult {
  opsPerSecond: number;
  elapsedMs: number;
  accepted: number;
  invocations: number;
  uniqueInvocations: number;
  duplicateInvocations: number;
  finalCounts: JobCounts;
}

function complete(counts: JobCounts, scale: number): boolean {
  return (
    counts.completed === scale &&
    counts.waiting === 0 &&
    counts.prioritized === 0 &&
    counts.active === 0 &&
    counts.failed === 0 &&
    counts.delayed === 0 &&
    counts.paused === 0 &&
    counts['waiting-children'] === 0
  );
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function cleanupSweep(worker: Worker, queue: Queue): Promise<void> {
  let failure: unknown;
  for (const operation of [
    () => worker.close(true),
    () => queue.obliterateAsync(),
    () => queue.close(),
  ]) {
    try {
      await operation();
    } catch (error) {
      failure ??= error;
    }
  }
  if (failure) throw new Error(`TCP process sweep cleanup failed: ${errorMessage(failure)}`);
}

export async function runTcpProcessSweepCase(
  options: TcpProcessSweepOptions
): Promise<TcpProcessSweepResult> {
  if (!Number.isSafeInteger(options.port) || options.port <= 0 || options.port > 65_535) {
    throw new Error('TCP process sweep port must be an integer between 1 and 65535');
  }
  const host = options.host ?? '127.0.0.1';
  const heartbeatInterval = options.heartbeatInterval ?? 5_000;
  const timeoutMs = options.timeoutMs ?? 120_000;
  const queueName = `sweep-${options.concurrency}-${options.batchSize}-${randomUUID()}`;
  const connection = {
    host,
    port: options.port,
    poolSize: 32,
    pingInterval: 0,
    commandTimeout: 60_000,
  };
  const acceptedIds = new Set<string>();
  const invokedIds = new Set<string>();
  let invocations = 0;
  let workerError: Error | undefined;

  const worker = new Worker(
    queueName,
    (job) => {
      invocations++;
      invokedIds.add(job.id);
      return { ok: true };
    },
    {
      embedded: false,
      connection,
      concurrency: options.concurrency,
      heartbeatInterval,
      batchSize: options.batchSize,
    }
  );
  worker.on('error', (error) => {
    workerError ??= error instanceof Error ? error : new Error(String(error));
  });
  const queue = new Queue(queueName, { embedded: false, connection });
  let result: TcpProcessSweepResult | undefined;
  let failure: unknown;

  try {
    await sleep(300);
    const startedAt = performance.now();
    for (let index = 0; index < options.scale; index += 500) {
      const size = Math.min(500, options.scale - index);
      const jobs = await Promise.all(
        Array.from({ length: size }, (_, offset) =>
          queue.add('job', { ...PAYLOAD, index: index + offset })
        )
      );
      for (const job of jobs) acceptedIds.add(job.id);
    }

    const deadline = Date.now() + timeoutMs;
    let counts = await queue.getJobCounts();
    while (!complete(counts, options.scale)) {
      if (workerError) throw workerError;
      if (Date.now() >= deadline) {
        throw new Error(
          `TCP process sweep timed out: ${JSON.stringify({
            scale: options.scale,
            invocations,
            uniqueInvocations: invokedIds.size,
            counts,
          })}`
        );
      }
      await sleep(10);
      counts = await queue.getJobCounts();
    }
    const elapsedMs = performance.now() - startedAt;

    if (workerError) throw workerError;
    if (acceptedIds.size !== options.scale) {
      throw new Error(`Broker accepted ${acceptedIds.size}/${options.scale} unique jobs`);
    }
    if (invokedIds.size !== options.scale) {
      throw new Error(`Worker invoked ${invokedIds.size}/${options.scale} unique jobs`);
    }
    for (const id of acceptedIds) {
      if (!invokedIds.has(id)) throw new Error(`Accepted job ${id} was never processed`);
    }
    if (invocations !== options.scale) {
      throw new Error(`Worker delivered ${invocations - options.scale} duplicate invocations`);
    }

    result = {
      opsPerSecond: Math.round((options.scale / elapsedMs) * 1000),
      elapsedMs,
      accepted: acceptedIds.size,
      invocations,
      uniqueInvocations: invokedIds.size,
      duplicateInvocations: invocations - invokedIds.size,
      finalCounts: counts,
    };
  } catch (error) {
    failure = error;
  }

  try {
    await cleanupSweep(worker, queue);
  } catch (error) {
    failure ??= error;
  }
  if (failure) throw failure;
  if (!result) throw new Error('TCP process sweep produced no result');
  return result;
}

function positiveInteger(raw: string | undefined, fallback: number, label: string): number {
  const value = Number(raw ?? fallback);
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${label} must be positive`);
  return value;
}

function casesFromEnv(raw: string | undefined): TcpProcessSweepCase[] {
  return (raw ?? '10:20,50:50,100:100,200:200').split(',').map((entry) => {
    const [concurrencyRaw, batchSizeRaw, extra] = entry.split(':');
    if (extra !== undefined) throw new Error(`Invalid BENCH_CASES entry: ${entry}`);
    return {
      concurrency: positiveInteger(concurrencyRaw, 0, 'concurrency'),
      batchSize: positiveInteger(batchSizeRaw, 0, 'batchSize'),
    };
  });
}

export async function main(): Promise<void> {
  const scale = positiveInteger(Bun.env.BENCH_SCALE, 5_000, 'BENCH_SCALE');
  const port = positiveInteger(Bun.env.BENCH_PORT, 6_789, 'BENCH_PORT');
  if (port > 65_535) throw new Error('BENCH_PORT must be at most 65535');
  const heartbeatInterval = positiveInteger(
    Bun.env.BENCH_HEARTBEAT_MS,
    5_000,
    'BENCH_HEARTBEAT_MS'
  );
  const timeoutMs = positiveInteger(Bun.env.BENCH_TIMEOUT_MS, 120_000, 'BENCH_TIMEOUT_MS');
  const host = Bun.env.BENCH_HOST ?? '127.0.0.1';
  const cases = casesFromEnv(Bun.env.BENCH_CASES);

  console.log(
    `\nTCP process throughput (scale=${scale}, heartbeat=${heartbeatInterval}ms, ${host}:${port})\n`
  );
  console.log('concurrency  batchSize   process(ops/s)   completed   duplicates');
  for (const entry of cases) {
    const result = await runTcpProcessSweepCase({
      ...entry,
      scale,
      host,
      port,
      heartbeatInterval,
      timeoutMs,
    });
    console.log(
      `${String(entry.concurrency).padStart(11)}  ${String(entry.batchSize).padStart(9)}   ` +
        `${String(result.opsPerSecond).padStart(13)}   ${String(result.finalCounts.completed).padStart(9)}   ` +
        `${String(result.duplicateInvocations).padStart(10)}`
    );
  }
  console.log('');
}

if (import.meta.main) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
