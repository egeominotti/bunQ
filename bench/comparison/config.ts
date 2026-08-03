export interface BenchResult {
  name: string;
  opsPerSec: number;
  totalMs: number;
  p99Ms?: number;
}

function positiveInteger(raw: string | undefined, fallback: number, label: string): number {
  const value = Number(raw ?? fallback);
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${label} must be positive`);
  return value;
}

function validPort(raw: string | undefined, fallback: number, label: string): number {
  const value = positiveInteger(raw, fallback, label);
  if (value > 65_535) throw new Error(`${label} must be at most 65535`);
  return value;
}

export const ITERATIONS = positiveInteger(Bun.env.BENCH_ITERATIONS, 10_000, 'BENCH_ITERATIONS');
export const TIMEOUT_MS = positiveInteger(Bun.env.BENCH_TIMEOUT_MS, 120_000, 'BENCH_TIMEOUT_MS');
export const BULK_SIZE = 100;
export const CONCURRENCY = 50;
export const BATCH_PULL = 20;
export const PAYLOAD = { data: 'x'.repeat(100) };
export const BUNQUEUE_HOST = Bun.env.BENCH_BUNQUEUE_HOST ?? '127.0.0.1';
export const BUNQUEUE_PORT = validPort(Bun.env.BENCH_BUNQUEUE_PORT, 6_789, 'BENCH_BUNQUEUE_PORT');
export const REDIS_HOST = Bun.env.BENCH_REDIS_HOST ?? '127.0.0.1';
export const REDIS_PORT = validPort(Bun.env.BENCH_REDIS_PORT, 6_379, 'BENCH_REDIS_PORT');
export const RUN_ID = (Bun.env.BENCH_RUN_ID ?? `${Date.now()}-${process.pid}`).replace(
  /[^a-zA-Z0-9_-]/g,
  '-'
);

export const queueName = (name: string): string => `${name}-${RUN_ID}`;

export const bunqueueConnection = {
  host: BUNQUEUE_HOST,
  port: BUNQUEUE_PORT,
  poolSize: 32,
  pingInterval: 0,
  commandTimeout: 60_000,
};
