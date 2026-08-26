function positiveInteger(name: string, fallback: number): number {
  const raw = Bun.env[name];
  const value = raw === undefined ? fallback : Number(raw);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive safe integer, received ${raw}`);
  }
  return value;
}

function integerList(name: string, fallback: readonly number[]): number[] {
  const raw = Bun.env[name];
  if (!raw) return [...fallback];
  const values = raw.split(',').map((value) => Number(value.trim()));
  if (values.length === 0 || values.some((value) => !Number.isSafeInteger(value) || value <= 0)) {
    throw new Error(`${name} must be a comma-separated list of positive integers`);
  }
  return [...new Set(values)];
}

function postgresMemory(name: string, fallback: string): string {
  const value = Bun.env[name] ?? fallback;
  if (!/^[1-9]\d*(?:kB|MB|GB)$/.test(value)) {
    throw new Error(`${name} must use PostgreSQL memory syntax such as 4MB, received ${value}`);
  }
  return value;
}

export const versions = integerList('BUNQUEUE_PG_BENCH_VERSIONS', [15, 16, 17, 18]);
export const brokerCounts = integerList('BUNQUEUE_PG_BENCH_BROKERS', [1, 2, 4]);
export const jobs = positiveInteger('BUNQUEUE_PG_BENCH_JOBS', 10_000);
export const batchSize = positiveInteger('BUNQUEUE_PG_BENCH_BATCH_SIZE', 100);
export const producerConnections = positiveInteger('BUNQUEUE_PG_BENCH_PRODUCERS', 4);
export const consumerConnections = positiveInteger('BUNQUEUE_PG_BENCH_CONSUMERS', 16);
export const poolSize = positiveInteger('BUNQUEUE_PG_BENCH_POOL_SIZE', 10);
export const pollIntervalMs = positiveInteger('BUNQUEUE_PG_BENCH_POLL_INTERVAL_MS', 250);
export const warmups = positiveInteger('BUNQUEUE_PG_BENCH_WARMUPS', 1);
export const runs = positiveInteger('BUNQUEUE_PG_BENCH_RUNS', 7);
export const workMem = postgresMemory('BUNQUEUE_PG_BENCH_WORK_MEM', '4MB');
