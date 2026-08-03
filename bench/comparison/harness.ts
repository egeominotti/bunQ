import type { BenchResult } from './config';

export async function benchmark(
  name: string,
  operation: () => Promise<void>,
  iterations: number
): Promise<BenchResult> {
  const latencies: number[] = [];
  for (let index = 0; index < Math.min(100, Math.floor(iterations / 10)); index++) {
    await operation();
  }

  const startedAt = performance.now();
  for (let index = 0; index < iterations; index++) {
    const operationStartedAt = performance.now();
    await operation();
    latencies.push(performance.now() - operationStartedAt);
  }
  const totalMs = performance.now() - startedAt;
  latencies.sort((left, right) => left - right);
  const p99Index = Math.min(latencies.length - 1, Math.floor(latencies.length * 0.99));

  return {
    name,
    opsPerSec: Math.round((iterations / totalMs) * 1000),
    totalMs: Math.round(totalMs),
    p99Ms: Math.round(latencies[p99Index] * 100) / 100,
  };
}

export async function benchmarkParallel(
  name: string,
  operation: () => Promise<void>,
  iterations: number,
  concurrency: number
): Promise<BenchResult> {
  for (let index = 0; index < Math.min(100, Math.floor(iterations / 10)); index++) {
    await operation();
  }

  let scheduled = 0;
  const startedAt = performance.now();
  await Promise.all(
    Array.from({ length: concurrency }, async () => {
      while (scheduled < iterations) {
        scheduled++;
        await operation();
      }
    })
  );
  const totalMs = performance.now() - startedAt;
  return {
    name,
    opsPerSec: Math.round((iterations / totalMs) * 1000),
    totalMs: Math.round(totalMs),
  };
}

export async function waitFor(
  predicate: () => Promise<boolean>,
  timeoutMs: number,
  timeoutMessage: () => string
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await predicate())) {
    if (Date.now() >= deadline) throw new Error(timeoutMessage());
    await Bun.sleep(10);
  }
}

export function assertExactDeliveries(
  accepted: Set<string>,
  invoked: Set<string>,
  invocations: number,
  expected: number
): void {
  if (accepted.size !== expected) throw new Error(`Accepted ${accepted.size}/${expected} IDs`);
  if (invoked.size !== expected) throw new Error(`Invoked ${invoked.size}/${expected} unique IDs`);
  for (const id of accepted) {
    if (!invoked.has(id)) throw new Error(`Accepted job ${id} was not processed`);
  }
  if (invocations !== expected) {
    throw new Error(`Observed ${invocations - expected} duplicate processor invocations`);
  }
}
