import { hostname } from 'node:os';
import type { PostgresStorageConfig } from './types';

function integerAtLeast(value: number | undefined, fallback: number, minimum: number): number {
  if (!Number.isFinite(value)) return fallback;
  return Math.max(minimum, Math.floor(value!));
}

export function resolvePostgresRuntimeConfig(config: PostgresStorageConfig) {
  const url = new URL(config.url);
  if (url.protocol !== 'postgres:' && url.protocol !== 'postgresql:') {
    throw new Error('PostgreSQL storage requires a postgres:// or postgresql:// URL');
  }
  if (
    config.maxQueueEvents !== undefined &&
    Number.isFinite(config.maxQueueEvents) &&
    config.maxQueueEvents < 1
  ) {
    throw new Error('PostgreSQL storage requires maxQueueEvents to be at least 1');
  }
  const poolSize = integerAtLeast(config.poolSize, 4, 2);
  const maxConcurrentOperations = integerAtLeast(config.maxConcurrentOperations, 16, 1);
  return {
    url: config.url,
    namespace: config.namespace?.trim() || 'default',
    brokerId:
      config.brokerId?.trim() || `${hostname()}:${process.pid}:${Bun.randomUUIDv7().slice(-12)}`,
    brokerSessionId: Bun.randomUUIDv7(),
    poolSize,
    leaseDurationMs: integerAtLeast(config.leaseDurationMs, 30_000, 1000),
    pollIntervalMs: integerAtLeast(config.pollIntervalMs, 250, 25),
    statementTimeoutMs: integerAtLeast(config.statementTimeoutMs, 30_000, 1),
    lockTimeoutMs: integerAtLeast(config.lockTimeoutMs, 5_000, 1),
    idleTransactionTimeoutMs: integerAtLeast(config.idleTransactionTimeoutMs, 30_000, 1),
    maxConcurrentOperations,
    maxQueuedOperations: integerAtLeast(config.maxQueuedOperations, 128, 0),
    maxSnapshotJobs: integerAtLeast(config.maxSnapshotJobs, 100_000, 1),
    maxSnapshotPayloadBytes: integerAtLeast(config.maxSnapshotPayloadBytes, 256 * 1024 * 1024, 1),
    maxQueueEvents: integerAtLeast(config.maxQueueEvents, 10_000, 1),
    maxMetricDataPoints: integerAtLeast(config.maxMetricDataPoints, 20_160, 0),
    maxCompletedJobs: integerAtLeast(config.maxCompletedJobs, 50_000, 1),
    maxJobResults: integerAtLeast(config.maxJobResults, 10_000, 0),
  };
}
