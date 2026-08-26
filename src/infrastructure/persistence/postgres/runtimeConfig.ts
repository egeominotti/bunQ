import { hostname } from 'node:os';
import type { PostgresStorageConfig } from './types';

export function resolvePostgresRuntimeConfig(config: PostgresStorageConfig) {
  const url = new URL(config.url);
  if (url.protocol !== 'postgres:' && url.protocol !== 'postgresql:') {
    throw new Error('PostgreSQL storage requires a postgres:// or postgresql:// URL');
  }
  if (config.maxQueueEvents !== undefined && config.maxQueueEvents < 1) {
    throw new Error('PostgreSQL storage requires maxQueueEvents to be at least 1');
  }
  return {
    url: config.url,
    namespace: config.namespace?.trim() || 'default',
    brokerId:
      config.brokerId?.trim() || `${hostname()}:${process.pid}:${Bun.randomUUIDv7().slice(-12)}`,
    poolSize: Math.max(2, config.poolSize ?? 10),
    leaseDurationMs: Math.max(1000, config.leaseDurationMs ?? 30_000),
    pollIntervalMs: Math.max(25, config.pollIntervalMs ?? 250),
    maxQueueEvents: Math.max(1, Math.floor(config.maxQueueEvents ?? 10_000)),
    maxMetricDataPoints: Math.max(0, Math.floor(config.maxMetricDataPoints ?? 20_160)),
    maxCompletedJobs: Math.max(1, Math.floor(config.maxCompletedJobs ?? 50_000)),
    maxJobResults: Math.max(0, Math.floor(config.maxJobResults ?? 10_000)),
  };
}
