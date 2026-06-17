/**
 * Monitoring Checks - Periodic threshold/state monitoring
 * Emits dashboard events for queue:idle, queue:threshold,
 * worker:overloaded, storage:size-warning, server:memory-warning
 */

import type { WorkerManager } from './workerManager';
import type { Shard } from '../domain/queue/shard';
import { shardIndex } from '../shared/hash';

/** Monitoring state — tracked across intervals */
export interface MonitoringState {
  /** queue → timestamp when it became idle */
  queueIdleSince: Map<string, number>;
  /** queues that already emitted threshold warning */
  queueThresholdEmitted: Set<string>;
  /** workerId → timestamp when became overloaded */
  workerOverloadedSince: Map<string, number>;
  /** Whether storage warning was already emitted */
  storageWarningEmitted: boolean;
  /** Whether memory warning was already emitted */
  memoryWarningEmitted: boolean;
}

export function createMonitoringState(): MonitoringState {
  return {
    queueIdleSince: new Map(),
    queueThresholdEmitted: new Set(),
    workerOverloadedSince: new Map(),
    storageWarningEmitted: false,
    memoryWarningEmitted: false,
  };
}

/** Config from env vars */
const QUEUE_IDLE_THRESHOLD_MS = parseInt(process.env.QUEUE_IDLE_THRESHOLD_MS ?? '30000', 10);
const QUEUE_SIZE_THRESHOLD = parseInt(process.env.QUEUE_SIZE_THRESHOLD ?? '0', 10); // 0 = disabled
const MEMORY_WARNING_MB = parseInt(process.env.MEMORY_WARNING_MB ?? '0', 10); // 0 = disabled
const STORAGE_WARNING_MB = parseInt(process.env.STORAGE_WARNING_MB ?? '0', 10); // 0 = disabled
const WORKER_OVERLOAD_THRESHOLD_MS = parseInt(
  process.env.WORKER_OVERLOAD_THRESHOLD_MS ?? '30000',
  10
);

export interface MonitoringContext {
  queueNamesCache: Set<string>;
  shards: Shard[];
  processingShards: Map<unknown, { queue: string }>[];
  workerManager: WorkerManager;
  storage: { getSize(): number } | null;
  dashboardEmit?: (event: string, data: Record<string, unknown>) => void;
  state: MonitoringState;
}

/** Check all monitoring conditions — called from cleanup interval (10s) */
export function runMonitoringChecks(ctx: MonitoringContext): void {
  if (!ctx.dashboardEmit) return;
  const now = Date.now();
  checkQueueIdle(ctx, now);
  checkQueueThreshold(ctx);
  checkWorkerOverload(ctx, now);
  checkMemoryPressure(ctx);
  checkStorageSize(ctx);
}

/** Get waiting count for a queue across shards */
function getQueueWaiting(queue: string, shards: Shard[], now: number): number {
  const idx = shardIndex(queue);
  const q = shards[idx].queues.get(queue);
  if (!q) return 0;
  let count = 0;
  for (const job of q.values()) {
    if (job.runAt <= now) count++;
  }
  return count;
}

/** Get active count for a queue across processing shards */
function getQueueActive(queue: string, procShards: Map<unknown, { queue: string }>[]): number {
  let count = 0;
  for (const shard of procShards) {
    for (const job of shard.values()) {
      if (job.queue === queue) count++;
    }
  }
  return count;
}

function checkQueueIdle(ctx: MonitoringContext, now: number): void {
  if (QUEUE_IDLE_THRESHOLD_MS <= 0) return;

  for (const queue of ctx.queueNamesCache) {
    const waiting = getQueueWaiting(queue, ctx.shards, now);
    const active = getQueueActive(queue, ctx.processingShards);

    if (waiting === 0 && active === 0) {
      if (!ctx.state.queueIdleSince.has(queue)) {
        ctx.state.queueIdleSince.set(queue, now);
      } else {
        const since = ctx.state.queueIdleSince.get(queue) ?? now;
        if (now - since >= QUEUE_IDLE_THRESHOLD_MS) {
          ctx.dashboardEmit?.('queue:idle', {
            queue,
            idleSeconds: Math.floor((now - since) / 1000),
          });
          ctx.state.queueIdleSince.delete(queue);
        }
      }
    } else {
      ctx.state.queueIdleSince.delete(queue);
    }
  }
}

function checkQueueThreshold(ctx: MonitoringContext): void {
  if (QUEUE_SIZE_THRESHOLD <= 0) return;
  const now = Date.now();

  for (const queue of ctx.queueNamesCache) {
    const waiting = getQueueWaiting(queue, ctx.shards, now);

    if (waiting >= QUEUE_SIZE_THRESHOLD) {
      if (!ctx.state.queueThresholdEmitted.has(queue)) {
        ctx.dashboardEmit?.('queue:threshold', {
          queue,
          size: waiting,
          threshold: QUEUE_SIZE_THRESHOLD,
        });
        ctx.state.queueThresholdEmitted.add(queue);
      }
    } else {
      ctx.state.queueThresholdEmitted.delete(queue);
    }
  }
}

function checkWorkerOverload(ctx: MonitoringContext, now: number): void {
  if (WORKER_OVERLOAD_THRESHOLD_MS <= 0) return;

  for (const worker of ctx.workerManager.list()) {
    const atCapacity = worker.concurrency > 0 && worker.activeJobs >= worker.concurrency;
    if (atCapacity) {
      if (!ctx.state.workerOverloadedSince.has(worker.id)) {
        ctx.state.workerOverloadedSince.set(worker.id, now);
      } else {
        const since = ctx.state.workerOverloadedSince.get(worker.id) ?? now;
        if (now - since >= WORKER_OVERLOAD_THRESHOLD_MS) {
          ctx.dashboardEmit?.('worker:overloaded', {
            workerId: worker.id,
            name: worker.name,
            activeJobs: worker.activeJobs,
            concurrency: worker.concurrency,
            overloadedSeconds: Math.floor((now - since) / 1000),
          });
          ctx.state.workerOverloadedSince.delete(worker.id);
        }
      }
    } else {
      ctx.state.workerOverloadedSince.delete(worker.id);
    }
  }
}

function checkMemoryPressure(ctx: MonitoringContext): void {
  if (MEMORY_WARNING_MB <= 0) return;
  const mem = process.memoryUsage();
  const heapMB = Math.round(mem.heapUsed / 1024 / 1024);

  if (heapMB >= MEMORY_WARNING_MB) {
    if (!ctx.state.memoryWarningEmitted) {
      ctx.dashboardEmit?.('server:memory-warning', {
        heapUsedMB: heapMB,
        thresholdMB: MEMORY_WARNING_MB,
        rssMB: Math.round(mem.rss / 1024 / 1024),
      });
      ctx.state.memoryWarningEmitted = true;
    }
  } else if (heapMB < MEMORY_WARNING_MB * 0.9) {
    ctx.state.memoryWarningEmitted = false;
  }
}

function checkStorageSize(ctx: MonitoringContext): void {
  if (STORAGE_WARNING_MB <= 0 || !ctx.storage) return;
  const sizeBytes = ctx.storage.getSize();
  const sizeMB = Math.round(sizeBytes / 1024 / 1024);

  if (sizeMB >= STORAGE_WARNING_MB) {
    if (!ctx.state.storageWarningEmitted) {
      ctx.dashboardEmit?.('storage:size-warning', { sizeMB, thresholdMB: STORAGE_WARNING_MB });
      ctx.state.storageWarningEmitted = true;
    }
  } else if (sizeMB < STORAGE_WARNING_MB * 0.9) {
    ctx.state.storageWarningEmitted = false;
  }
}
