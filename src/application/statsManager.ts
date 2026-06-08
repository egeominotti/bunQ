/**
 * Stats Manager - Queue statistics and memory management
 * Provides system metrics and memory compaction utilities
 */

import { SHARD_COUNT, shardIndex } from '../shared/hash';
import type { StatsContext } from './types';

/** Count jobs belonging to `queueName` across one or more job iterables. */
function countByQueue(sources: Array<Iterable<{ queue: string }>>, queueName: string): number {
  let count = 0;
  for (const src of sources) {
    for (const job of src) if (job.queue === queueName) count++;
  }
  return count;
}

export interface QueueStats {
  waiting: number;
  prioritized: number;
  delayed: number;
  active: number;
  dlq: number;
  completed: number;
  'waiting-children': number;
  totalPushed: bigint;
  totalPulled: bigint;
  totalCompleted: bigint;
  totalFailed: bigint;
  uptime: number;
  cronJobs: number;
  cronPending: number;
}

export interface PerQueueStats {
  waiting: number;
  prioritized: number;
  delayed: number;
  active: number;
  dlq: number;
}

export interface MemoryStats {
  jobIndex: number;
  completedJobs: number;
  jobResults: number;
  jobLogs: number;
  customIdMap: number;
  jobLocks: number;
  clientJobs: number;
  clientJobsTotal: number;
  pendingDepChecks: number;
  stalledCandidates: number;
  processingTotal: number;
  queuedTotal: number;
  waitingDepsTotal: number;
  temporalIndexTotal: number;
  delayedHeapTotal: number;
}

/**
 * Get queue statistics - uses running counters + priority scan
 */
export function getStats(
  ctx: StatsContext,
  cronScheduler: { getStats(): { total: number; pending: number } }
): QueueStats {
  let waiting = 0,
    prioritized = 0,
    delayed = 0,
    active = 0,
    dlq = 0,
    waitingChildren = 0;

  const now = Date.now();

  for (let i = 0; i < SHARD_COUNT; i++) {
    const shardStats = ctx.shards[i].getStats();
    delayed += shardStats.delayedJobs;
    dlq += shardStats.dlqJobs;
    active += ctx.processingShards[i].size;
    // getJobState reports BOTH waitingChildren (flow parents) and waitingDeps
    // (jobs blocked on dependsOn) as state 'waiting-children', and getJobs lists
    // both — so the count must include both or it undercounts vs state/list (#95 class).
    waitingChildren += ctx.shards[i].waitingChildren.size + ctx.shards[i].waitingDeps.size;

    // Scan queues to split waiting vs prioritized (BullMQ v5 compat)
    for (const queue of ctx.shards[i].queues.values()) {
      for (const job of queue.values()) {
        if (job.runAt <= now) {
          if (job.priority > 0) {
            prioritized++;
          } else {
            waiting++;
          }
        }
      }
    }
  }

  const cronStats = cronScheduler.getStats();
  return {
    waiting,
    prioritized,
    delayed,
    active,
    dlq,
    completed: ctx.completedJobs.size,
    'waiting-children': waitingChildren,
    totalPushed: ctx.metrics.totalPushed.value,
    totalPulled: ctx.metrics.totalPulled.value,
    totalCompleted: ctx.metrics.totalCompleted.value,
    totalFailed: ctx.metrics.totalFailed.value,
    uptime: Date.now() - ctx.startTime,
    cronJobs: cronStats.total,
    cronPending: cronStats.pending,
  };
}

/**
 * Get detailed memory statistics for debugging memory issues.
 * Returns counts of entries in all major collections.
 */
export function getMemoryStats(ctx: StatsContext): MemoryStats {
  let processingTotal = 0;
  let queuedTotal = 0;
  let waitingDepsTotal = 0;
  let temporalIndexTotal = 0;
  let delayedHeapTotal = 0;

  for (let i = 0; i < SHARD_COUNT; i++) {
    processingTotal += ctx.processingShards[i].size;
    const shardStats = ctx.shards[i].getStats();
    queuedTotal += shardStats.queuedJobs;
    waitingDepsTotal += ctx.shards[i].waitingDeps.size;
    // Get internal structure sizes
    const internalSizes = ctx.shards[i].getInternalSizes();
    temporalIndexTotal += internalSizes.temporalIndex;
    delayedHeapTotal += internalSizes.delayedHeap;
  }

  // Count total jobs across all clients
  let clientJobsTotal = 0;
  for (const jobs of ctx.clientJobs.values()) {
    clientJobsTotal += jobs.size;
  }

  return {
    jobIndex: ctx.jobIndex.size,
    completedJobs: ctx.completedJobs.size,
    jobResults: ctx.jobResults.size,
    jobLogs: ctx.jobLogs.size,
    customIdMap: ctx.customIdMap.size,
    jobLocks: ctx.jobLocks.size,
    clientJobs: ctx.clientJobs.size,
    clientJobsTotal,
    pendingDepChecks: ctx.pendingDepChecks.size,
    stalledCandidates: ctx.stalledCandidates.size,
    processingTotal,
    queuedTotal,
    waitingDepsTotal,
    temporalIndexTotal,
    delayedHeapTotal,
  };
}

/**
 * Get per-queue statistics by iterating shards to aggregate per queue name.
 * Uses shard hashing to look up each queue in its designated shard.
 */
export function getPerQueueStats(
  ctx: StatsContext,
  queueNames: Set<string>
): Map<string, PerQueueStats> {
  const result = new Map<string, PerQueueStats>();
  const now = Date.now();

  for (const name of queueNames) {
    const idx = shardIndex(name);
    const shard = ctx.shards[idx];
    const queue = shard.queues.get(name);

    let waiting = 0;
    let prioritized = 0;
    let delayed = 0;
    if (queue) {
      for (const job of queue.values()) {
        if (job.runAt > now) {
          delayed++;
        } else if (job.priority > 0) {
          prioritized++;
        } else {
          waiting++;
        }
      }
    }

    const dlq = shard.getDlqCount(name);

    result.set(name, { waiting, prioritized, delayed, active: 0, dlq });
  }

  // Count active jobs per queue from all processing shards
  for (let i = 0; i < SHARD_COUNT; i++) {
    for (const job of ctx.processingShards[i].values()) {
      const entry = result.get(job.queue);
      if (entry) {
        entry.active++;
      }
    }
  }

  return result;
}

/**
 * Get job counts for a specific queue
 */
export function getQueueJobCounts(
  queueName: string,
  ctx: StatsContext
): {
  waiting: number;
  prioritized: number;
  delayed: number;
  active: number;
  completed: number;
  failed: number;
  'waiting-children': number;
  totalCompleted: number;
  totalFailed: number;
} {
  const idx = shardIndex(queueName);
  const shard = ctx.shards[idx];
  const queue = shard.queues.get(queueName);
  const now = Date.now();

  // Count waiting vs prioritized vs delayed jobs in the queue
  let waiting = 0;
  let prioritized = 0;
  let delayed = 0;
  if (queue) {
    for (const job of queue.values()) {
      if (job.runAt > now) {
        delayed++;
      } else if (job.priority > 0) {
        prioritized++;
      } else {
        waiting++;
      }
    }
  }

  // Count active jobs (processing) for this queue
  let active = 0;
  for (const procShard of ctx.processingShards) {
    for (const job of procShard.values()) {
      if (job.queue === queueName) {
        active++;
      }
    }
  }

  // Count completed jobs for this queue
  let completed = 0;
  for (const [jobId, loc] of ctx.jobIndex) {
    if (loc.type === 'completed' && loc.queueName === queueName && ctx.completedJobs.has(jobId)) {
      completed++;
    }
  }

  // Count failed (DLQ) jobs for this queue
  const failed = shard.getDlq(queueName).length;

  // Count waiting-children jobs. getJobState/getJobs treat BOTH waitingChildren
  // (flow parents) and waitingDeps (jobs blocked on dependsOn) as 'waiting-children',
  // so count both to stay consistent with state/list (#95 class).
  const waitingChildrenCount = countByQueue(
    [shard.waitingChildren.values(), shard.waitingDeps.values()],
    queueName
  );

  // Per-queue cumulative counters
  const perQueue = ctx.perQueueMetrics?.get(queueName);
  const totalCompleted = Number(perQueue?.totalCompleted ?? 0n);
  const totalFailed = Number(perQueue?.totalFailed ?? 0n);

  return {
    waiting,
    prioritized,
    delayed,
    active,
    completed,
    failed,
    'waiting-children': waitingChildrenCount,
    totalCompleted,
    totalFailed,
  };
}

/**
 * Force compact all collections to reduce memory usage.
 * Use after large batch operations or when memory pressure is high.
 */
export function compactMemory(ctx: StatsContext): void {
  // Compact priority queues that have high stale ratios
  for (let i = 0; i < SHARD_COUNT; i++) {
    for (const q of ctx.shards[i].queues.values()) {
      if (q.needsCompaction(0.1)) {
        // More aggressive: 10% stale threshold
        q.compact();
      }
    }
  }

  // Clean up empty client tracking entries
  for (const [clientId, jobs] of ctx.clientJobs) {
    if (jobs.size === 0) {
      ctx.clientJobs.delete(clientId);
    }
  }

  // Clean orphaned job locks (jobs no longer in processing)
  for (const jobId of ctx.jobLocks.keys()) {
    const loc = ctx.jobIndex.get(jobId);
    if (loc?.type !== 'processing') {
      ctx.jobLocks.delete(jobId);
    }
  }
}
