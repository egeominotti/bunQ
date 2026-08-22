/**
 * Advanced DLQ Manager
 * Dead Letter Queue operations with auto-retry, metadata, and lifecycle management
 */

import type { Job, JobId } from '../domain/types/job';
import { MAX_TIMELINE_ENTRIES } from '../domain/types/job';
import type { JobLocation } from '../domain/types/queue';
import type { Shard } from '../domain/queue/shard';
import type { DlqEntry, DlqConfig, DlqFilter, DlqStats } from '../domain/types/dlq';
import { FailureReason, setDlqRetryState } from '../domain/types/dlq';
import { shardIndex } from '../shared/hash';
import type { SqliteStorage } from '../infrastructure/persistence/sqlite';

/** Context for DLQ operations */
export interface DlqContext {
  shards: Shard[];
  jobIndex: Map<JobId, JobLocation>;
  jobResults: { delete(id: JobId): boolean };
  jobLogs: { delete(id: JobId): boolean };
  // Required (nullable): see LockContext.storage — an optional field lets a
  // builder silently drop persistence (#110-class bug).
  storage: SqliteStorage | null;
}

export { processAutoRetry, retryDlqByFilter, retryDlqJob, retryDlqJobs } from './dlqRetry';

/** Get jobs from DLQ (backward compatible) */
export function getDlqJobs(queue: string, ctx: DlqContext, count?: number): Job[] {
  const idx = shardIndex(queue);
  return ctx.shards[idx].getDlq(queue, count);
}

/** Get DLQ entries with full metadata */
export function getDlqEntries(queue: string, ctx: DlqContext, filter?: DlqFilter): DlqEntry[] {
  const idx = shardIndex(queue);
  const shard = ctx.shards[idx];

  if (filter) {
    return shard.getDlqFiltered(queue, filter);
  }
  return shard.getDlqEntries(queue);
}

/** Get DLQ statistics */
export function getDlqStats(queue: string, ctx: DlqContext): DlqStats {
  const idx = shardIndex(queue);
  const entries = ctx.shards[idx].getDlqEntries(queue);
  const config = ctx.shards[idx].getDlqConfig(queue);
  const now = Date.now();

  const stats: DlqStats = {
    total: entries.length,
    byReason: {
      [FailureReason.ExplicitFail]: 0,
      [FailureReason.MaxAttemptsExceeded]: 0,
      [FailureReason.Timeout]: 0,
      [FailureReason.Stalled]: 0,
      [FailureReason.TtlExpired]: 0,
      [FailureReason.WorkerLost]: 0,
      [FailureReason.Unknown]: 0,
    },
    byQueue: { [queue]: entries.length },
    pendingRetry: 0,
    expired: 0,
    oldestEntry: null,
    newestEntry: null,
  };

  for (const entry of entries) {
    stats.byReason[entry.reason]++;

    if (entry.nextRetryAt && entry.nextRetryAt <= now && entry.retryCount < config.maxAutoRetries) {
      stats.pendingRetry++;
    }

    if (entry.expiresAt && entry.expiresAt <= now) {
      stats.expired++;
    }

    if (stats.oldestEntry === null || entry.enteredAt < stats.oldestEntry) {
      stats.oldestEntry = entry.enteredAt;
    }
    if (stats.newestEntry === null || entry.enteredAt > stats.newestEntry) {
      stats.newestEntry = entry.enteredAt;
    }
  }

  return stats;
}

/** Purge expired entries from DLQ */
export function purgeExpiredDlq(queue: string, ctx: DlqContext): number {
  const idx = shardIndex(queue);
  const shard = ctx.shards[idx];
  const now = Date.now();

  // Use one timestamp for the snapshot and mutation so every purged entry is
  // represented in the cleanup set.
  const expiredEntries = shard.getExpiredEntries(queue, now);
  if (expiredEntries.length === 0) return 0;

  cleanupPurgedEntries(queue, expiredEntries, ctx, false);
  shard.purgeExpired(queue, now);

  return expiredEntries.length;
}

/** Purge all jobs from DLQ */
export function purgeDlqJobs(queue: string, ctx: DlqContext): number {
  const idx = shardIndex(queue);
  const shard = ctx.shards[idx];
  const entries = shard.getDlqEntries(queue);

  cleanupPurgedEntries(queue, entries, ctx, true);
  shard.clearDlq(queue);
  return entries.length;
}

/** Remove every durable and global reference owned by terminal DLQ entries. */
function cleanupPurgedEntries(
  queue: string,
  entries: readonly DlqEntry[],
  ctx: DlqContext,
  clearQueue: boolean
): void {
  const jobIds = [...new Set(entries.map((entry) => entry.job.id))];
  const terminalJobIds = jobIds.filter((jobId) => {
    const location = ctx.jobIndex.get(jobId);
    return location === undefined || (location.type === 'dlq' && location.queueName === queue);
  });

  // One SQLite transaction removes DLQ rows, any orphan jobs/result rows, and
  // pending buffered inserts before in-memory references become unobservable.
  // A newer live generation can reuse the same custom id; its jobs row and
  // auxiliary state must survive cleanup of the stale terminal generation.
  ctx.storage?.purgeDlqEntries(queue, jobIds, terminalJobIds, clearQueue);

  for (const jobId of terminalJobIds) {
    const location = ctx.jobIndex.get(jobId);
    if (location?.type === 'dlq' && location.queueName === queue) {
      ctx.jobIndex.delete(jobId);
    }
    ctx.jobResults.delete(jobId);
    ctx.jobLogs.delete(jobId);
  }
}

/** Configure DLQ for a queue */
export function configureDlq(queue: string, ctx: DlqContext, config: Partial<DlqConfig>): void {
  const idx = shardIndex(queue);
  ctx.shards[idx].setDlqConfig(queue, config);
}

/** Get DLQ configuration */
export function getDlqConfig(queue: string, ctx: DlqContext): DlqConfig {
  const idx = shardIndex(queue);
  return ctx.shards[idx].getDlqConfig(queue);
}

/** Extended context for retryCompleted */
export interface RetryCompletedContext extends DlqContext {
  completedJobs: { has(id: JobId): boolean; delete(id: JobId): boolean } & Iterable<JobId>;
  completedJobsData: {
    get(id: JobId): Job | undefined;
    delete(id: JobId): boolean;
  };
  jobResults: { delete(id: JobId): boolean };
}

/** Retry completed jobs */
export function retryCompletedJobs(
  queue: string,
  ctx: RetryCompletedContext,
  jobId?: JobId,
  options: { limit?: number; timestamp?: number } = {}
): number {
  if (jobId) {
    if (!ctx.completedJobs.has(jobId)) return 0;
    const job = ctx.completedJobsData.get(jobId) ?? ctx.storage?.getJob(jobId);
    if (job?.queue !== queue) return 0;
    return requeueCompletedJob(job, ctx);
  }

  const limit =
    options.limit === undefined
      ? Number.POSITIVE_INFINITY
      : Number.isFinite(options.limit) && options.limit > 0
        ? Math.floor(options.limit)
        : 0;
  if (limit === 0) return 0;

  const timestamp = options.timestamp ?? Number.POSITIVE_INFINITY;
  let count = 0;
  for (const id of [...ctx.completedJobs]) {
    if (count >= limit) break;
    const job = ctx.completedJobsData.get(id) ?? ctx.storage?.getJob(id);
    if (job?.queue === queue && job.completedAt !== null && job.completedAt <= timestamp) {
      count += requeueCompletedJob(job, ctx);
    }
  }
  return count;
}

/** Requeue a single completed job */
function requeueCompletedJob(job: Job, ctx: RetryCompletedContext): number {
  const now = Date.now();
  const timeline = [...job.timeline];
  if (timeline.length < MAX_TIMELINE_ENTRIES) {
    timeline.push({ state: 'waiting', timestamp: now });
  }
  const requeued: Job = {
    ...job,
    attempts: 0,
    startedAt: null,
    completedAt: null,
    runAt: now,
    progress: 0,
    progressMessage: null,
    lastHeartbeat: now,
    timeline,
  };
  setDlqRetryState(requeued, null);

  // Commit the durable transition before publishing the new in-memory state.
  // A SQLite failure therefore leaves the completed generation authoritative.
  ctx.storage?.requeueCompletedJob(requeued);

  const idx = shardIndex(requeued.queue);
  const shard = ctx.shards[idx];
  shard.getQueue(requeued.queue).push(requeued);
  shard.incrementQueued(requeued.id, false, requeued.createdAt, requeued.queue, requeued.runAt);
  ctx.jobIndex.set(requeued.id, {
    type: 'queue',
    shardIdx: idx,
    queueName: requeued.queue,
  });
  ctx.completedJobs.delete(requeued.id);
  ctx.completedJobsData.delete(requeued.id);
  ctx.jobResults.delete(requeued.id);
  shard.notify(requeued.queue);
  return 1;
}
