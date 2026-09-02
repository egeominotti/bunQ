/**
 * Advanced DLQ Manager
 * Dead Letter Queue operations with auto-retry, metadata, and lifecycle management
 */

import type { Job, JobId } from '../domain/types/job';
import type { JobLocation } from '../domain/types/queue';
import type { Shard } from '../domain/queue/shard';
import type { DlqEntry, DlqConfig, DlqFilter, DlqStats } from '../domain/types/dlq';
import { FailureReason } from '../domain/types/dlq';
import { shardIndex } from '../shared/hash';
import type { SqliteStorage } from '../infrastructure/persistence/sqlite';

/** Context for DLQ operations */
export interface DlqContext {
  shards: Shard[];
  jobIndex: Map<JobId, JobLocation>;
  jobResults: { delete(id: JobId): boolean };
  jobResultQueues: Map<JobId, string>;
  dependencyResults: { releaseConsumer(id: JobId): void };
  customIdMap: {
    get(id: string): JobId | undefined;
    set(id: string, jobId: JobId): void;
    delete(id: string): boolean;
  };
  jobLogs: { delete(id: JobId): boolean };
  jobLogQueues: Map<JobId, string>;
  // Required (nullable): see LockContext.storage — an optional field lets a
  // builder silently drop persistence (#110-class bug).
  storage: SqliteStorage | null;
}

export { processAutoRetry, retryDlqByFilter, retryDlqJob, retryDlqJobs } from './dlqRetry';
export { retryCompletedJobs, type RetryCompletedContext } from './completedRetry';

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

/** Remove one DLQ entry without re-enqueuing it. */
export function removeDlqJob(queue: string, id: JobId, ctx: DlqContext): boolean {
  const shard = ctx.shards[shardIndex(queue)];
  const entries = shard.getDlqEntries(queue).filter((entry) => entry.job.id === id);
  if (entries.length === 0) return false;

  const location = ctx.jobIndex.get(id);
  const terminalJobIds =
    location === undefined || (location.type === 'dlq' && location.queueName === queue) ? [id] : [];

  // Keep the in-memory entry visible until the durable transaction succeeds.
  // This prevents a failed SQLite write from leaving memory and disk out of sync.
  ctx.storage?.purgeDlqEntries(queue, [id], terminalJobIds, false);
  shard.removeAllFromDlq(queue, id);
  cleanupTerminalEntries(queue, entries, terminalJobIds, ctx);
  return true;
}

function cleanupTerminalEntries(
  queue: string,
  entries: readonly DlqEntry[],
  terminalJobIds: readonly JobId[],
  ctx: DlqContext
): void {
  const terminalIds = new Set(terminalJobIds);
  for (const entry of entries) {
    if (!terminalIds.has(entry.job.id)) continue;
    const customId = entry.job.customId;
    if (customId && ctx.customIdMap.get(customId) === entry.job.id) {
      ctx.customIdMap.delete(customId);
    }
  }
  for (const jobId of terminalJobIds) {
    const location = ctx.jobIndex.get(jobId);
    if (location?.type === 'dlq' && location.queueName === queue) ctx.jobIndex.delete(jobId);
    ctx.jobResults.delete(jobId);
    ctx.jobResultQueues.delete(jobId);
    ctx.jobLogs.delete(jobId);
    ctx.jobLogQueues.delete(jobId);
    ctx.dependencyResults.releaseConsumer(jobId);
  }
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

  cleanupTerminalEntries(queue, entries, terminalJobIds, ctx);
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
