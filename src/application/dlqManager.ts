/**
 * Advanced DLQ Manager
 * Dead Letter Queue operations with auto-retry, metadata, and lifecycle management
 */

import type { Job, JobId } from '../domain/types/job';
import { MAX_TIMELINE_ENTRIES } from '../domain/types/job';
import type { JobLocation } from '../domain/types/queue';
import type { Shard } from '../domain/queue/shard';
import type { DlqEntry, DlqConfig, DlqFilter, DlqStats } from '../domain/types/dlq';
import { FailureReason, scheduleNextRetry } from '../domain/types/dlq';
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

/** Retry a single job from DLQ */
export function retryDlqJob(queue: string, jobId: JobId, ctx: DlqContext): Job | null {
  const idx = shardIndex(queue);
  const shard = ctx.shards[idx];
  const now = Date.now();

  const entry = shard.removeFromDlq(queue, jobId);
  if (!entry) return null;

  // Delete from SQLite
  ctx.storage?.deleteDlqEntry(jobId);

  const job = entry.job;
  job.attempts = 0;
  job.runAt = now;
  job.stallCount = 0;
  job.lastHeartbeat = now;
  if (job.timeline.length < MAX_TIMELINE_ENTRIES) {
    job.timeline.push({ state: 'waiting', timestamp: now });
  }

  shard.getQueue(queue).push(job);
  const isDelayed = job.runAt > now;
  shard.incrementQueued(job.id, isDelayed, job.createdAt, queue, job.runAt);
  ctx.jobIndex.set(job.id, { type: 'queue', shardIdx: idx, queueName: queue });
  // Re-insert jobs row (deleted when job entered DLQ) so retry survives restart
  ctx.storage?.insertJob(job, true);

  return job;
}

/** Retry jobs from DLQ (backward compatible) */
export function retryDlqJobs(
  queue: string,
  ctx: DlqContext,
  jobId?: JobId,
  limit?: number
): number {
  if (jobId) {
    return retryDlqJob(queue, jobId, ctx) ? 1 : 0;
  }

  // Bounded retry (#111-class: `retryJobs({ state:'failed', count })`). Retry
  // only the first `limit` DLQ entries, reusing the tested single-entry path so
  // the remaining entries stay in the DLQ (memory + SQLite) instead of the
  // clear-all fast path below wrongly draining the whole queue. Any provided
  // `limit` engages the bounded path; a negative/NaN/fractional cap is clamped
  // to a safe non-negative integer so it can never fall through to clear-all
  // (skeptic: `limit >= 0` let `-1`/`NaN` drain the entire DLQ). `undefined`
  // (no cap requested) still means "retry all" via the fast path below.
  if (limit !== undefined) {
    const cap = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : 0;
    const idx = shardIndex(queue);
    const shard = ctx.shards[idx];
    // Snapshot ids first: retryDlqJob mutates the DLQ as it goes.
    const ids = shard
      .getDlqEntries(queue)
      .slice(0, cap)
      .map((e) => e.job.id);
    let retried = 0;
    for (const id of ids) {
      if (retryDlqJob(queue, id, ctx)) retried++;
    }
    return retried;
  }

  const idx = shardIndex(queue);
  const shard = ctx.shards[idx];
  const entries = shard.getDlqEntries(queue);
  const count = entries.length;

  // Clear all entries from memory
  shard.clearDlq(queue);
  // Clear from SQLite
  ctx.storage?.clearDlqQueue(queue);

  const now = Date.now();
  for (const entry of entries) {
    const job = entry.job;
    job.attempts = 0;
    job.runAt = now;
    job.stallCount = 0;
    job.lastHeartbeat = now;
    if (job.timeline.length < MAX_TIMELINE_ENTRIES) {
      job.timeline.push({ state: 'waiting', timestamp: now });
    }

    shard.getQueue(queue).push(job);
    const isDelayed = job.runAt > now;
    shard.incrementQueued(job.id, isDelayed, job.createdAt, queue, job.runAt);
    ctx.jobIndex.set(job.id, { type: 'queue', shardIdx: idx, queueName: queue });
    ctx.storage?.insertJob(job, true);
  }

  return count;
}

/** Retry jobs by filter */
export function retryDlqByFilter(queue: string, ctx: DlqContext, filter: DlqFilter): number {
  const idx = shardIndex(queue);
  const shard = ctx.shards[idx];
  const entries = shard.getDlqFiltered(queue, filter);

  let count = 0;
  const now = Date.now();

  for (const entry of entries) {
    const removed = shard.removeFromDlq(queue, entry.job.id);
    if (!removed) continue;

    // Delete from SQLite
    ctx.storage?.deleteDlqEntry(entry.job.id);

    const job = entry.job;
    job.attempts = 0;
    job.runAt = now;
    job.stallCount = 0;
    job.lastHeartbeat = now;
    if (job.timeline.length < MAX_TIMELINE_ENTRIES) {
      job.timeline.push({ state: 'waiting', timestamp: now });
    }

    shard.getQueue(queue).push(job);
    const isDelayed = job.runAt > now;
    shard.incrementQueued(job.id, isDelayed, job.createdAt, queue, job.runAt);
    ctx.jobIndex.set(job.id, { type: 'queue', shardIdx: idx, queueName: queue });
    ctx.storage?.insertJob(job, true);
    count++;
  }

  return count;
}

/** Process auto-retry for a queue */
export function processAutoRetry(queue: string, ctx: DlqContext): number {
  const idx = shardIndex(queue);
  const shard = ctx.shards[idx];
  const config = shard.getDlqConfig(queue);

  if (!config.autoRetry) return 0;

  const now = Date.now();
  const entries = shard.getAutoRetryEntries(queue, now);

  let count = 0;
  for (const entry of entries) {
    // Update retry tracking
    scheduleNextRetry(entry, config);

    // Remove from DLQ
    const removed = shard.removeFromDlq(queue, entry.job.id);
    if (!removed) continue;

    // Re-queue job
    const job = entry.job;
    job.attempts = 0;
    job.runAt = now;
    job.stallCount = 0;
    job.lastHeartbeat = now;
    if (job.timeline.length < MAX_TIMELINE_ENTRIES) {
      job.timeline.push({ state: 'waiting', timestamp: now });
    }

    shard.getQueue(queue).push(job);
    const isDelayed = job.runAt > now;
    shard.incrementQueued(job.id, isDelayed, job.createdAt, queue, job.runAt);
    ctx.jobIndex.set(job.id, { type: 'queue', shardIdx: idx, queueName: queue });
    ctx.storage?.insertJob(job, true);
    count++;
  }

  return count;
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
  jobResults: { delete(id: JobId): boolean };
}

/** Retry completed jobs */
export function retryCompletedJobs(
  queue: string,
  ctx: RetryCompletedContext,
  jobId?: JobId
): number {
  if (jobId) {
    if (!ctx.completedJobs.has(jobId)) return 0;
    const job = ctx.storage?.getJob(jobId);
    if (job?.queue !== queue) return 0;
    return requeueCompletedJob(job, ctx);
  }

  let count = 0;
  for (const id of ctx.completedJobs) {
    const job = ctx.storage?.getJob(id);
    if (job?.queue === queue) count += requeueCompletedJob(job, ctx);
  }
  return count;
}

/** Requeue a single completed job */
function requeueCompletedJob(job: Job, ctx: RetryCompletedContext): number {
  job.attempts = 0;
  job.startedAt = null;
  job.completedAt = null;
  job.runAt = Date.now();
  job.progress = 0;
  if (job.timeline.length < MAX_TIMELINE_ENTRIES) {
    job.timeline.push({ state: 'waiting', timestamp: job.runAt });
  }

  const idx = shardIndex(job.queue);
  const shard = ctx.shards[idx];
  shard.getQueue(job.queue).push(job);
  shard.incrementQueued(job.id, false, job.createdAt, job.queue, job.runAt);
  ctx.jobIndex.set(job.id, { type: 'queue', shardIdx: idx, queueName: job.queue });
  ctx.completedJobs.delete(job.id);
  ctx.jobResults.delete(job.id);
  ctx.storage?.updateForRetry(job);
  shard.notify(job.queue);
  return 1;
}
