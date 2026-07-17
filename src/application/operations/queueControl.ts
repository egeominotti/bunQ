/**
 * Queue Control Operations
 * Pause, resume, drain, obliterate, clean, list queues
 */

import type { Job, JobId } from '../../domain/types/job';
import type { Shard } from '../../domain/queue/shard';
import type { SqliteStorage } from '../../infrastructure/persistence/sqlite';
import type { MapLike, SetLike } from '../../shared/lru';
import { shardIndex, SHARD_COUNT } from '../../shared/hash';

/** Context for queue control operations */
export interface QueueControlContext {
  shards: Shard[];
  jobIndex: Map<JobId, { type: string; shardIdx?: number; queueName?: string }>;
  processingShards?: Map<JobId, Job>[];
  completedJobs?: SetLike<JobId>;
  completedJobsData?: MapLike<JobId, Job>;
  jobResults?: MapLike<JobId, unknown>;
  jobLogs?: MapLike<JobId, unknown>;
  // Required (nullable): see LockContext.storage — an optional field lets a
  // builder silently drop persistence (#110-class bug).
  storage: SqliteStorage | null;
}

/** Pause a queue */
export function pauseQueue(queue: string, ctx: QueueControlContext): void {
  const idx = shardIndex(queue);
  ctx.shards[idx].pause(queue);
}

/** Resume a queue */
export function resumeQueue(queue: string, ctx: QueueControlContext): void {
  const idx = shardIndex(queue);
  ctx.shards[idx].resume(queue);
}

/** Check if queue is paused */
export function isQueuePaused(queue: string, ctx: QueueControlContext): boolean {
  const idx = shardIndex(queue);
  return ctx.shards[idx].isPaused(queue);
}

/** Drain all waiting jobs from queue */
export function drainQueue(queue: string, ctx: QueueControlContext): number {
  const idx = shardIndex(queue);
  const { count, jobIds } = ctx.shards[idx].drain(queue);
  // Clean up jobIndex AND the SQLite row for every drained job so they cannot
  // resurrect from a stale on-disk row (parity with clean/obliterate).
  // safeDeleteJob also clears the pending write-buffer entry, so a buffered
  // (not-yet-flushed) add cannot land on disk after the drain.
  for (const jobId of jobIds) {
    ctx.jobIndex.delete(jobId);
    safeDeleteJob(ctx, jobId);
  }
  return count;
}

/** Remove all queue data */
export function obliterateQueue(queue: string, ctx: QueueControlContext): JobId[] {
  const idx = shardIndex(queue);
  return ctx.shards[idx].obliterate(queue);
}

/** List all queue names */
export function listAllQueues(ctx: QueueControlContext): string[] {
  const queues = new Set<string>();
  for (let i = 0; i < SHARD_COUNT; i++) {
    for (const name of ctx.shards[i].getQueueNames()) {
      queues.add(name);
    }
  }
  return Array.from(queues);
}

/** Normalize BullMQ-compatible state aliases */
function normalizeCleanState(state?: string): string | undefined {
  if (!state) return undefined;
  if (state === 'wait') return 'waiting';
  return state;
}

function safeDeleteJob(ctx: QueueControlContext, jobId: JobId): void {
  try {
    ctx.storage?.deleteJob(jobId);
  } catch {
    // SQLite write may fail (e.g. SQLITE_FULL). In-memory state already cleared;
    // orphan row will be GC'd by crash-recovery on restart.
  }
}

function safeDeleteDlqEntry(ctx: QueueControlContext, jobId: JobId): void {
  try {
    ctx.storage?.deleteDlqEntry(jobId);
  } catch {
    // Same rationale as safeDeleteJob.
  }
}

function cleanWaitingLike(
  queue: string,
  graceMs: number,
  ctx: QueueControlContext,
  maxJobs: number
): JobId[] {
  const idx = shardIndex(queue);
  const shard = ctx.shards[idx];
  const q = shard.getQueue(queue);
  const oldJobs = shard.getOldJobs(queue, graceMs, maxJobs);
  const removed: JobId[] = [];
  for (const { jobId } of oldJobs) {
    if (q.has(jobId)) {
      q.remove(jobId);
      shard.decrementQueued(jobId);
      shard.removeFromTemporalIndex(jobId);
      ctx.jobIndex.delete(jobId);
      safeDeleteJob(ctx, jobId);
      removed.push(jobId);
    }
  }
  return removed;
}

function cleanCompleted(
  queue: string,
  graceMs: number,
  ctx: QueueControlContext,
  maxJobs: number
): JobId[] {
  if (!ctx.completedJobs || !ctx.completedJobsData) return [];
  const threshold = Date.now() - graceMs;
  const toRemove: JobId[] = [];
  for (const [jid, loc] of ctx.jobIndex) {
    if (loc.type !== 'completed' || loc.queueName !== queue) continue;
    const job = ctx.completedJobsData.get(jid) ?? ctx.storage?.getJob(jid) ?? null;
    const ts = job?.completedAt ?? job?.createdAt ?? 0;
    if (ts && ts > threshold) continue;
    toRemove.push(jid);
    if (toRemove.length >= maxJobs) break;
  }
  for (const jid of toRemove) {
    ctx.completedJobs.delete(jid);
    ctx.completedJobsData.delete(jid);
    ctx.jobResults?.delete(jid);
    ctx.jobLogs?.delete(jid);
    ctx.jobIndex.delete(jid);
    safeDeleteJob(ctx, jid);
  }
  return toRemove;
}

function cleanFailed(
  queue: string,
  graceMs: number,
  ctx: QueueControlContext,
  maxJobs: number
): JobId[] {
  const idx = shardIndex(queue);
  const shard = ctx.shards[idx];
  const entries = shard.getDlqEntries(queue);
  const threshold = Date.now() - graceMs;
  const toRemove: JobId[] = [];
  for (const entry of entries) {
    const ts = entry.enteredAt ?? entry.job.createdAt;
    if (ts > threshold) continue;
    toRemove.push(entry.job.id);
    if (toRemove.length >= maxJobs) break;
  }
  for (const jid of toRemove) {
    shard.removeFromDlq(queue, jid);
    ctx.jobIndex.delete(jid);
    ctx.jobResults?.delete(jid);
    ctx.jobLogs?.delete(jid);
    safeDeleteDlqEntry(ctx, jid);
    safeDeleteJob(ctx, jid);
  }
  return toRemove;
}

/**
 * Clean old jobs from queue.
 * Supported states: waiting/wait, delayed, prioritized, paused, completed, failed.
 * State='active' is intentionally unsupported: cleaning in-flight jobs races with
 * the worker's ack path and would leak concurrency/uniqueKey/groupId slots. Use
 * `fail(jobId)` or `cancelJob(jobId)` to terminate an active job safely.
 * @returns Array of removed JobIds.
 */
export function cleanQueue(
  queue: string,
  graceMs: number,
  ctx: QueueControlContext,
  state?: string,
  limit?: number
): JobId[] {
  const maxJobs = limit ?? 1000;
  const normalized = normalizeCleanState(state);

  switch (normalized) {
    case undefined:
    case 'waiting':
    case 'delayed':
    case 'prioritized':
    case 'paused':
      return cleanWaitingLike(queue, graceMs, ctx, maxJobs);
    case 'completed':
      return cleanCompleted(queue, graceMs, ctx, maxJobs);
    case 'failed':
      return cleanFailed(queue, graceMs, ctx, maxJobs);
    case 'active':
    default:
      return [];
  }
}

/** Get count of jobs in queue */
export function getQueueCount(queue: string, ctx: QueueControlContext): number {
  const idx = shardIndex(queue);
  const q = ctx.shards[idx].getQueue(queue);
  return q.size;
}
