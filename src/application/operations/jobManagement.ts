/**
 * Job Management Operations
 * Cancel, update progress, change priority, promote, move to delayed, discard
 */

import type { Job, JobId, JobLock } from '../../domain/types/job';
import { type JobLocation, EventType } from '../../domain/types/queue';
import type { Shard } from '../../domain/queue/shard';
import type { SqliteStorage } from '../../infrastructure/persistence/sqlite';
import type { WebhookManager } from '../webhookManager';
import type { EventsManager } from '../eventsManager';
import { processingShardIndex } from '../../shared/hash';
import { webhookLog } from '../../shared/logger';
import { type RWLock, withWriteLock } from '../../shared/lock';
import type { DependencyResultTracker } from '../dependencyResultTracker';
import {
  type DependencyCompletionTracker,
  releaseDependencyCompletionPins,
} from '../dependencyCompletions';

export { discardJob, moveJobToDelayed } from './jobMoveOperations';

const FLOW_METADATA_KEYS = [
  '__parentId',
  '__parentQueue',
  '__childrenIds',
  '__flowParentId',
  '__flowParentIds',
] as const;

function dataRecord(value: unknown): Record<string, unknown> | null {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

/** Merge caller data without allowing it to erase or forge broker-owned flow metadata. */
function updatedJobData(job: Pick<Job, 'data'>, userData: unknown): unknown {
  const current = dataRecord(job.data);
  const metadata = current
    ? Object.fromEntries(
        FLOW_METADATA_KEYS.filter((key) => Object.hasOwn(current, key)).map((key) => [
          key,
          current[key],
        ])
      )
    : {};
  if (Object.keys(metadata).length === 0) return userData;

  const next = dataRecord(userData);
  if (!next) throw new Error('flow job data must be an object');
  for (const key of Object.keys(next)) {
    if (key.startsWith('__')) throw new Error(`flow job data key is reserved: ${key}`);
  }
  return { ...next, ...metadata };
}

/** Context for job management operations */
export interface JobManagementContext {
  storage: SqliteStorage | null;
  shards: Shard[];
  shardLocks: RWLock[];
  processingShards: Map<JobId, Job>[];
  processingLocks: RWLock[];
  jobIndex: Map<JobId, JobLocation>;
  jobLocks: Map<JobId, JobLock>;
  clientJobs: Map<string, Set<JobId>>;
  webhookManager: WebhookManager;
  eventsManager: EventsManager;
  repeatChain?: Map<JobId, JobId>;
  dependencyResults: DependencyResultTracker;
  depCompletions?: DependencyCompletionTracker;
  maxDependencyCompletions: number;
}

/** Cancel a job (remove from queue) */
export async function cancelJob(jobId: JobId, ctx: JobManagementContext): Promise<boolean> {
  const location = ctx.jobIndex.get(jobId);
  if (!location) return false;

  if (location.type === 'queue') {
    const result = await withWriteLock(ctx.shardLocks[location.shardIdx], () => {
      const shard = ctx.shards[location.shardIdx];
      const job = shard.getQueue(location.queueName).remove(jobId);
      if (job) {
        // Update running counters for O(1) stats
        shard.decrementQueued(jobId);
        if (job.uniqueKey) {
          shard.releaseUniqueKeyIfOwned(location.queueName, job.uniqueKey, job.id);
        }
        ctx.jobIndex.delete(jobId);
        ctx.storage?.deleteJob(jobId);
        ctx.dependencyResults.releaseConsumer(jobId);
        return { success: true, queueName: location.queueName, released: [] };
      }
      // Not in the run queue — it may be parked in waitingChildren (moved via
      // moveToWaitingChildren, which already released its resources and does not
      // track it in the queued counter, so do NOT decrement/release here).
      const parked = shard.waitingChildren.get(jobId);
      if (parked) {
        shard.waitingChildren.delete(jobId);
        ctx.jobIndex.delete(jobId);
        ctx.storage?.deleteJob(jobId);
        ctx.dependencyResults.releaseConsumer(jobId);
        return { success: true, queueName: location.queueName, released: [] };
      }
      // Or parked in waitingDeps (flow-chain dependent inserted by push.ts while
      // its predecessors are unresolved). These are NOT counted in the queued
      // counter (insertJobToShard skips incrementQueued when needsWaiting), so do
      // NOT decrement — but they DO hold their uniqueKey reservation and register
      // entries in the dependency index, both of which must be released here.
      const waiting = shard.waitingDeps.get(jobId);
      if (waiting) {
        shard.waitingDeps.delete(jobId);
        shard.unregisterDependencies(jobId, waiting.dependsOn);
        if (waiting.uniqueKey) {
          shard.releaseUniqueKeyIfOwned(location.queueName, waiting.uniqueKey, waiting.id);
        }
        ctx.jobIndex.delete(jobId);
        ctx.storage?.deleteJob(jobId);
        ctx.dependencyResults.releaseConsumer(jobId);
        return {
          success: true,
          queueName: location.queueName,
          released: waiting.dependsOn,
        };
      }
      return { success: false, queueName: location.queueName, released: [] };
    });

    if (result.success) {
      releaseDependencyCompletionPins(result.released, ctx);
      // Emit removed event (BullMQ v5)
      ctx.eventsManager.broadcast({
        eventType: EventType.Removed,
        jobId,
        queue: result.queueName,
        timestamp: Date.now(),
        prev: 'waiting',
      });
    }
    return result.success;
  }
  return false;
}

/** Update job progress */
export async function updateJobProgress(
  jobId: JobId,
  progress: number,
  ctx: JobManagementContext,
  message?: string
): Promise<boolean> {
  const location = ctx.jobIndex.get(jobId);
  if (location?.type !== 'processing') return false;

  const procIdx = processingShardIndex(jobId);
  return withWriteLock(ctx.processingLocks[procIdx], () => {
    const job = ctx.processingShards[procIdx].get(jobId);
    if (!job) return false;

    const clampedProgress = Math.max(0, Math.min(100, progress));
    const effectiveMessage = message === undefined ? job.progressMessage : message;
    const heartbeat = Date.now();
    ctx.storage?.updateJobProgress(jobId, clampedProgress, effectiveMessage, heartbeat);
    job.progress = clampedProgress;
    job.progressMessage = effectiveMessage;
    job.lastHeartbeat = heartbeat;

    // Broadcast progress event to internal subscribers
    ctx.eventsManager.broadcast({
      eventType: 'progress' as EventType,
      jobId,
      queue: job.queue,
      timestamp: Date.now(),
      progress: job.progress,
      data: { progress: job.progress, message: job.progressMessage },
    });

    ctx.webhookManager
      .trigger('job.progress', String(jobId), job.queue, { progress: job.progress })
      .catch((err: unknown) => {
        webhookLog.error('Progress webhook failed', {
          jobId: String(jobId),
          queue: job.queue,
          error: String(err),
        });
      });

    return true;
  });
}

/** Update job data */
export async function updateJobData(
  jobId: JobId,
  data: unknown,
  ctx: JobManagementContext
): Promise<boolean> {
  const location = ctx.jobIndex.get(jobId);

  if (location?.type === 'queue') {
    return withWriteLock(ctx.shardLocks[location.shardIdx], () => {
      const shard = ctx.shards[location.shardIdx];
      const job =
        shard.getQueue(location.queueName).find(jobId) ??
        shard.waitingDeps.get(jobId) ??
        shard.waitingChildren.get(jobId);
      if (job) {
        const updated = updatedJobData(job, data);
        ctx.storage?.updateJobData(jobId, updated);
        (job as { data: unknown }).data = updated;
        return true;
      }
      return false;
    });
  } else if (location?.type === 'processing') {
    const procIdx = processingShardIndex(jobId);
    return withWriteLock(ctx.processingLocks[procIdx], () => {
      const job = ctx.processingShards[procIdx].get(jobId);
      if (job) {
        const updated = updatedJobData(job, data);
        ctx.storage?.updateJobData(jobId, updated);
        (job as { data: unknown }).data = updated;
        return true;
      }
      return false;
    });
  }

  // Job is completed or not found - check for repeat chain successor
  return updateRepeatSuccessor(jobId, data, ctx);
}

/**
 * Follow the repeat chain to find and update the successor job's data.
 * When a repeated job completes, handleRepeat creates a new job (the successor).
 * This allows updateJobData on the completed job to propagate to the next repeat.
 */
async function updateRepeatSuccessor(
  originalId: JobId,
  data: unknown,
  ctx: JobManagementContext
): Promise<boolean> {
  if (!ctx.repeatChain) return false;

  const successorId = ctx.repeatChain.get(originalId);
  if (!successorId) return false;

  const successorLoc = ctx.jobIndex.get(successorId);
  if (successorLoc?.type !== 'queue') return false;

  return withWriteLock(ctx.shardLocks[successorLoc.shardIdx], () => {
    const job = ctx.shards[successorLoc.shardIdx]
      .getQueue(successorLoc.queueName)
      .find(successorId);
    if (job) {
      const updated = updatedJobData(job, data);
      ctx.storage?.updateJobData(successorId, updated);
      (job as { data: unknown }).data = updated;
      return true;
    }
    return false;
  });
}

/** Change job priority */
export async function changeJobPriority(
  jobId: JobId,
  priority: number,
  ctx: JobManagementContext,
  lifo?: boolean
): Promise<boolean> {
  const location = ctx.jobIndex.get(jobId);
  if (location?.type !== 'queue') return false;

  return withWriteLock(ctx.shardLocks[location.shardIdx], () => {
    const q = ctx.shards[location.shardIdx].getQueue(location.queueName);
    if (!q.updatePriority(jobId, priority, lifo)) return false;

    // updatePriority replaces the immutable Job held by the indexed heap.
    // Persist the effective LIFO value from that replacement so an omitted
    // optional argument preserves the existing tie-break across recovery.
    const updatedJob = q.find(jobId);
    if (!updatedJob) return false;
    ctx.storage?.updateJobPriority(jobId, updatedJob.priority, updatedJob.lifo);
    return true;
  });
}
