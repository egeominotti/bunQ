/**
 * Job Management Operations
 * Cancel, update progress, change priority, promote, move to delayed, discard
 */

import type { Job, JobId } from '../../domain/types/job';
import { type JobLocation, EventType } from '../../domain/types/queue';
import type { Shard } from '../../domain/queue/shard';
import type { SqliteStorage } from '../../infrastructure/persistence/sqlite';
import type { WebhookManager } from '../webhookManager';
import type { EventsManager } from '../eventsManager';
import { shardIndex, processingShardIndex } from '../../shared/hash';
import { webhookLog } from '../../shared/logger';
import { type RWLock, withWriteLock } from '../../shared/lock';

/** Context for job management operations */
export interface JobManagementContext {
  storage: SqliteStorage | null;
  shards: Shard[];
  shardLocks: RWLock[];
  processingShards: Map<JobId, Job>[];
  processingLocks: RWLock[];
  jobIndex: Map<JobId, JobLocation>;
  webhookManager: WebhookManager;
  eventsManager: EventsManager;
  repeatChain?: Map<JobId, JobId>;
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
        if (job.uniqueKey) shard.releaseUniqueKey(location.queueName, job.uniqueKey);
        ctx.jobIndex.delete(jobId);
        ctx.storage?.deleteJob(jobId);
        return { success: true, queueName: location.queueName };
      }
      // Not in the run queue — it may be parked in waitingChildren (moved via
      // moveToWaitingChildren, which already released its resources and does not
      // track it in the queued counter, so do NOT decrement/release here).
      const parked = shard.waitingChildren.get(jobId);
      if (parked) {
        shard.waitingChildren.delete(jobId);
        ctx.jobIndex.delete(jobId);
        ctx.storage?.deleteJob(jobId);
        return { success: true, queueName: location.queueName };
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
        if (waiting.uniqueKey) shard.releaseUniqueKey(location.queueName, waiting.uniqueKey);
        ctx.jobIndex.delete(jobId);
        ctx.storage?.deleteJob(jobId);
        return { success: true, queueName: location.queueName };
      }
      return { success: false, queueName: location.queueName };
    });

    if (result.success) {
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

    job.progress = Math.max(0, Math.min(100, progress));
    if (message !== undefined) job.progressMessage = message;
    job.lastHeartbeat = Date.now();

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
      const job = shard.getQueue(location.queueName).find(jobId) ?? shard.waitingDeps.get(jobId);
      if (job) {
        (job as { data: unknown }).data = data;
        return true;
      }
      return false;
    });
  } else if (location?.type === 'processing') {
    const procIdx = processingShardIndex(jobId);
    return withWriteLock(ctx.processingLocks[procIdx], () => {
      const job = ctx.processingShards[procIdx].get(jobId);
      if (job) {
        (job as { data: unknown }).data = data;
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
      (job as { data: unknown }).data = data;
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
    return q.updatePriority(jobId, priority, lifo);
  });
}

/** Promote delayed job to waiting */
export async function promoteJob(jobId: JobId, ctx: JobManagementContext): Promise<boolean> {
  const location = ctx.jobIndex.get(jobId);
  if (location?.type !== 'queue') return false;

  return withWriteLock(ctx.shardLocks[location.shardIdx], () => {
    const q = ctx.shards[location.shardIdx].getQueue(location.queueName);
    const job = q.find(jobId);
    if (!job || job.runAt <= Date.now()) return false;

    q.updateRunAt(jobId, Date.now());
    return true;
  });
}

/** Move active job back to delayed */
export async function moveJobToDelayed(
  jobId: JobId,
  delay: number,
  ctx: JobManagementContext
): Promise<boolean> {
  const location = ctx.jobIndex.get(jobId);
  if (location?.type !== 'processing') return false;

  const procIdx = processingShardIndex(jobId);

  // First remove from processing with lock
  const job = await withWriteLock(ctx.processingLocks[procIdx], () => {
    const job = ctx.processingShards[procIdx].get(jobId);
    if (job) {
      ctx.processingShards[procIdx].delete(jobId);
    }
    return job;
  });

  if (!job) return false;

  // Then add back to queue with lock
  const now = Date.now();
  job.runAt = now + delay;
  job.startedAt = null;
  const idx = shardIndex(job.queue);
  const queueName = job.queue;

  await withWriteLock(ctx.shardLocks[idx], () => {
    const shard = ctx.shards[idx];
    // Release the concurrency slot (+group+uniqueKey) acquired at pull before
    // re-queueing, mirroring moveActiveToWait (jobStateTransitions.ts) --
    // otherwise the slot leaks and setConcurrency(N) wedges after N moves.
    // Dropping the uniqueKey reservation on requeue matches the sibling
    // requeue paths (failJob retry, moveActiveToWait, requeueExpiredJob).
    shard.releaseJobResources(job.queue, job.uniqueKey, job.groupId);
    shard.getQueue(job.queue).push(job);
    // Update running counters for O(1) stats and temporal index (job is delayed since delay > 0)
    const isDelayed = job.runAt > now;
    shard.incrementQueued(jobId, isDelayed, job.createdAt, job.queue, job.runAt);
    ctx.jobIndex.set(jobId, { type: 'queue', shardIdx: idx, queueName: job.queue });
    // The freed slot may unblock waiting jobs for long-pollers.
    shard.notify(job.queue);
  });

  // Persist the move so the delay survives a restart: the on-disk row was
  // `state='active'` with the old run_at, which recovery would otherwise treat as a
  // stalled active job. updateRunAt re-derives 'delayed' from the future run_at and
  // clears started_at.
  ctx.storage?.updateRunAt(jobId, job.runAt);

  // Emit delayed event (BullMQ v5)
  ctx.eventsManager.broadcast({
    eventType: EventType.Delayed,
    jobId,
    queue: queueName,
    timestamp: Date.now(),
    delay,
  });

  return true;
}

/** Discard job to DLQ */
export async function discardJob(jobId: JobId, ctx: JobManagementContext): Promise<boolean> {
  const location = ctx.jobIndex.get(jobId);
  if (!location) return false;

  let job: Job | null = null;

  if (location.type === 'queue') {
    job = await withWriteLock(ctx.shardLocks[location.shardIdx], () => {
      const shard = ctx.shards[location.shardIdx];
      const removed = shard.getQueue(location.queueName).remove(jobId);
      if (removed) {
        // Update running counters for O(1) stats
        shard.decrementQueued(jobId);
      }
      return removed;
    });
  } else if (location.type === 'processing') {
    const procIdx = processingShardIndex(jobId);
    job = await withWriteLock(ctx.processingLocks[procIdx], () => {
      const j = ctx.processingShards[procIdx].get(jobId) ?? null;
      if (j) ctx.processingShards[procIdx].delete(jobId);
      return j;
    });
  }

  if (job) {
    const validJob = job; // Local reference for closure
    const idx = shardIndex(validJob.queue);
    const fromProcessing = location.type === 'processing';
    const entry = await withWriteLock(ctx.shardLocks[idx], () => {
      const shard = ctx.shards[idx];
      if (fromProcessing) {
        // The pull that activated this job acquired a concurrency slot (and
        // possibly a FIFO group); return them before the DLQ move or the slot
        // leaks and setConcurrency(N) wedges after N discards. The uniqueKey
        // is freed too: every sibling terminal DLQ path releases the full
        // reservation on entry (failJob, ack.ts; handleMaxStallsExceeded,
        // lockManager.ts), so the DLQ entry never keeps it.
        shard.releaseJobResources(validJob.queue, validJob.uniqueKey, validJob.groupId);
        // The freed slot may unblock waiting jobs for long-pollers.
        shard.notify(validJob.queue);
      } else if (validJob.uniqueKey) {
        // Queue branch: the waiting job never acquired a slot or group at
        // pull, so a full releaseJobResources would free a slot legitimately
        // held by ANOTHER active job. Release only the uniqueKey reservation,
        // matching cancelJob and the DLQ-entry semantics above.
        shard.releaseUniqueKey(validJob.queue, validJob.uniqueKey);
      }
      // addToDlq already updates dlq counter and returns entry
      const dlqEntry = shard.addToDlq(validJob);
      ctx.jobIndex.set(jobId, { type: 'dlq', queueName: validJob.queue });
      return dlqEntry;
    });
    ctx.storage?.saveDlqEntry(entry);
    ctx.storage?.deleteJob(jobId);
    return true;
  }
  return false;
}
