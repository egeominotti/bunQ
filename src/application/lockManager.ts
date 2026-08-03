/**
 * Lock Manager - Job lock and client tracking
 * Handles BullMQ-style lock-based job ownership
 */

import type { Job, JobId, JobLock } from '../domain/types/job';
import { isLockExpired } from '../domain/types/job';
import { FailureReason, recordJobFailureAttempt } from '../domain/types/dlq';
import { StallAction } from '../domain/types/stall';
import { EventType } from '../domain/types/queue';
import type { IndexedPriorityQueue } from '../domain/queue/priorityQueue';
import { shardIndex, processingShardIndex } from '../shared/hash';
import { withWriteLock } from '../shared/lock';
import type { LockContext } from './types';

// Re-export lock operations
export {
  createLock,
  verifyLock,
  renewJobLock,
  renewJobLockBatch,
  releaseLock,
  getLockInfo,
} from './lockOperations';

// Re-export client tracking
export {
  registerClientJob,
  unregisterClientJob,
  releaseClientJobs,
  forceReleaseClientJobs,
} from './clientTracking';

/**
 * Check and handle expired locks.
 * Jobs with expired locks are requeued for retry.
 *
 * Uses proper locking to prevent race conditions.
 * Lock hierarchy: shards[N] -> processingShards[N] (per CLAUDE.md)
 */
export async function checkExpiredLocks(ctx: LockContext): Promise<void> {
  const now = Date.now();

  // Phase 1: Collect expired locks and look up their jobs (read-only from processing shards)
  // We need to know the queue name to determine shard index
  const expired: Array<{
    jobId: JobId;
    lock: JobLock;
    procIdx: number;
    shardIdx: number;
    job: Job;
  }> = [];

  for (const [jobId, lock] of ctx.jobLocks) {
    if (isLockExpired(lock, now)) {
      const procIdx = processingShardIndex(jobId);
      const job = ctx.processingShards[procIdx].get(jobId);
      if (job) {
        const shardIdx = shardIndex(job.queue);
        expired.push({ jobId, lock, procIdx, shardIdx, job });
      } else {
        // Job not in processing - just clean up the orphan lock
        ctx.jobLocks.delete(jobId);
      }
    }
  }

  if (expired.length === 0) return;

  // Phase 2: Group by shard index first (primary), then by processing shard (secondary)
  // This ensures we acquire locks in the correct hierarchy order
  const byShard = new Map<number, Map<number, typeof expired>>();
  for (const item of expired) {
    let procMap = byShard.get(item.shardIdx);
    if (!procMap) {
      procMap = new Map();
      byShard.set(item.shardIdx, procMap);
    }
    let list = procMap.get(item.procIdx);
    if (!list) {
      list = [];
      procMap.set(item.procIdx, list);
    }
    list.push(item);
  }

  // Phase 3: Process with correct lock hierarchy: shardLock -> processingLock
  for (const [shardIdx, procMap] of byShard) {
    await withWriteLock(ctx.shardLocks[shardIdx], async () => {
      for (const [procIdx, items] of procMap) {
        await withWriteLock(ctx.processingLocks[procIdx], async () => {
          for (const { jobId, lock, job } of items) {
            const currentJob = ctx.processingShards[procIdx].get(jobId);
            const currentLock = ctx.jobLocks.get(jobId);
            if (
              currentJob !== job ||
              currentLock !== lock ||
              !isLockExpired(currentLock, Date.now())
            ) {
              continue;
            }
            processExpiredLockInner(jobId, lock, job, shardIdx, procIdx, ctx, now);
          }
        });
      }
    });
  }
}

/**
 * Process a single expired lock (called with both locks already held)
 * Lock hierarchy already satisfied: shardLock -> processingLock held by caller
 */
// biome-ignore lint/complexity/useMaxParams: lock-processing inner fn needs the full lock context (7 params)
function processExpiredLockInner(
  jobId: JobId,
  lock: JobLock,
  job: Job,
  shardIdx: number,
  procIdx: number,
  ctx: LockContext,
  now: number
): void {
  const shard = ctx.shards[shardIdx];
  const queue = shard.getQueue(job.queue);

  // Remove from processing
  ctx.processingShards[procIdx].delete(jobId);
  ctx.timeoutScheduler.cancel(jobId);

  // Discard cron jobs with preventOverlap instead of re-queuing (#75).
  // During graceful shutdown, heartbeats stop and the lock expires before
  // the job finishes. Re-queuing would cause "starts right away on reconnect".
  // The cron scheduler will re-create the job at the next scheduled tick.
  if (job.uniqueKey?.startsWith('cron:')) {
    ctx.retiredCronLeaseTokens.set(jobId, lock.token);
    shard.releaseJobResources(job.queue, job.uniqueKey, job.groupId, job.id);
    ctx.jobIndex.delete(jobId);
    ctx.storage?.deleteJob(jobId);
    ctx.jobLocks.delete(jobId);
    return;
  }

  // Increment attempts and reset state
  job.attempts++;
  job.lastHeartbeat = now;
  job.stallCount++;

  // A lost lock consumes both retry budgets. Never requeue beyond either bound.
  const stallConfig = shard.getStallConfig(job.queue);
  const attemptsExhausted = job.attempts >= job.maxAttempts;
  const stallsExhausted = stallConfig.maxStalls > 0 && job.stallCount >= stallConfig.maxStalls;
  if (attemptsExhausted || stallsExhausted) {
    handleRecoveryBoundExceeded({
      jobId,
      job,
      lock,
      shard,
      ctx,
      now,
      attemptsExhausted,
    });
  } else {
    requeueExpiredJob({ jobId, job, lock, queue, idx: shardIdx, ctx, now });
  }

  // Remove the expired lock
  ctx.jobLocks.delete(jobId);

  ctx.dashboardEmit?.('job:lock-expired', {
    jobId: String(jobId),
    queue: job.queue,
    renewalCount: lock.renewalCount,
  });
}

/** Options for handling max stalls exceeded */
interface RecoveryBoundOptions {
  jobId: JobId;
  job: Job;
  lock: JobLock;
  shard: LockContext['shards'][number];
  ctx: LockContext;
  now: number;
  attemptsExhausted: boolean;
}

/** Move job to DLQ when either crash-recovery budget is exhausted. */
function handleRecoveryBoundExceeded(opts: RecoveryBoundOptions): void {
  const { jobId, job, lock, shard, ctx, now, attemptsExhausted } = opts;
  // Release the concurrency slot (+group+uniqueKey) acquired at pull before
  // moving to DLQ — otherwise the slot leaks (mirrors
  // stallDetection.moveStalliedJobToDlq).
  shard.releaseJobResources(job.queue, job.uniqueKey, job.groupId, job.id);
  const entry = shard.addToDlq(
    job,
    attemptsExhausted ? FailureReason.MaxAttemptsExceeded : FailureReason.Stalled,
    attemptsExhausted
      ? `Lock expired at max attempts (${job.maxAttempts})`
      : `Lock expired after ${lock.renewalCount} renewals`
  );
  job.startedAt = null;
  ctx.jobIndex.set(jobId, { type: 'dlq', queueName: job.queue });
  // Persist the DLQ move like the sibling paths (ack.moveFailedJobToDlq,
  // stallDetection.moveStalliedJobToDlq, backgroundTasks startup-recovery).
  // Without these two writes the jobs row survives in SQLite as an orphan and
  // the DLQ entry lives only in memory — a later retry then re-INSERTs the
  // surviving row and throws `UNIQUE constraint failed: jobs.id` (#97).
  ctx.storage?.saveDlqEntry(entry);
  ctx.storage?.deleteJob(jobId);

  ctx.eventsManager.broadcast({
    eventType: EventType.Stalled,
    jobId,
    queue: job.queue,
    timestamp: now,
    data: { stallCount: job.stallCount, action: StallAction.MoveToDlq },
  });
  ctx.eventsManager.broadcast({
    eventType: EventType.Failed,
    jobId,
    queue: job.queue,
    timestamp: now,
    error: attemptsExhausted
      ? 'Lock expired (max attempts reached)'
      : 'Lock expired (max stalls reached)',
  });
}

/** Options for requeuing expired job */
interface RequeueOptions {
  jobId: JobId;
  job: Job;
  lock: JobLock;
  queue: IndexedPriorityQueue;
  idx: number;
  ctx: LockContext;
  now: number;
}

/** Requeue job for retry */
function requeueExpiredJob(opts: RequeueOptions): void {
  const { jobId, job, queue, idx, ctx, now } = opts;
  const shard = ctx.shards[idx];
  recordJobFailureAttempt(job, FailureReason.Stalled, 'Lock expired', now);
  job.startedAt = null;
  // Release the concurrency slot (+group+uniqueKey) acquired at pull before
  // re-pushing — otherwise the slot leaks and the queue wedges (mirrors
  // stallDetection.retryStalliedJob).
  shard.releaseJobResources(job.queue, job.uniqueKey, job.groupId, job.id);
  queue.push(job);
  const isDelayed = job.runAt > now;
  shard.incrementQueued(jobId, isDelayed, job.createdAt, job.queue, job.runAt);
  ctx.jobIndex.set(jobId, { type: 'queue', shardIdx: idx, queueName: job.queue });
  ctx.storage?.updateForRetry(job);
  shard.notify(job.queue);

  ctx.eventsManager.broadcast({
    eventType: EventType.Stalled,
    jobId,
    queue: job.queue,
    timestamp: now,
  });
}
