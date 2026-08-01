/**
 * Ack/Fail Operations
 * Job acknowledgement and failure handling
 */

import {
  type Job,
  type JobId,
  calculateBackoff,
  canRetry,
  normalizeStacktrace,
  MAX_TIMELINE_ENTRIES,
} from '../../domain/types/job';
import { type JobLocation, EventType } from '../../domain/types/queue';
import { FailureReason } from '../../domain/types/dlq';
import type { Shard } from '../../domain/queue/shard';
import type { SqliteStorage } from '../../infrastructure/persistence/sqlite';
import type { RWLock } from '../../shared/lock';
import { withWriteLock } from '../../shared/lock';
import { shardIndex, processingShardIndex } from '../../shared/hash';
import { latencyTracker } from '../latencyTracker';
import { throughputTracker } from '../throughputTracker';
import type { SetLike, MapLike } from '../../shared/lru';
import {
  groupByProcShard,
  groupItemsByProcShard,
  extractJobs,
  extractJobsWithResults,
  groupByQueueShard,
  releaseResources,
  finalizeBatchAck,
} from './ackHelpers';
import type { DependencyResultTracker } from '../dependencyResultTracker';
import type { FlowFailureRecord, FlowFailureMode } from '../../domain/types/flow';
import {
  commitRemovedCompletion,
  type DependencyCompletionTracker,
} from '../dependencyCompletions';

/** Ack operation context */
export interface AckContext {
  storage: SqliteStorage | null;
  shards: Shard[];
  shardLocks: RWLock[];
  processingShards: Map<JobId, Job>[];
  processingLocks: RWLock[];
  completedJobs: SetLike<JobId>;
  completedJobsData: MapLike<JobId, Job>;
  /** Bare completion ids for removeOnComplete jobs so dependents can unblock */
  depCompletions?: DependencyCompletionTracker;
  maxDependencyCompletions: number;
  jobResults: MapLike<JobId, unknown>;
  dependencyResults: DependencyResultTracker;
  jobIndex: Map<JobId, JobLocation>;
  customIdMap?: MapLike<string, JobId>;
  totalCompleted: { value: bigint };
  totalFailed: { value: bigint };
  perQueueMetrics?: MapLike<string, { totalCompleted: bigint; totalFailed: bigint }>;
  broadcast: (event: {
    eventType: EventType;
    queue: string;
    jobId: JobId;
    timestamp: number;
    data?: unknown;
    error?: string;
    prev?: string;
  }) => void;
  onJobCompleted: (jobId: JobId) => void;
  onJobFailed?: (jobId: JobId) => void;
  onJobsCompleted?: (jobIds: JobId[]) => void;
  needsBroadcast?: () => boolean;
  emitDashboardEvent?: (event: string, data: Record<string, unknown>) => void;
  hasPendingDeps?: () => boolean;
  onRepeat?: (job: Job) => void;
  /** Called when a child job with failParentOnFailure terminally fails */
  onChildTerminalFailure?: (childJob: Job, error: string | undefined) => Promise<void>;
  /** Called when a child job with removeDependencyOnFailure/ignoreDependencyOnFailure/continueParentOnFailure terminally fails */
  onChildDependencyOption?: (childJob: Job, error: string | undefined) => Promise<void>;
}

/**
 * Acknowledge job completion
 */
export async function ackJob(jobId: JobId, result: unknown, ctx: AckContext): Promise<void> {
  const startNs = Bun.nanoseconds();
  const procIdx = processingShardIndex(jobId);

  const job = await withWriteLock(ctx.processingLocks[procIdx], () => {
    const job = ctx.processingShards[procIdx].get(jobId);
    if (job) {
      ctx.processingShards[procIdx].delete(jobId);
    }
    return job;
  });

  if (!job) {
    throw new Error(`Job not found or not in processing state: ${jobId}`);
  }

  const idx = shardIndex(job.queue);
  await withWriteLock(ctx.shardLocks[idx], () => {
    const shard = ctx.shards[idx];
    shard.releaseJobResources(job.queue, job.uniqueKey, job.groupId, job.id);
    // Releasing either queue concurrency or an active FIFO group can make a
    // parked job eligible. Wake only waiters for this queue.
    shard.notify(job.queue);
  });

  // Release customId so it can be reused
  if (job.customId && ctx.customIdMap) {
    ctx.customIdMap.delete(job.customId);
  }

  if (!job.removeOnComplete) {
    const now = Date.now();
    job.completedAt = now;
    if (job.timeline.length < MAX_TIMELINE_ENTRIES) {
      job.timeline.push({ state: 'completed', timestamp: now });
    }
    ctx.completedJobs.add(jobId);
    ctx.completedJobsData.set(jobId, job);
    if (result !== undefined) {
      ctx.jobResults.set(jobId, result);
      ctx.storage?.storeResult(jobId, result);
    }
    ctx.jobIndex.set(jobId, { type: 'completed', queueName: job.queue });
    ctx.storage?.markCompleted(jobId, now, job.timeline);
  } else {
    commitRemovedCompletion(job, ctx);
    ctx.jobIndex.delete(jobId);
    // removeOnComplete drops the full job (index + data + persisted row) to bound
    // memory, but dependent jobs gate readiness on completedJobs.has(parentId).
    // Record the bare completion id (no payload) so dependents still unblock,
    // without making the job appear in state/stats queries.
  }

  if (result !== undefined) ctx.dependencyResults.retain(jobId, result);
  ctx.dependencyResults.releaseConsumer(jobId);

  ctx.totalCompleted.value++;
  if (ctx.perQueueMetrics) {
    const pq = ctx.perQueueMetrics.get(job.queue);
    if (pq) {
      pq.totalCompleted++;
    } else {
      ctx.perQueueMetrics.set(job.queue, { totalCompleted: 1n, totalFailed: 0n });
    }
  }
  throughputTracker.completeRate.increment();
  ctx.broadcast({
    eventType: 'completed' as EventType,
    queue: job.queue,
    jobId,
    timestamp: Date.now(),
    data: result,
  });

  ctx.onJobCompleted(jobId);

  if (job.repeat && ctx.onRepeat) {
    const shouldRepeat = job.repeat.limit === undefined || job.repeat.count < job.repeat.limit;
    if (shouldRepeat) {
      ctx.onRepeat(job);
    }
  }

  latencyTracker.ack.observe((Bun.nanoseconds() - startNs) / 1e6);
}

/** Move a permanently-failed job to DLQ (terminal path in failJob). */
function moveFailedJobToDlq(input: {
  job: Job;
  jobId: JobId;
  error: string | undefined;
  shard: Shard;
  ctx: AckContext;
  flowFailure: FlowFailureRecord | null;
}): void {
  const { job, jobId, error, shard, ctx, flowFailure } = input;
  const entry = shard.addToDlq(job, FailureReason.MaxAttemptsExceeded, error ?? null);
  ctx.jobIndex.set(jobId, { type: 'dlq', queueName: job.queue });
  ctx.storage?.commitFailedJob(jobId, entry, flowFailure);
  ctx.totalFailed.value++;
  if (ctx.perQueueMetrics) {
    const pq = ctx.perQueueMetrics.get(job.queue);
    if (pq) pq.totalFailed++;
    else ctx.perQueueMetrics.set(job.queue, { totalCompleted: 0n, totalFailed: 1n });
  }
  throughputTracker.failRate.increment();
  if (job.customId && ctx.customIdMap) ctx.customIdMap.delete(job.customId);
  ctx.emitDashboardEvent?.('dlq:added', {
    queue: job.queue,
    jobId: String(jobId),
    reason: FailureReason.MaxAttemptsExceeded,
  });
  if (job.parentId) {
    ctx.emitDashboardEvent?.('flow:failed', {
      parentJobId: String(job.parentId),
      failedChildId: String(jobId),
      queue: job.queue,
      error: error ?? 'Max attempts exceeded',
    });
  }
}

function flowFailureRecord(job: Job, error: string | undefined): FlowFailureRecord | null {
  if (!job.parentId) return null;
  let mode: FlowFailureMode | null = null;
  if (job.failParentOnFailure) mode = 'fail';
  else if (job.continueParentOnFailure) mode = 'continue';
  else if (job.ignoreDependencyOnFailure) mode = 'ignore';
  else if (job.removeDependencyOnFailure) mode = 'remove';
  if (!mode) return null;
  return {
    parentId: job.parentId,
    childId: job.id,
    childQueue: job.queue,
    mode,
    error: error ?? 'unknown error',
    createdAt: Date.now(),
  };
}

/**
 * Mark job as failed
 */
export async function failJob(
  jobId: JobId,
  error: string | undefined,
  ctx: AckContext,
  unrecoverable = false,
  stack?: string[]
): Promise<void> {
  const procIdx = processingShardIndex(jobId);

  const job = await withWriteLock(ctx.processingLocks[procIdx], () => {
    const job = ctx.processingShards[procIdx].get(jobId);
    if (job) {
      ctx.processingShards[procIdx].delete(jobId);
    }
    return job;
  });

  if (!job) {
    throw new Error(`Job not found or not in processing state: ${jobId}`);
  }

  job.attempts++;
  // #74: persist the LAST failure's stack on the job (authoritative cap at
  // stackTraceLimit) BEFORE branching, so both the retried job and the DLQ
  // entry carry it. An absent stack (old clients) leaves any previous one intact.
  if (stack) {
    job.stacktrace = normalizeStacktrace(stack, job.stackTraceLimit);
  }
  const failNow = Date.now();
  if (job.timeline.length < MAX_TIMELINE_ENTRIES) {
    job.timeline.push({ state: 'failed', timestamp: failNow, error, attempt: job.attempts });
  }

  const idx = shardIndex(job.queue);
  let wasRetried = false;
  const willRetry = !unrecoverable && canRetry(job);
  const flowFailure = willRetry ? null : flowFailureRecord(job, error);
  await withWriteLock(ctx.shardLocks[idx], () => {
    const shard = ctx.shards[idx];
    shard.releaseJobResources(job.queue, job.uniqueKey, job.groupId, job.id);

    if (willRetry) {
      const now = Date.now();
      job.runAt = now + calculateBackoff(job);
      shard.getQueue(job.queue).push(job);
      shard.incrementQueued(jobId, true, job.createdAt, job.queue, job.runAt);
      ctx.jobIndex.set(jobId, { type: 'queue', shardIdx: idx, queueName: job.queue });
      ctx.storage?.updateForRetry(job);
      wasRetried = true;
      if (job.timeline.length < MAX_TIMELINE_ENTRIES) {
        job.timeline.push({ state: 'waiting', timestamp: now, attempt: job.attempts + 1 });
      }
    } else if (job.removeOnFail) {
      ctx.jobIndex.delete(jobId);
      ctx.storage?.commitFailedJob(jobId, null, flowFailure);
      ctx.totalFailed.value++;
      if (ctx.perQueueMetrics) {
        const pq = ctx.perQueueMetrics.get(job.queue);
        if (pq) {
          pq.totalFailed++;
        } else {
          ctx.perQueueMetrics.set(job.queue, { totalCompleted: 0n, totalFailed: 1n });
        }
      }
      throughputTracker.failRate.increment();
      // Release customId when job is removed on fail
      if (job.customId && ctx.customIdMap) {
        ctx.customIdMap.delete(job.customId);
      }
    } else {
      moveFailedJobToDlq({ job, jobId, error, shard, ctx, flowFailure });
    }

    // Terminal failure and retry both release queue concurrency (and possibly
    // an active FIFO group), so another job may now be eligible.
    shard.notify(job.queue);
  });

  ctx.broadcast({
    eventType: 'failed' as EventType,
    queue: job.queue,
    jobId,
    timestamp: Date.now(),
    error,
    data: job.data,
  });

  // Emit retried event if job was requeued for retry (BullMQ v5)
  if (wasRetried) {
    ctx.broadcast({
      eventType: EventType.Retried,
      queue: job.queue,
      jobId,
      timestamp: Date.now(),
      prev: 'failed',
    });
  } else {
    ctx.dependencyResults.releaseConsumer(jobId);
    ctx.onJobFailed?.(jobId);
  }

  // BullMQ v5: failParentOnFailure — propagate terminal failure to parent
  if (!wasRetried && job.failParentOnFailure && job.parentId && ctx.onChildTerminalFailure) {
    await ctx.onChildTerminalFailure(job, error);
  }

  // removeDependencyOnFailure / ignoreDependencyOnFailure / continueParentOnFailure
  if (
    !wasRetried &&
    job.parentId &&
    ctx.onChildDependencyOption &&
    (job.removeDependencyOnFailure || job.ignoreDependencyOnFailure || job.continueParentOnFailure)
  ) {
    await ctx.onChildDependencyOption(job, error);
  }
}

/**
 * Acknowledge multiple jobs - optimized batch processing
 * Groups jobs by shard to minimize lock acquisitions: O(shards) instead of O(n)
 */
export async function ackJobBatch(jobIds: JobId[], ctx: AckContext): Promise<void> {
  if (jobIds.length === 0) return;

  // Small batches - use parallel individual acks
  if (jobIds.length <= 4) {
    await Promise.all(jobIds.map((id) => ackJob(id, undefined, ctx)));
    return;
  }

  const batchCtx = {
    processingShards: ctx.processingShards,
    processingLocks: ctx.processingLocks,
    shards: ctx.shards,
    shardLocks: ctx.shardLocks,
  };

  // Step 1-2: Group and extract
  const byProcShard = groupByProcShard(jobIds);
  const extractedJobs = await extractJobs(byProcShard, batchCtx);

  // Step 3-4: Group by queue shard and release
  const byQueueShard = groupByQueueShard(extractedJobs);
  await releaseResources(byQueueShard, batchCtx);
  for (const [idx, jobs] of byQueueShard) {
    notifyReleasedQueues(ctx.shards[idx], jobs);
  }

  // Step 5: Finalize
  finalizeBatchAck(extractedJobs, ctx, false);
}

/**
 * Acknowledge multiple jobs with individual results - optimized batch processing
 */
export async function ackJobBatchWithResults(
  items: Array<{ id: JobId; result: unknown }>,
  ctx: AckContext
): Promise<void> {
  if (items.length === 0) return;

  // Small batches - use parallel individual acks
  if (items.length <= 4) {
    await Promise.all(items.map((item) => ackJob(item.id, item.result, ctx)));
    return;
  }

  const batchCtx = {
    processingShards: ctx.processingShards,
    processingLocks: ctx.processingLocks,
    shards: ctx.shards,
    shardLocks: ctx.shardLocks,
  };

  // Step 1-2: Group and extract with results
  const byProcShard = groupItemsByProcShard(items);
  const extractedJobs = await extractJobsWithResults(byProcShard, batchCtx);

  // Step 3-4: Group by queue shard and release
  const byQueueShard = groupByQueueShard(extractedJobs);
  await releaseResources(byQueueShard, batchCtx);
  for (const [idx, jobs] of byQueueShard) {
    notifyReleasedQueues(ctx.shards[idx], jobs);
  }

  // Step 5: Finalize with results
  finalizeBatchAck(extractedJobs, ctx, true);
}

/** Wake queue-scoped waiters for concurrency/group slots released by a batch. */
function notifyReleasedQueues(shard: Shard, jobs: Job[]): void {
  const releasedByQueue = new Map<string, number>();
  for (const job of jobs) {
    releasedByQueue.set(job.queue, (releasedByQueue.get(job.queue) ?? 0) + 1);
  }
  for (const [queue, count] of releasedByQueue) shard.notifyBatch(queue, count);
}
