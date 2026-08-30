/** Shared batch acknowledgement utilities. */
import type { Job, JobId } from '../../domain/types/job';
import { MAX_TIMELINE_ENTRIES } from '../../domain/types/job';
import { setDlqRetryState } from '../../domain/types/dlq';
import { EventType } from '../../domain/types/queue';
import { withWriteLock } from '../../shared/lock';
import { shardIndex, processingShardIndex } from '../../shared/hash';
import { throughputTracker } from '../throughputTracker';
import { commitRemovedCompletion } from '../dependencyCompletions';
import type { BatchContext, ExtractedJob, FinalizeContext } from '../types/ack';

export type { BatchContext, ExtractedJob, FinalizeContext } from '../types/ack';

/** Batch only when no per-job durable result/removal ordering must be preserved. */
function persistCompletedBatch<T>(
  extractedJobs: readonly ExtractedJob<T>[],
  ctx: FinalizeContext,
  includeResults: boolean,
  now: number
): boolean {
  const storage = ctx.storage;
  if (!storage || extractedJobs.length === 0) return false;
  if (
    extractedJobs.some(
      ({ job, removeOnComplete }) => job.removeOnComplete || removeOnComplete === true
    ) ||
    (includeResults && extractedJobs.some(({ result }) => result !== undefined))
  ) {
    return false;
  }
  try {
    storage.markCompletedBatch(
      extractedJobs.map(({ id: jobId, job }) => ({
        jobId,
        completedAt: now,
        timeline:
          job.timeline.length < MAX_TIMELINE_ENTRIES
            ? [...job.timeline, { state: 'completed' as const, timestamp: now }]
            : job.timeline,
      }))
    );
    return true;
  } catch {
    return false;
  }
}

/** Group job IDs by processing shard. */
export function groupByProcShard(jobIds: JobId[]): Map<number, JobId[]> {
  const byProcShard = new Map<number, JobId[]>();
  for (const jobId of jobIds) {
    const procIdx = processingShardIndex(jobId);
    let group = byProcShard.get(procIdx);
    if (!group) {
      group = [];
      byProcShard.set(procIdx, group);
    }
    group.push(jobId);
  }
  return byProcShard;
}

/**
 * Group items by processing shard (with results)
 */
export function groupItemsByProcShard<T extends { id: JobId }>(items: T[]): Map<number, T[]> {
  const byProcShard = new Map<number, T[]>();
  for (const item of items) {
    const procIdx = processingShardIndex(item.id);
    let group = byProcShard.get(procIdx);
    if (!group) {
      group = [];
      byProcShard.set(procIdx, group);
    }
    group.push(item);
  }
  return byProcShard;
}

/**
 * Extract jobs from processing shards (one lock per shard)
 */
export async function extractJobs(
  byProcShard: Map<number, JobId[]>,
  ctx: BatchContext
): Promise<ExtractedJob[]> {
  const extractedJobs: ExtractedJob[] = [];

  await Promise.all(
    Array.from(byProcShard.entries()).map(async ([procIdx, ids]) => {
      await withWriteLock(ctx.processingLocks[procIdx], () => {
        for (const jobId of ids) {
          const job = ctx.processingShards[procIdx].get(jobId);
          if (job) {
            ctx.processingShards[procIdx].delete(jobId);
            extractedJobs.push({ id: jobId, job });
          }
        }
      });
    })
  );

  return extractedJobs;
}

/**
 * Extract jobs with results from processing shards
 */
export async function extractJobsWithResults<T>(
  byProcShard: Map<number, Array<{ id: JobId; result: T; removeOnComplete?: boolean }>>,
  ctx: BatchContext
): Promise<Array<ExtractedJob<T>>> {
  const extractedJobs: Array<ExtractedJob<T>> = [];

  await Promise.all(
    Array.from(byProcShard.entries()).map(async ([procIdx, items]) => {
      await withWriteLock(ctx.processingLocks[procIdx], () => {
        for (const item of items) {
          const job = ctx.processingShards[procIdx].get(item.id);
          if (job) {
            ctx.processingShards[procIdx].delete(item.id);
            extractedJobs.push({
              id: item.id,
              job,
              result: item.result,
              removeOnComplete: item.removeOnComplete,
            });
          }
        }
      });
    })
  );

  return extractedJobs;
}

/**
 * Group extracted jobs by queue shard
 */
export function groupByQueueShard(extractedJobs: ExtractedJob[]): Map<number, Job[]> {
  const byQueueShard = new Map<number, Job[]>();
  for (let i = 0; i < extractedJobs.length; i++) {
    const job = extractedJobs[i].job;
    const idx = shardIndex(job.queue);
    let group = byQueueShard.get(idx);
    if (!group) {
      group = [];
      byQueueShard.set(idx, group);
    }
    group.push(job);
  }
  return byQueueShard;
}

/**
 * Release resources for each queue shard (one lock per shard)
 */
export async function releaseResources(
  byQueueShard: Map<number, Job[]>,
  ctx: BatchContext
): Promise<void> {
  await Promise.all(
    Array.from(byQueueShard.entries()).map(async ([idx, jobs]) => {
      await withWriteLock(ctx.shardLocks[idx], () => {
        const shard = ctx.shards[idx];
        for (const job of jobs) {
          shard.releaseJobResources(job.queue, job.uniqueKey, job.groupId, job.id);
        }
      });
    })
  );
}

/**
 * Finalize batch ack - update indexes, metrics, broadcast, notify
 */
// eslint-disable-next-line complexity
export function finalizeBatchAck<T>(
  extractedJobs: Array<ExtractedJob<T>>,
  ctx: FinalizeContext,
  includeResults: boolean
): void {
  const now = Date.now();
  const storage = ctx.storage;
  const hasStorage = storage !== null;
  const jobCount = extractedJobs.length;

  const needsBroadcast = ctx.needsBroadcast?.() ?? true;
  const hasPendingDeps = ctx.hasPendingDeps?.() ?? true;
  const completionBatchPersisted = persistCompletedBatch(extractedJobs, ctx, includeResults, now);

  // Batch counter update
  ctx.totalCompleted.value += BigInt(jobCount);

  // Per-queue completed counter update
  if (ctx.perQueueMetrics) {
    for (let i = 0; i < jobCount; i++) {
      const qName = extractedJobs[i].job.queue;
      const pq = ctx.perQueueMetrics.get(qName);
      if (pq) {
        pq.totalCompleted++;
      } else {
        ctx.perQueueMetrics.set(qName, { totalCompleted: 1n, totalFailed: 0n });
      }
    }
  }

  // Main loop
  // IMPORTANT: Order of operations matters for race condition prevention.
  // jobResults must be set BEFORE completedJobs.add() so that dependent jobs
  // checking completedJobs.has(jobId) will always find the result available.
  for (let i = 0; i < jobCount; i++) {
    const { id: jobId, job, result } = extractedJobs[i];
    setDlqRetryState(job, null);

    // Release customId so it can be reused
    if (job.customId && ctx.customIdMap) {
      ctx.customIdMap.delete(job.customId);
    }

    if (!(job.removeOnComplete || extractedJobs[i].removeOnComplete === true)) {
      // Mark completion timestamp + timeline
      job.completedAt = now;
      if (job.timeline.length < MAX_TIMELINE_ENTRIES) {
        job.timeline.push({ state: 'completed', timestamp: now });
      }
      // 1. Store result first (if any) - must happen before completedJobs.add()
      if (includeResults && result !== undefined) {
        ctx.jobResults.set(jobId, result);
        if (hasStorage) storage.storeResult(jobId, result);
      }
      // 2. Update job index and store job data for completed listing
      ctx.jobIndex.set(jobId, { type: 'completed' as const, queueName: job.queue });
      ctx.completedJobsData.set(jobId, job);
      if (hasStorage && !completionBatchPersisted) {
        storage.markCompleted(jobId, now, job.timeline);
      }
      // 3. Mark as completed LAST - this is the signal other threads wait for
      ctx.completedJobs.add(jobId);
    } else {
      commitRemovedCompletion(job, ctx, now);
      ctx.jobIndex.delete(jobId);
      // The helper publishes payload-free dependency evidence only after its
      // durable job-removal transaction commits.
    }
  }

  for (let i = 0; i < jobCount; i++) {
    const { id, result } = extractedJobs[i];
    if (includeResults && result !== undefined) ctx.dependencyResults?.retain(id, result);
    ctx.dependencyResults?.releaseConsumer(id);
  }

  // Broadcast events
  if (needsBroadcast) {
    const events = extractedJobs.map(({ id: jobId, job, result }) => ({
      eventType: 'completed' as EventType,
      queue: job.queue,
      jobId,
      timestamp: now,
      data: includeResults ? result : undefined,
    }));
    if (ctx.broadcastBatch) ctx.broadcastBatch(events);
    else {
      for (const event of events) ctx.broadcast(event);
    }
  }

  // Notify completions
  if (hasPendingDeps && ctx.onJobsCompleted) {
    const completedIds = extractedJobs.map((e) => e.id);
    ctx.onJobsCompleted(completedIds);
  } else if (hasPendingDeps) {
    for (let i = 0; i < jobCount; i++) {
      ctx.onJobCompleted(extractedJobs[i].id);
    }
  }

  // Throughput tracking
  throughputTracker.completeRate.increment(jobCount);

  // Schedule repeat jobs
  if (ctx.onRepeat) {
    for (let i = 0; i < jobCount; i++) {
      const job = extractedJobs[i].job;
      if (job.repeat) {
        const shouldRepeat = job.repeat.limit === undefined || job.repeat.count < job.repeat.limit;
        if (shouldRepeat) {
          ctx.onRepeat(job);
        }
      }
    }
  }
}
