/**
 * Cleanup Tasks - Periodic maintenance and garbage collection
 * Handles orphaned entries, stale data, and memory management
 */

import type { JobId } from '../domain/types/job';
import { processingShardIndex, SHARD_COUNT } from '../shared/hash';
import { withWriteLock } from '../shared/lock';
import type { BackgroundContext } from './types';
import { releaseDependencyCompletionPins } from './dependencyCompletions';

/**
 * Main cleanup function - called periodically to maintain system health
 * Cleans orphaned entries, stale data, and manages memory
 */
export async function cleanup(ctx: BackgroundContext): Promise<void> {
  const now = Date.now();
  const stallTimeout = 30 * 60 * 1000; // 30 minutes max for processing

  // Refresh delayed counters
  for (let i = 0; i < SHARD_COUNT; i++) {
    ctx.shards[i].refreshDelayedCount(now);
  }

  // Compact priority queues if stale ratio > 20%
  for (let i = 0; i < SHARD_COUNT; i++) {
    for (const q of ctx.shards[i].queues.values()) {
      if (q.needsCompaction(0.2)) {
        q.compact();
      }
    }
  }

  await cleanOrphanedProcessingEntries(ctx, now, stallTimeout);
  await cleanStaleWaitingDependencies(ctx, now);
  cleanUniqueKeysAndGroups(ctx, now);
  cleanStalledCandidates(ctx);
  await cleanOrphanedJobIndex(ctx);
  cleanOrphanedJobLocks(ctx);
  cleanEmptyQueues(ctx);
}

async function cleanOrphanedProcessingEntries(
  ctx: BackgroundContext,
  now: number,
  stallTimeout: number
): Promise<void> {
  for (let i = 0; i < SHARD_COUNT; i++) {
    // Phase 1: Collect candidates (read-only, no lock needed)
    const orphaned: JobId[] = [];
    for (const [jobId, job] of ctx.processingShards[i]) {
      if (job.startedAt && now - job.startedAt > stallTimeout) {
        orphaned.push(jobId);
      }
    }

    if (orphaned.length === 0) continue;

    // Phase 2: Delete with lock to prevent race conditions
    await withWriteLock(ctx.processingLocks[i], () => {
      let removed = 0;
      for (const jobId of orphaned) {
        const job = ctx.processingShards[i].get(jobId);
        if (job) {
          ctx.processingShards[i].delete(jobId);
          ctx.timeoutScheduler.cancel(jobId);
          ctx.jobIndex.delete(jobId);
          removed++;
        }
      }
      if (removed > 0) ctx.dashboardEmit?.('cleanup:orphans-removed', { count: removed });
    });
  }
}

async function cleanStaleWaitingDependencies(ctx: BackgroundContext, now: number): Promise<void> {
  const depTimeout = 60 * 60 * 1000; // 1 hour

  for (let i = 0; i < SHARD_COUNT; i++) {
    const shard = ctx.shards[i];
    const staleIds: JobId[] = [];
    for (const [_id, job] of shard.waitingDeps) {
      if (now - job.createdAt > depTimeout) {
        staleIds.push(job.id);
      }
    }

    if (staleIds.length === 0) continue;

    const removed = await withWriteLock(ctx.shardLocks[i], () => {
      let count = 0;
      const released: JobId[] = [];
      for (const id of staleIds) {
        const job = shard.waitingDeps.get(id);
        if (!job || now - job.createdAt <= depTimeout) continue;

        // Persistence first: deleteJob also removes a pending buffered insert.
        // If SQLite throws, the in-memory lifecycle remains live and coherent.
        ctx.storage?.deleteJob(job.id);
        shard.waitingDeps.delete(job.id);
        shard.unregisterDependencies(job.id, job.dependsOn);
        released.push(...job.dependsOn);
        if (job.uniqueKey && shard.getUniqueKeyEntry(job.queue, job.uniqueKey)?.jobId === job.id) {
          shard.releaseUniqueKeyIfOwned(job.queue, job.uniqueKey, job.id);
        }
        if (job.customId && ctx.customIdMap.get(job.customId) === job.id) {
          ctx.customIdMap.delete(job.customId);
        }
        ctx.dependencyResults?.releaseConsumer(job.id);
        ctx.jobIndex.delete(job.id);
        count++;
      }
      return { count, released };
    });

    releaseDependencyCompletionPins(removed.released, ctx);
    if (removed.count > 0) {
      ctx.dashboardEmit?.('cleanup:stale-deps-removed', { count: removed.count });
    }
  }
}

function cleanUniqueKeysAndGroups(ctx: BackgroundContext, now: number): void {
  for (let i = 0; i < SHARD_COUNT; i++) {
    const shard = ctx.shards[i];
    shard.cleanExpiredGroupWindows(now);

    // Clean expired unique keys
    shard.cleanExpiredUniqueKeys();

    // Trim if too many keys remain
    for (const [_queueName, keys] of shard.uniqueKeys) {
      if (keys.size > 1000) {
        const toRemove = Math.floor(keys.size / 2);
        const iter = keys.keys();
        for (let j = 0; j < toRemove; j++) {
          const { value, done } = iter.next();
          if (done) break;
          keys.delete(value);
        }
      }
    }
  }
}

function cleanStalledCandidates(ctx: BackgroundContext): void {
  for (const jobId of ctx.stalledCandidates) {
    const loc = ctx.jobIndex.get(jobId);
    // Remove from stalledCandidates if the job is no longer actively processing:
    // 1. Job no longer exists in jobIndex (completed/removed)
    // 2. Job was moved back to queue (retried)
    // 3. Any other state that's not 'processing'
    if (loc?.type !== 'processing') {
      ctx.stalledCandidates.delete(jobId);
    }
  }
}

async function cleanOrphanedJobIndex(ctx: BackgroundContext): Promise<void> {
  // Expensive operation - only run when index is large
  if (ctx.jobIndex.size <= 100_000) return;

  // Phase 1: Collect candidates grouped by shard (read-only)
  const processingCandidates = new Map<number, JobId[]>();
  const queueCandidates = new Map<number, Array<{ jobId: JobId; queueName: string }>>();

  for (const [jobId, loc] of ctx.jobIndex) {
    if (loc.type === 'processing') {
      const procIdx = processingShardIndex(jobId);
      let list = processingCandidates.get(procIdx);
      if (!list) {
        list = [];
        processingCandidates.set(procIdx, list);
      }
      list.push(jobId);
    } else if (loc.type === 'queue') {
      let list = queueCandidates.get(loc.shardIdx);
      if (!list) {
        list = [];
        queueCandidates.set(loc.shardIdx, list);
      }
      list.push({ jobId, queueName: loc.queueName });
    }
  }

  // Phase 2: Check and delete processing entries with locks
  for (const [procIdx, candidates] of processingCandidates) {
    await withWriteLock(ctx.processingLocks[procIdx], () => {
      for (const jobId of candidates) {
        if (!ctx.processingShards[procIdx].has(jobId)) {
          ctx.jobIndex.delete(jobId);
        }
      }
    });
  }

  // Phase 3: Check and delete queue entries with locks
  for (const [shardIdx, candidates] of queueCandidates) {
    await withWriteLock(ctx.shardLocks[shardIdx], () => {
      const shard = ctx.shards[shardIdx];
      for (const { jobId, queueName } of candidates) {
        if (!shard.getQueue(queueName).has(jobId)) {
          ctx.jobIndex.delete(jobId);
        }
      }
    });
  }
}

function cleanOrphanedJobLocks(ctx: BackgroundContext): void {
  for (const jobId of ctx.jobLocks.keys()) {
    const loc = ctx.jobIndex.get(jobId);
    if (loc?.type !== 'processing') {
      ctx.jobLocks.delete(jobId);
    }
  }
}

/** Check if any processing shard has a job belonging to the given queue */
function hasProcessingJobsForQueue(ctx: BackgroundContext, queueName: string): boolean {
  for (let i = 0; i < SHARD_COUNT; i++) {
    for (const job of ctx.processingShards[i].values()) {
      if (job.queue === queueName) return true;
    }
  }
  return false;
}

/** Check if any shard has waitingDeps jobs belonging to the given queue */
function hasWaitingDepsForQueue(ctx: BackgroundContext, queueName: string): boolean {
  for (let i = 0; i < SHARD_COUNT; i++) {
    for (const job of ctx.shards[i].waitingDeps.values()) {
      if (job.queue === queueName) return true;
    }
  }
  return false;
}

function cleanEmptyQueues(ctx: BackgroundContext): void {
  for (let i = 0; i < SHARD_COUNT; i++) {
    const shard = ctx.shards[i];
    const emptyQueues: string[] = [];

    for (const [queueName, queue] of shard.queues) {
      // `shard.dlq` is a getter that rebuilds a Map of EVERY queue's DLQ entries
      // on each access — calling it once per queue in this loop was O(Q²) per
      // shard per tick. getDlqCount(queueName) is an O(1) counter lookup.
      if (
        queue.size === 0 &&
        shard.getDlqCount(queueName) === 0 &&
        !hasProcessingJobsForQueue(ctx, queueName) &&
        !hasWaitingDepsForQueue(ctx, queueName)
      ) {
        emptyQueues.push(queueName);
      }
    }

    for (const queueName of emptyQueues) {
      shard.queues.delete(queueName);
      shard.dlq.delete(queueName);
      shard.uniqueKeys.delete(queueName);
      shard.queueState.delete(queueName);
      shard.clearEmptyGroupRuntime(queueName);
      shard.clearQueueLimiters(queueName);
      shard.stallConfig.delete(queueName);
      shard.dlqConfig.delete(queueName);
      // NOTE: perQueueMetrics is intentionally NOT pruned here — it is an
      // LRU-bounded map and these counters are cumulative, so they must survive
      // a transient drain (a busy queue momentarily empty must not reset to 0).
      // obliterate() reclaims it explicitly; the LRU cap bounds growth for
      // ephemeral/dynamically-named queues.
      ctx.dashboardEmit?.('queue:removed', { queue: queueName });
      ctx.unregisterQueueName(queueName);
    }

    // Clean orphaned temporal index entries
    shard.cleanOrphanedTemporalEntries();
  }
}
