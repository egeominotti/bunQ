import type { AtomicFlowBatchInput, AtomicFlowBatchResult } from '../../domain/types/flow';
import { createJob, type Job, type JobId } from '../../domain/types/job';
import { EventType } from '../../domain/types/queue';
import type { LockGuard } from '../../shared/lock';
import { shardIndex } from '../../shared/hash';
import { latencyTracker } from '../latencyTracker';
import { throughputTracker } from '../throughputTracker';
import { emitWaitingChildrenAdmission, initialJobState, insertJobToShard } from './pushInsert';
import { type PushContext, withPendingQueueAdmissions } from './pushContext';
import { validateAtomicFlowBatch } from './flowValidation';

function existingJobError(id: JobId): Error {
  return new Error(`Flow job ${String(id)} already exists`);
}

function assertIdsAvailable(batch: AtomicFlowBatchInput, ctx: PushContext): void {
  for (const planned of batch.jobs) {
    if (
      ctx.jobIndex.has(planned.id) ||
      ctx.completedJobs.has(planned.id) ||
      (ctx.depCompletions?.has(planned.id) ?? false) ||
      ctx.jobResults.has(planned.id) ||
      (ctx.timedOutJobs?.has(planned.id) ?? false) ||
      ctx.shards.some((shard) => (shard.getJobsWaitingFor(planned.id)?.size ?? 0) > 0) ||
      ctx.storage?.getJob(planned.id) ||
      ctx.storage?.hasDlqEntry(planned.id)
    ) {
      throw existingJobError(planned.id);
    }
    if (planned.input.customId) {
      const owner = ctx.customIdMap.get(planned.input.customId);
      if (owner !== undefined) throw existingJobError(owner);
    }
  }
}

function prepareJobs(batch: AtomicFlowBatchInput, ctx: PushContext, now: number): Job[] {
  return batch.jobs.map((planned) => {
    const job = createJob(planned.id, planned.queue, planned.input, now);
    ctx.shards[shardIndex(planned.queue)].assignGroupFifoOrder(job);
    job.timeline.push({ state: initialJobState(job, ctx, now), timestamp: now });
    return job;
  });
}

function publishJobs(jobs: Job[], ctx: PushContext): Map<string, number> {
  const notifications = new Map<string, number>();
  for (const job of jobs) {
    const idx = shardIndex(job.queue);
    insertJobToShard(job, { queue: job.queue, shard: ctx.shards[idx], shardIdx: idx }, ctx, false);
    if (job.customId) ctx.customIdMap.set(job.customId, job.id);
    notifications.set(job.queue, (notifications.get(job.queue) ?? 0) + 1);
  }
  return notifications;
}

function assertGroupCapacity(
  batch: AtomicFlowBatchInput,
  jobs: readonly Job[],
  ctx: PushContext
): void {
  const counts = new Map<string, number>();
  for (let index = 0; index < jobs.length; index++) {
    const job = jobs[index];
    if (!job.groupId || job.timeline[0]?.state === 'waiting-children') continue;
    const key = `${job.queue}\0${job.groupId}`;
    const current =
      counts.get(key) ??
      ctx.shards[shardIndex(job.queue)].getGroupJobsCount(job.queue, job.groupId);
    const maxSize = batch.jobs[index].input.groupMaxSize;
    if (maxSize !== undefined && current >= maxSize) {
      throw new Error(`Group ${job.groupId} has reached its maximum size of ${maxSize}`);
    }
    counts.set(key, current + 1);
  }
}

async function acquireFlowLocks(
  batch: AtomicFlowBatchInput,
  ctx: PushContext
): Promise<LockGuard[]> {
  const guards: LockGuard[] = [];
  try {
    if (batch.jobs.some((job) => job.input.customId)) {
      guards.push(await ctx.customIdLock.acquireWrite());
    }
    const indexes = [...new Set(batch.jobs.map((job) => shardIndex(job.queue)))].sort(
      (a, b) => a - b
    );
    for (const index of indexes) guards.push(await ctx.shardLocks[index].acquireWrite());
    return guards;
  } catch (error) {
    for (let index = guards.length - 1; index >= 0; index--) guards[index].release();
    throw error;
  }
}

/**
 * Commit a complete flow graph as one broker-side operation.
 *
 * SQLite is committed before the graph is published in memory. Workers acquire
 * the same shard locks, so the first visible leaf always observes every edge.
 */
export async function pushFlowBatch(
  batch: AtomicFlowBatchInput,
  ctx: PushContext
): Promise<AtomicFlowBatchResult> {
  const startNs = Bun.nanoseconds();
  validateAtomicFlowBatch(batch);
  if (batch.jobs.length === 0) return { jobs: [] };
  return withPendingQueueAdmissions(
    batch.jobs.map((job) => job.queue),
    ctx,
    async () => {
      const guards = await acquireFlowLocks(batch, ctx);
      let jobs: Job[];
      let notifications: Map<string, number>;
      try {
        assertIdsAvailable(batch, ctx);
        const now = Date.now();
        jobs = prepareJobs(batch, ctx, now);
        assertGroupCapacity(batch, jobs, ctx);
        ctx.storage?.insertJobsBatch(jobs, true);
        notifications = publishJobs(jobs, ctx);
      } finally {
        for (let index = guards.length - 1; index >= 0; index--) guards[index].release();
      }

      for (const [queue, count] of notifications) {
        ctx.shards[shardIndex(queue)].notifyBatch(queue, count);
      }
      ctx.totalPushed.value += BigInt(jobs.length);
      throughputTracker.pushRate.increment(jobs.length);
      const timestamp = Date.now();
      for (const job of jobs) {
        ctx.broadcast({
          eventType: EventType.Pushed,
          queue: job.queue,
          jobId: job.id,
          timestamp,
        });
      }
      ctx.dashboardEmit?.('flow:pushed', {
        jobs: jobs.length,
        queues: notifications.size,
      });
      latencyTracker.push.observe((Bun.nanoseconds() - startNs) / 1e6);
      for (const job of jobs) emitWaitingChildrenAdmission(job, ctx);
      return { jobs };
    }
  );
}
