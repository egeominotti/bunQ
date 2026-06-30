/**
 * Job State Transition Operations
 * BullMQ v5 compatible manual state transitions:
 * - moveActiveToWait: Active -> Waiting
 * - changeWaitingDelay: Waiting -> Delayed
 * - moveToWaitingChildren: Active -> WaitingChildren
 */

import type { JobId } from '../../domain/types/job';
import type { EventType } from '../../domain/types/queue';
import type { JobManagementContext } from './jobManagement';
import { shardIndex, processingShardIndex } from '../../shared/hash';
import { withWriteLock } from '../../shared/lock';

/** Move active job back to waiting */
export async function moveActiveToWait(jobId: JobId, ctx: JobManagementContext): Promise<boolean> {
  const location = ctx.jobIndex.get(jobId);
  if (location?.type !== 'processing') return false;

  const procIdx = processingShardIndex(jobId);

  const job = await withWriteLock(ctx.processingLocks[procIdx], () => {
    const job = ctx.processingShards[procIdx].get(jobId);
    if (job) {
      ctx.processingShards[procIdx].delete(jobId);
    }
    return job;
  });

  if (!job) return false;

  const now = Date.now();
  job.runAt = now;
  job.startedAt = null;
  const idx = shardIndex(job.queue);

  await withWriteLock(ctx.shardLocks[idx], () => {
    const shard = ctx.shards[idx];
    // Release concurrency/uniqueKey/group slots before re-queueing
    shard.releaseJobResources(job.queue, job.uniqueKey, job.groupId);
    shard.getQueue(job.queue).push(job);
    shard.incrementQueued(jobId, false, job.createdAt, job.queue, job.runAt);
    ctx.jobIndex.set(jobId, { type: 'queue', shardIdx: idx, queueName: job.queue });
    shard.notify();
  });

  ctx.eventsManager.broadcast({
    eventType: 'waiting' as EventType,
    jobId,
    queue: job.queue,
    timestamp: now,
    prev: 'active',
  });

  return true;
}

/** Change delay for a waiting/delayed job in queue */
export async function changeWaitingDelay(
  jobId: JobId,
  delay: number,
  ctx: JobManagementContext
): Promise<boolean> {
  const location = ctx.jobIndex.get(jobId);
  if (location?.type !== 'queue') return false;

  const now = Date.now();
  const newRunAt = now + delay;

  return withWriteLock(ctx.shardLocks[location.shardIdx], () => {
    const q = ctx.shards[location.shardIdx].getQueue(location.queueName);
    const job = q.find(jobId);
    if (!job) return false;

    q.updateRunAt(jobId, newRunAt);
    // Persist so the delay survives a restart (otherwise recovery reloads the
    // stale on-disk run_at and the job is immediately pullable again).
    ctx.storage?.updateRunAt(jobId, newRunAt);
    return true;
  });
}

/** Move active job to waiting-children state */
export async function moveToWaitingChildren(
  jobId: JobId,
  ctx: JobManagementContext
): Promise<boolean> {
  const location = ctx.jobIndex.get(jobId);
  if (location?.type !== 'processing') return false;

  const procIdx = processingShardIndex(jobId);

  const job = await withWriteLock(ctx.processingLocks[procIdx], () => {
    const job = ctx.processingShards[procIdx].get(jobId);
    if (job) {
      ctx.processingShards[procIdx].delete(jobId);
    }
    return job;
  });

  if (!job) return false;

  const idx = shardIndex(job.queue);

  await withWriteLock(ctx.shardLocks[idx], () => {
    const shard = ctx.shards[idx];
    // Release concurrency/uniqueKey/group slots before moving to waitingChildren
    shard.releaseJobResources(job.queue, job.uniqueKey, job.groupId);
    shard.waitingChildren.set(jobId, job);
    ctx.jobIndex.set(jobId, { type: 'queue', shardIdx: idx, queueName: job.queue });
  });

  return true;
}
