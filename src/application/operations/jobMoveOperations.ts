/**
 * Job move operations that claim active jobs for delayed or DLQ state.
 */

import type { Job, JobId } from '../../domain/types/job';
import { EventType } from '../../domain/types/queue';
import { processingShardIndex, shardIndex } from '../../shared/hash';
import { withWriteLock } from '../../shared/lock';
import { releaseClaimedJobOwnership } from './jobClaim';
import type { JobManagementContext } from './jobManagement';

/** Move active job back to delayed. */
export async function moveJobToDelayed(
  jobId: JobId,
  delay: number,
  ctx: JobManagementContext
): Promise<boolean> {
  const location = ctx.jobIndex.get(jobId);
  if (location?.type !== 'processing') return false;

  const procIdx = processingShardIndex(jobId);
  const job = await withWriteLock(ctx.processingLocks[procIdx], () => {
    const claimed = ctx.processingShards[procIdx].get(jobId);
    if (claimed) {
      ctx.processingShards[procIdx].delete(jobId);
      releaseClaimedJobOwnership(jobId, ctx);
    }
    return claimed;
  });
  if (!job) return false;

  const now = Date.now();
  job.runAt = now + delay;
  job.startedAt = null;
  const idx = shardIndex(job.queue);
  const queueName = job.queue;

  await withWriteLock(ctx.shardLocks[idx], () => {
    const shard = ctx.shards[idx];
    shard.releaseJobResources(job.queue, job.uniqueKey, job.groupId, job.id);
    shard.getQueue(job.queue).push(job);
    const isDelayed = job.runAt > now;
    shard.incrementQueued(jobId, isDelayed, job.createdAt, job.queue, job.runAt);
    ctx.jobIndex.set(jobId, { type: 'queue', shardIdx: idx, queueName: job.queue });
    shard.notify(job.queue);
  });

  ctx.storage?.updateRunAt(jobId, job.runAt);
  ctx.eventsManager.broadcast({
    eventType: EventType.Delayed,
    jobId,
    queue: queueName,
    timestamp: Date.now(),
    delay,
  });
  return true;
}

/** Discard a waiting or active job to the DLQ. */
export async function discardJob(jobId: JobId, ctx: JobManagementContext): Promise<boolean> {
  const location = ctx.jobIndex.get(jobId);
  if (!location) return false;

  let job: Job | null = null;
  if (location.type === 'queue') {
    job = await withWriteLock(ctx.shardLocks[location.shardIdx], () => {
      const shard = ctx.shards[location.shardIdx];
      const removed = shard.getQueue(location.queueName).remove(jobId);
      if (removed) shard.decrementQueued(jobId);
      return removed;
    });
  } else if (location.type === 'processing') {
    const procIdx = processingShardIndex(jobId);
    job = await withWriteLock(ctx.processingLocks[procIdx], () => {
      const claimed = ctx.processingShards[procIdx].get(jobId) ?? null;
      if (claimed) {
        ctx.processingShards[procIdx].delete(jobId);
        releaseClaimedJobOwnership(jobId, ctx);
      }
      return claimed;
    });
  }

  if (!job) return false;
  const validJob = job;
  const idx = shardIndex(validJob.queue);
  const fromProcessing = location.type === 'processing';
  await withWriteLock(ctx.shardLocks[idx], () => {
    const shard = ctx.shards[idx];
    if (fromProcessing) {
      shard.releaseJobResources(validJob.queue, validJob.uniqueKey, validJob.groupId, validJob.id);
      shard.notify(validJob.queue);
    } else if (validJob.uniqueKey) {
      shard.releaseUniqueKeyIfOwned(validJob.queue, validJob.uniqueKey, validJob.id);
    }
    const dlqEntry = shard.addToDlq(validJob);
    ctx.jobIndex.set(jobId, { type: 'dlq', queueName: validJob.queue });
    // SQLite writes are synchronous. Keeping them in this critical section
    // prevents a concurrent permanent removal from racing a late DLQ insert.
    ctx.storage?.saveDlqEntry(dlqEntry);
    ctx.storage?.deleteJob(jobId);
    ctx.dependencyResults.releaseConsumer(jobId);
    if (validJob.customId && ctx.customIdMap.get(validJob.customId) === jobId) {
      ctx.customIdMap.delete(validJob.customId);
    }
  });
  return true;
}
