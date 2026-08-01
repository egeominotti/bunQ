/** DLQ manual and automatic retry transitions. */

import { MAX_TIMELINE_ENTRIES, type Job, type JobId } from '../domain/types/job';
import type { DlqFilter } from '../domain/types/dlq';
import { retainDlqRetryState, scheduleNextRetry, setDlqRetryState } from '../domain/types/dlq';
import { shardIndex } from '../shared/hash';
import type { DlqContext } from './dlqManager';

function prepareRetry(job: Job, now: number): void {
  job.attempts = 0;
  job.runAt = now;
  job.startedAt = null;
  job.stallCount = 0;
  job.lastHeartbeat = now;
  if (job.timeline.length < MAX_TIMELINE_ENTRIES) {
    job.timeline.push({ state: 'waiting', timestamp: now });
  }
}

function enqueueRetry(queue: string, job: Job, now: number, ctx: DlqContext): void {
  const idx = shardIndex(queue);
  const shard = ctx.shards[idx];
  prepareRetry(job, now);
  shard.getQueue(queue).push(job);
  shard.incrementQueued(job.id, job.runAt > now, job.createdAt, queue, job.runAt);
  ctx.jobIndex.set(job.id, { type: 'queue', shardIdx: idx, queueName: queue });
  ctx.storage?.requeueDlqJob(job);
}

/** Retry a single job from DLQ and reset the automatic retry generation. */
export function retryDlqJob(queue: string, jobId: JobId, ctx: DlqContext): Job | null {
  const shard = ctx.shards[shardIndex(queue)];
  const entry = shard.removeFromDlq(queue, jobId);
  if (!entry) return null;

  setDlqRetryState(entry.job, null);
  enqueueRetry(queue, entry.job, Date.now(), ctx);
  return entry.job;
}

/** Retry jobs from DLQ, optionally bounded to the oldest `limit` entries. */
export function retryDlqJobs(
  queue: string,
  ctx: DlqContext,
  jobId?: JobId,
  limit?: number
): number {
  if (jobId) return retryDlqJob(queue, jobId, ctx) ? 1 : 0;

  const shard = ctx.shards[shardIndex(queue)];
  if (limit !== undefined) {
    const cap = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : 0;
    const ids = shard
      .getDlqEntries(queue)
      .slice(0, cap)
      .map((entry) => entry.job.id);
    let retried = 0;
    for (const id of ids) {
      if (retryDlqJob(queue, id, ctx)) retried++;
    }
    return retried;
  }

  const entries = [...shard.getDlqEntries(queue)];
  shard.clearDlq(queue);
  ctx.storage?.clearDlqQueue(queue);
  const now = Date.now();
  for (const entry of entries) {
    setDlqRetryState(entry.job, null);
    enqueueRetry(queue, entry.job, now, ctx);
  }
  return entries.length;
}

/** Retry all DLQ entries selected by a metadata filter. */
export function retryDlqByFilter(queue: string, ctx: DlqContext, filter: DlqFilter): number {
  const shard = ctx.shards[shardIndex(queue)];
  const entries = [...shard.getDlqFiltered(queue, filter)];
  const now = Date.now();
  let retried = 0;

  for (const entry of entries) {
    const removed = shard.removeFromDlq(queue, entry.job.id);
    if (!removed) continue;
    setDlqRetryState(removed.job, null);
    enqueueRetry(queue, removed.job, now, ctx);
    retried++;
  }
  return retried;
}

/** Dispatch due auto-retries while preserving their bounded retry chain. */
export function processAutoRetry(queue: string, ctx: DlqContext): number {
  const shard = ctx.shards[shardIndex(queue)];
  const config = shard.getDlqConfig(queue);
  if (!config.autoRetry) return 0;

  const now = Date.now();
  const entries = [...shard.getAutoRetryEntries(queue, now)];
  let retried = 0;

  for (const entry of entries) {
    const removed = shard.removeFromDlq(queue, entry.job.id);
    if (!removed) continue;
    scheduleNextRetry(removed, config);
    retainDlqRetryState(removed.job, removed);
    enqueueRetry(queue, removed.job, now, ctx);
    retried++;
  }
  return retried;
}
