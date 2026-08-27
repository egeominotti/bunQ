/** DLQ manual and automatic retry transitions. */

import { MAX_TIMELINE_ENTRIES, type Job, type JobId } from '../domain/types/job';
import type { DlqEntry, DlqFilter } from '../domain/types/dlq';
import { retainDlqRetryState, scheduleNextRetry, setDlqRetryState } from '../domain/types/dlq';
import { shardIndex } from '../shared/hash';
import type { DlqContext } from './dlqManager';

function prepareRetry(job: Job, now: number): Job {
  const timeline = [...job.timeline];
  if (timeline.length < MAX_TIMELINE_ENTRIES) timeline.push({ state: 'waiting', timestamp: now });
  return {
    ...job,
    attempts: 0,
    lastHeartbeat: now,
    runAt: now,
    stallCount: 0,
    startedAt: null,
    timeline,
  };
}

function canRetry(queue: string, job: Job, ctx: DlqContext): boolean {
  const idx = shardIndex(queue);
  const shard = ctx.shards[idx];
  const location = ctx.jobIndex.get(job.id);
  if (location?.type !== 'dlq' || location.queueName !== queue) return false;
  const customOwner = job.customId ? ctx.customIdMap.get(job.customId) : undefined;
  if (customOwner && customOwner !== job.id) return false;
  const keyOwner = job.uniqueKey ? shard.getUniqueKeyEntry(queue, job.uniqueKey) : null;
  return keyOwner === null || keyOwner.jobId === job.id;
}

function publishRetry(queue: string, job: Job, ctx: DlqContext): void {
  const idx = shardIndex(queue);
  const shard = ctx.shards[idx];
  shard.getQueue(queue).push(job);
  shard.incrementQueued(job.id, false, job.createdAt, queue, job.runAt);
  ctx.jobIndex.set(job.id, { type: 'queue', shardIdx: idx, queueName: queue });
  if (job.customId) ctx.customIdMap.set(job.customId, job.id);
  if (job.uniqueKey) {
    shard.registerUniqueKeyWithTtl(queue, job.uniqueKey, job.id, job.deduplicationTtl ?? undefined);
  }
}

function retryEntry(
  queue: string,
  entry: DlqEntry,
  ctx: DlqContext,
  now: number,
  prepareState: (job: Job) => void
): Job | null {
  if (!canRetry(queue, entry.job, ctx)) return null;
  const job = prepareRetry(entry.job, now);
  prepareState(job);
  ctx.storage?.requeueDlqJob(job);
  const removed = ctx.shards[shardIndex(queue)].removeFromDlq(queue, entry.job.id);
  if (!removed) throw new Error('DLQ entry disappeared after its retry committed');
  publishRetry(queue, job, ctx);
  return job;
}

/** Retry a single job from DLQ and reset the automatic retry generation. */
export function retryDlqJob(queue: string, jobId: JobId, ctx: DlqContext): Job | null {
  const shard = ctx.shards[shardIndex(queue)];
  const entry = shard.getDlqEntries(queue).find((candidate) => candidate.job.id === jobId);
  if (!entry) return null;
  return retryEntry(queue, entry, ctx, Date.now(), (job) => setDlqRetryState(job, null));
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

  const ids = shard.getDlqEntries(queue).map((entry) => entry.job.id);
  let retried = 0;
  for (const id of ids) if (retryDlqJob(queue, id, ctx)) retried++;
  return retried;
}

/** Retry all DLQ entries selected by a metadata filter. */
export function retryDlqByFilter(queue: string, ctx: DlqContext, filter: DlqFilter): number {
  const shard = ctx.shards[shardIndex(queue)];
  const entries = [...shard.getDlqFiltered(queue, filter)];
  let retried = 0;

  for (const entry of entries) {
    if (retryDlqJob(queue, entry.job.id, ctx)) retried++;
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
    const scheduled = { ...entry, attempts: [...entry.attempts] };
    scheduleNextRetry(scheduled, config);
    const job = retryEntry(queue, entry, ctx, now, (candidate) => {
      retainDlqRetryState(candidate, scheduled);
    });
    if (job) retried++;
  }
  return retried;
}
