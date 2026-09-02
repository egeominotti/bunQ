import { setDlqRetryState } from '../domain/types/dlq';
import { MAX_TIMELINE_ENTRIES, type Job, type JobId } from '../domain/types/job';
import type { SqliteStorage } from '../infrastructure/persistence/sqlite';
import { shardIndex } from '../shared/hash';
import type { DlqContext } from './dlqManager';

/** Completed-job retry state shared by embedded control surfaces. */
export interface RetryCompletedContext extends DlqContext {
  completedJobs: { has(id: JobId): boolean; delete(id: JobId): boolean } & Iterable<JobId>;
  completedJobsData: {
    get(id: JobId): Job | undefined;
    delete(id: JobId): boolean;
  };
  jobResults: { delete(id: JobId): boolean };
}

const RETRY_PAGE_SIZE = 500;

/** Retry a selected completed generation or an oldest-first bounded set. */
export function retryCompletedJobs(
  queue: string,
  ctx: RetryCompletedContext,
  targetId?: JobId,
  options: { limit?: number; timestamp?: number } = {}
): number {
  if (targetId) {
    const job = ctx.storage
      ? ctx.storage.getCompletedJob(targetId)
      : ctx.completedJobsData.get(targetId);
    if (job?.queue !== queue || (!ctx.storage && !ctx.completedJobs.has(targetId))) return 0;
    return requeueCompletedJob(job, ctx);
  }

  const limit = normalizeRetryLimit(options.limit);
  if (limit === 0) return 0;
  const timestamp = options.timestamp ?? Number.POSITIVE_INFINITY;
  if (Number.isNaN(timestamp) || timestamp === Number.NEGATIVE_INFINITY) return 0;

  const storage = ctx.storage;
  return storage
    ? retryPersistedCompleted(queue, timestamp, limit, storage, ctx)
    : retryMemoryCompleted(queue, timestamp, limit, ctx);
}

function normalizeRetryLimit(limit: number | undefined): number {
  if (limit === undefined) return Number.POSITIVE_INFINITY;
  return Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : 0;
}

function retryPersistedCompleted(
  queue: string,
  timestamp: number,
  limit: number,
  storage: SqliteStorage,
  ctx: RetryCompletedContext
): number {
  const completedBefore = Number.isFinite(timestamp) ? timestamp : Number.MAX_SAFE_INTEGER;
  let cursorTimestamp = Number.MIN_SAFE_INTEGER;
  let cursorId = '';
  let count = 0;
  while (count < limit) {
    const page = storage.loadCompletedJobsForRetry(
      queue,
      completedBefore,
      cursorTimestamp,
      cursorId,
      RETRY_PAGE_SIZE
    );
    if (page.length === 0) break;
    for (const job of page) {
      cursorTimestamp = job.completedAt ?? job.createdAt;
      cursorId = job.id;
      count += requeueCompletedJob(job, ctx);
      if (count >= limit) break;
    }
    if (page.length < RETRY_PAGE_SIZE) break;
  }
  return count;
}

function retryMemoryCompleted(
  queue: string,
  timestamp: number,
  limit: number,
  ctx: RetryCompletedContext
): number {
  let count = 0;
  for (const id of [...ctx.completedJobs]) {
    if (count >= limit) break;
    const job = ctx.completedJobsData.get(id);
    if (job?.queue === queue && job.completedAt !== null && job.completedAt <= timestamp) {
      count += requeueCompletedJob(job, ctx);
    }
  }
  return count;
}

function requeueCompletedJob(job: Job, ctx: RetryCompletedContext): number {
  const currentLocation = ctx.jobIndex.get(job.id);
  if (
    (currentLocation && currentLocation.type !== 'completed') ||
    (currentLocation?.type === 'completed' && currentLocation.queueName !== job.queue) ||
    (!currentLocation && !ctx.storage)
  ) {
    return 0;
  }
  const shard = ctx.shards[shardIndex(job.queue)];
  const keyOwner = job.uniqueKey ? shard.getUniqueKeyEntry(job.queue, job.uniqueKey) : null;
  if (keyOwner && keyOwner.jobId !== job.id) return 0;

  const now = Date.now();
  const timeline = [...job.timeline];
  if (timeline.length < MAX_TIMELINE_ENTRIES) timeline.push({ state: 'waiting', timestamp: now });
  const requeued: Job = {
    ...job,
    attempts: 0,
    startedAt: null,
    completedAt: null,
    runAt: now,
    progress: 0,
    progressMessage: null,
    lastHeartbeat: now,
    timeline,
  };
  setDlqRetryState(requeued, null);
  shard.assignGroupFifoOrder(requeued);

  ctx.storage?.requeueCompletedJob(requeued);

  const shardIdx = shardIndex(requeued.queue);
  shard.getQueue(requeued.queue).push(requeued);
  shard.incrementQueued(requeued.id, false, requeued.createdAt, requeued.queue, requeued.runAt);
  ctx.jobIndex.set(requeued.id, {
    type: 'queue',
    shardIdx,
    queueName: requeued.queue,
  });
  ctx.completedJobs.delete(requeued.id);
  ctx.completedJobsData.delete(requeued.id);
  ctx.jobResults.delete(requeued.id);
  ctx.jobResultQueues.delete(requeued.id);
  if (requeued.customId) ctx.customIdMap.set(requeued.customId, requeued.id);
  if (requeued.uniqueKey) {
    shard.registerUniqueKeyWithTtl(
      requeued.queue,
      requeued.uniqueKey,
      requeued.id,
      requeued.deduplicationTtl ?? undefined
    );
  }
  shard.notify(requeued.queue);
  return 1;
}
