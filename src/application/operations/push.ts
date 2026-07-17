/**
 * Push Operations
 * Job push and batch push logic
 */

import { type Job, type JobId, type JobInput, createJob } from '../../domain/types/job';
import { type JobLocation, EventType } from '../../domain/types/queue';
import type { Shard } from '../../domain/queue/shard';
import type { SqliteStorage } from '../../infrastructure/persistence/sqlite';
import type { RWLock } from '../../shared/lock';
import { shardIndex } from '../../shared/hash';
import type { SetLike, MapLike } from '../../shared/lru';
import { latencyTracker } from '../latencyTracker';
import { throughputTracker } from '../throughputTracker';
import { handleCustomId } from './customId';
import { insertJobToShard } from './pushInsert';
import { withPushWriteLocks } from './pushLocks';

/** Push operation context */
export interface PushContext {
  storage: SqliteStorage | null;
  shards: Shard[];
  shardLocks: RWLock[];
  completedJobs: SetLike<JobId>;
  completedJobsData: MapLike<JobId, Job>;
  /** Bare completion ids for removeOnComplete jobs so dependents start ready */
  depCompletions?: SetLike<JobId>;
  /** Timeout markers — cleared on custom-id reuse so a recycled id starts clean */
  timedOutJobs?: SetLike<JobId>;
  jobResults: MapLike<JobId, unknown>;
  customIdMap: MapLike<string, JobId>;
  jobIndex: Map<JobId, JobLocation>;
  totalPushed: { value: bigint };
  broadcast: (event: {
    eventType: EventType;
    queue: string;
    jobId: JobId;
    timestamp: number;
  }) => void;
  dashboardEmit?: (event: string, data: Record<string, unknown>) => void;
}

/** Result of deduplication check */
type DedupResult = { skip: true; existingId: JobId } | { skip: false };

/**
 * Handle unique key deduplication
 * Returns existing job ID if duplicate found and should skip, or allows insert
 */
function handleDeduplication(
  job: Job,
  input: JobInput,
  queue: string,
  shard: Shard,
  ctx: PushContext
): DedupResult {
  if (!job.uniqueKey) {
    return { skip: false };
  }

  const q = shard.getQueue(queue);
  const existingEntry = shard.getUniqueKeyEntry(queue, job.uniqueKey);

  if (!existingEntry) {
    shard.registerUniqueKeyWithTtl(queue, job.uniqueKey, job.id, input.dedup?.ttl);
    return { skip: false };
  }

  const dedupOpts = input.dedup;

  // Replace strategy: remove old, insert new
  if (dedupOpts?.replace) {
    const existingJob = q.find(existingEntry.jobId);
    if (existingJob) {
      q.remove(existingEntry.jobId);
      shard.decrementQueued(existingEntry.jobId);
      ctx.jobIndex.delete(existingEntry.jobId);
    }
    shard.releaseUniqueKey(queue, job.uniqueKey);
    shard.registerUniqueKeyWithTtl(queue, job.uniqueKey, job.id, dedupOpts?.ttl);
    return { skip: false };
  }

  // Extend strategy: reset TTL, return existing
  if (dedupOpts?.extend && dedupOpts?.ttl) {
    shard.extendUniqueKeyTtl(queue, job.uniqueKey, dedupOpts.ttl);
    if (input.customId) ctx.customIdMap.delete(input.customId);
    const existingJob = q.find(existingEntry.jobId);
    if (existingJob) {
      ctx.dashboardEmit?.('job:deduplicated', {
        queue,
        jobId: String(existingEntry.jobId),
        strategy: 'extend',
      });
      return { skip: true, existingId: existingEntry.jobId };
    }
    throw new Error('Duplicate unique_key (extended TTL)');
  }

  // Default: return existing job (BullMQ-style)
  // Block if job is waiting (in priority queue) OR active (uniqueKey held until ack,
  // jobIndex entry type 'processing' means the worker is still running it).
  if (input.customId) ctx.customIdMap.delete(input.customId);
  const existingJob = q.find(existingEntry.jobId);
  if (existingJob || ctx.jobIndex.has(existingEntry.jobId)) {
    ctx.broadcast({
      eventType: EventType.Duplicated,
      queue,
      jobId: existingEntry.jobId,
      timestamp: Date.now(),
    });
    ctx.dashboardEmit?.('job:deduplicated', {
      queue,
      jobId: String(existingEntry.jobId),
      strategy: 'default',
    });
    return { skip: true, existingId: existingEntry.jobId };
  }

  // Job is completed/failed (not in jobIndex) - allow new insert
  shard.registerUniqueKeyWithTtl(queue, job.uniqueKey, job.id, input.dedup?.ttl);
  return { skip: false };
}

/**
 * Push a single job to queue
 * NOTE: customId check happens INSIDE lock to prevent race conditions
 */
export async function pushJob(queue: string, input: JobInput, ctx: PushContext): Promise<Job> {
  const startNs = Bun.nanoseconds();
  const idx = shardIndex(queue);
  const now = Date.now();
  let result: { job: Job; persisted: boolean } | undefined;

  await withPushWriteLocks(queue, [input], ctx, (lockedShardIndexes) => {
    const shard = ctx.shards[idx];

    // Check custom ID idempotency INSIDE lock to prevent race conditions
    const customIdResult = handleCustomId(input, ctx, lockedShardIndexes);
    if (customIdResult.skip) {
      // Idempotent re-add: return the live queued job if we have it, otherwise
      // (active / waiting-children) a placeholder carrying the existing id so the
      // caller sees the right id without inserting a duplicate row.
      result = {
        job:
          'existingJob' in customIdResult
            ? customIdResult.existingJob
            : createJob(customIdResult.existingId, queue, input, now),
        persisted: false,
      };
      return;
    }

    const job = createJob(customIdResult.id, queue, input, now);

    // Check deduplication
    const dedupResult = handleDeduplication(job, input, queue, shard, ctx);
    if (dedupResult.skip) {
      const existingJob = shard.getQueue(queue).find(dedupResult.existingId);
      // Return existing waiting job if found; if active (not in queue), use a
      // placeholder with the correct ID so the caller sees the right ID without
      // inserting a duplicate.
      result = {
        job: existingJob ?? { ...job, id: dedupResult.existingId },
        persisted: false,
      };
      return;
    }

    // Insert to shard
    insertJobToShard(job, queue, shard, idx, ctx);
    shard.notify(queue);
    result = { job, persisted: true };
  });

  if (!result) {
    console.error('[Push] Push failed unexpectedly', { queue, input });
    throw new Error('Push failed');
  }

  if (result.persisted) {
    ctx.storage?.insertJob(result.job, input.durable);
    ctx.totalPushed.value++;
    throughputTracker.pushRate.increment();
    ctx.broadcast({
      eventType: 'pushed' as EventType,
      queue,
      jobId: result.job.id,
      timestamp: now,
    });
  }

  latencyTracker.push.observe((Bun.nanoseconds() - startNs) / 1e6);
  return result.job;
}

/**
 * Push multiple jobs to queue
 * NOTE: customId check happens INSIDE lock (safer for concurrent batch inserts)
 */
export async function pushJobBatch(
  queue: string,
  inputs: JobInput[],
  ctx: PushContext
): Promise<JobId[]> {
  const startNs = Bun.nanoseconds();
  const now = Date.now();
  const idx = shardIndex(queue);
  const resultIds: JobId[] = [];
  const jobsToInsert: Job[] = [];
  // Jobs flagged durable must bypass the 10ms write buffer (immediate fsync
  // path), exactly like a single durable push — otherwise addBulk silently
  // downgrades the documented "durable" guarantee.
  const durableJobs: Job[] = [];

  await withPushWriteLocks(queue, inputs, ctx, (lockedShardIndexes) => {
    const shard = ctx.shards[idx];

    for (const input of inputs) {
      // Check custom ID idempotency
      const customIdResult = handleCustomId(input, ctx, lockedShardIndexes);
      if (customIdResult.skip) {
        // Idempotent re-add (queued, active, or waiting-children) — no insert.
        resultIds.push(
          'existingJob' in customIdResult
            ? customIdResult.existingJob.id
            : customIdResult.existingId
        );
        continue;
      }

      const job = createJob(customIdResult.id, queue, input, now);

      // Check deduplication
      const dedupResult = handleDeduplication(job, input, queue, shard, ctx);
      if (dedupResult.skip) {
        resultIds.push(dedupResult.existingId);
        continue;
      }

      // Insert to shard
      insertJobToShard(job, queue, shard, idx, ctx);
      jobsToInsert.push(job);
      if (input.durable) durableJobs.push(job);
      resultIds.push(job.id);
    }

    if (jobsToInsert.length > 0) {
      shard.notifyBatch(queue, jobsToInsert.length);
    }
  });

  if (jobsToInsert.length > 0) {
    if (durableJobs.length === 0) {
      ctx.storage?.insertJobsBatch(jobsToInsert);
    } else {
      // Durable jobs bypass the write buffer (immediate disk write); the rest
      // still go through the batched buffer for throughput.
      const durableSet = new Set(durableJobs);
      const buffered = jobsToInsert.filter((j) => !durableSet.has(j));
      if (buffered.length > 0) ctx.storage?.insertJobsBatch(buffered);
      ctx.storage?.insertJobsBatch(durableJobs, true);
    }
    ctx.totalPushed.value += BigInt(jobsToInsert.length);
    throughputTracker.pushRate.increment(jobsToInsert.length);

    for (const job of jobsToInsert) {
      ctx.broadcast({
        eventType: 'pushed' as EventType,
        queue: job.queue,
        jobId: job.id,
        timestamp: now,
      });
    }
  }

  if (jobsToInsert.length > 0 && inputs.length > 1) {
    ctx.dashboardEmit?.('batch:pushed', {
      queue,
      total: inputs.length,
      inserted: jobsToInsert.length,
      duplicates: inputs.length - jobsToInsert.length,
    });
  }

  latencyTracker.push.observe((Bun.nanoseconds() - startNs) / 1e6);
  return resultIds;
}
