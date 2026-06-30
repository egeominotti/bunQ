/**
 * Push Operations
 * Job push and batch push logic
 */

import {
  type Job,
  type JobId,
  type JobInput,
  createJob,
  generateJobId,
  jobId,
} from '../../domain/types/job';
import { type JobLocation, EventType } from '../../domain/types/queue';
import type { Shard } from '../../domain/queue/shard';
import type { SqliteStorage } from '../../infrastructure/persistence/sqlite';
import type { RWLock } from '../../shared/lock';
import { withWriteLock } from '../../shared/lock';
import { shardIndex } from '../../shared/hash';
import type { SetLike, MapLike } from '../../shared/lru';
import { latencyTracker } from '../latencyTracker';
import { throughputTracker } from '../throughputTracker';

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

/** Result of checking custom ID */
type CustomIdResult =
  | { skip: true; existingJob: Job }
  | { skip: true; existingId: JobId }
  | { skip: false; id: JobId };

/** Result of deduplication check */
type DedupResult = { skip: true; existingId: JobId } | { skip: false };

/**
 * Handle custom ID idempotency check
 * Returns existing job if found, or new ID to use
 */
function handleCustomId(input: JobInput, shard: Shard, ctx: PushContext): CustomIdResult {
  if (!input.customId) {
    return { skip: false, id: generateJobId() };
  }

  const id = jobId(input.customId);
  const existing = ctx.customIdMap.get(input.customId);

  // Re-adding an existing custom id while its job is UNFINISHED is an idempotent
  // no-op (BullMQ parity): return the existing job, never a second insert of the
  // same deterministic id (which would collide on the jobs.id PRIMARY KEY). Three
  // unfinished states are covered:
  //   • waiting/delayed/prioritized — live in the priority queue → return the job.
  //   • waiting-children — tracked under 'queue' but held in shard.waitingDeps (its
  //     row is already persisted) → idempotent skip via the existing id.
  //   • active (processing) — popped from the queue, still running on a worker; its
  //     row survives on disk → idempotent skip via the existing id.
  // PushContext has no processingShards, so for the latter two we cannot fetch the
  // live Job here; pushJob rebuilds a placeholder { ...job, id } after createJob,
  // exactly like handleDeduplication's active path. Completed (#92) and DLQ jobs
  // are terminal and fall through to the reuse path below.
  if (existing && !ctx.completedJobs.has(id)) {
    const location = ctx.jobIndex.get(existing);
    if (location?.type === 'queue') {
      const existingJob = shard.getQueue(location.queueName).find(existing);
      if (existingJob) {
        return { skip: true, existingJob };
      }
      if (shard.waitingDeps.has(existing)) {
        return { skip: true, existingId: id };
      }
    } else if (location?.type === 'processing') {
      return { skip: true, existingId: id };
    }
  }

  // Reuse path: the id is free in the queue (no mapping, or the prior job is
  // processing/completed). If the prior job COMPLETED, its row survives on disk
  // (markCompleted does an UPDATE, not a DELETE) and it is still in completedJobs.
  // Reusing the same deterministic id would then (a) make getJobState return
  // 'completed' for the brand-new job and (b) collide on the `jobs.id` PRIMARY KEY
  // at flush time. Evict the stale completed job so the reused id starts fresh as
  // 'waiting' (#92). Checked regardless of customIdMap state — the mapping may have
  // been cleared on completion, which would otherwise skip this path entirely.
  if (ctx.completedJobs.has(id)) {
    ctx.completedJobs.delete(id);
    ctx.completedJobsData.delete(id);
    ctx.jobResults.delete(id);
    ctx.jobIndex.delete(id);
    ctx.storage?.deleteJob(id); // removes the surviving row + result + any buffered insert
  }
  // NOTE on ORPHAN rows: a durable jobs row can outlive its in-memory tracking when
  // obliterate() (fire-and-forget void over TCP) or a buffer flush races an in-flight
  // durable insert. Reaching the reuse path with such a stale row no longer throws —
  // the insert statements use ON CONFLICT(id) DO UPDATE (upsert), so the orphan is
  // overwritten in place at insert time with ZERO per-push cost. We deliberately do
  // NOT pre-delete here: deleteJob() is a synchronous DELETE + O(buffer) scan inside
  // the shard lock and would halve customId push/addBulk throughput.
  // A recycled custom id may carry a stale timeout marker from a prior job (which
  // may have DLQ'd, so it is NOT in completedJobs above). Clear it so the new
  // job's stall-retry recovery is not wrongly discarded — otherwise the
  // timeout-resurrection guard would reintroduce #33/#75 duplicate execution for
  // reused ids.
  ctx.timedOutJobs?.delete(id);
  ctx.customIdMap.set(input.customId, id);
  return { skip: false, id };
}

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
 * Insert job into shard (queue or waitingDeps)
 */
function insertJobToShard(
  job: Job,
  queue: string,
  shard: Shard,
  shardIdx: number,
  ctx: PushContext
): void {
  const hasDeps = job.dependsOn.length > 0;
  const needsWaiting =
    hasDeps &&
    !job.dependsOn.every(
      (depId) => ctx.completedJobs.has(depId) || (ctx.depCompletions?.has(depId) ?? false)
    );

  const now = Date.now();
  if (needsWaiting) {
    shard.waitingDeps.set(job.id, job);
    shard.registerDependencies(job.id, job.dependsOn);
    job.timeline.push({ state: 'waiting-children', timestamp: now });
    ctx.dashboardEmit?.('job:waiting-children', {
      jobId: String(job.id),
      queue,
      dependsOn: job.dependsOn.map(String),
    });
  } else {
    shard.getQueue(queue).push(job);
    const isDelayed = job.runAt > now;
    shard.incrementQueued(job.id, isDelayed, job.createdAt, queue, job.runAt);
    // Timeline: initial state based on scheduling and priority
    const state = isDelayed ? 'delayed' : job.priority > 0 ? 'prioritized' : 'waiting';
    job.timeline.push({ state, timestamp: now });
  }

  ctx.jobIndex.set(job.id, { type: 'queue', shardIdx, queueName: queue });
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

  await withWriteLock(ctx.shardLocks[idx], () => {
    const shard = ctx.shards[idx];

    // Check custom ID idempotency INSIDE lock to prevent race conditions
    const customIdResult = handleCustomId(input, shard, ctx);
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
    shard.notify();
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

  await withWriteLock(ctx.shardLocks[idx], () => {
    const shard = ctx.shards[idx];

    for (const input of inputs) {
      // Check custom ID idempotency
      const customIdResult = handleCustomId(input, shard, ctx);
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
      shard.notifyBatch(jobsToInsert.length);
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
