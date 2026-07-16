/**
 * Pull Operations
 * Job pull logic with timeout support
 */

import {
  type Job,
  type JobId,
  isExpired,
  isReady,
  MAX_TIMELINE_ENTRIES,
} from '../../domain/types/job';
import type { JobLocation, EventType } from '../../domain/types/queue';
import type { Shard } from '../../domain/queue/shard';
import type { SqliteStorage } from '../../infrastructure/persistence/sqlite';
import type { RWLock } from '../../shared/lock';
import { withWriteLock } from '../../shared/lock';
import { shardIndex, processingShardIndex } from '../../shared/hash';
import { latencyTracker } from '../latencyTracker';
import { throughputTracker } from '../throughputTracker';

/** Pull operation context */
export interface PullContext {
  storage: SqliteStorage | null;
  shards: Shard[];
  shardLocks: RWLock[];
  processingShards: Map<JobId, Job>[];
  processingLocks: RWLock[];
  jobIndex: Map<JobId, JobLocation>;
  totalPulled: { value: bigint };
  broadcast: (event: {
    eventType: EventType;
    queue: string;
    jobId: JobId;
    timestamp: number;
  }) => void;
  dashboardEmit?: (event: string, data: Record<string, unknown>) => void;
}

/** Result of trying to dequeue a job */
type DequeueResult = { status: 'job'; job: Job } | { status: 'stop'; nextRunAt: number | null };

interface PullAttempt {
  job: Job | null;
  nextRunAt: number | null;
}

interface BatchPullAttempt {
  jobs: Job[];
  nextRunAt: number | null;
}

/**
 * Dequeue the highest-ranked eligible job without letting a delayed or
 * group-blocked heap root stall unrelated work.
 *
 * The priority heap cannot encode time-varying readiness or active-group
 * membership in its comparator. Temporarily park ineligible roots, select the
 * first eligible entry (therefore the best eligible entry in heap order), and
 * restore every parked job before leaving the shard critical section. Parked
 * jobs remain logically queued throughout: their shard counters and jobIndex
 * entries are deliberately untouched.
 *
 * Complexity is O((k + 1) log n) time and O(k) scratch space, where k is the
 * number of higher-ranked ineligible jobs skipped. A separate delayed heap and
 * per-group eligible-head heap can reduce this to O(log n), but this scan keeps
 * the existing queue representation and state transitions intact.
 */
function tryDequeueNextJob(
  shard: Shard,
  queue: string,
  now: number,
  ctx: PullContext
): DequeueResult {
  const q = shard.getQueue(queue);
  const parked: Job[] = [];
  let nextRunAt: number | null = null;

  try {
    while (true) {
      const job = q.peek();
      if (!job) return { status: 'stop', nextRunAt };

      // Expired entries are not parked: they are genuinely removed from the
      // logical queue and therefore update counters/index exactly once.
      if (isExpired(job, now)) {
        q.pop();
        shard.decrementQueued(job.id);
        ctx.jobIndex.delete(job.id);
        ctx.dashboardEmit?.('job:expired', {
          queue,
          jobId: String(job.id),
          ttl: job.ttl,
          age: now - job.createdAt,
        });
        continue;
      }

      if (!isReady(job, now)) {
        const delayed = q.pop();
        if (delayed) parked.push(delayed);
        nextRunAt = nextRunAt === null ? job.runAt : Math.min(nextRunAt, job.runAt);
        continue;
      }

      if (job.groupId && shard.isGroupActive(queue, job.groupId)) {
        const blocked = q.pop();
        if (blocked) parked.push(blocked);
        continue;
      }

      // Check capacity only after proving that an eligible job exists. Check
      // concurrency first because that slot can be returned if rate limiting
      // rejects; rate-limit tokens have no rollback operation.
      if (!shard.tryAcquireConcurrency(queue)) {
        ctx.dashboardEmit?.('concurrency:rejected', { queue });
        return { status: 'stop', nextRunAt };
      }
      if (!shard.tryAcquireRateLimit(queue)) {
        shard.releaseConcurrency(queue);
        ctx.dashboardEmit?.('ratelimit:rejected', { queue });
        return { status: 'stop', nextRunAt };
      }

      // Dequeue and update. q.peek() and q.pop() are synchronous under the
      // shard write lock, so pop returns the same eligible entry selected above.
      const dequeued = q.pop();
      if (!dequeued) {
        shard.releaseConcurrency(queue);
        return { status: 'stop', nextRunAt };
      }
      shard.decrementQueued(dequeued.id);

      if (dequeued.groupId) {
        shard.activateGroup(queue, dequeued.groupId);
      }

      dequeued.startedAt = now;
      dequeued.lastHeartbeat = now;
      if (dequeued.timeline.length < MAX_TIMELINE_ENTRIES) {
        dequeued.timeline.push({ state: 'active', timestamp: now });
      }

      // Make the queue -> processing transition atomic for observers. From the
      // pop above, the job is no longer findable in the shard queue, so a
      // jobIndex entry still saying 'queue' would make getJob/getJobState return
      // a false null/'unknown' for an existing job (JOB -> null -> JOB(active)
      // flicker), and cleanOrphanedJobIndex would even drop its index entry.
      // Insert into processingShards and flip the index HERE, in the same
      // synchronous critical section as the pop (the caller holds the shard
      // write lock; JS is single-threaded, so no other task can interleave).
      // Writing processingShards without its RWLock is safe: until this
      // statement the index said 'queue', so no id-targeted critical section
      // (ack/fail/discard operate only on ids they find as 'processing') can be
      // mid-operation on this id; a sync Map.set cannot corrupt a paused
      // critical section; and lock-free sync access to processingShards is
      // established practice (stallDetection phases 1/2, collectActiveJobs,
      // getJobProgress). Acquiring the async RWLock here would instead hold the
      // hot shard write lock across an await.
      const procIdx = processingShardIndex(dequeued.id);
      ctx.processingShards[procIdx].set(dequeued.id, dequeued);
      ctx.jobIndex.set(dequeued.id, { type: 'processing', shardIdx: procIdx });

      return { status: 'job', job: dequeued };
    }
  } finally {
    // Restore only the priority-queue membership. The logical queue counters,
    // temporal index and global jobIndex never changed for parked entries.
    for (const parkedJob of parked) q.push(parkedJob);
  }
}

/** Wait until either a notification arrives, the next delay matures, or the pull deadline. */
async function waitForNextCandidate(
  shard: Shard,
  queue: string,
  deadline: number,
  now: number,
  nextRunAt: number | null
): Promise<void> {
  const remaining = deadline - now;
  const untilNextRun = nextRunAt === null ? remaining : Math.max(1, nextRunAt - now);
  await shard.waitForJob(queue, Math.min(remaining, untilNextRun));
}

/**
 * Finish the handoff of a dequeued job: persist active state and broadcast.
 *
 * The job was ALREADY inserted into processingShards and jobIndex was flipped
 * to 'processing' inside the dequeue critical section (tryDequeueNextJob),
 * atomically with the queue pop, so readers never see a stale 'queue'
 * location. Only the bookkeeping that may follow an await happens here.
 *
 * Returns false when the job is no longer in processingShards: a management
 * op (discardJob, moveJobToDelayed, obliterate) claimed it between the
 * dequeue and this call. The claimer owns the job now; the pull must NOT
 * deliver it to a worker nor mark it active.
 */
function finalizeProcessing(job: Job, queue: string, ctx: PullContext): boolean {
  const procIdx = processingShardIndex(job.id);
  if (!ctx.processingShards[procIdx].has(job.id)) return false;

  const now = Date.now();
  try {
    ctx.storage?.markActive(job.id, job.startedAt ?? now, job.timeline);
  } catch {
    // Non-fatal: job is already in processingShards (in-memory source of truth).
    // On crash, SQLite recovery will handle the stale state.
  }
  ctx.totalPulled.value++;
  throughputTracker.pullRate.increment();
  ctx.broadcast({
    eventType: 'pulled' as EventType,
    queue,
    jobId: job.id,
    timestamp: now,
  });
  return true;
}

/**
 * Finish the handoff for a batch of dequeued jobs.
 * Returns the jobs actually delivered (claimed jobs are skipped, see above).
 */
function finalizeProcessingBatch(jobs: Job[], queue: string, ctx: PullContext): Job[] {
  const delivered: Job[] = [];
  for (const job of jobs) {
    if (finalizeProcessing(job, queue, ctx)) delivered.push(job);
  }
  return delivered;
}

/**
 * Requeue a job whose handoff to the worker failed after dequeue.
 * The dequeue critical section moved the job to processingShards and flipped
 * jobIndex to 'processing'; this restores both, keeping the same atomicity
 * rule: the job becomes findable in the queue (push + index flip in ONE
 * shard-lock section) BEFORE the processing entry is dropped, so a reader
 * never sees a location that misses. Lock order shard -> processing matches
 * the documented hierarchy.
 */
async function requeueJob(job: Job, queue: string, idx: number, ctx: PullContext): Promise<void> {
  const procIdx = processingShardIndex(job.id);
  let requeued = false;

  await withWriteLock(ctx.shardLocks[idx], () => {
    // A management op (discardJob, moveJobToDelayed) may have claimed the job
    // from processingShards after the failed handoff; it owns the job now.
    if (!ctx.processingShards[procIdx].has(job.id)) return;
    const shard = ctx.shards[idx];
    if (job.groupId) shard.releaseGroup(queue, job.groupId);
    shard.releaseConcurrency(queue);
    // Reset job state (was modified in tryDequeueNextJob)
    job.startedAt = null;
    shard.getQueue(queue).push(job);
    shard.incrementQueued(job.id, false, job.createdAt, queue, job.runAt);
    ctx.jobIndex.set(job.id, { type: 'queue', shardIdx: idx, queueName: queue });
    shard.notify(queue);
    requeued = true;
  });

  if (!requeued) return;

  await withWriteLock(ctx.processingLocks[procIdx], () => {
    // A concurrent pull may have re-popped the job between the two lock
    // sections and flipped the index back to 'processing'; do not delete the
    // (re-inserted, same-object) entry out from under it.
    if (ctx.jobIndex.get(job.id)?.type !== 'processing') {
      ctx.processingShards[procIdx].delete(job.id);
    }
  });
}

/**
 * Pull next job from queue
 */
export async function pullJob(
  queue: string,
  timeoutMs: number,
  ctx: PullContext
): Promise<Job | null> {
  const startNs = Bun.nanoseconds();
  const deadline = timeoutMs > 0 ? Date.now() + timeoutMs : 0;
  const idx = shardIndex(queue);

  while (true) {
    const attempt = await tryPullFromShard(queue, idx, ctx);
    const job = attempt.job;

    if (job) {
      let delivered = false;
      try {
        delivered = finalizeProcessing(job, queue, ctx);
      } catch {
        // Safety net: if the handoff fails, push job back to prevent loss
        await requeueJob(job, queue, idx, ctx);
        return null;
      }
      // Claimed by a management op during the handoff: try the next job
      if (!delivered) continue;
      latencyTracker.pull.observe((Bun.nanoseconds() - startNs) / 1e6);
      return job;
    }

    // No job available, check timeout
    // Cache Date.now() to avoid multiple syscalls per iteration
    const now = Date.now();
    if (deadline === 0 || now >= deadline) {
      return null;
    }

    await waitForNextCandidate(ctx.shards[idx], queue, deadline, now, attempt.nextRunAt);
  }
}

/**
 * Try to pull a job from a specific shard
 */
async function tryPullFromShard(
  queue: string,
  idx: number,
  ctx: PullContext
): Promise<PullAttempt> {
  return await withWriteLock(ctx.shardLocks[idx], () => {
    const shard = ctx.shards[idx];
    const state = shard.getState(queue);

    if (state.paused) {
      return { job: null, nextRunAt: null };
    }

    const now = Date.now();
    const result = tryDequeueNextJob(shard, queue, now, ctx);
    return result.status === 'job'
      ? { job: result.job, nextRunAt: null }
      : { job: null, nextRunAt: result.nextRunAt };
  });
}

/**
 * Pull multiple jobs from queue
 */
export async function pullJobBatch(
  queue: string,
  count: number,
  timeoutMs: number,
  ctx: PullContext
): Promise<Job[]> {
  const startNs = Bun.nanoseconds();
  const deadline = timeoutMs > 0 ? Date.now() + timeoutMs : 0;
  const idx = shardIndex(queue);

  while (true) {
    const attempt = await tryPullBatchFromShard(queue, idx, count, ctx);
    const jobs = attempt.jobs;

    if (jobs.length > 0) {
      let delivered: Job[];
      try {
        delivered = finalizeProcessingBatch(jobs, queue, ctx);
      } catch {
        // Safety net: push all jobs back to prevent loss (requeueJob skips
        // jobs a management op already claimed)
        for (const job of jobs) {
          await requeueJob(job, queue, idx, ctx);
        }
        return [];
      }
      if (delivered.length > 0) {
        if (delivered.length > 1) {
          ctx.dashboardEmit?.('batch:pulled', { queue, count: delivered.length });
        }
        latencyTracker.pull.observe((Bun.nanoseconds() - startNs) / 1e6);
        return delivered;
      }
      // Every dequeued job was claimed by a management op: fall through to
      // the timeout/wait logic below (nothing to deliver this round)
    }

    // No jobs available, check timeout
    // Cache Date.now() to avoid multiple syscalls per iteration
    const now = Date.now();
    if (deadline === 0 || now >= deadline) {
      return [];
    }

    await waitForNextCandidate(ctx.shards[idx], queue, deadline, now, attempt.nextRunAt);
  }
}

/**
 * Try to pull multiple jobs from a shard
 */
async function tryPullBatchFromShard(
  queue: string,
  idx: number,
  count: number,
  ctx: PullContext
): Promise<BatchPullAttempt> {
  return await withWriteLock(ctx.shardLocks[idx], () => {
    const shard = ctx.shards[idx];
    const state = shard.getState(queue);
    const jobs: Job[] = [];

    if (state.paused) return { jobs, nextRunAt: null };

    const now = Date.now();
    let nextRunAt: number | null = null;

    while (jobs.length < count) {
      const result = tryDequeueNextJob(shard, queue, now, ctx);

      if (result.status === 'job') {
        jobs.push(result.job);
      } else {
        nextRunAt = result.nextRunAt;
        break;
      }
    }

    return { jobs, nextRunAt };
  });
}
