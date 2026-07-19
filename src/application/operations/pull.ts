/**
 * Pull Operations
 * Job pull logic with timeout support
 */

import type { Job } from '../../domain/types/job';
import type { EventType } from '../../domain/types/queue';
import type { Shard } from '../../domain/queue/shard';
import { withWriteLock } from '../../shared/lock';
import { shardIndex, processingShardIndex } from '../../shared/hash';
import { latencyTracker } from '../latencyTracker';
import { throughputTracker } from '../throughputTracker';
import {
  createDequeueScan,
  requeueJob,
  restoreParkedJobs,
  tryDequeueNextJob,
  type PullContext,
} from './pullStateTransition';

export type { PullContext } from './pullStateTransition';

interface PullAttempt {
  job: Job | null;
  nextRunAt: number | null;
}

interface BatchPullAttempt {
  jobs: Job[];
  nextRunAt: number | null;
}

/** Wait until either a notification arrives, the next delay matures, or the pull deadline. */
async function waitForNextCandidate(options: {
  shard: Shard;
  queue: string;
  deadline: number;
  now: number;
  nextRunAt: number | null;
  signal?: AbortSignal;
}): Promise<void> {
  const { shard, queue, deadline, now, nextRunAt, signal } = options;
  const remaining = deadline - now;
  const untilNextRun = nextRunAt === null ? remaining : Math.max(1, nextRunAt - now);
  await shard.waitForJob(queue, Math.min(remaining, untilNextRun), signal);
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
 * Pull next job from queue
 */
export async function pullJob(
  queue: string,
  timeoutMs: number,
  ctx: PullContext,
  signal?: AbortSignal
): Promise<Job | null> {
  const startNs = Bun.nanoseconds();
  const deadline = timeoutMs > 0 ? Date.now() + timeoutMs : 0;
  const idx = shardIndex(queue);

  while (true) {
    if (signal?.aborted) return null;
    const attempt = await tryPullFromShard(queue, idx, ctx, signal);
    const job = attempt.job;

    if (job) {
      if (signal?.aborted) {
        await requeueJob(job, queue, idx, ctx);
        return null;
      }
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

    await waitForNextCandidate({
      shard: ctx.shards[idx],
      queue,
      deadline,
      now,
      nextRunAt: attempt.nextRunAt,
      signal,
    });
  }
}

/**
 * Try to pull a job from a specific shard
 */
async function tryPullFromShard(
  queue: string,
  idx: number,
  ctx: PullContext,
  signal?: AbortSignal
): Promise<PullAttempt> {
  return await withWriteLock(ctx.shardLocks[idx], () => {
    if (signal?.aborted) return { job: null, nextRunAt: null };
    const shard = ctx.shards[idx];
    const state = shard.getState(queue);

    if (state.paused) {
      return { job: null, nextRunAt: null };
    }

    const now = Date.now();
    const scan = createDequeueScan();
    try {
      const result = tryDequeueNextJob(shard, queue, now, ctx, scan);
      return result.status === 'job'
        ? { job: result.job, nextRunAt: null }
        : { job: null, nextRunAt: scan.nextRunAt };
    } finally {
      restoreParkedJobs(shard, queue, scan);
    }
  });
}

/**
 * Pull multiple jobs from queue
 */
export async function pullJobBatch(
  queue: string,
  count: number,
  timeoutMs: number,
  ctx: PullContext,
  signal?: AbortSignal
): Promise<Job[]> {
  const startNs = Bun.nanoseconds();
  const deadline = timeoutMs > 0 ? Date.now() + timeoutMs : 0;
  const idx = shardIndex(queue);

  while (true) {
    if (signal?.aborted) return [];
    const attempt = await tryPullBatchFromShard(queue, idx, count, ctx, signal);
    const jobs = attempt.jobs;

    if (jobs.length > 0) {
      if (signal?.aborted) {
        for (const job of jobs) {
          await requeueJob(job, queue, idx, ctx);
        }
        return [];
      }
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

    await waitForNextCandidate({
      shard: ctx.shards[idx],
      queue,
      deadline,
      now,
      nextRunAt: attempt.nextRunAt,
      signal,
    });
  }
}

/**
 * Try to pull multiple jobs from a shard
 */
async function tryPullBatchFromShard(
  queue: string,
  idx: number,
  count: number,
  ctx: PullContext,
  signal?: AbortSignal
): Promise<BatchPullAttempt> {
  return await withWriteLock(ctx.shardLocks[idx], () => {
    if (signal?.aborted) return { jobs: [], nextRunAt: null };
    const shard = ctx.shards[idx];
    const state = shard.getState(queue);
    const jobs: Job[] = [];

    if (state.paused) return { jobs, nextRunAt: null };

    const now = Date.now();
    const scan = createDequeueScan();

    try {
      while (jobs.length < count) {
        const result = tryDequeueNextJob(shard, queue, now, ctx, scan);

        if (result.status === 'job') {
          jobs.push(result.job);
        } else {
          break;
        }
      }

      return { jobs, nextRunAt: scan.nextRunAt };
    } finally {
      restoreParkedJobs(shard, queue, scan);
    }
  });
}
