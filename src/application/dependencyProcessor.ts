/**
 * Dependency Processor - Job dependency resolution
 * Uses reverse index for O(m) where m = jobs waiting on completed deps
 */

import type { Job, JobId } from '../domain/types/job';
import { MAX_TIMELINE_ENTRIES } from '../domain/types/job';
import { SHARD_COUNT } from '../shared/hash';
import { withWriteLock } from '../shared/lock';
import type { BackgroundContext } from './types';
import { releaseDependencyCompletionPins } from './dependencyCompletions';

type DependencyReadyState = 'delayed' | 'prioritized' | 'waiting';

export function dependencyReadyState(job: Job, now: number): DependencyReadyState {
  if (job.runAt > now) return 'delayed';
  return job.priority > 0 ? 'prioritized' : 'waiting';
}

export function checkpointDependencyPromotion(
  job: Job,
  state: DependencyReadyState,
  now: number,
  storage: BackgroundContext['storage']
): void {
  if (job.timeline.at(-1)?.state !== state && job.timeline.length < MAX_TIMELINE_ENTRIES) {
    job.timeline.push({ state, timestamp: now });
  }
  storage?.updateFlowParentResolution(job, state);
}

/**
 * Process pending dependency checks
 * Resolves jobs whose dependencies have been completed
 */
export async function processPendingDependencies(ctx: BackgroundContext): Promise<void> {
  if (ctx.pendingDepChecks.size === 0) return;

  const completedIds = Array.from(ctx.pendingDepChecks);
  const completedNow = new Set(completedIds);
  ctx.pendingDepChecks.clear();

  const jobsToCheckByShard = new Map<number, Set<JobId>>();
  const releasedDependencyIds = new Set<JobId>();

  // Find all jobs waiting for the completed dependencies
  for (const completedId of completedIds) {
    for (let i = 0; i < SHARD_COUNT; i++) {
      const waitingJobIds = ctx.shards[i].getJobsWaitingFor(completedId);
      if (waitingJobIds && waitingJobIds.size > 0) {
        let shardJobs = jobsToCheckByShard.get(i);
        if (!shardJobs) {
          shardJobs = new Set();
          jobsToCheckByShard.set(i, shardJobs);
        }
        for (const jobId of waitingJobIds) {
          shardJobs.add(jobId);
        }
      }
    }
  }

  // Process each shard in parallel
  // Lock acquired BEFORE reading waitingDeps to prevent TOCTOU race
  await Promise.all(
    Array.from(jobsToCheckByShard.entries()).map(async ([i, jobIdsToCheck]) => {
      await withWriteLock(ctx.shardLocks[i], () => {
        const shard = ctx.shards[i];
        const jobsToPromote: Job[] = [];

        // Check which jobs have all dependencies satisfied (inside lock).
        // A removeOnComplete parent is not in completedJobs (its full record was
        // dropped to bound memory), so also honor its bare-id depCompletions entry.
        for (const jobId of jobIdsToCheck) {
          const job = shard.waitingDeps.get(jobId);
          if (
            job?.dependsOn.every(
              (dep) =>
                completedNow.has(dep) ||
                ctx.completedJobs.has(dep) ||
                (ctx.depCompletions?.has(dep) ?? false)
            )
          ) {
            jobsToPromote.push(job);
          }
        }

        // Promote jobs with all dependencies satisfied
        if (jobsToPromote.length > 0) {
          promoteJobsToQueue(jobsToPromote, shard, ctx, i, releasedDependencyIds);
        }
      });
    })
  );

  releaseDependencyCompletionPins(releasedDependencyIds, ctx);
}

/** Move jobs from waitingDeps to the active queue */
function promoteJobsToQueue(
  jobsToPromote: Job[],
  shard: BackgroundContext['shards'][number],
  ctx: BackgroundContext,
  shardIdx: number,
  releasedDependencyIds: Set<JobId>
): void {
  const now = Date.now();

  for (const job of jobsToPromote) {
    if (shard.waitingDeps.has(job.id)) {
      const state = dependencyReadyState(job, now);
      checkpointDependencyPromotion(job, state, now, ctx.storage);
      shard.waitingDeps.delete(job.id);
      shard.unregisterDependencies(job.id, job.dependsOn);
      for (const dependencyId of job.dependsOn) releasedDependencyIds.add(dependencyId);
      shard.getQueue(job.queue).push(job);
      const isDelayed = state === 'delayed';
      shard.incrementQueued(job.id, isDelayed, job.createdAt, job.queue, job.runAt);
      ctx.jobIndex.set(job.id, { type: 'queue', shardIdx, queueName: job.queue });
    }
  }

  if (jobsToPromote.length > 0) {
    const promotedByQueue = new Map<string, number>();
    for (const job of jobsToPromote) {
      promotedByQueue.set(job.queue, (promotedByQueue.get(job.queue) ?? 0) + 1);
    }
    for (const [queue, count] of promotedByQueue) shard.notifyBatch(queue, count);
    for (const job of jobsToPromote) {
      ctx.dashboardEmit?.('job:dependencies-resolved', { jobId: String(job.id), queue: job.queue });
    }
  }
}
