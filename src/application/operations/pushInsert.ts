import type { Shard } from '../../domain/queue/shard';
import type { Job, JobId } from '../../domain/types/job';
import type { JobLocation } from '../../domain/types/queue';
import type { SetLike } from '../../shared/lru';
import type { MapLike } from '../../shared/lru';
import type { DependencyResultTracker } from '../dependencyResultTracker';

export interface PushInsertContext {
  completedJobs: SetLike<JobId>;
  depCompletions?: SetLike<JobId>;
  jobResults: MapLike<JobId, unknown>;
  dependencyResults: DependencyResultTracker;
  jobIndex: Map<JobId, JobLocation>;
  dashboardEmit?: (event: string, data: Record<string, unknown>) => void;
}

/** Resolve the first externally visible state without mutating queue structures. */
export function initialJobState(
  job: Job,
  ctx: Pick<PushInsertContext, 'completedJobs' | 'depCompletions'>,
  now: number = Date.now()
): 'waiting-children' | 'delayed' | 'prioritized' | 'waiting' {
  const needsWaiting =
    job.dependsOn.length > 0 &&
    !job.dependsOn.every(
      (depId) => ctx.completedJobs.has(depId) || (ctx.depCompletions?.has(depId) ?? false)
    );
  if (needsWaiting) return 'waiting-children';
  if (job.runAt > now) return 'delayed';
  return job.priority > 0 ? 'prioritized' : 'waiting';
}

/** Insert a new job into the runnable heap or dependency wait set. */
export function insertJobToShard(
  job: Job,
  target: { queue: string; shard: Shard; shardIdx: number },
  ctx: PushInsertContext,
  recordTimeline = true
): void {
  const { queue, shard, shardIdx } = target;
  const now = Date.now();
  const state = initialJobState(job, ctx, now);
  if (state === 'waiting-children') {
    shard.waitingDeps.set(job.id, job);
    shard.registerDependencies(job.id, job.dependsOn);
    if (recordTimeline) job.timeline.push({ state, timestamp: now });
    ctx.dashboardEmit?.('job:waiting-children', {
      jobId: String(job.id),
      queue,
      dependsOn: job.dependsOn.map(String),
    });
  } else {
    shard.getQueue(queue).push(job);
    const isDelayed = job.runAt > now;
    shard.incrementQueued(job.id, isDelayed, job.createdAt, queue, job.runAt);
    if (recordTimeline) job.timeline.push({ state, timestamp: now });
  }

  ctx.jobIndex.set(job.id, { type: 'queue', shardIdx, queueName: queue });
  ctx.dependencyResults.registerConsumer(job.id, job.dependsOn);
  for (const dependencyId of job.dependsOn) {
    if (ctx.jobResults.has(dependencyId)) {
      ctx.dependencyResults.retain(dependencyId, ctx.jobResults.get(dependencyId));
    }
  }
}
