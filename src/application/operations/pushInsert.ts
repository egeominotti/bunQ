import type { Shard } from '../../domain/queue/shard';
import type { Job, JobId } from '../../domain/types/job';
import type { JobLocation } from '../../domain/types/queue';
import type { SetLike } from '../../shared/lru';

export interface PushInsertContext {
  completedJobs: SetLike<JobId>;
  depCompletions?: SetLike<JobId>;
  jobIndex: Map<JobId, JobLocation>;
  dashboardEmit?: (event: string, data: Record<string, unknown>) => void;
}

/** Insert a new job into the runnable heap or dependency wait set. */
export function insertJobToShard(
  job: Job,
  queue: string,
  shard: Shard,
  shardIdx: number,
  ctx: PushInsertContext
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
    const state = isDelayed ? 'delayed' : job.priority > 0 ? 'prioritized' : 'waiting';
    job.timeline.push({ state, timestamp: now });
  }

  ctx.jobIndex.set(job.id, { type: 'queue', shardIdx, queueName: queue });
}
