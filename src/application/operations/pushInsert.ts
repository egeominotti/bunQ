import type { Shard } from '../../domain/queue/shard';
import type { Job, JobId } from '../../domain/types/job';
import type { JobLocation } from '../../domain/types/queue';
import type { SetLike } from '../../shared/lru';
import type { MapLike } from '../../shared/lru';
import type { DependencyResultTracker } from '../dependencyResultTracker';
import type { DependencyCompletionTracker } from '../dependencyCompletions';
import type { SqliteStorage } from '../../infrastructure/persistence/sqlite';

export interface PushInsertContext {
  storage: SqliteStorage | null;
  completedJobs: SetLike<JobId>;
  depCompletions?: DependencyCompletionTracker;
  jobResults: MapLike<JobId, unknown>;
  dependencyResults: DependencyResultTracker;
  jobIndex: Map<JobId, JobLocation>;
  dashboardEmit?: (event: string, data: Record<string, unknown>) => void;
}

export interface PreparedJobInsertion {
  state: 'waiting-children' | 'delayed' | 'prioritized' | 'waiting';
  completionPins: JobId[];
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

/** Complete persistence-sensitive preparation before changing queue membership. */
export function prepareJobInsertion(
  job: Job,
  ctx: PushInsertContext,
  recordTimeline = true,
  now: number = Date.now()
): PreparedJobInsertion {
  const state = initialJobState(job, ctx, now);
  const completionPins =
    state === 'waiting-children'
      ? [...new Set(job.dependsOn)].filter((id) => ctx.depCompletions?.has(id) ?? false)
      : [];
  if (recordTimeline) job.timeline.push({ state, timestamp: now });
  return { state, completionPins };
}

/** Apply only non-throwing in-memory membership changes for a prepared job. */
export function insertPreparedJobToShard(
  job: Job,
  target: { queue: string; shard: Shard; shardIdx: number },
  ctx: PushInsertContext,
  prepared: PreparedJobInsertion
): void {
  const { queue, shard, shardIdx } = target;
  const { state } = prepared;
  for (const id of prepared.completionPins) ctx.depCompletions?.pin(id);
  if (state === 'waiting-children') {
    shard.waitingDeps.set(job.id, job);
    shard.registerDependencies(job.id, job.dependsOn);
  } else {
    shard.getQueue(queue).push(job);
    shard.incrementQueued(job.id, state === 'delayed', job.createdAt, queue, job.runAt);
  }

  ctx.jobIndex.set(job.id, { type: 'queue', shardIdx, queueName: queue });
  ctx.dependencyResults.registerConsumer(job.id, job.dependsOn);
  for (const dependencyId of job.dependsOn) {
    if (ctx.jobResults.has(dependencyId)) {
      ctx.dependencyResults.retain(dependencyId, ctx.jobResults.get(dependencyId));
    }
  }

  if (state === 'waiting-children') {
    try {
      ctx.dashboardEmit?.('job:waiting-children', {
        jobId: String(job.id),
        queue,
        dependsOn: job.dependsOn.map(String),
      });
    } catch (error) {
      console.error('[Push] Dashboard event failed after job admission', { error });
    }
  }
}

/** Insert a new job into the runnable heap or dependency wait set. */
export function insertJobToShard(
  job: Job,
  target: { queue: string; shard: Shard; shardIdx: number },
  ctx: PushInsertContext,
  recordTimeline = true
): void {
  const prepared = prepareJobInsertion(job, ctx, recordTimeline);
  insertPreparedJobToShard(job, target, ctx, prepared);
}
