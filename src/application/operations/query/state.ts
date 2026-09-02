import type { JobId } from '../../../domain/types/job';
import { JobState } from '../../../domain/types/job';
import { withReadLock } from '../../../shared/lock';
import type { QueryContext } from '../../types/query';

function resolveStateFromStorage(
  jobId: JobId,
  storage: QueryContext['storage']
): JobState | 'unknown' {
  if (!storage) return 'unknown';
  if (storage.hasDlqEntry(jobId)) return JobState.Failed;
  const persisted = storage.getBufferedJobState(jobId) ?? storage.getJobStateRaw(jobId);
  if (persisted === 'completed') return JobState.Completed;
  if (persisted === 'active') return JobState.Active;
  if (persisted === 'waiting-children') return 'waiting-children' as JobState;
  if (persisted !== 'waiting' && persisted !== 'delayed' && persisted !== 'prioritized') {
    return 'unknown';
  }
  const row = storage.getBufferedJob(jobId) ?? storage.getJob(jobId);
  if (!row) return 'unknown';
  if (row.runAt > Date.now()) return JobState.Delayed;
  return row.priority > 0 ? JobState.Prioritized : JobState.Waiting;
}

export async function getJobState(jobId: JobId, ctx: QueryContext): Promise<JobState | 'unknown'> {
  for (let pass = 0; pass < 4; pass++) {
    const location = ctx.jobIndex.get(jobId);
    if (ctx.completedJobs.has(jobId)) return JobState.Completed;
    if (!location) return resolveStateFromStorage(jobId, ctx.storage);

    switch (location.type) {
      case 'queue': {
        const result = await withReadLock(ctx.shardLocks[location.shardIdx], () => {
          const shard = ctx.shards[location.shardIdx];
          const queueJob = shard.getQueue(location.queueName).find(jobId);
          if (queueJob) return { job: queueJob, waitingDeps: false, waitingChildren: false };
          const depsJob = shard.waitingDeps.get(jobId);
          if (depsJob) return { job: depsJob, waitingDeps: true, waitingChildren: false };
          const childrenJob = shard.waitingChildren.get(jobId);
          if (childrenJob) return { job: childrenJob, waitingDeps: false, waitingChildren: true };
          return null;
        });
        if (!result) {
          if (ctx.jobIndex.get(jobId) === location) return 'unknown';
          break;
        }
        if (result.waitingDeps || result.waitingChildren) return 'waiting-children' as JobState;
        if (result.job.runAt > Date.now()) return JobState.Delayed;
        return result.job.priority > 0 ? JobState.Prioritized : JobState.Waiting;
      }
      case 'processing':
        return JobState.Active;
      case 'completed':
        return JobState.Completed;
      case 'dlq':
        return JobState.Failed;
    }
  }
  return 'unknown';
}
