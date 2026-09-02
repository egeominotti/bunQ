import type { Job, JobId } from '../domain/types/job';
import type { SqliteStorage } from '../infrastructure/persistence/sqlite';
import { compareSqliteBinaryText } from '../shared/serialization';
import type { DependencyResultTracker } from './dependencyResultTracker';

export interface CompletedCleanupContext {
  jobIndex: Map<JobId, { type: string; queueName?: string }>;
  completedJobs: { delete(jobId: JobId): boolean };
  completedJobsData: { get(jobId: JobId): Job | undefined; delete(jobId: JobId): boolean };
  jobResults?: { delete(jobId: JobId): boolean };
  jobResultQueues?: {
    get(jobId: JobId): string | undefined;
    delete(jobId: JobId): boolean;
  };
  jobLogs?: { delete(jobId: JobId): boolean };
  jobLogQueues?: {
    get(jobId: JobId): string | undefined;
    delete(jobId: JobId): boolean;
  };
  dependencyResults: DependencyResultTracker;
  storage: SqliteStorage | null;
}

function memoryCandidates(
  queue: string | null,
  completedBefore: number,
  limit: number,
  ctx: CompletedCleanupContext
): Array<{ jobId: JobId; queue: string }> {
  return [...ctx.jobIndex]
    .flatMap(([jobId, location]) => {
      if (location.type !== 'completed') return [];
      if (queue !== null && location.queueName !== queue) return [];
      if (ctx.dependencyResults.hasConsumers(jobId)) return [];
      const job = ctx.completedJobsData.get(jobId);
      const timestamp = job?.completedAt ?? job?.createdAt ?? 0;
      return timestamp <= completedBefore ? [{ jobId, timestamp }] : [];
    })
    .sort(
      (left, right) =>
        left.timestamp - right.timestamp || compareSqliteBinaryText(left.jobId, right.jobId)
    )
    .slice(0, limit)
    .map(({ jobId }) => ({ jobId, queue: ctx.jobIndex.get(jobId)?.queueName ?? '' }));
}

/** Delete completed rows first, then converge every bounded in-memory projection. */
export function cleanCompletedJobs(
  queue: string | null,
  completedBefore: number,
  limit: number,
  ctx: CompletedCleanupContext
): JobId[] {
  const normalizedLimit = Number.isFinite(limit) ? Math.max(0, Math.trunc(limit)) : 0;
  if (normalizedLimit === 0 || !Number.isFinite(completedBefore)) return [];
  const removals = ctx.storage
    ? ctx.storage.cleanCompletedJobs(queue, completedBefore, normalizedLimit, (jobId) =>
        ctx.dependencyResults.hasConsumers(jobId)
      )
    : memoryCandidates(queue, completedBefore, normalizedLimit, ctx);

  for (const { jobId, queue: ownerQueue } of removals) {
    const location = ctx.jobIndex.get(jobId);
    const ownsCurrentCompletion =
      location?.type === 'completed' && location.queueName === ownerQueue;
    const hasNoCurrentGeneration = location === undefined;
    const resultOwner = ctx.jobResultQueues?.get(jobId);
    if (resultOwner === ownerQueue || (resultOwner === undefined && ownsCurrentCompletion)) {
      ctx.jobResults?.delete(jobId);
      ctx.jobResultQueues?.delete(jobId);
    }
    const logOwner = ctx.jobLogQueues?.get(jobId);
    if (logOwner === ownerQueue || (logOwner === undefined && ownsCurrentCompletion)) {
      ctx.jobLogs?.delete(jobId);
      ctx.jobLogQueues?.delete(jobId);
    }
    if (!hasNoCurrentGeneration && !ownsCurrentCompletion) continue;
    ctx.completedJobs.delete(jobId);
    ctx.completedJobsData.delete(jobId);
    ctx.jobIndex.delete(jobId);
    ctx.dependencyResults.releaseConsumer(jobId);
  }
  return removals.map(({ jobId }) => jobId);
}
