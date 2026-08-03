import type { Job, JobId } from '../../../domain/types/job';
import { shardIndex } from '../../../shared/hash';
import { withReadLock } from '../../../shared/lock';
import type { QueryContext } from '../../types/query';

export async function getJob(jobId: JobId, ctx: QueryContext): Promise<Job | null> {
  for (let pass = 0; pass < 4; pass++) {
    const location = ctx.jobIndex.get(jobId);
    if (!location) {
      if (ctx.storage) {
        const job = ctx.storage.getJob(jobId);
        if (job) return job;
        const dlqEntry = ctx.storage.getDlqEntry(jobId);
        if (dlqEntry) return dlqEntry.job;
      }
      return ctx.completedJobsData.get(jobId) ?? null;
    }

    let found: Job | null = null;
    switch (location.type) {
      case 'queue': {
        found = await withReadLock(ctx.shardLocks[location.shardIdx], () => {
          const shard = ctx.shards[location.shardIdx];
          return (
            shard.getQueue(location.queueName).find(jobId) ??
            shard.waitingDeps.get(jobId) ??
            shard.waitingChildren.get(jobId) ??
            null
          );
        });
        break;
      }
      case 'processing': {
        found = await withReadLock(ctx.processingLocks[location.shardIdx], () => {
          return ctx.processingShards[location.shardIdx].get(jobId) ?? null;
        });
        break;
      }
      case 'completed':
        found = ctx.storage?.getJob(jobId) ?? ctx.completedJobsData.get(jobId) ?? null;
        break;
      case 'dlq': {
        if (ctx.storage) {
          const dlqEntry = ctx.storage.getDlqEntry(jobId);
          if (dlqEntry) return dlqEntry.job;
          const job = ctx.storage.getJob(jobId);
          if (job) return job;
        }
        const dlqShardIdx = shardIndex(location.queueName);
        const dlqJobs = ctx.shards[dlqShardIdx].getDlq(location.queueName);
        found = dlqJobs.find((job) => job.id === jobId) ?? null;
        break;
      }
    }
    if (found) return found;
    if (ctx.jobIndex.get(jobId) === location) return null;
  }
  return null;
}

export function getJobResult(jobId: JobId, ctx: QueryContext): unknown {
  if (ctx.jobResults.has(jobId)) return ctx.jobResults.get(jobId);
  if (ctx.dependencyResults.has(jobId)) return ctx.dependencyResults.get(jobId);
  const stored = ctx.storage?.getResult(jobId);
  if (stored !== null) return stored;
  return ctx.storage?.hasResult(jobId) ? null : undefined;
}

export function getJobByCustomId(customId: string, ctx: QueryContext): Job | null {
  const jobId = ctx.customIdMap.get(customId);
  if (!jobId) return null;

  const location = ctx.jobIndex.get(jobId);
  if (!location) return null;

  if (location.type === 'queue') {
    const shard = ctx.shards[location.shardIdx];
    return (
      shard.getQueue(location.queueName).find(jobId) ??
      shard.waitingDeps.get(jobId) ??
      shard.waitingChildren.get(jobId) ??
      null
    );
  }
  if (location.type === 'processing') {
    return ctx.processingShards[location.shardIdx].get(jobId) ?? null;
  }
  if (location.type === 'completed') {
    return ctx.storage?.getJob(jobId) ?? ctx.completedJobsData.get(jobId) ?? null;
  }
  if (location.type === 'dlq') {
    if (ctx.storage) {
      const job = ctx.storage.getJob(jobId);
      if (job) return job;
    }
    const dlqShardIdx = shardIndex(location.queueName);
    const dlqJobs = ctx.shards[dlqShardIdx].getDlq(location.queueName);
    return dlqJobs.find((job) => job.id === jobId) ?? null;
  }
  return null;
}

export function getJobProgress(
  jobId: JobId,
  ctx: QueryContext
): { progress: number; message: string | null } | null {
  const location = ctx.jobIndex.get(jobId);
  if (location?.type !== 'processing') return null;
  const job = ctx.processingShards[location.shardIdx].get(jobId);
  if (!job) return null;
  return { progress: job.progress, message: job.progressMessage };
}
