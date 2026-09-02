import type { Job, JobId } from '../../../domain/types/job';
import type { Shard } from '../../../domain/queue/shard';
import type { AckContext, CompletionOptions } from '../../types/ack';
import {
  extractJobs,
  extractJobsWithResults,
  finalizeBatchAck,
  groupByProcShard,
  groupByQueueShard,
  groupItemsByProcShard,
  releaseResources,
} from '../ackHelpers';
import { ackJob } from './completion';

function notifyReleasedQueues(shard: Shard, jobs: Job[]): void {
  const releasedByQueue = new Map<string, number>();
  for (const job of jobs) {
    releasedByQueue.set(job.queue, (releasedByQueue.get(job.queue) ?? 0) + 1);
  }
  for (const [queue, count] of releasedByQueue) shard.notifyBatch(queue, count);
}

function alignApplied(jobIds: JobId[], appliedIds: JobId[]): boolean[] {
  const counts = new Map<JobId, number>();
  for (const id of appliedIds) counts.set(id, (counts.get(id) ?? 0) + 1);
  return jobIds.map((id) => {
    const remaining = counts.get(id) ?? 0;
    if (remaining === 0) return false;
    counts.set(id, remaining - 1);
    return true;
  });
}

function retainOwnedProcessing<T extends { id: JobId; job: Job }>(jobs: T[], ctx: AckContext): T[] {
  return jobs.filter(({ id, job }) => {
    const location = ctx.jobIndex.get(id);
    return location?.type === 'processing' && location.queueName === job.queue;
  });
}

export async function ackJobBatch(
  jobIds: JobId[],
  ctx: AckContext,
  leaseTokens?: Array<string | undefined>
): Promise<boolean[]> {
  if (jobIds.length === 0) return [];
  if (jobIds.length <= 4) {
    return Promise.all(
      jobIds.map((id, index) => ackJob(id, undefined, ctx, { leaseToken: leaseTokens?.[index] }))
    );
  }

  const batchContext = {
    processingShards: ctx.processingShards,
    processingLocks: ctx.processingLocks,
    shards: ctx.shards,
    shardLocks: ctx.shardLocks,
  };
  const byProcessingShard = groupByProcShard(jobIds);
  const extractedJobs = await extractJobs(byProcessingShard, batchContext);
  const byQueueShard = groupByQueueShard(extractedJobs);
  await releaseResources(byQueueShard, batchContext);
  for (const [index, jobs] of byQueueShard) notifyReleasedQueues(ctx.shards[index], jobs);
  const ownedJobs = retainOwnedProcessing(extractedJobs, ctx);
  finalizeBatchAck(ownedJobs, ctx, false);
  return alignApplied(
    jobIds,
    ownedJobs.map((entry) => entry.id)
  );
}

export async function ackJobBatchWithResults(
  items: Array<{ id: JobId; result: unknown; token?: string } & CompletionOptions>,
  ctx: AckContext
): Promise<boolean[]> {
  if (items.length === 0) return [];
  if (items.length <= 4) {
    return Promise.all(
      items.map((item) => ackJob(item.id, item.result, ctx, { ...item, leaseToken: item.token }))
    );
  }

  const batchContext = {
    processingShards: ctx.processingShards,
    processingLocks: ctx.processingLocks,
    shards: ctx.shards,
    shardLocks: ctx.shardLocks,
  };
  const byProcessingShard = groupItemsByProcShard(items);
  const extractedJobs = await extractJobsWithResults(byProcessingShard, batchContext);
  const byQueueShard = groupByQueueShard(extractedJobs);
  await releaseResources(byQueueShard, batchContext);
  for (const [index, jobs] of byQueueShard) notifyReleasedQueues(ctx.shards[index], jobs);
  const ownedJobs = retainOwnedProcessing(extractedJobs, ctx);
  finalizeBatchAck(ownedJobs, ctx, true);
  return alignApplied(
    items.map((item) => item.id),
    ownedJobs.map((entry) => entry.id)
  );
}
