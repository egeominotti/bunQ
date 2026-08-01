import type { Job, JobId } from '../../../domain/types/job';
import type { Shard } from '../../../domain/queue/shard';
import type { AckContext } from '../../types/ack';
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

export async function ackJobBatch(jobIds: JobId[], ctx: AckContext): Promise<void> {
  if (jobIds.length === 0) return;
  if (jobIds.length <= 4) {
    await Promise.all(jobIds.map((id) => ackJob(id, undefined, ctx)));
    return;
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
  finalizeBatchAck(extractedJobs, ctx, false);
}

export async function ackJobBatchWithResults(
  items: Array<{ id: JobId; result: unknown }>,
  ctx: AckContext
): Promise<void> {
  if (items.length === 0) return;
  if (items.length <= 4) {
    await Promise.all(items.map((item) => ackJob(item.id, item.result, ctx)));
    return;
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
  finalizeBatchAck(extractedJobs, ctx, true);
}
