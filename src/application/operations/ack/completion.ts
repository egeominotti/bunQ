import { setDlqRetryState } from '../../../domain/types/dlq';
import { MAX_TIMELINE_ENTRIES, type JobId } from '../../../domain/types/job';
import { EventType } from '../../../domain/types/queue';
import { processingShardIndex, shardIndex } from '../../../shared/hash';
import { withWriteLock } from '../../../shared/lock';
import { commitRemovedCompletion } from '../../dependencyCompletions';
import { latencyTracker } from '../../latencyTracker';
import { throughputTracker } from '../../throughputTracker';
import type { AckContext, CompletionOptions } from '../../types/ack';

export async function ackJob(
  jobId: JobId,
  result: unknown,
  ctx: AckContext,
  options: CompletionOptions = {}
): Promise<boolean> {
  const startNs = Bun.nanoseconds();
  const processingIndex = processingShardIndex(jobId);

  const job = await withWriteLock(ctx.processingLocks[processingIndex], () => {
    const job = ctx.processingShards[processingIndex].get(jobId);
    if (job) ctx.processingShards[processingIndex].delete(jobId);
    return job;
  });

  if (!job) return false;

  const index = shardIndex(job.queue);
  let stillOwned = false;
  await withWriteLock(ctx.shardLocks[index], () => {
    const location = ctx.jobIndex.get(jobId);
    if (location?.type !== 'processing' || location.queueName !== job.queue) return;
    stillOwned = true;
    const shard = ctx.shards[index];
    shard.releaseJobResources(job.queue, job.uniqueKey, job.groupId, job.id);
    shard.notify(job.queue);
  });
  const location = ctx.jobIndex.get(jobId);
  if (!stillOwned || location?.type !== 'processing' || location.queueName !== job.queue) {
    return false;
  }

  if (job.customId && ctx.customIdMap) ctx.customIdMap.delete(job.customId);
  setDlqRetryState(job, null);

  if (!(job.removeOnComplete || options.removeOnComplete === true)) {
    const now = Date.now();
    job.completedAt = now;
    if (job.timeline.length < MAX_TIMELINE_ENTRIES) {
      job.timeline.push({ state: 'completed', timestamp: now });
    }
    ctx.completedJobs.add(jobId);
    ctx.completedJobsData.set(jobId, job);
    if (result !== undefined) {
      ctx.jobResults.set(jobId, result);
      ctx.jobResultQueues.set(jobId, job.queue);
      ctx.storage?.storeResult(jobId, result);
    }
    ctx.jobIndex.set(jobId, { type: 'completed', queueName: job.queue });
    ctx.storage?.markCompleted(jobId, now, job.timeline);
  } else {
    commitRemovedCompletion(job, ctx);
    ctx.jobIndex.delete(jobId);
  }

  if (result !== undefined) ctx.dependencyResults.retain(jobId, result);
  ctx.dependencyResults.releaseConsumer(jobId);
  ctx.totalCompleted.value++;

  if (ctx.perQueueMetrics) {
    const queueMetrics = ctx.perQueueMetrics.get(job.queue);
    if (queueMetrics) queueMetrics.totalCompleted++;
    else ctx.perQueueMetrics.set(job.queue, { totalCompleted: 1n, totalFailed: 0n });
  }

  throughputTracker.completeRate.increment();
  ctx.broadcast({
    eventType: 'completed' as EventType,
    queue: job.queue,
    jobId,
    timestamp: Date.now(),
    data: result,
  });
  ctx.onJobCompleted(jobId);

  if (job.repeat && ctx.onRepeat) {
    const shouldRepeat = job.repeat.limit === undefined || job.repeat.count < job.repeat.limit;
    if (shouldRepeat) ctx.onRepeat(job);
  }

  latencyTracker.ack.observe((Bun.nanoseconds() - startNs) / 1e6);
  return true;
}
