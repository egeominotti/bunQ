import { shardIndex } from '../../../shared/hash';
import type { BackgroundContext } from '../../types';
import { RECOVERY_BATCH_SIZE, type QueueStateRecord } from './shared';

export function restoreDlq(ctx: BackgroundContext): void {
  if (!ctx.storage) return;
  const dlqEntries = ctx.storage.loadDlq();
  for (const [queue, entries] of dlqEntries) {
    const shard = ctx.shards[shardIndex(queue)];
    for (const entry of entries) {
      shard.restoreDlqEntry(queue, entry);
      ctx.jobIndex.set(entry.job.id, { type: 'dlq', queueName: queue });
    }
    ctx.registerQueueName(queue);
  }
}

export function restoreQueueState(ctx: BackgroundContext, queueStates: QueueStateRecord[]): void {
  for (const queueState of queueStates) {
    const shard = ctx.shards[shardIndex(queueState.name)];
    if (queueState.paused) shard.pause(queueState.name);
    if (queueState.rateLimit !== null) {
      const remainingTtl =
        queueState.rateLimitExpiresAt === null
          ? undefined
          : queueState.rateLimitExpiresAt - Date.now();
      if (remainingTtl === undefined || remainingTtl > 0) {
        shard.setRateLimit(
          queueState.name,
          queueState.rateLimit,
          queueState.rateLimitDuration ?? undefined,
          remainingTtl
        );
      }
    }
    if (queueState.concurrencyLimit !== null) {
      shard.setConcurrency(queueState.name, queueState.concurrencyLimit);
    }
    if (shard.getConfiguredQueueNames().includes(queueState.name)) {
      ctx.registerQueueName(queueState.name);
    } else {
      ctx.storage?.deleteQueueState(queueState.name);
    }
  }
}

export function recoverCompletedJobs(ctx: BackgroundContext): void {
  if (!ctx.storage) return;
  const completedCap = ctx.config.maxCompletedJobs;
  let completedLoaded = 0;
  let completedOffset = 0;

  while (completedLoaded < completedCap) {
    const remaining = completedCap - completedLoaded;
    const batchSize = Math.min(RECOVERY_BATCH_SIZE, remaining);
    const completedBatch = ctx.storage.loadCompletedJobs(batchSize, completedOffset);
    if (completedBatch.length === 0) break;

    for (const job of completedBatch) {
      ctx.jobIndex.set(job.id, { type: 'completed', queueName: job.queue });
      ctx.completedJobs.add(job.id);
      ctx.completedJobsData.set(job.id, job);
      ctx.registerQueueName(job.queue);
    }

    completedLoaded += completedBatch.length;
    completedOffset += completedBatch.length;
    if (completedBatch.length < batchSize) break;
  }
}
