import { shardIndex } from '../shared/hash';
import type { BackgroundContext } from './types';

function collectOccupiedQueues(ctx: BackgroundContext): Set<string> {
  const occupied = new Set<string>();
  for (const [jobId, location] of ctx.jobIndex) {
    if (location.type !== 'processing') continue;
    occupied.add(location.queueName);
    const processingJob = ctx.processingShards[location.shardIdx]?.get(jobId);
    if (processingJob) occupied.add(processingJob.queue);
  }
  for (const processing of ctx.processingShards) {
    for (const job of processing.values()) occupied.add(job.queue);
  }
  for (const shard of ctx.shards) {
    for (const job of shard.waitingDeps.values()) occupied.add(job.queue);
    for (const job of shard.waitingChildren.values()) occupied.add(job.queue);
  }
  for (const [queue, count] of ctx.pendingQueueAdmissions) {
    if (count > 0) occupied.add(queue);
  }
  return occupied;
}

function collectConfiguredQueues(ctx: BackgroundContext): Set<string> {
  const configured = new Set<string>();
  for (const shard of ctx.shards) {
    for (const queue of shard.getConfiguredQueueNames()) configured.add(queue);
  }
  return configured;
}

function collectBufferedCompletedQueues(ctx: BackgroundContext): Set<string> {
  const queues = new Set<string>();
  for (const job of ctx.storage?.getBufferedJobs() ?? []) {
    if (job.completedAt === null) continue;
    const location = ctx.jobIndex.get(job.id);
    if (location && (location.type !== 'completed' || location.queueName !== job.queue)) continue;
    queues.add(job.queue);
  }
  return queues;
}

function collectCompletedCounts(
  ctx: BackgroundContext,
  registered: readonly string[]
): Map<string, number> {
  if (ctx.storage) return ctx.storage.countCompletedJobsByQueue(registered);
  const registeredSet = new Set(registered);
  const counts = new Map<string, number>();
  for (const [jobId, location] of ctx.jobIndex) {
    if (
      location.type !== 'completed' ||
      !registeredSet.has(location.queueName) ||
      !ctx.completedJobs.has(jobId)
    ) {
      continue;
    }
    counts.set(location.queueName, (counts.get(location.queueName) ?? 0) + 1);
  }
  return counts;
}

/** Reconcile registered names with every live or durable queue projection. */
export function cleanEmptyQueues(ctx: BackgroundContext): void {
  const registered = Array.from(ctx.queueNamesCache);
  const completedCounts = collectCompletedCounts(ctx, registered);
  const bufferedCompletedQueues = collectBufferedCompletedQueues(ctx);
  const occupied = collectOccupiedQueues(ctx);
  const configured = collectConfiguredQueues(ctx);
  const removed: string[] = [];

  for (const queueName of registered) {
    const shard = ctx.shards[shardIndex(queueName)];
    const hasLiveRuntime =
      (shard.queues.get(queueName)?.size ?? 0) > 0 ||
      shard.getDlqCount(queueName) > 0 ||
      occupied.has(queueName);
    if (hasLiveRuntime) continue;

    // Completed history and durable policies own the public queue name, not an
    // empty heap or secondary group indexes.
    shard.queues.delete(queueName);
    shard.clearEmptyGroupRuntime(queueName);
    if (
      (completedCounts.get(queueName) ?? 0) > 0 ||
      bufferedCompletedQueues.has(queueName) ||
      configured.has(queueName)
    ) {
      continue;
    }

    ctx.storage?.deleteQueueState(queueName);
    ctx.storage?.deleteGroupState(queueName);
    ctx.unregisterQueueName(queueName);
    shard.obliterate(queueName);
    removed.push(queueName);
  }

  for (const shard of ctx.shards) shard.cleanOrphanedTemporalEntries();
  for (const queue of removed) ctx.dashboardEmit?.('queue:removed', { queue });
}
