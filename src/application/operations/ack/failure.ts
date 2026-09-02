import { FailureReason, recordJobFailureAttempt } from '../../../domain/types/dlq';
import type { FlowFailureMode, FlowFailureRecord } from '../../../domain/types/flow';
import {
  calculateBackoff,
  canRetry,
  MAX_TIMELINE_ENTRIES,
  normalizeStacktrace,
  type Job,
  type JobId,
} from '../../../domain/types/job';
import { EventType } from '../../../domain/types/queue';
import { processingShardIndex, shardIndex } from '../../../shared/hash';
import { withWriteLock } from '../../../shared/lock';
import { throughputTracker } from '../../throughputTracker';
import type { AckContext, FailJobOptions, TerminalFailureInput } from '../../types/ack';

function moveFailedJobToDlq(input: TerminalFailureInput): void {
  const { job, jobId, error, shard, ctx, flowFailure, reason } = input;
  const entry = shard.addToDlq(job, reason, error ?? null);
  ctx.jobIndex.set(jobId, { type: 'dlq', queueName: job.queue });
  ctx.storage?.commitFailedJob(jobId, entry, flowFailure);
  ctx.totalFailed.value++;
  if (ctx.perQueueMetrics) {
    const queueMetrics = ctx.perQueueMetrics.get(job.queue);
    if (queueMetrics) queueMetrics.totalFailed++;
    else ctx.perQueueMetrics.set(job.queue, { totalCompleted: 0n, totalFailed: 1n });
  }
  throughputTracker.failRate.increment();
  if (job.customId && ctx.customIdMap) ctx.customIdMap.delete(job.customId);
  ctx.emitDashboardEvent?.('dlq:added', {
    queue: job.queue,
    jobId: String(jobId),
    reason,
  });
  if (job.parentId) {
    ctx.emitDashboardEvent?.('flow:failed', {
      parentJobId: String(job.parentId),
      failedChildId: String(jobId),
      queue: job.queue,
      error: error ?? 'Max attempts exceeded',
    });
  }
}

function flowFailureRecord(job: Job, error: string | undefined): FlowFailureRecord | null {
  if (!job.parentId) return null;
  let mode: FlowFailureMode | null = null;
  if (job.failParentOnFailure) mode = 'fail';
  else if (job.continueParentOnFailure) mode = 'continue';
  else if (job.ignoreDependencyOnFailure) mode = 'ignore';
  else if (job.removeDependencyOnFailure) mode = 'remove';
  if (!mode) return null;
  return {
    parentId: job.parentId,
    childId: job.id,
    childQueue: job.queue,
    mode,
    error: error ?? 'unknown error',
    createdAt: Date.now(),
  };
}

export async function failJob(
  jobId: JobId,
  error: string | undefined,
  ctx: AckContext,
  options: FailJobOptions = {}
): Promise<void> {
  const { unrecoverable = false, stack, failureReason, removeOnFail } = options;
  const processingIndex = processingShardIndex(jobId);
  const job = await withWriteLock(ctx.processingLocks[processingIndex], () => {
    const job = ctx.processingShards[processingIndex].get(jobId);
    if (job) {
      options.onClaim?.(job);
      ctx.processingShards[processingIndex].delete(jobId);
    }
    return job;
  });

  if (!job) throw new Error(`Job not found or not in processing state: ${jobId}`);

  job.attempts++;
  if (stack) job.stacktrace = normalizeStacktrace(stack, job.stackTraceLimit);
  const failedAt = Date.now();
  if (job.timeline.length < MAX_TIMELINE_ENTRIES) {
    job.timeline.push({ state: 'failed', timestamp: failedAt, error, attempt: job.attempts });
  }

  const index = shardIndex(job.queue);
  let wasRetried = false;
  const willRetry = !unrecoverable && canRetry(job);
  const flowFailure = willRetry ? null : flowFailureRecord(job, error);
  if (willRetry) {
    recordJobFailureAttempt(
      job,
      failureReason ?? FailureReason.ExplicitFail,
      error ?? null,
      failedAt
    );
  }

  await withWriteLock(ctx.shardLocks[index], () => {
    const location = ctx.jobIndex.get(jobId);
    if (location?.type !== 'processing' || location.queueName !== job.queue) {
      throw new Error(`Job not found or not in processing state: ${jobId}`);
    }
    const shard = ctx.shards[index];
    shard.releaseJobResources(job.queue, job.uniqueKey, job.groupId, job.id);

    if (willRetry) {
      const now = Date.now();
      job.runAt = now + calculateBackoff(job);
      job.startedAt = null;
      if (job.timeline.length < MAX_TIMELINE_ENTRIES) {
        job.timeline.push({ state: 'waiting', timestamp: now, attempt: job.attempts + 1 });
      }
      shard.getQueue(job.queue).push(job);
      shard.incrementQueued(jobId, true, job.createdAt, job.queue, job.runAt);
      ctx.jobIndex.set(jobId, { type: 'queue', shardIdx: index, queueName: job.queue });
      ctx.storage?.updateForRetry(job);
      wasRetried = true;
    } else if (job.removeOnFail || removeOnFail === true) {
      ctx.jobIndex.delete(jobId);
      ctx.storage?.commitFailedJob(jobId, null, flowFailure);
      ctx.totalFailed.value++;
      if (ctx.perQueueMetrics) {
        const queueMetrics = ctx.perQueueMetrics.get(job.queue);
        if (queueMetrics) queueMetrics.totalFailed++;
        else ctx.perQueueMetrics.set(job.queue, { totalCompleted: 0n, totalFailed: 1n });
      }
      throughputTracker.failRate.increment();
      if (job.customId && ctx.customIdMap) ctx.customIdMap.delete(job.customId);
    } else {
      moveFailedJobToDlq({
        job,
        jobId,
        error,
        shard,
        ctx,
        flowFailure,
        reason: failureReason ?? FailureReason.MaxAttemptsExceeded,
      });
    }

    shard.notify(job.queue);
  });

  ctx.broadcast({
    eventType: 'failed' as EventType,
    queue: job.queue,
    jobId,
    timestamp: Date.now(),
    error,
    data: job.data,
    terminal: !wasRetried,
  });

  if (wasRetried) {
    ctx.broadcast({
      eventType: EventType.Retried,
      queue: job.queue,
      jobId,
      timestamp: Date.now(),
      prev: 'failed',
    });
  } else {
    ctx.dependencyResults.releaseConsumer(jobId);
    ctx.onJobFailed?.(jobId);
  }

  if (!wasRetried && job.failParentOnFailure && job.parentId && ctx.onChildTerminalFailure) {
    await ctx.onChildTerminalFailure(job, error);
  }

  if (
    !wasRetried &&
    job.parentId &&
    ctx.onChildDependencyOption &&
    (job.removeDependencyOnFailure || job.ignoreDependencyOnFailure || job.continueParentOnFailure)
  ) {
    await ctx.onChildDependencyOption(job, error);
  }
}
