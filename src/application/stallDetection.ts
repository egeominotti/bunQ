/**
 * Stall Detection - Job stall detection and recovery
 * Uses two-phase detection (like BullMQ) to prevent false positives
 */

import type { Job } from '../domain/types/job';
import { calculateBackoff, MAX_TIMELINE_ENTRIES } from '../domain/types/job';
import { FailureReason, recordJobFailureAttempt } from '../domain/types/dlq';
import { StallAction, getStallAction, incrementStallCount } from '../domain/types/stall';
import { EventType } from '../domain/types/queue';
import type { WebhookEvent } from '../domain/types/webhook';
import { queueLog } from '../shared/logger';
import { shardIndex, processingShardIndex, SHARD_COUNT } from '../shared/hash';
import { withWriteLock } from '../shared/lock';
import type { BackgroundContext } from './types';

/**
 * Check for stalled jobs and handle them
 * Uses two-phase detection to prevent false positives
 */
export function checkStalledJobs(ctx: BackgroundContext): void {
  const now = Date.now();
  const confirmedStalled: Array<{ job: Job; action: StallAction }> = [];

  // Phase 1: Check jobs that were candidates from previous cycle
  for (const jobId of ctx.stalledCandidates) {
    const procIdx = processingShardIndex(jobId);
    const job = ctx.processingShards[procIdx].get(jobId);

    if (!job) {
      ctx.stalledCandidates.delete(jobId);
      continue;
    }

    const stallConfig = ctx.shards[shardIndex(job.queue)].getStallConfig(job.queue);
    if (!stallConfig.enabled) {
      ctx.stalledCandidates.delete(jobId);
      continue;
    }

    const action = getStallAction(job, stallConfig, now);
    if (action !== StallAction.Keep) {
      confirmedStalled.push({ job, action });
    }

    ctx.stalledCandidates.delete(jobId);
  }

  // Phase 2: Mark current processing jobs as candidates for NEXT check
  for (let i = 0; i < SHARD_COUNT; i++) {
    const procShard = ctx.processingShards[i];

    for (const [jobId, job] of procShard) {
      const stallConfig = ctx.shards[shardIndex(job.queue)].getStallConfig(job.queue);
      if (!stallConfig.enabled) continue;

      const action = getStallAction(job, stallConfig, now);
      if (action !== StallAction.Keep) {
        ctx.stalledCandidates.add(jobId);
      }
    }
  }

  // Process confirmed stalled jobs
  for (const { job, action } of confirmedStalled) {
    handleStalledJob(job, action, ctx).catch((err: unknown) => {
      queueLog.error('Failed to handle stalled job', {
        jobId: String(job.id),
        error: String(err),
      });
    });
  }
}

/**
 * Handle a stalled job based on the action
 * Uses proper locking to prevent race conditions
 */
async function handleStalledJob(
  job: Job,
  action: StallAction,
  ctx: BackgroundContext
): Promise<void> {
  const idx = shardIndex(job.queue);
  const procIdx = processingShardIndex(job.id);

  // Lock order: shardLocks BEFORE processingLocks (per lock hierarchy in CLAUDE.md)
  // Broadcast events AFTER verifying job is still stalled to avoid false positives
  let handled = false;
  let handledAction = action;

  await withWriteLock(ctx.shardLocks[idx], async () => {
    await withWriteLock(ctx.processingLocks[procIdx], () => {
      // Verify job is still in processing (might have been handled already)
      if (!ctx.processingShards[procIdx].has(job.id)) {
        return; // Job already completed, don't broadcast stalled event
      }

      const shard = ctx.shards[idx];
      const attemptsExhausted = job.attempts + 1 >= job.maxAttempts;

      if (action === StallAction.MoveToDlq || attemptsExhausted) {
        handledAction = StallAction.MoveToDlq;
        moveStalliedJobToDlq(job, ctx, shard, procIdx, attemptsExhausted);
      } else {
        retryStalliedJob(job, ctx, shard, procIdx, idx);
      }

      handled = true;
    });
  });

  // Broadcast events AFTER confirming job is actually stalled and handled
  if (handled) {
    ctx.dashboardEmit?.('job:stalled', {
      jobId: String(job.id),
      queue: job.queue,
      stallCount: job.stallCount,
      action: handledAction,
    });
    ctx.eventsManager.broadcast({
      eventType: EventType.Stalled,
      queue: job.queue,
      jobId: job.id,
      timestamp: Date.now(),
      data: { stallCount: job.stallCount, action: handledAction },
    });
    void ctx.webhookManager.trigger('stalled' as WebhookEvent, String(job.id), job.queue, {
      data: { stallCount: job.stallCount, action: handledAction },
    });
  }
}

/** Move stalled job to DLQ */
function moveStalliedJobToDlq(
  job: Job,
  ctx: BackgroundContext,
  shard: BackgroundContext['shards'][number],
  procIdx: number,
  attemptsExhausted: boolean
): void {
  ctx.processingShards[procIdx].delete(job.id);
  shard.releaseJobResources(job.queue, job.uniqueKey, job.groupId, job.id);

  // Discard cron jobs with preventOverlap instead of sending to DLQ (#73).
  if (job.uniqueKey?.startsWith('cron:')) {
    ctx.jobIndex.delete(job.id);
    ctx.storage?.deleteJob(job.id);
    return;
  }

  incrementStallCount(job);
  job.attempts++;
  job.startedAt = null;
  job.lastHeartbeat = Date.now();
  const entry = shard.addToDlq(
    job,
    attemptsExhausted ? FailureReason.MaxAttemptsExceeded : FailureReason.Stalled,
    attemptsExhausted
      ? `Job stalled at max attempts (${job.maxAttempts})`
      : `Job stalled ${job.stallCount} times`
  );
  ctx.jobIndex.set(job.id, { type: 'dlq', queueName: job.queue });
  ctx.storage?.saveDlqEntry(entry);
  ctx.storage?.deleteJob(job.id);
}

/** Retry stalled job */
function retryStalliedJob(
  job: Job,
  ctx: BackgroundContext,
  shard: BackgroundContext['shards'][number],
  procIdx: number,
  idx: number
): void {
  // Discard cron jobs with preventOverlap instead of retrying (#73).
  // The cron scheduler will re-create them at the next scheduled tick.
  if (job.uniqueKey?.startsWith('cron:')) {
    ctx.processingShards[procIdx].delete(job.id);
    shard.releaseJobResources(job.queue, job.uniqueKey, job.groupId, job.id);
    ctx.jobIndex.delete(job.id);
    ctx.storage?.deleteJob(job.id);
    return;
  }

  incrementStallCount(job);
  job.attempts++;
  const stallNow = Date.now();
  recordJobFailureAttempt(job, FailureReason.Stalled, 'stalled', stallNow);
  job.startedAt = null;
  job.runAt = stallNow + calculateBackoff(job);
  job.lastHeartbeat = stallNow;
  if (job.timeline.length < MAX_TIMELINE_ENTRIES) {
    job.timeline.push({
      state: 'failed',
      timestamp: stallNow,
      error: 'stalled',
      attempt: job.attempts,
    });
    job.timeline.push({ state: 'waiting', timestamp: stallNow, attempt: job.attempts + 1 });
  }

  ctx.processingShards[procIdx].delete(job.id);
  shard.releaseJobResources(job.queue, job.uniqueKey, job.groupId, job.id);

  shard.getQueue(job.queue).push(job);
  const isDelayed = job.runAt > Date.now();
  shard.incrementQueued(job.id, isDelayed, job.createdAt, job.queue, job.runAt);
  ctx.jobIndex.set(job.id, { type: 'queue', shardIdx: idx, queueName: job.queue });
  ctx.storage?.updateForRetry(job);
}
