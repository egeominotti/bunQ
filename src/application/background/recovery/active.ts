import { createDlqEntry, FailureReason, recordJobFailureAttempt } from '../../../domain/types/dlq';
import { calculateBackoff, type JobId } from '../../../domain/types/job';
import { isCorruptDependsOn } from '../../../infrastructure/persistence/sqliteSerializer';
import { shardIndex } from '../../../shared/hash';
import type { BackgroundContext } from '../../types';
import { quarantineCorruptDependsOn, RECOVERY_BATCH_SIZE } from './shared';

export function restoreGroupPolicies(
  ctx: BackgroundContext,
  groupStates: ReturnType<NonNullable<BackgroundContext['storage']>['loadGroupState']>
): void {
  for (const state of groupStates) {
    const shard = ctx.shards[shardIndex(state.queue)];
    if (state.rateLimit !== null && state.rateDuration !== null) {
      shard.setGroupRateLimit(state.queue, state.groupId, state.rateLimit, state.rateDuration);
    }
    if (state.concurrencyLimit !== null) {
      shard.setGroupConcurrency(state.queue, state.groupId, state.concurrencyLimit);
    }
    if (state.paused) shard.pauseGroup(state.queue, state.groupId);
    ctx.registerQueueName(state.queue);
  }
}

export function restoreRecoveryPolicies(
  ctx: BackgroundContext,
  queueStates: ReturnType<NonNullable<BackgroundContext['storage']>['loadQueueState']>
): void {
  for (const queueState of queueStates) {
    if (queueState.stallConfig) {
      ctx.shards[shardIndex(queueState.name)].setStallConfig(
        queueState.name,
        queueState.stallConfig
      );
    }
    if (queueState.dlqConfig) {
      ctx.shards[shardIndex(queueState.name)].setDlqConfig(queueState.name, queueState.dlqConfig);
    }
  }
}

export function recoverActiveJobs(
  ctx: BackgroundContext,
  dlqJobIds: Set<JobId>,
  now: number
): void {
  if (!ctx.storage) return;

  while (true) {
    const activeJobs = ctx.storage.loadActiveJobs(RECOVERY_BATCH_SIZE, 0);
    if (activeJobs.length === 0) break;

    for (const job of activeJobs) {
      const shard = ctx.shards[shardIndex(job.queue)];
      const stallConfig = shard.getStallConfig(job.queue);

      if (job.uniqueKey?.startsWith('cron:')) {
        ctx.storage.deleteJob(job.id);
        ctx.registerQueueName(job.queue);
        continue;
      }
      if (dlqJobIds.has(job.id)) {
        ctx.storage.deleteJob(job.id);
        ctx.registerQueueName(job.queue);
        continue;
      }
      if (isCorruptDependsOn(job)) {
        quarantineCorruptDependsOn(ctx, job);
        continue;
      }

      job.stallCount = (job.stallCount || 0) + 1;
      job.attempts++;
      job.lastHeartbeat = now;
      const maxStalls = stallConfig.maxStalls ?? 3;
      const attemptsExhausted = job.attempts >= job.maxAttempts;
      const stallsExhausted = maxStalls > 0 && job.stallCount >= maxStalls;

      if (attemptsExhausted || stallsExhausted) {
        const reason = attemptsExhausted
          ? FailureReason.MaxAttemptsExceeded
          : FailureReason.Stalled;
        const error = attemptsExhausted
          ? `Job reached max attempts (${job.maxAttempts}) during startup recovery`
          : `Job stalled ${job.stallCount} times (recovered at startup)`;
        const entry = createDlqEntry(job, reason, error, shard.getDlqConfig(job.queue));
        job.startedAt = null;
        ctx.storage.saveDlqEntry(entry);
        ctx.storage.deleteJob(job.id);
      } else {
        recordJobFailureAttempt(job, FailureReason.Stalled, 'Interrupted during recovery', now);
        job.startedAt = null;
        job.runAt = now + calculateBackoff(job);
        ctx.storage.updateForRetry(job);
      }

      ctx.registerQueueName(job.queue);
    }

    if (activeJobs.length < RECOVERY_BATCH_SIZE) break;
  }
}
