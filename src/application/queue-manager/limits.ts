import type { Job, JobId } from '../../domain/types/job';
import type { DlqEntry, DlqFilter, DlqStats } from '../../domain/types/dlq';
import { DEFAULT_DLQ_CONFIG } from '../../domain/types/dlq';
import { DEFAULT_STALL_CONFIG } from '../../domain/types/stall';
import { shardIndex, SHARD_COUNT } from '../../shared/hash';
import * as dlqOps from '../dlqManager';
import * as queryOps from '../operations/queryOperations';
import { QueueManagerControl } from './control';

export class QueueManagerLimits extends QueueManagerControl {
  getCountsPerPriority(queue: string): Record<number, number> {
    const counts = this.shards[shardIndex(queue)].getCountsPerPriority(queue);
    return Object.fromEntries(counts);
  }

  getJobs(
    queue: string,
    options: { state?: string | string[]; start?: number; end?: number; asc?: boolean } = {}
  ): Job[] {
    const index = shardIndex(queue);
    return queryOps.getJobs(queue, index, options, {
      ...this.contextFactory.getQueryContext(),
      shardCount: SHARD_COUNT,
    });
  }

  getDlq(queue: string, count?: number): Job[] {
    return dlqOps.getDlqJobs(queue, this.contextFactory.getDlqContext(), count);
  }

  getDlqEntries(queue: string, filter?: DlqFilter): DlqEntry[] {
    return dlqOps.getDlqEntries(queue, this.contextFactory.getDlqContext(), filter);
  }

  getDlqCount(queue: string): number {
    return this.shards[shardIndex(queue)].getDlqCount(queue);
  }

  getDlqStats(queue: string): DlqStats {
    return dlqOps.getDlqStats(queue, this.contextFactory.getDlqContext());
  }

  retryDlq(queue: string, jobId?: JobId, limit?: number): number {
    return dlqOps.retryDlqJobs(queue, this.contextFactory.getDlqContext(), jobId, limit);
  }

  retryDlqByFilter(queue: string, filter: DlqFilter): number {
    return dlqOps.retryDlqByFilter(queue, this.contextFactory.getDlqContext(), filter);
  }

  purgeDlq(queue: string): number {
    return dlqOps.purgeDlqJobs(queue, this.contextFactory.getDlqContext());
  }

  /** Remove one terminal DLQ job without re-enqueuing it. */
  removeDlqJob(queue: string, jobId: JobId): boolean {
    return dlqOps.removeDlqJob(queue, jobId, this.contextFactory.getDlqContext());
  }

  retryCompleted(
    queue: string,
    jobId?: JobId,
    options?: { limit?: number; timestamp?: number }
  ): number {
    return dlqOps.retryCompletedJobs(
      queue,
      this.contextFactory.getRetryCompletedContext(),
      jobId,
      options
    );
  }

  setRateLimit(queue: string, limit: number, durationMs?: number, ttlMs?: number): void {
    this.shards[shardIndex(queue)].setRateLimit(queue, limit, durationMs, ttlMs);
    this.persistQueueState(queue);
  }

  clearRateLimit(queue: string): void {
    this.shards[shardIndex(queue)].clearRateLimit(queue);
    this.persistQueueState(queue);
  }

  setConcurrency(queue: string, limit: number): void {
    this.shards[shardIndex(queue)].setConcurrency(queue, limit);
    this.persistQueueState(queue);
  }

  clearConcurrency(queue: string): void {
    this.shards[shardIndex(queue)].clearConcurrency(queue);
    this.persistQueueState(queue);
  }

  protected override persistQueueState(queue: string): void {
    if (!this.storage) return;
    const shard = this.shards[shardIndex(queue)];
    const state = shard.getState(queue);
    const stallConfig = shard.getStallConfig(queue);
    const dlqConfig = shard.getDlqConfig(queue);
    const hasCustomStallConfig =
      stallConfig.enabled !== DEFAULT_STALL_CONFIG.enabled ||
      stallConfig.stallInterval !== DEFAULT_STALL_CONFIG.stallInterval ||
      stallConfig.maxStalls !== DEFAULT_STALL_CONFIG.maxStalls ||
      stallConfig.gracePeriod !== DEFAULT_STALL_CONFIG.gracePeriod;
    const hasCustomDlqConfig =
      dlqConfig.autoRetry !== DEFAULT_DLQ_CONFIG.autoRetry ||
      dlqConfig.autoRetryInterval !== DEFAULT_DLQ_CONFIG.autoRetryInterval ||
      dlqConfig.maxAutoRetries !== DEFAULT_DLQ_CONFIG.maxAutoRetries ||
      dlqConfig.maxAge !== DEFAULT_DLQ_CONFIG.maxAge ||
      dlqConfig.maxEntries !== DEFAULT_DLQ_CONFIG.maxEntries;
    if (
      !state.paused &&
      state.rateLimit === null &&
      state.concurrencyLimit === null &&
      !hasCustomStallConfig &&
      !hasCustomDlqConfig
    ) {
      this.storage.deleteQueueState(queue);
      return;
    }
    this.storage.saveQueueState(queue, {
      paused: state.paused,
      rateLimit: state.rateLimit,
      concurrencyLimit: state.concurrencyLimit,
      rateLimitDuration: state.rateLimitDuration,
      rateLimitExpiresAt: state.rateLimitExpiresAt,
      stallConfig,
      dlqConfig,
    });
  }

  getQueueLimits(queue: string): {
    rateLimit: number | null;
    concurrencyLimit: number | null;
  } {
    const shard = this.shards[shardIndex(queue)];
    shard.expireRateLimitIfNeeded(queue);
    const state = shard.getState(queue);
    return { rateLimit: state.rateLimit, concurrencyLimit: state.concurrencyLimit };
  }

  getQueueLimitStatus(queue: string, maxJobs?: number) {
    const shard = this.shards[shardIndex(queue)];
    return {
      rateLimit: shard.getRateLimit(queue),
      rateLimitTtl: shard.getRateLimitTtl(queue, maxJobs),
      concurrencyLimit: shard.getConcurrency(queue),
      maxed: shard.isConcurrencyMaxed(queue),
    };
  }
}
