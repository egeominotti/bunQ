import type { JobId } from '../../types/job';
import type { QueueState } from '../../types/queue';
import { ShardKeys } from './keys';

/** Queue rate, concurrency, and resource ownership operations. */
export class ShardLimits extends ShardKeys {
  setRateLimit(queue: string, limit: number, durationMs?: number, ttlMs?: number): void {
    this.limiterManager.setRateLimit(queue, limit, durationMs, ttlMs);
  }
  clearRateLimit(queue: string): void {
    this.limiterManager.clearRateLimit(queue);
  }
  expireRateLimitIfNeeded(queue: string): void {
    this.limiterManager.expireRateLimitIfNeeded(queue);
  }
  tryAcquireRateLimit(queue: string): boolean {
    return this.limiterManager.tryAcquireRateLimit(queue);
  }
  getRateLimit(queue: string): { max: number; duration: number } | null {
    return this.limiterManager.getRateLimit(queue);
  }
  getRateLimitTtl(queue: string, maxJobs?: number): number {
    return this.limiterManager.getRateLimitTtl(queue, maxJobs);
  }
  setConcurrency(queue: string, limit: number): void {
    this.limiterManager.setConcurrency(queue, limit);
  }
  clearConcurrency(queue: string): void {
    this.limiterManager.clearConcurrency(queue);
  }
  tryAcquireConcurrency(queue: string): boolean {
    return this.limiterManager.tryAcquireConcurrency(queue);
  }
  releaseConcurrency(queue: string): void {
    this.limiterManager.releaseConcurrency(queue);
  }
  getConcurrency(queue: string): number | null {
    return this.limiterManager.getConcurrency(queue);
  }
  isConcurrencyMaxed(queue: string): boolean {
    return this.limiterManager.isConcurrencyMaxed(queue);
  }
  get queueState(): Map<string, QueueState> {
    return this.limiterManager.getStateMap();
  }
  clearQueueLimiters(queue: string): void {
    this.limiterManager.deleteQueue(queue);
  }
  releaseJobResources(
    queue: string,
    uniqueKey: string | null,
    groupId: string | null,
    ownerId?: JobId
  ): void {
    if (uniqueKey) {
      if (ownerId) this.releaseUniqueKeyIfOwned(queue, uniqueKey, ownerId);
      else this.releaseUniqueKey(queue, uniqueKey);
    }
    if (groupId) this.releaseGroup(queue, groupId);
    this.releaseConcurrency(queue);
  }
}
