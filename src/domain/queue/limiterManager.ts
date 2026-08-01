/**
 * LimiterManager - Rate limiting and concurrency control
 */

import { type QueueState, createQueueState, RateLimiter, ConcurrencyLimiter } from '../types/queue';

/**
 * Manages rate limiting and concurrency for queues
 */
export class LimiterManager {
  /** Queue state (pause, rate limit, concurrency) */
  private readonly queueState = new Map<string, QueueState>();

  /** Rate limiters per queue */
  private readonly rateLimiters = new Map<string, RateLimiter>();

  /** Concurrency limiters per queue */
  private readonly concurrencyLimiters = new Map<string, ConcurrencyLimiter>();

  /** Get queue state */
  getState(name: string): QueueState {
    let state = this.queueState.get(name);
    if (!state) {
      state = createQueueState(name);
      this.queueState.set(name, state);
    }
    return state;
  }

  /** Check if queue is paused */
  isPaused(name: string): boolean {
    return this.queueState.get(name)?.paused ?? false;
  }

  /** Pause queue */
  pause(name: string): void {
    this.getState(name).paused = true;
  }

  /** Resume queue */
  resume(name: string): void {
    this.getState(name).paused = false;
  }

  // ============ Rate Limiting ============

  /**
   * Set rate limit for queue.
   * @param durationMs window the limit applies to (default 1000ms): `limit`
   *   tokens refill over `durationMs`. Non-finite or non-positive → default.
   * @param ttlMs auto-expiry: the limit clears itself after this many ms
   *   (lazy, checked on acquire/read). Non-finite or non-positive → permanent.
   */
  setRateLimit(queue: string, limit: number, durationMs?: number, ttlMs?: number): void {
    const duration = Number.isFinite(durationMs) && (durationMs as number) > 0 ? durationMs! : null;
    const ttl = Number.isFinite(ttlMs) && (ttlMs as number) > 0 ? ttlMs! : null;
    const windowSeconds = (duration ?? 1000) / 1000;
    this.rateLimiters.set(queue, new RateLimiter(limit, limit / windowSeconds));
    const state = this.getState(queue);
    state.rateLimit = limit;
    state.rateLimitDuration = duration;
    state.rateLimitExpiresAt = ttl === null ? null : Date.now() + ttl;
  }

  /** Clear rate limit */
  clearRateLimit(queue: string): void {
    this.rateLimiters.delete(queue);
    const state = this.queueState.get(queue);
    if (state) {
      state.rateLimit = null;
      state.rateLimitDuration = null;
      state.rateLimitExpiresAt = null;
    }
  }

  /**
   * Drop the rate limit if its TTL has elapsed. Lazy expiry: called from the
   * acquire path and from limit reads, so no timer is needed and an expired
   * limit can never throttle a pull.
   */
  expireRateLimitIfNeeded(queue: string): void {
    const expiresAt = this.queueState.get(queue)?.rateLimitExpiresAt;
    if (expiresAt !== null && expiresAt !== undefined && Date.now() >= expiresAt) {
      this.clearRateLimit(queue);
    }
  }

  /** Try to acquire rate limit token */
  tryAcquireRateLimit(queue: string): boolean {
    this.expireRateLimitIfNeeded(queue);
    const limiter = this.rateLimiters.get(queue);
    return !limiter || limiter.tryAcquire();
  }

  /** Read the active rate-limit configuration after applying lazy expiry. */
  getRateLimit(queue: string): { max: number; duration: number } | null {
    this.expireRateLimitIfNeeded(queue);
    const state = this.queueState.get(queue);
    if (state?.rateLimit === null || state?.rateLimit === undefined) return null;
    return { max: state.rateLimit, duration: state.rateLimitDuration ?? 1000 };
  }

  /** Read the remaining temporary-limit TTL or token-bucket cooldown. */
  getRateLimitTtl(queue: string, maxJobs?: number): number {
    this.expireRateLimitIfNeeded(queue);
    const state = this.queueState.get(queue);
    if (state?.rateLimit === null || state?.rateLimit === undefined) return -2;
    if (state.rateLimitExpiresAt !== null) {
      return Math.max(0, state.rateLimitExpiresAt - Date.now());
    }
    return this.rateLimiters.get(queue)?.getTtl(maxJobs) ?? -2;
  }

  // ============ Concurrency Limiting ============

  /** Set concurrency limit for queue */
  setConcurrency(queue: string, limit: number): void {
    let limiter = this.concurrencyLimiters.get(queue);
    if (limiter) {
      limiter.setLimit(limit);
    } else {
      limiter = new ConcurrencyLimiter(limit);
      this.concurrencyLimiters.set(queue, limiter);
    }
    this.getState(queue).concurrencyLimit = limit;
  }

  /** Clear concurrency limit */
  clearConcurrency(queue: string): void {
    this.concurrencyLimiters.delete(queue);
    const state = this.queueState.get(queue);
    if (state) state.concurrencyLimit = null;
  }

  /** Try to acquire concurrency slot */
  tryAcquireConcurrency(queue: string): boolean {
    const limiter = this.concurrencyLimiters.get(queue);
    return !limiter || limiter.tryAcquire();
  }

  /** Release concurrency slot */
  releaseConcurrency(queue: string): void {
    this.concurrencyLimiters.get(queue)?.release();
  }

  /** Read the configured global concurrency limit. */
  getConcurrency(queue: string): number | null {
    return this.queueState.get(queue)?.concurrencyLimit ?? null;
  }

  /** Whether all configured global concurrency slots are currently occupied. */
  isConcurrencyMaxed(queue: string): boolean {
    const limiter = this.concurrencyLimiters.get(queue);
    return limiter ? limiter.getActive() >= limiter.getLimit() : false;
  }

  // ============ Queue Management ============

  /** Get all queue names with state */
  getQueueNames(): string[] {
    return Array.from(this.queueState.keys());
  }

  /** Delete queue data */
  deleteQueue(queue: string): void {
    this.queueState.delete(queue);
    this.rateLimiters.delete(queue);
    this.concurrencyLimiters.delete(queue);
  }

  /** Get the underlying state map (for backward compatibility) */
  getStateMap(): Map<string, QueueState> {
    return this.queueState;
  }
}
