/**
 * WorkerRateLimiter - Rate limiting for worker job processing
 * BullMQ v5 compatible sliding window rate limiter
 *
 * Uses a head-pointer approach instead of Array.filter() for O(1) amortized
 * token expiration. Tokens are always pushed in chronological order, so
 * expired tokens are always at the front — we just advance the head pointer.
 */

import type { RateLimiterOptions } from '../types';

export class WorkerRateLimiter {
  private limiterTokens: number[] = [];
  private head = 0;
  private rateLimitExpiration = 0;

  constructor(private readonly limiter: RateLimiterOptions | null) {
    if (
      limiter &&
      (!Number.isInteger(limiter.max) ||
        limiter.max <= 0 ||
        !Number.isFinite(limiter.duration) ||
        limiter.duration <= 0)
    ) {
      throw new RangeError('Worker limiter max and duration must be positive finite values');
    }
  }

  /**
   * Check if rate limiter allows processing another job.
   * Returns true if we can process, false if rate limited.
   * O(1) amortized — advances head pointer past expired tokens.
   */
  canProcessWithinLimit(): boolean {
    return this.getAvailableSlots() > 0;
  }

  /** Atomically reserve starts in the current window. */
  tryAcquire(count = 1): boolean {
    const now = Date.now();
    if (now < this.rateLimitExpiration) return false;
    if (!this.limiter) return true;

    const windowStart = now - this.limiter.duration;
    this.evictExpired(windowStart);
    if (this.activeCount() + count > this.limiter.max) return false;

    for (let index = 0; index < count; index++) this.limiterTokens.push(now);
    return true;
  }

  /** Record a synthetic start, retained for diagnostics and focused unit tests. */
  recordJobForLimiter(): void {
    if (!this.limiter) return;
    this.limiterTokens.push(Date.now());
  }

  /** Number of starts that can be admitted without waiting. */
  getAvailableSlots(): number {
    const now = Date.now();
    if (now < this.rateLimitExpiration) return 0;
    if (!this.limiter) return Number.POSITIVE_INFINITY;

    this.evictExpired(now - this.limiter.duration);
    return Math.max(0, this.limiter.max - this.activeCount());
  }

  /**
   * Get time until rate limiter allows next job (ms).
   * Returns 0 if not rate limited.
   * O(1) — oldest active token is at this.limiterTokens[this.head].
   */
  getTimeUntilNextSlot(): number {
    const now = Date.now();
    const overrideWait = Math.max(0, this.rateLimitExpiration - now);
    if (!this.limiter) return overrideWait;

    this.evictExpired(now - this.limiter.duration);
    if (this.activeCount() < this.limiter.max) return overrideWait;

    const oldestToken = this.limiterTokens[this.head];
    const windowWait = Math.max(0, oldestToken + this.limiter.duration - now);
    return Math.max(overrideWait, windowWait);
  }

  /** Get rate limiter info (for debugging/monitoring). */
  getRateLimiterInfo(): { current: number; max: number; duration: number } | null {
    if (!this.limiter) return null;

    const windowStart = Date.now() - this.limiter.duration;
    this.evictExpired(windowStart);

    return {
      current: this.activeCount(),
      max: this.limiter.max,
      duration: this.limiter.duration,
    };
  }

  /**
   * Apply rate limiting (BullMQ v5 compatible).
   * The worker will not process jobs until the rate limit expires.
   */
  rateLimit(expireTimeMs: number): void {
    if (!Number.isFinite(expireTimeMs) || expireTimeMs <= 0) return;

    this.rateLimitExpiration = Math.max(this.rateLimitExpiration, Date.now() + expireTimeMs);
  }

  /** Check if worker is currently rate limited. */
  isRateLimited(): boolean {
    return Date.now() < this.rateLimitExpiration;
  }

  /** Number of active (non-expired) tokens. */
  private activeCount(): number {
    return this.limiterTokens.length - this.head;
  }

  /** Advance head pointer past expired tokens. Compact when head > half the array. */
  private evictExpired(windowStart: number): void {
    while (this.head < this.limiterTokens.length && this.limiterTokens[this.head] <= windowStart) {
      this.head++;
    }

    // Compact: reclaim memory when more than half the array is dead space
    if (this.head > 0 && this.head > this.limiterTokens.length / 2) {
      this.limiterTokens = this.limiterTokens.slice(this.head);
      this.head = 0;
    }
  }
}
