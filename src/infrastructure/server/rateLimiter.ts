/**
 * Protocol Rate Limiter
 * Prevents abuse by limiting requests per client
 * Uses sliding window with O(1) amortized check instead of O(n) filter
 */

export interface RateLimiterConfig {
  windowMs: number;
  maxRequests: number;
  cleanupIntervalMs?: number;
}

const DEFAULT_CONFIG: RateLimiterConfig = {
  windowMs: parseInt(Bun.env.RATE_LIMIT_WINDOW_MS ?? '60000', 10),
  maxRequests: parseInt(Bun.env.RATE_LIMIT_MAX_REQUESTS ?? '10000', 10),
  cleanupIntervalMs: parseInt(Bun.env.RATE_LIMIT_CLEANUP_MS ?? '60000', 10),
};

/**
 * Sliding window deque for O(1) amortized rate limiting
 * Timestamps are stored in sorted order (oldest first)
 * Expired timestamps are removed lazily from the head
 */
class SlidingWindowDeque {
  private timestamps: number[] = [];
  private head = 0; // Index of first valid element

  /** Add a timestamp and return current count in window */
  add(now: number, windowMs: number): number {
    // Remove expired timestamps from head - O(k) where k = expired count
    while (this.head < this.timestamps.length && now - this.timestamps[this.head] >= windowMs) {
      this.head++;
    }

    // Compact array if head has moved too far (prevents memory leak)
    if (this.head > 1000) {
      this.timestamps = this.timestamps.slice(this.head);
      this.head = 0;
    }

    // Add new timestamp
    this.timestamps.push(now);

    // Return count of valid timestamps
    return this.timestamps.length - this.head;
  }

  /** Get current count in window */
  getCount(now: number, windowMs: number): number {
    // Remove expired timestamps from head
    while (this.head < this.timestamps.length && now - this.timestamps[this.head] >= windowMs) {
      this.head++;
    }
    return this.timestamps.length - this.head;
  }

  /** Check if empty */
  isEmpty(): boolean {
    return this.head >= this.timestamps.length;
  }

  /** Clear all timestamps */
  clear(): void {
    this.timestamps = [];
    this.head = 0;
  }
}

/** Rate limiter for protocol-level request limiting */
export class ProtocolRateLimiter {
  private readonly requests = new Map<string, SlidingWindowDeque>();
  private readonly config: RateLimiterConfig;
  private cleanupInterval: ReturnType<typeof setInterval> | null = null;

  constructor(config: Partial<RateLimiterConfig> = {}) {
    this.config = { ...DEFAULT_CONFIG, ...config };
    this.startCleanup();
  }

  /** Check if a request from clientId is allowed - O(1) amortized */
  isAllowed(clientId: string): boolean {
    const now = Date.now();
    let deque = this.requests.get(clientId);

    if (!deque) {
      deque = new SlidingWindowDeque();
      this.requests.set(clientId, deque);
    }

    // Get current count before adding
    const currentCount = deque.getCount(now, this.config.windowMs);

    if (currentCount >= this.config.maxRequests) {
      return false;
    }

    // Add new timestamp
    deque.add(now, this.config.windowMs);
    return true;
  }

  /** Get remaining requests for a client - O(1) amortized */
  getRemaining(clientId: string): number {
    const now = Date.now();
    const deque = this.requests.get(clientId);

    if (!deque) {
      return this.config.maxRequests;
    }

    const currentCount = deque.getCount(now, this.config.windowMs);
    return Math.max(0, this.config.maxRequests - currentCount);
  }

  /** Remove a client from tracking */
  removeClient(clientId: string): void {
    this.requests.delete(clientId);
  }

  /** Start cleanup interval */
  private startCleanup(): void {
    if (this.config.cleanupIntervalMs) {
      this.cleanupInterval = setInterval(() => {
        this.cleanup();
      }, this.config.cleanupIntervalMs);
      this.cleanupInterval.unref();
    }
  }

  /** Clean up old entries - O(n) but runs infrequently */
  private cleanup(): void {
    const now = Date.now();
    const toDelete: string[] = [];

    for (const [clientId, deque] of this.requests) {
      // Force count update to clean expired timestamps
      deque.getCount(now, this.config.windowMs);

      if (deque.isEmpty()) {
        toDelete.push(clientId);
      }
    }

    for (const clientId of toDelete) {
      this.requests.delete(clientId);
    }
  }

  /** Stop the rate limiter */
  stop(): void {
    if (this.cleanupInterval) {
      clearInterval(this.cleanupInterval);
      this.cleanupInterval = null;
    }
  }
}

/** Global rate limiter instance */
let globalRateLimiter: ProtocolRateLimiter | null = null;

/** Get or create global rate limiter */
export function getRateLimiter(config?: Partial<RateLimiterConfig>): ProtocolRateLimiter {
  globalRateLimiter ??= new ProtocolRateLimiter(config);
  return globalRateLimiter;
}

/** Stop and cleanup global rate limiter */
export function stopRateLimiter(): void {
  if (globalRateLimiter) {
    globalRateLimiter.stop();
    globalRateLimiter = null;
  }
}
