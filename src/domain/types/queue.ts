/**
 * Queue domain types
 * Queue state and configuration
 */

/** Queue state configuration */
export interface QueueState {
  readonly name: string;
  paused: boolean;
  rateLimit: number | null;
  /** Rate-limit window in ms (null = default 1000ms bucket). */
  rateLimitDuration: number | null;
  /** Epoch ms after which the rate limit self-expires (null = permanent). */
  rateLimitExpiresAt: number | null;
  concurrencyLimit: number | null;
  activeCount: number;
}

/** Create default queue state */
export function createQueueState(name: string): QueueState {
  return {
    name,
    paused: false,
    rateLimit: null,
    rateLimitDuration: null,
    rateLimitExpiresAt: null,
    concurrencyLimit: null,
    activeCount: 0,
  };
}

/** Rate limiter using token bucket algorithm */
export class RateLimiter {
  private tokens: number;
  private lastRefill: number;

  constructor(
    private readonly capacity: number,
    private readonly refillRate: number = capacity
  ) {
    this.tokens = capacity;
    this.lastRefill = Date.now();
  }

  /** Try to acquire a token */
  tryAcquire(): boolean {
    this.refill();
    if (this.tokens >= 1) {
      this.tokens -= 1;
      return true;
    }
    return false;
  }

  /** Refill tokens based on elapsed time */
  private refill(): void {
    const now = Date.now();
    const elapsed = (now - this.lastRefill) / 1000; // seconds
    const tokensToAdd = elapsed * this.refillRate;
    this.tokens = Math.min(this.capacity, this.tokens + tokensToAdd);
    this.lastRefill = now;
  }

  /** Get current token count */
  getTokens(): number {
    this.refill();
    return this.tokens;
  }

  /**
   * Milliseconds until the bucket can admit another job. When `maxJobs` is
   * provided, report 0 until at least that many tokens have been consumed.
   */
  getTtl(maxJobs?: number): number {
    const tokens = this.getTokens();
    const consumed = this.capacity - tokens;
    if (maxJobs !== undefined && consumed < maxJobs) return 0;
    if (tokens >= 1 || this.refillRate <= 0) return 0;
    return Math.ceil(((1 - tokens) / this.refillRate) * 1000);
  }
}

/** Concurrency limiter */
export class ConcurrencyLimiter {
  private active: number = 0;

  constructor(private limit: number) {}

  /** Try to acquire a slot */
  tryAcquire(): boolean {
    if (this.active < this.limit) {
      this.active += 1;
      return true;
    }
    return false;
  }

  /** Release a slot */
  release(): void {
    if (this.active > 0) {
      this.active -= 1;
    }
  }

  /** Get active count */
  getActive(): number {
    return this.active;
  }

  /** Get limit */
  getLimit(): number {
    return this.limit;
  }

  /** Update limit */
  setLimit(limit: number): void {
    this.limit = limit;
  }
}

/** Job location for indexing */
export type JobLocation =
  | { type: 'queue'; shardIdx: number; queueName: string }
  | { type: 'processing'; shardIdx: number; queueName: string }
  | { type: 'completed'; queueName: string }
  | { type: 'dlq'; queueName: string };

/** Event types for subscribers */
export const enum EventType {
  Pushed = 'pushed',
  Pulled = 'pulled',
  Completed = 'completed',
  Failed = 'failed',
  Progress = 'progress',
  Stalled = 'stalled',
  // BullMQ v5 additional events
  Removed = 'removed',
  Delayed = 'delayed',
  Duplicated = 'duplicated',
  Retried = 'retried',
  WaitingChildren = 'waiting-children',
  Drained = 'drained',
  Paused = 'paused',
  Resumed = 'resumed',
}

/** Job event for subscribers */
export interface JobEvent {
  readonly eventType: EventType;
  readonly queue: string;
  readonly jobId: string;
  readonly timestamp: number;
  readonly data?: unknown;
  readonly error?: string;
  readonly progress?: number;
  /** Previous state (for removed, retried events) */
  readonly prev?: string;
  /** Delay in ms (for delayed event) */
  readonly delay?: number;
  /** Internal marker used to exclude retry-attempt failures from terminal metrics. */
  readonly terminal?: boolean;
}

/** Webhook configuration */
export interface Webhook {
  readonly id: string;
  readonly url: string;
  readonly events: EventType[];
  readonly queue: string | null;
  readonly secret: string | null;
}
