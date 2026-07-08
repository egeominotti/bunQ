/**
 * Bunqueue Simple Mode — type definitions.
 * 1:1 port of src/client/bunqueue/types.ts from the official client, minus
 * the embedded-mode options (this SDK is TCP-only).
 */

import type { TlsOption } from '../connection.js';
import type { Job } from '../job.js';
import type { JobOptions } from '../types.js';
import type { Processor } from '../worker-types.js';

/** Connection options accepted by Simple Mode (mirrors ConnectionOptions). */
export interface BunqueueConnection {
  host?: string;
  port?: number;
  token?: string;
  tls?: TlsOption;
}

/** Middleware function: receives job and next(), returns result. */
export type BunqueueMiddleware<T = unknown, R = unknown> = (
  job: Job<T>,
  next: () => Promise<R>
) => Promise<R>;

/** Retry strategy for advanced backoff. */
export type RetryStrategy = 'fixed' | 'exponential' | 'jitter' | 'fibonacci' | 'custom';

/** Advanced retry configuration (in-process; the job stays active). */
export interface RetryConfig {
  /** Max attempts (default: 3). */
  maxAttempts?: number;
  /** Base delay in ms (default: 1000). */
  delay?: number;
  /** Strategy (default: exponential). */
  strategy?: RetryStrategy;
  /** Custom backoff function: attempt, error -> delay in ms. */
  customBackoff?: (attempt: number, error: Error) => number;
  /** Only retry if this returns true. */
  retryIf?: (error: Error, attempt: number) => boolean;
}

/** Circuit breaker configuration. */
export interface CircuitBreakerConfig {
  /** Max consecutive failures before opening (default: 5). */
  threshold?: number;
  /** Time in ms before half-open retry (default: 30000). */
  resetTimeout?: number;
  onOpen?: (failures: number) => void;
  onClose?: () => void;
  onHalfOpen?: () => void;
}

/** Circuit breaker state. */
export type CircuitState = 'closed' | 'open' | 'half-open';

/** Event trigger rule. */
export interface TriggerRule<T = unknown> {
  /** Job name that triggers this rule. */
  on: string;
  /** Event type (default: completed). */
  event?: 'completed' | 'failed';
  /** Job name to create. */
  create: string;
  /** Data builder from the triggering job. */
  data: (result: unknown, job: Job<T>) => T;
  /** Optional job options. */
  opts?: JobOptions;
  /** Optional condition. */
  condition?: (result: unknown, job: Job<T>) => boolean;
}

/** Priority aging configuration. */
export interface PriorityAgingConfig {
  /** Check interval in ms (default: 60000). */
  interval?: number;
  /** Min age in ms before boost (default: 60000). */
  minAge?: number;
  /** Priority boost per interval (default: 1). */
  boost?: number;
  /** Max priority cap (default: 100). */
  maxPriority?: number;
  /** Max jobs to scan per tick (default: 100). */
  maxScan?: number;
}

/** Batch processor function. */
export type BatchProcessor<T = unknown, R = unknown> = (jobs: Array<Job<T>>) => Promise<R[]>;

/** Batch processing configuration. */
export interface BatchConfig<T = unknown, R = unknown> {
  /** Batch size (default: 10). */
  size: number;
  /** Max wait in ms before flushing a partial batch (default: 5000). */
  timeout?: number;
  processor: BatchProcessor<T, R>;
}

/** Job TTL configuration. */
export interface JobTtlConfig {
  /** Default TTL in ms (0 = no TTL). */
  defaultTtl?: number;
  /** Per-job-name TTL overrides. */
  perName?: Record<string, number>;
}

/** Deduplication defaults for Simple Mode. */
export interface BunqueueDeduplicationConfig {
  /** Default deduplication TTL in ms (default: 3600000 = 1 hour). */
  ttl?: number;
  /** Extend TTL when a duplicate arrives (default: false). */
  extend?: boolean;
  /** Replace data when a duplicate arrives in delayed state (default: false). */
  replace?: boolean;
}

/** Debounce defaults for Simple Mode. */
export interface BunqueueDebounceConfig {
  /** Debounce TTL in ms. */
  ttl: number;
}

/** DLQ configuration for Simple Mode (forwarded to the server). */
export interface BunqueueDlqConfig {
  autoRetry?: boolean;
  autoRetryInterval?: number;
  maxAutoRetries?: number;
  maxAge?: number | null;
  maxEntries?: number;
}

/** Client-side rate limiter options (max job starts per duration window). */
export interface RateLimiterOptions {
  max: number;
  duration: number;
  /** Group jobs by this data field (e.g. per-customer limits). */
  groupKey?: string;
}

/** Bunqueue Simple Mode options. */
export interface BunqueueOptions<T = unknown, R = unknown> {
  processor?: Processor<T, R>;
  routes?: Record<string, Processor<T, R>>;
  batch?: BatchConfig<T, R>;
  concurrency?: number;
  connection?: BunqueueConnection;
  defaultJobOptions?: JobOptions;
  autorun?: boolean;
  /** Worker heartbeat interval in ms (official-client unit). */
  heartbeatInterval?: number;
  batchSize?: number;
  /** Long-poll timeout in ms. */
  pollTimeout?: number;
  limiter?: RateLimiterOptions;
  removeOnComplete?: boolean;
  removeOnFail?: boolean;
  retry?: RetryConfig;
  circuitBreaker?: CircuitBreakerConfig;
  ttl?: JobTtlConfig;
  priorityAging?: PriorityAgingConfig;
  /** Job deduplication defaults, merged into every add(). */
  deduplication?: BunqueueDeduplicationConfig;
  /** Job debouncing defaults, merged into every add(). */
  debounce?: BunqueueDebounceConfig;
  /** Rate limiting for the worker (alias of limiter, takes precedence). */
  rateLimit?: RateLimiterOptions;
  /** Dead letter queue auto-management (forwarded to the server). */
  dlq?: BunqueueDlqConfig;
  /** Namespace prefix prepended to the queue name on the server. */
  prefixKey?: string;
}
