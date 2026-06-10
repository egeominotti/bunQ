/**
 * Client Types
 * Pure type definitions for the bunqueue client API
 */

/** Job state type */
export type JobStateType =
  | 'waiting'
  | 'prioritized'
  | 'delayed'
  | 'active'
  | 'completed'
  | 'failed'
  | 'waiting-children'
  | 'unknown';

/** Job JSON representation (BullMQ v5 compatible) */
export interface JobJson<T = unknown> {
  id: string;
  name: string;
  data: T;
  opts: JobOptions;
  progress: number;
  delay: number;
  timestamp: number;
  attemptsMade: number;
  stacktrace: string[] | null;
  returnvalue?: unknown;
  failedReason?: string;
  finishedOn?: number;
  processedOn?: number;
  queueQualifiedName: string;
  parentKey?: string;
}

/** Job JSON raw representation (BullMQ v5 compatible) */
export interface JobJsonRaw {
  id: string;
  name: string;
  data: string; // JSON stringified
  opts: string; // JSON stringified
  progress: string; // JSON stringified
  delay: string;
  timestamp: string;
  attemptsMade: string;
  stacktrace: string | null; // JSON stringified
  returnvalue?: string; // JSON stringified
  failedReason?: string;
  finishedOn?: string;
  processedOn?: string;
  parentKey?: string;
}

/** Options for changePriority (BullMQ v5 compatible) */
export interface ChangePriorityOpts {
  priority: number;
  lifo?: boolean;
}

/** Options for getDependencies (BullMQ v5 compatible) */
export interface GetDependenciesOpts {
  processed?: { cursor?: number; count?: number };
  unprocessed?: { cursor?: number; count?: number };
}

/** Dependencies result (BullMQ v5 compatible) */
export interface JobDependencies {
  processed: Record<string, unknown>;
  unprocessed: string[];
  nextProcessedCursor?: number;
  nextUnprocessedCursor?: number;
}

/** Dependencies count result (BullMQ v5 compatible) */
export interface JobDependenciesCount {
  processed: number;
  unprocessed: number;
}

/** Job interface exposed to users */
export interface Job<T = unknown> {
  // Core properties (existing)
  id: string;
  name: string;
  data: T;
  queueName: string;
  attemptsMade: number;
  timestamp: number;
  progress: number;
  returnvalue?: unknown;
  failedReason?: string;
  /** Parent job reference (if this job is part of a flow) */
  parent?: { id: string; queueQualifiedName: string };

  // BullMQ v5 properties
  /** Delay in ms before job becomes available for processing */
  delay: number;
  /** Timestamp when job started processing */
  processedOn?: number;
  /** Timestamp when job finished (completed or failed) */
  finishedOn?: number;
  /** Stack traces from failed attempts */
  stacktrace: string[] | null;
  /** Number of times this job has been stalled */
  stalledCounter: number;
  /** Job priority (higher = processed sooner) */
  priority: number;
  /** Parent key in format queueName:jobId */
  parentKey?: string;
  /** Original job options */
  opts: JobOptions;
  /** Lock token for this job (if processing) */
  token?: string;
  /** Worker/client identifier processing this job */
  processedBy?: string;
  /** Deduplication ID (if set via jobId) */
  deduplicationId?: string;
  /** Repeat job key (for repeatable jobs) */
  repeatJobKey?: string;
  /** Number of times job processing has been started (includes retries) */
  attemptsStarted: number;

  // Core methods (existing)
  /** Update job progress (0-100) */
  updateProgress(progress: number, message?: string): Promise<void>;
  /** Log a message to the job */
  log(message: string): Promise<void>;
  /** Get current job state */
  getState(): Promise<JobStateType>;
  /** Remove this job from the queue */
  remove(): Promise<void>;
  /** Retry this job */
  retry(): Promise<void>;
  /**
   * Get the return values of all children jobs (BullMQ v5 compatible).
   * Returns a Record where keys are job keys (queueName:jobId) and values are return values.
   * @template R - Type of the children return values
   */
  getChildrenValues<R = unknown>(): Promise<Record<string, R>>;

  // BullMQ v5 state check methods
  /** Check if job is in waiting state */
  isWaiting(): Promise<boolean>;
  /** Check if job is currently active/processing */
  isActive(): Promise<boolean>;
  /** Check if job is delayed */
  isDelayed(): Promise<boolean>;
  /** Check if job has completed successfully */
  isCompleted(): Promise<boolean>;
  /** Check if job has failed */
  isFailed(): Promise<boolean>;
  /** Check if job is waiting for children to complete */
  isWaitingChildren(): Promise<boolean>;

  // BullMQ v5 mutation methods
  /** Update job data */
  updateData(data: T): Promise<void>;
  /** Promote delayed job to waiting */
  promote(): Promise<void>;
  /** Change job delay */
  changeDelay(delay: number): Promise<void>;
  /** Change job priority */
  changePriority(opts: ChangePriorityOpts): Promise<void>;
  /** Extend job lock duration */
  extendLock(token: string, duration: number): Promise<number>;
  /** Clear job logs */
  clearLogs(keepLogs?: number): Promise<void>;

  // BullMQ v5 dependency methods
  /** Get job dependencies (children) */
  getDependencies(opts?: GetDependenciesOpts): Promise<JobDependencies>;
  /** Get count of job dependencies */
  getDependenciesCount(opts?: GetDependenciesOpts): Promise<JobDependenciesCount>;

  // BullMQ v5 serialization methods
  /** Get job as JSON object */
  toJSON(): JobJson<T>;
  /** Get job as raw JSON (stringified values) */
  asJSON(): JobJsonRaw;

  // BullMQ v5 move methods
  /**
   * Move job to completed state.
   * @param returnValue - The return value of the job
   * @param token - Lock token (optional in embedded mode)
   * @param fetchNext - Whether to fetch the next job (default: true)
   * @returns The next job data or null
   */
  moveToCompleted(returnValue: unknown, token?: string, fetchNext?: boolean): Promise<unknown>;
  /**
   * Move job to failed state.
   * @param error - The error that caused the failure
   * @param token - Lock token (optional in embedded mode)
   * @param fetchNext - Whether to fetch the next job (default: true)
   */
  moveToFailed(error: Error, token?: string, fetchNext?: boolean): Promise<void>;
  /**
   * Move job back to waiting state.
   * @param token - Lock token (optional in embedded mode)
   * @returns true if job was moved
   */
  moveToWait(token?: string): Promise<boolean>;
  /**
   * Move job to delayed state.
   * @param timestamp - When the job should become available
   * @param token - Lock token (optional in embedded mode)
   */
  moveToDelayed(timestamp: number, token?: string): Promise<void>;
  /**
   * Move job to waiting-children state.
   * Job will wait for all children to complete before processing.
   * @param token - Lock token (optional in embedded mode)
   * @param opts - Options for the operation
   * @returns true if job was moved
   */
  moveToWaitingChildren(
    token?: string,
    opts?: { child?: { id: string; queue: string } }
  ): Promise<boolean>;
  /**
   * Wait until the job has finished (completed or failed).
   * @param queueEvents - QueueEvents instance to listen on
   * @param ttl - Maximum time to wait in ms (optional)
   * @returns The job's return value
   * @throws Error if job fails or times out
   */
  waitUntilFinished(queueEvents: unknown, ttl?: number): Promise<unknown>;

  // BullMQ v5 additional methods
  /**
   * Discard this job - marks it to not be processed further.
   * The job will be moved to failed state with a "discarded" reason.
   */
  discard(): void;
  /**
   * Get return values of failed children jobs.
   * @returns Record where keys are job keys (queueName:jobId) and values are failure reasons
   */
  getFailedChildrenValues(): Promise<Record<string, string>>;
  /**
   * Get child job failures that were explicitly ignored (via ignoreDependencyOnFailure).
   * @returns Record where keys are job keys and values are failure reasons
   */
  getIgnoredChildrenFailures(): Promise<Record<string, string>>;
  /**
   * Remove this job's dependency relationship with its parent.
   * @returns true if dependency was removed
   */
  removeChildDependency(): Promise<boolean>;
  /**
   * Remove the deduplication key associated with this job.
   * @returns true if key was removed
   */
  removeDeduplicationKey(): Promise<boolean>;
  /**
   * Remove all unprocessed child jobs of this job.
   */
  removeUnprocessedChildren(): Promise<void>;
}

/** Parent job reference for flow dependencies */
export interface ParentOpts {
  /** Parent job ID */
  id: string;
  /** Parent job queue name */
  queue: string;
}

/** Backoff configuration (BullMQ v5 compatible) */
export interface BackoffOptions {
  /** Backoff strategy type */
  type: 'fixed' | 'exponential';
  /** Base delay in milliseconds */
  delay: number;
}

/** Keep jobs configuration (BullMQ v5 compatible) */
export interface KeepJobs {
  /** Maximum age in milliseconds */
  age?: number;
  /** Maximum count of jobs to keep */
  count?: number;
}

/** Deduplication options (BullMQ v5 compatible) */
export interface DeduplicationOptions {
  /** Unique deduplication ID */
  id: string;
  /** TTL in milliseconds for the deduplication key */
  ttl?: number;
  /** Extend TTL when duplicate job arrives (for debounce mode) */
  extend?: boolean;
  /** Replace job data when duplicate arrives while in delayed state */
  replace?: boolean;
}

/** Debounce options (BullMQ v5 compatible) */
export interface DebounceOptions {
  /** Unique debounce ID */
  id: string;
  /** TTL in milliseconds for the debounce window */
  ttl: number;
}

/** Repeat configuration (BullMQ v5 compatible) */
export interface RepeatOptions {
  /** Repeat every N milliseconds */
  every?: number;
  /** Maximum repetitions (omit for infinite) */
  limit?: number;
  /** Cron pattern (alternative to every) */
  pattern?: string;
  /** Start date for repeat jobs */
  startDate?: Date | string | number;
  /** End date for repeat jobs */
  endDate?: Date | string | number;
  /** Timezone for cron pattern */
  tz?: string;
  /** Execute immediately on start */
  immediately?: boolean;
  /** Current repeat count (internal) */
  count?: number;
  /** Previous execution timestamp (internal) */
  prevMillis?: number;
  /** Offset in milliseconds */
  offset?: number;
  /** Custom job ID for repeat jobs */
  jobId?: string;
}

/** Job options when adding to queue */
export interface JobOptions {
  /** Job priority (higher = processed sooner) */
  priority?: number;
  /** Delay in milliseconds before job becomes available */
  delay?: number;
  /** Maximum number of attempts */
  attempts?: number;
  /** Backoff delay or configuration between retries */
  backoff?: number | BackoffOptions;
  /** Processing timeout in milliseconds */
  timeout?: number;
  /** Custom job ID (for idempotent/deduplication) */
  jobId?: string;
  /**
   * Remove job on completion. Boolean only — age/count retention
   * (number | KeepJobs) is not implemented and was silently ignored, so the
   * type is narrowed to prevent the misuse. See issue #90.
   */
  removeOnComplete?: boolean;
  /** Remove job on failure. Boolean only — see removeOnComplete. */
  removeOnFail?: boolean;
  /** Stall timeout in ms - job is stalled if no heartbeat after this time */
  stallTimeout?: number;
  /** Repeat configuration for recurring jobs */
  repeat?: RepeatOptions;
  /**
   * Force immediate persistence to disk (bypass write buffer).
   * Use for critical jobs where data loss is unacceptable.
   * Default: false (uses buffered writes for better throughput)
   */
  durable?: boolean;
  /**
   * Parent job reference for flow dependencies (BullMQ v5 compatible).
   * When set, this job becomes a child of the specified parent.
   * The parent will wait for all children to complete before processing.
   */
  parent?: ParentOpts;

  // BullMQ v5 additional options
  /** Process jobs in LIFO order (newest first) */
  lifo?: boolean;
  /** Maximum stack trace lines to store on failure */
  stackTraceLimit?: number;
  /** Maximum number of log entries to keep */
  keepLogs?: number;
  /** Maximum job data size in bytes */
  sizeLimit?: number;
  /** Fail parent job if this child job fails */
  failParentOnFailure?: boolean;
  /** Remove dependency relationship if this job fails */
  removeDependencyOnFailure?: boolean;
  /** Continue parent processing even if this child fails */
  continueParentOnFailure?: boolean;
  /** Move job to parent's failed dependencies instead of blocking parent */
  ignoreDependencyOnFailure?: boolean;
  /** Job creation timestamp (default: Date.now()) */
  timestamp?: number;
  /** Deduplication configuration */
  deduplication?: DeduplicationOptions;
  /** Debounce configuration */
  debounce?: DebounceOptions;
}

/**
 * TLS options for client connections — single definition lives in tcp/types
 * to prevent drift between the public and internal ConnectionOptions.
 */
export type { ClientTlsOptions } from './tcp/types';
import type { ClientTlsOptions } from './tcp/types';

/** Connection options for TCP mode */
export interface ConnectionOptions {
  /** Server host (default: localhost, ignored if socketPath is set) */
  host?: string;
  /** Server port (default: 6789, ignored if socketPath is set) */
  port?: number;
  /** Unix socket path (takes priority over host/port) */
  socketPath?: string;
  /** Enable TLS to the server: true (system CAs) or custom options (default: off) */
  tls?: boolean | ClientTlsOptions;
  /** Auth token */
  token?: string;
  /** Connection pool size for parallel operations (default: 1, set >1 to enable pooling) */
  poolSize?: number;
  /** Ping interval in ms for health checks (default: 30000, 0 to disable) */
  pingInterval?: number;
  /** Command timeout in ms (default: 30000) */
  commandTimeout?: number;
  /**
   * Consecutive command timeouts (no intervening success) before the connection
   * is concluded dead and a reconnect is forced (default: 3, 0 to disable).
   * Recovery path for a half-open socket independent of the health-check ping. See #94.
   */
  maxCommandTimeouts?: number;
  /** Enable TCP pipelining (default: true) */
  pipelining?: boolean;
  /** Max commands in flight per connection (default: 100) */
  maxInFlight?: number;
}

/** Auto-batching configuration for queue.add() */
export interface AutoBatchOptions {
  /** Enable auto-batching (default: true for TCP, false for embedded) */
  enabled?: boolean;
  /** Max items before auto-flush (default: 50) */
  maxSize?: number;
  /** Max delay in ms before auto-flush (default: 5) */
  maxDelayMs?: number;
}

/** Queue options */
export interface QueueOptions {
  defaultJobOptions?: JobOptions;
  /** Connection options - if omitted, connects to localhost:6789 */
  connection?: ConnectionOptions;
  /** Use embedded mode (in-process SQLite) instead of TCP */
  embedded?: boolean;
  /** SQLite database path for embedded mode (overrides DATA_PATH env var) */
  dataPath?: string;
  /**
   * Auto-batching for queue.add() calls in TCP mode.
   * Buffers individual add() calls and sends them as a single PUSHB command.
   * Gives 10-100x throughput improvement for sequential add() calls over TCP.
   * Default: enabled for TCP mode, disabled for embedded mode.
   */
  autoBatch?: AutoBatchOptions;
  /**
   * Namespace prefix prepended to the queue name on the server.
   * Lets multiple environments (e.g. `dev:`, `prod:`) share the same broker
   * without their jobs, workers, cron schedulers, stats, or DLQ overlapping.
   *
   * The user-facing `queue.name` stays the logical name; only the server-side
   * key and the cron scheduler IDs are prefixed. A Worker that wants to consume
   * from the namespaced queue must use the same `prefixKey`.
   */
  prefixKey?: string;
}

/** Rate limiter configuration for Worker (BullMQ v5 compatible) */
export interface RateLimiterOptions {
  /** Maximum number of jobs to process in the duration window */
  max: number;
  /** Duration window in milliseconds */
  duration: number;
  /** Optional group key for per-group rate limiting */
  groupKey?: string;
}

/** Worker options */
export interface WorkerOptions {
  concurrency?: number;
  autorun?: boolean;
  /** Heartbeat interval in ms (default: 10000). Set to 0 to disable. */
  heartbeatInterval?: number;
  /** Connection options - if omitted, connects to localhost:6789 */
  connection?: ConnectionOptions;
  /** Use embedded mode (in-process SQLite) instead of TCP */
  embedded?: boolean;
  /** SQLite database path for embedded mode (overrides DATA_PATH env var) */
  dataPath?: string;
  /** Batch size for pulling jobs (default: 10, max: 1000). Higher = fewer round-trips */
  batchSize?: number;
  /** Long poll timeout in ms when queue is empty (default: 0 = no wait, max: 30000) */
  pollTimeout?: number;
  /**
   * Use lock-based job ownership (BullMQ-style).
   * When enabled, each pulled job gets a lock that must be renewed via heartbeat.
   * Disable for high-throughput scenarios where stall detection is sufficient.
   * Default: true
   */
  useLocks?: boolean;
  /**
   * Rate limiter for controlling job processing rate (BullMQ v5 compatible).
   * When set, limits the number of jobs processed within the duration window.
   */
  limiter?: RateLimiterOptions;
  /** Lock duration in ms (default: 30000). Passed to server on pull; also used by stall detection. */
  lockDuration?: number;
  /** Max stalls before moving to failed (default: 1). Passed to stall config when using embedded mode. */
  maxStalledCount?: number;
  /** Skip stalled job check — disables stalled event subscription (default: false) */
  skipStalledCheck?: boolean;
  /** Skip lock renewal via heartbeat (default: false) */
  skipLockRenewal?: boolean;
  /** Delay in ms between polls when queue is drained (default: 50) */
  drainDelay?: number;
  /** Remove jobs on complete — applied as default for all jobs processed by this worker */
  removeOnComplete?: boolean | number | { age?: number; count?: number };
  /** Remove jobs on fail — applied as default for all jobs processed by this worker */
  removeOnFail?: boolean | number | { age?: number; count?: number };
  /**
   * Namespace prefix prepended to the queue name on the server.
   * Must match the `prefixKey` used by the producing `Queue` for the worker
   * to consume the same namespaced queue. See `QueueOptions.prefixKey`.
   */
  prefixKey?: string;
}

/** Stall configuration for a queue */
export interface StallConfig {
  /** Enable stall detection (default: true) */
  enabled?: boolean;
  /** Stall timeout in ms (default: 30000) */
  stallInterval?: number;
  /** Max stalls before moving to DLQ (default: 3) */
  maxStalls?: number;
  /** Grace period after job start (default: 5000) */
  gracePeriod?: number;
}

/** DLQ configuration for a queue */
export interface DlqConfig {
  /** Enable auto-retry from DLQ */
  autoRetry?: boolean;
  /** Auto-retry interval in ms (default: 3600000 = 1 hour) */
  autoRetryInterval?: number;
  /** Max auto-retries (default: 3) */
  maxAutoRetries?: number;
  /** Max age before auto-purge in ms (default: 604800000 = 7 days) */
  maxAge?: number | null;
  /** Max entries per queue (default: 10000) */
  maxEntries?: number;
}

/** Failure reason for DLQ entries */
export type FailureReason =
  | 'explicit_fail'
  | 'max_attempts_exceeded'
  | 'timeout'
  | 'stalled'
  | 'ttl_expired'
  | 'worker_lost'
  | 'unknown';

/** DLQ entry with metadata */
export interface DlqEntry<T = unknown> {
  job: Job<T>;
  enteredAt: number;
  reason: FailureReason;
  error: string | null;
  attempts: Array<{
    attempt: number;
    startedAt: number;
    failedAt: number;
    reason: FailureReason;
    error: string | null;
    duration: number;
  }>;
  retryCount: number;
  lastRetryAt: number | null;
  nextRetryAt: number | null;
  expiresAt: number | null;
}

/** DLQ statistics */
export interface DlqStats {
  total: number;
  byReason: Record<FailureReason, number>;
  pendingRetry: number;
  expired: number;
  oldestEntry: number | null;
  newestEntry: number | null;
}

/** DLQ filter options */
export interface DlqFilter {
  reason?: FailureReason;
  olderThan?: number;
  newerThan?: number;
  retriable?: boolean;
  expired?: boolean;
  limit?: number;
  offset?: number;
}

/**
 * Flow-injected fields automatically added to job data by FlowProducer.
 * These fields are present on job.data when a job is part of a flow.
 */
export interface FlowJobData {
  /** Parent job ID (injected by FlowProducer.addChain / addTree) */
  __flowParentId?: string;
  /** Array of parent job IDs (injected by FlowProducer.addBulkThen for merge flows) */
  __flowParentIds?: string[];
  /** Parent job ID (injected by FlowProducer.add for BullMQ v5 compatible flows) */
  __parentId?: string;
  /** Parent job queue name (injected by FlowProducer.add for BullMQ v5 compatible flows) */
  __parentQueue?: string;
  /** Children job IDs (injected by FlowProducer.add for BullMQ v5 compatible flows) */
  __childrenIds?: string[];
}

/** Job processor function */
export type Processor<T = unknown, R = unknown> = (job: Job<T & FlowJobData>) => Promise<R> | R;

/** Queue events */
export type QueueEventType =
  | 'waiting'
  | 'active'
  | 'completed'
  | 'failed'
  | 'progress'
  | 'removed'
  | 'drained';

// Re-export conversion functions for backwards compatibility
export { createPublicJob, toPublicJob, toDlqEntry } from './jobConversion';
export type { CreatePublicJobOptions, ToPublicJobOptions } from './jobConversion';
