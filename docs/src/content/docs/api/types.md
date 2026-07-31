---
title: "TypeScript Types: Job, Queue, Worker & DLQ"
description: "Complete TypeScript type definitions for bunqueue. Includes Job, Queue, Worker, DLQ, and connection interfaces with full generic support."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/api/types.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">api reference · types</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Every TypeScript type, <em>spelled out.</em></h1>
  <p class="bq-hero-sub">bunqueue is written in TypeScript and provides comprehensive type definitions. All public types are exported from <code>bunqueue/client</code>.</p>
</div>

## Job Types

### JobStateType

```typescript
type JobStateType =
  | 'waiting'             // In queue, priority = 0
  | 'prioritized'         // In queue, priority > 0 (BullMQ v5)
  | 'delayed'             // Waiting for delay to expire
  | 'active'              // Currently being processed
  | 'completed'           // Successfully finished
  | 'failed'              // Failed after all retries (DLQ)
  | 'waiting-children'    // Waiting for child jobs to complete (flows)
  | 'unknown';            // Job not found or invalid state
```

:::note[BullMQ v5 State Machine]
bunqueue implements the full BullMQ v5 job state machine:

<div class="bq-diag">
  <div class="bq-diag-head"><b>Job state machine</b><span>BullMQ v5</span></div>
  <div class="bq-diag-flow">
    <div class="bq-diag-cell">push <i>priority = 0</i></div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell">waiting</div>
  </div>
  <div class="bq-diag-flow">
    <div class="bq-diag-cell">push <i>priority &gt; 0</i></div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell">prioritized</div>
  </div>
  <div class="bq-diag-flow">
    <div class="bq-diag-cell">push <i>delay &gt; 0</i></div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell">delayed</div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell">waiting / prioritized <i>delay expires</i></div>
  </div>
  <div class="bq-diag-arrow">↓ pull</div>
  <div class="bq-diag-cell bq-diag-accent">active</div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell">completed <i>ack</i></div>
    <div class="bq-diag-cell">failed <i>fail (terminal)</i></div>
    <div class="bq-diag-cell">waiting / prioritized <i>retry</i></div>
  </div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">flow dependencies</span>
    <div class="bq-diag-flow">
      <div class="bq-diag-cell">active</div>
      <div class="bq-diag-arrow">→</div>
      <div class="bq-diag-cell">waiting-children</div>
      <div class="bq-diag-arrow">→</div>
      <div class="bq-diag-cell">waiting <i>children complete</i></div>
    </div>
  </div>
</div>

**Key differences from BullMQ v5:**
- `failed` = BullMQ's failed state. Internally stored in DLQ with metadata (reason, attempt history).
- `prioritized` = BullMQ's prioritized state. Jobs with `priority > 0` are in a separate logical state but share the same priority queue data structure.
- `waiting-children` = Parent jobs waiting for child flows to complete before becoming processable.
:::

### Job

The main job interface returned by Queue methods and passed to worker processors.

```typescript
interface Job<T = unknown> {
  // ── Core Properties ──────────────────────────────────────────

  /** Unique job identifier (UUIDv7) */
  id: string;

  /** Job name/type */
  name: string;

  /** Job payload data */
  data: T;

  /** Queue name this job belongs to */
  queueName: string;

  /** Number of processing attempts made */
  attemptsMade: number;

  /** Job creation timestamp (ms since epoch) */
  timestamp: number;

  /** Current progress (0-100) */
  progress: number;

  /** Return value after successful completion */
  returnvalue?: unknown;

  /** Error message if the job failed */
  failedReason?: string;

  /** Parent job reference (if this job is part of a flow) */
  parent?: { id: string; queueQualifiedName: string };

  // ── Scheduling & Timing ──────────────────────────────────────

  /** Delay in ms before job becomes available for processing */
  delay: number;

  /** Timestamp when job started processing */
  processedOn?: number;

  /** Timestamp when job finished (completed or failed) */
  finishedOn?: number;

  /** Job priority (higher = processed sooner) */
  priority: number;

  // ── Failure & Stall Tracking ─────────────────────────────────

  /** Stack traces from failed attempts */
  stacktrace: string[] | null;

  /** Number of times this job has been stalled */
  stalledCounter: number;

  // ── Metadata ─────────────────────────────────────────────────

  /** Parent key in format queueName:jobId */
  parentKey?: string;

  /** Original job options used when adding this job */
  opts: JobOptions;

  /** Lock token for this job (present when processing) */
  token?: string;

  /** Worker/client identifier processing this job */
  processedBy?: string;

  /** Deduplication ID (if set via jobId or deduplication option) */
  deduplicationId?: string;

  /** Repeat job key (for repeatable jobs) */
  repeatJobKey?: string;

  /** Number of times job processing has been started (includes retries) */
  attemptsStarted: number;

  // ── Core Methods ─────────────────────────────────────────────

  /** Update job progress (0-100) with optional status message */
  updateProgress(progress: number, message?: string): Promise<void>;

  /** Add a log entry to the job */
  log(message: string): Promise<void>;

  /** Get the current state of the job */
  getState(): Promise<JobStateType>;

  /** Remove this job from the queue */
  remove(): Promise<void>;

  /**
   * Retry this job. State-dispatched per BullMQ v5 contract:
   * - `failed` → requeue from DLQ (throws if not present)
   * - `active` → move to waiting (throws if move fails)
   * - `waiting`/`prioritized`/`delayed` → no-op
   * - other states → throws
   */
  retry(): Promise<void>;

  /**
   * Get the return values of all children jobs.
   * Keys are job keys (queueName:jobId), values are return values.
   */
  getChildrenValues<R = unknown>(): Promise<Record<string, R>>;

  // ── State Check Methods ──────────────────────────────────────

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

  // ── Mutation Methods ─────────────────────────────────────────

  /** Update the job's data payload */
  updateData(data: T): Promise<void>;

  /** Promote a delayed job to the waiting state */
  promote(): Promise<void>;

  /** Change the delay on a delayed job */
  changeDelay(delay: number): Promise<void>;

  /** Change the job's priority */
  changePriority(opts: ChangePriorityOpts): Promise<void>;

  /**
   * Extend the job's lock duration. Returns the new duration on success, 0 if the lock
   * could not be extended (wrong token, lock expired, or no active lock).
   */
  extendLock(token: string, duration: number): Promise<number>;

  /** Clear job logs, optionally keeping the last N entries */
  clearLogs(keepLogs?: number): Promise<void>;

  /**
   * Discard this job. Marks it to not be processed further.
   * The job will be moved to failed state with a "discarded" reason.
   */
  discard(): void;

  // ── Dependency Methods ───────────────────────────────────────

  /** Get job dependencies (children) with pagination */
  getDependencies(opts?: GetDependenciesOpts): Promise<JobDependencies>;

  /** Get count of job dependencies */
  getDependenciesCount(opts?: GetDependenciesOpts): Promise<JobDependenciesCount>;

  /** Get return values of failed children jobs */
  getFailedChildrenValues(): Promise<Record<string, string>>;

  /** Get ignored child failures (via ignoreDependencyOnFailure) */
  getIgnoredChildrenFailures(): Promise<Record<string, string>>;

  /** Remove this job's dependency relationship with its parent */
  removeChildDependency(): Promise<boolean>;

  /**
   * Remove the deduplication key associated with this job.
   * @throws Not implemented, no server primitive available. Use `queue.removeDeduplicationKey()` instead.
   */
  removeDeduplicationKey(): Promise<boolean>;

  /** Remove all unprocessed child jobs of this job */
  removeUnprocessedChildren(): Promise<void>;

  // ── Move Methods ─────────────────────────────────────────────

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
   * @param opts - Options including child reference
   * @returns true if job was moved
   * @throws In TCP mode, no server command available. Embedded mode only.
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

  // ── Serialization Methods ────────────────────────────────────

  /** Get job as a typed JSON object */
  toJSON(): JobJson<T>;

  /** Get job as raw JSON (all values stringified) */
  asJSON(): JobJsonRaw;
}
```

### JobJson

Typed JSON representation of a job.

```typescript
interface JobJson<T = unknown> {
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
```

### JobJsonRaw

Raw JSON representation with all values as strings.

```typescript
interface JobJsonRaw {
  id: string;
  name: string;
  data: string;        // JSON stringified
  opts: string;        // JSON stringified
  progress: string;    // JSON stringified
  delay: string;
  timestamp: string;
  attemptsMade: string;
  stacktrace: string | null; // JSON stringified
  returnvalue?: string;      // JSON stringified
  failedReason?: string;
  finishedOn?: string;
  processedOn?: string;
  parentKey?: string;
}
```

### ChangePriorityOpts

```typescript
interface ChangePriorityOpts {
  /** New priority value */
  priority: number;
  /** Process in LIFO order after priority change */
  lifo?: boolean;
}
```

### GetDependenciesOpts

```typescript
interface GetDependenciesOpts {
  processed?: { cursor?: number; count?: number };
  unprocessed?: { cursor?: number; count?: number };
}
```

### JobDependencies

```typescript
interface JobDependencies {
  processed: Record<string, unknown>;
  unprocessed: string[];
  nextProcessedCursor?: number;
  nextUnprocessedCursor?: number;
}
```

### JobDependenciesCount

```typescript
interface JobDependenciesCount {
  processed: number;
  unprocessed: number;
}
```

### JobOptions

Options when adding a job to a queue.

```typescript
interface JobOptions {
  /** Job priority (higher = processed sooner, default: 0) */
  priority?: number;

  /** Delay in milliseconds before job becomes available (default: 0) */
  delay?: number;

  /** Maximum number of processing attempts (default: 3) */
  attempts?: number;

  /**
   * Backoff between retries. Either a delay in ms or a BackoffOptions object.
   * Default: 1000
   */
  backoff?: number | BackoffOptions;

  /** Processing timeout in milliseconds. Job fails if exceeded. */
  timeout?: number;

  /**
   * Custom job ID for idempotent/deduplication.
   * If a job with this ID already exists, the existing job is returned.
   */
  jobId?: string;

  /**
   * Remove job on completion. Boolean only: age/count retention
   * (number | KeepJobs) is not implemented and would be silently
   * ignored, so the type is narrowed to prevent the misuse.
   * Default: false
   */
  removeOnComplete?: boolean;

  /** Remove job on failure. Boolean only, see removeOnComplete. Default: false */
  removeOnFail?: boolean;

  /** Stall timeout in ms. Job is stalled if no heartbeat after this time. */
  stallTimeout?: number;

  /** Repeat configuration for recurring jobs */
  repeat?: RepeatOptions;

  /**
   * Force immediate persistence to disk (bypass write buffer).
   * Use for critical jobs where data loss is unacceptable.
   * Default: false (uses buffered writes for ~100k jobs/sec throughput)
   */
  durable?: boolean;

  /**
   * Parent job reference for flow dependencies.
   * When set, this job becomes a child of the specified parent.
   * The parent will wait for all children to complete before processing.
   */
  parent?: ParentOpts;

  /** Process jobs in LIFO order (newest first, default: false) */
  lifo?: boolean;

  /** Maximum stack trace lines to store on failure (default: 10) */
  stackTraceLimit?: number;

  /** Maximum number of log entries to keep per job */
  keepLogs?: number;

  /** Maximum job data size in bytes. Jobs exceeding this are rejected. */
  sizeLimit?: number;

  /** Fail parent job if this child job fails (default: false) */
  failParentOnFailure?: boolean;

  /** Remove dependency relationship if this job fails (default: false) */
  removeDependencyOnFailure?: boolean;

  /** Continue parent processing even if this child fails (default: false) */
  continueParentOnFailure?: boolean;

  /** Move job to parent's failed dependencies instead of blocking parent (default: false) */
  ignoreDependencyOnFailure?: boolean;

  /** Job creation timestamp in ms (default: Date.now()) */
  timestamp?: number;

  /** Deduplication configuration */
  deduplication?: DeduplicationOptions;

  /** Debounce configuration */
  debounce?: DebounceOptions;
}
```

### ParentOpts

```typescript
interface ParentOpts {
  /** Parent job ID */
  id: string;
  /** Parent job queue name */
  queue: string;
}
```

### BackoffOptions

```typescript
interface BackoffOptions {
  /** Backoff strategy type */
  type: 'fixed' | 'exponential';
  /** Base delay in milliseconds */
  delay: number;
}
```

All backoff delays include automatic **jitter** to prevent thundering herd:
- **Exponential**: ±50% jitter around the computed delay
- **Fixed**: ±20% jitter around the configured delay

Delays are capped at 1 hour by default. This prevents runaway delays at high attempt counts.

### KeepJobs

Exported for BullMQ compatibility. Not accepted by per-job `removeOnComplete`/`removeOnFail` (those are boolean only); the equivalent shape is accepted by `WorkerOptions.removeOnComplete`/`removeOnFail`.

```typescript
interface KeepJobs {
  /** Maximum age in milliseconds */
  age?: number;
  /** Maximum count of jobs to keep */
  count?: number;
}
```

### RepeatOptions

Configuration for recurring/repeatable jobs.

```typescript
interface RepeatOptions {
  /** Repeat every N milliseconds (alternative to pattern) */
  every?: number;

  /** Maximum repetitions (omit or null for infinite) */
  limit?: number;

  /** Cron pattern (alternative to every) */
  pattern?: string;

  /** Start date for repeat jobs */
  startDate?: Date | string | number;

  /** End date for repeat jobs */
  endDate?: Date | string | number;

  /** Timezone for cron pattern (e.g. 'America/New_York') */
  tz?: string;

  /** Execute immediately on start (default: false) */
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
```

### DeduplicationOptions

Prevent duplicate jobs from being added to the queue.

```typescript
interface DeduplicationOptions {
  /** Unique deduplication ID (required) */
  id: string;

  /** TTL in milliseconds for the deduplication key */
  ttl?: number;

  /** Extend TTL when a duplicate job arrives (for debounce mode) */
  extend?: boolean;

  /** Replace job data when duplicate arrives while in delayed state */
  replace?: boolean;
}
```

### DebounceOptions

Debounce job creation within a time window.

```typescript
interface DebounceOptions {
  /** Unique debounce ID (required) */
  id: string;

  /** TTL in milliseconds for the debounce window (required) */
  ttl: number;
}
```

### Processor

```typescript
type Processor<T = unknown, R = unknown> = (job: Job<T & FlowJobData>) => Promise<R> | R;
```

`FlowJobData` contains the optional flow-injected fields (`__parentId`, `__parentQueue`, `__childrenIds`, `__flowParentId`, `__flowParentIds`) that FlowProducer adds to `job.data` when a job is part of a flow.

### JobCounts

Returned by `queue.getJobCounts()`.

```typescript
interface JobCounts {
  waiting: number;
  prioritized: number;
  active: number;
  completed: number;
  failed: number;
  delayed: number;
  paused: number;
}
```

## Queue Types

### QueueOptions

```typescript
interface QueueOptions {
  /** Default job options applied to all jobs in this queue */
  defaultJobOptions?: JobOptions;

  /** TCP connection options (for server mode) */
  connection?: ConnectionOptions;

  /** Use embedded mode (in-process SQLite, default: false) */
  embedded?: boolean;

  /** SQLite database path for embedded mode (overrides the DATA_PATH env vars) */
  dataPath?: string;

  /**
   * Auto-batching for queue.add() calls in TCP mode.
   * Buffers concurrent add() calls and sends them as a single PUSHB command.
   * Default: enabled for TCP mode, disabled for embedded mode.
   */
  autoBatch?: AutoBatchOptions;

  /**
   * Namespace prefix prepended to the queue name on the server.
   * Lets multiple environments (e.g. `dev:`, `prod:`) or tenants share
   * the same broker without their jobs, workers, cron schedulers, stats,
   * pause state, DLQ, or rate limits overlapping. `Queue.name` keeps
   * returning the logical name; only the server-side key is prefixed.
   * See the [Namespace Isolation](/guide/queue/advanced/#namespace-isolation-prefixkey)
   * guide.
   */
  prefixKey?: string;
}
```

### AutoBatchOptions

```typescript
interface AutoBatchOptions {
  /** Enable auto-batching (default: true for TCP, false for embedded) */
  enabled?: boolean;
  /** Max items before auto-flush (default: 50) */
  maxSize?: number;
  /** Max delay in ms before auto-flush (default: 5) */
  maxDelayMs?: number;
}
```

Jobs added with `durable: true` bypass the batcher and are sent as individual PUSH commands.

### ConnectionOptions

```typescript
interface ConnectionOptions {
  /** Server hostname (default: 'localhost', ignored if socketPath is set) */
  host?: string;

  /** TCP port (default: 6789, ignored if socketPath is set) */
  port?: number;

  /** Unix socket path (takes priority over host/port) */
  socketPath?: string;

  /** Enable TLS to the server: true (system CAs) or custom options (default: off) */
  tls?: boolean | ClientTlsOptions;

  /** Authentication token */
  token?: string;

  /**
   * Connection pool size for parallel operations.
   * Source JSDoc default: 1. Runtime default for Queue/FlowProducer: 4.
   * Set >1 to enable connection pooling.
   */
  poolSize?: number;

  /** Ping interval in ms for health checks (default: 30000, 0 to disable) */
  pingInterval?: number;

  /** Command timeout in ms (default: 30000) */
  commandTimeout?: number;

  /**
   * Consecutive command timeouts (no intervening success) before the connection
   * is concluded dead and a reconnect is forced (default: 3, 0 to disable).
   * Recovery path for a half-open socket, independent of the health-check ping.
   */
  maxCommandTimeouts?: number;

  /** Enable TCP pipelining (default: true) */
  pipelining?: boolean;

  /** Max commands in flight per connection (default: 100) */
  maxInFlight?: number;
}
```

### ClientTlsOptions

```typescript
interface ClientTlsOptions {
  /** Verify the server certificate (default: true). Set false for self-signed in dev. */
  rejectUnauthorized?: boolean;
  /** Path to a PEM CA certificate to trust (e.g. the self-signed server cert) */
  caFile?: string;
}
```

### RateLimiterOptions

```typescript
interface RateLimiterOptions {
  /** Maximum number of jobs to process in the duration window */
  max: number;

  /** Duration window in milliseconds */
  duration: number;

  /** Optional group key for per-group rate limiting */
  groupKey?: string;
}
```

## Worker Types

### WorkerOptions

```typescript
interface WorkerOptions {
  /** Number of concurrent jobs (default: 1) */
  concurrency?: number;

  /** Auto-run on creation (default: true) */
  autorun?: boolean;

  /** Heartbeat interval in ms (default: 10000, 0 to disable) */
  heartbeatInterval?: number;

  /** TCP connection options (for server mode) */
  connection?: ConnectionOptions;

  /** Use embedded mode (in-process SQLite, default: false) */
  embedded?: boolean;

  /** SQLite database path for embedded mode (overrides the DATA_PATH env vars) */
  dataPath?: string;

  /** Number of jobs to pull per batch (default: 10, max: 1000) */
  batchSize?: number;

  /** Long poll timeout in ms when queue is empty (default: 0, max: 30000) */
  pollTimeout?: number;

  /**
   * Use lock-based job ownership.
   * When enabled, each pulled job gets a lock renewed via heartbeat.
   * Disable for high-throughput scenarios where stall detection is sufficient.
   * Default: true
   */
  useLocks?: boolean;

  /** Rate limiter configuration for controlling job processing rate */
  limiter?: RateLimiterOptions;

  /** Lock duration in ms (default: 30000). Sent to the server on pull; also used by stall detection. */
  lockDuration?: number;

  /** Max stalls before moving to failed (default: 1). Applied to stall config in embedded mode. */
  maxStalledCount?: number;

  /** Skip stalled job check, disables the stalled event subscription (default: false) */
  skipStalledCheck?: boolean;

  /** Skip lock renewal via heartbeat (default: false) */
  skipLockRenewal?: boolean;

  /** Delay in ms between polls when the queue is drained (default: 50) */
  drainDelay?: number;

  /** Remove jobs on complete, applied as default for all jobs processed by this worker */
  removeOnComplete?: boolean | number | { age?: number; count?: number };

  /** Remove jobs on fail, applied as default for all jobs processed by this worker */
  removeOnFail?: boolean | number | { age?: number; count?: number };

  /**
   * Namespace prefix; must match the producing `Queue.prefixKey` to
   * consume its jobs. The Worker is registered under `prefixKey + name`
   * on the server, so two workers with the same logical queue name but
   * different prefixes never see each other's jobs. See the
   * [Namespace Isolation](/guide/queue/advanced/#namespace-isolation-prefixkey) guide.
   */
  prefixKey?: string;
}
```

### Worker Events

```typescript
// Worker emits these events:
worker.on('ready', () => void);
worker.on('active', (job: Job) => void);
worker.on('completed', (job: Job, result: R) => void);
worker.on('failed', (job: Job, error: Error) => void);
worker.on('progress', (job: Job | null, progress: number) => void);
worker.on('stalled', (jobId: string, reason: string) => void);
worker.on('cancelled', (data: { jobId: string; reason: string }) => void);
worker.on('log', (job: Job, message: string) => void);
worker.on('error', (error: Error) => void);
worker.on('drained', () => void);
worker.on('closed', () => void);
```

## QueueEvents Types

### QueueEvents

Event listener class for monitoring queue activity without processing jobs.

```typescript
class QueueEvents<R = unknown, P = unknown> extends EventEmitter {
  /** Queue name being monitored */
  readonly name: string;

  constructor(name: string);

  /** Wait until the QueueEvents instance is ready to receive events */
  waitUntilReady(): Promise<void>;

  /** Close the event listener and stop receiving events */
  close(): void;

  /** Disconnect from the event stream (alias for close) */
  disconnect(): Promise<void>;
}
```

### QueueEvents Event Payloads

Each event emitted by `QueueEvents` has a typed payload:

```typescript
/** Emitted when a job is added to the queue */
interface WaitingEvent {
  jobId: string;
}

/** Emitted when a job begins processing */
interface ActiveEvent {
  jobId: string;
}

/** Emitted when a job completes successfully */
interface CompletedEvent<R = unknown> {
  jobId: string;
  returnvalue: R;
}

/** Emitted when a job fails */
interface FailedEvent {
  jobId: string;
  failedReason: string;
  data?: unknown;
}

/** Emitted when job progress is updated */
interface ProgressEvent<P = unknown> {
  jobId: string;
  data: P;
}

/** Emitted when a job stalls (no heartbeat) */
interface StalledEvent {
  jobId: string;
}

/** Emitted when a job is removed from the queue */
interface RemovedEvent {
  jobId: string;
  prev: string;
}

/** Emitted when a job is moved to delayed state */
interface DelayedEvent {
  jobId: string;
  delay: number;
}

/** Emitted when a duplicate job is detected */
interface DuplicatedEvent {
  jobId: string;
}

/** Emitted when a job is retried */
interface RetriedEvent {
  jobId: string;
  prev: string;
}

/** Emitted when a job enters waiting-children state */
interface WaitingChildrenEvent {
  jobId: string;
}

/** Emitted when the queue has no more waiting jobs */
interface DrainedEvent {
  id: string;
}
```

### QueueEvents Usage

```typescript
const events = new QueueEvents('my-queue');

events.on('waiting', ({ jobId }) => { /* ... */ });
events.on('active', ({ jobId }) => { /* ... */ });
events.on('completed', ({ jobId, returnvalue }) => { /* ... */ });
events.on('failed', ({ jobId, failedReason }) => { /* ... */ });
events.on('progress', ({ jobId, data }) => { /* ... */ });
events.on('stalled', ({ jobId }) => { /* ... */ });
events.on('removed', ({ jobId, prev }) => { /* ... */ });
events.on('delayed', ({ jobId, delay }) => { /* ... */ });
events.on('duplicated', ({ jobId }) => { /* ... */ });
events.on('retried', ({ jobId, prev }) => { /* ... */ });
events.on('waiting-children', ({ jobId }) => { /* ... */ });
events.on('drained', ({ id }) => { /* ... */ });
events.on('paused', () => { /* ... */ });
events.on('resumed', () => { /* ... */ });
events.on('error', (error: Error) => { /* ... */ });
```

## QueueEventType

```typescript
type QueueEventType =
  | 'waiting'
  | 'active'
  | 'completed'
  | 'failed'
  | 'progress'
  | 'removed'
  | 'drained';
```

## FlowProducer Types

### FlowProducerOptions

```typescript
interface FlowProducerOptions {
  /** Use embedded mode (no server) */
  embedded?: boolean;
  /** TCP connection options */
  connection?: ConnectionOptions;
}
```

:::note
FlowProducer extends `EventEmitter` (BullMQ v5 compatible). You can listen for events using `.on()`, `.once()`, etc. The `close()` method returns `Promise<void>` and the `closing` property tracks shutdown state.
:::

### FlowOpts

Per-flow options passed as the second argument to `flow.add(flowJob, opts)`.

```typescript
interface FlowOpts {
  /**
   * Default job options per queue name.
   * Applied as defaults; per-job opts override these.
   * jobId is intentionally excluded because identity belongs to each flow node.
   */
  queuesOptions?: Record<string, Omit<Partial<JobOptions>, 'jobId'>>;
}
```

Set `jobId` on a `FlowJob.opts` object when a node needs a custom identity.
`jobId` is rejected inside `queuesOptions`: a shared default could assign the
same identity to multiple nodes and make the atomic graph ambiguous. Python
follows the same rule using `job_id` inside each node's `opts`, never inside
`queues_options`.

**Example:**
```typescript
await flow.add(
  {
    name: 'parent',
    queueName: 'reports',
    children: [
      { name: 'fetch', queueName: 'api', data: { url: '...' } },
      { name: 'parse', queueName: 'cpu', data: {} },
    ],
  },
  {
    queuesOptions: {
      api: { attempts: 5, backoff: 2000 },  // All jobs in 'api' queue
      cpu: { timeout: 60000 },               // All jobs in 'cpu' queue
    },
  }
);
```

### FlowJob

A job definition within a flow. Children are processed before the parent.

```typescript
interface FlowJob<T = unknown> {
  /** Job name */
  name: string;
  /** Queue name */
  queueName: string;
  /** Job data */
  data?: T;
  /** Job options */
  opts?: JobOptions;
  /** Child jobs (processed BEFORE this job) */
  children?: FlowJob<T>[];
}
```

### JobNode

Result from adding a flow. Contains the created job and its children.

```typescript
interface JobNode<T = unknown> {
  /** The created job instance */
  job: Job<T>;
  /** Child nodes (if any) */
  children?: JobNode<T>[];
}
```

### GetFlowOpts

```typescript
interface GetFlowOpts {
  /** Job ID to get the flow for */
  id: string;
  /** Queue name where the job is located */
  queueName: string;
  /** Maximum depth to traverse (default: unlimited) */
  depth?: number;
  /** Maximum number of children to fetch per level (default: unlimited) */
  maxChildren?: number;
}
```

## SandboxedWorker Types

### SandboxedWorkerOptions

```typescript
interface SandboxedWorkerOptions {
  /** Path to processor file (must export default async function) */
  processor: string;

  /** Number of worker processes (default: 1) */
  concurrency?: number;

  /** Job timeout in ms (default: 30000, 0 = disabled) */
  timeout?: number;

  /** Max memory per worker in MB (default: 256, uses smol mode if <= 64) */
  maxMemory?: number;

  /** Max restarts before giving up (default: 10) */
  maxRestarts?: number;

  /** Auto-restart crashed workers (default: true) */
  autoRestart?: boolean;

  /** Poll interval in ms when no workers are idle (default: 10) */
  pollInterval?: number;

  /** Heartbeat interval in ms for TCP lock renewal (default: 10000 for TCP, 0 for embedded) */
  heartbeatInterval?: number;

  /** TCP connection options (omit for embedded mode) */
  connection?: ConnectionOptions;

  /** Auto-stop after this many ms of inactivity (0 = disabled, default: 0) */
  idleTimeout?: number;

  /** Recycle individual idle worker processes after this many ms (default: 30000, 0 = disabled) */
  idleRecycleMs?: number;

  /** Auto-restart the worker pool when new jobs arrive after idle shutdown (default: false) */
  autoStart?: boolean;

  /** Poll interval in ms for checking new jobs while in idle-shutdown state (default: 5000) */
  autoStartPollMs?: number;
}
```

### SandboxedWorker Stats

Returned by `sandboxedWorker.getStats()`.

```typescript
{
  total: number;    // Total worker processes
  busy: number;     // Currently processing
  idle: number;     // Alive and available for work
  recycled: number; // Workers recycled after idling
  restarts: number; // Total restarts across all workers
}
```

## Stall Detection Types

### StallConfig

```typescript
interface StallConfig {
  /** Enable stall detection (default: true) */
  enabled?: boolean;

  /** Stall timeout in ms (default: 30000) */
  stallInterval?: number;

  /** Max stalls before moving to DLQ (default: 3) */
  maxStalls?: number;

  /** Grace period after job start in ms (default: 5000) */
  gracePeriod?: number;
}
```

## DLQ Types

### DlqConfig

```typescript
interface DlqConfig {
  /** Enable auto-retry from DLQ (default: false) */
  autoRetry?: boolean;

  /** Auto-retry interval in ms (default: 3600000 = 1 hour) */
  autoRetryInterval?: number;

  /** Max auto-retries before giving up (default: 3) */
  maxAutoRetries?: number;

  /** Max age before auto-purge in ms (default: 604800000 = 7 days, null = never) */
  maxAge?: number | null;

  /** Max entries per queue (default: 10000) */
  maxEntries?: number;
}
```

### FailureReason

```typescript
type FailureReason =
  | 'explicit_fail'        // Job explicitly failed via fail() or thrown error
  | 'max_attempts_exceeded' // Exceeded all retry attempts
  | 'timeout'              // Job processing timed out (exceeded timeout option)
  | 'stalled'              // Job stalled (no heartbeat within stallInterval)
  | 'ttl_expired'          // Time-to-live expired before processing
  | 'worker_lost'          // Worker disconnected while processing (TCP mode)
  | 'unknown';             // Catch-all for edge cases
```

:::note[When is 'unknown' used?]
The `unknown` reason is a catch-all for rare edge cases:
- Job data corruption during serialization
- Internal queue manager errors
- Jobs recovered from database without failure metadata
- Race conditions during shutdown

If you see many `unknown` failures, check logs for underlying errors.
:::

### DlqEntry

```typescript
interface DlqEntry<T = unknown> {
  /** The failed job */
  job: Job<T>;

  /** When job entered DLQ (ms since epoch) */
  enteredAt: number;

  /** Last failure reason */
  reason: FailureReason;

  /** Last error message */
  error: string | null;

  /** Full attempt history */
  attempts: Array<AttemptRecord>;

  /** Number of retry attempts from DLQ */
  retryCount: number;

  /** Last retry timestamp */
  lastRetryAt: number | null;

  /** Next scheduled auto-retry (null = no auto-retry) */
  nextRetryAt: number | null;

  /** When entry expires for auto-purge (null = never) */
  expiresAt: number | null;
}
```

### AttemptRecord

```typescript
interface AttemptRecord {
  /** Attempt number (1-based) */
  attempt: number;

  /** When this attempt started (ms since epoch) */
  startedAt: number;

  /** When this attempt failed (ms since epoch) */
  failedAt: number;

  /** Failure reason for this attempt */
  reason: FailureReason;

  /** Error message if any */
  error: string | null;

  /** Duration of this attempt in ms */
  duration: number;
}
```

### DlqFilter

```typescript
interface DlqFilter {
  /** Filter by failure reason */
  reason?: FailureReason;

  /** Only entries older than this timestamp */
  olderThan?: number;

  /** Only entries newer than this timestamp */
  newerThan?: number;

  /** Only entries that can be retried */
  retriable?: boolean;

  /** Only entries that are expired */
  expired?: boolean;

  /** Limit number of results */
  limit?: number;

  /** Offset for pagination */
  offset?: number;
}
```

### DlqStats

```typescript
interface DlqStats {
  /** Total DLQ entries */
  total: number;

  /** Entries grouped by failure reason */
  byReason: Record<FailureReason, number>;

  /** Entries awaiting auto-retry */
  pendingRetry: number;

  /** Expired entries (awaiting cleanup) */
  expired: number;

  /** Oldest entry timestamp (null if empty) */
  oldestEntry: number | null;

  /** Newest entry timestamp (null if empty) */
  newestEntry: number | null;
}
```

## Generic Type Helpers

bunqueue supports generic types for type-safe job data and results:

```typescript
// Define typed job data
interface EmailJobData {
  to: string;
  subject: string;
  body: string;
}

interface EmailResult {
  sent: boolean;
  messageId: string;
}

// Queue with typed data
const queue = new Queue<EmailJobData>('emails');

// TypeScript enforces the data shape
await queue.add('welcome', {
  to: 'user@example.com',
  subject: 'Welcome!',
  body: 'Hello and welcome.',
});

// Worker with typed data and result
const worker = new Worker<EmailJobData, EmailResult>(
  'emails',
  async (job) => {
    // job.data is typed as EmailJobData
    const { to, subject, body } = job.data;
    return { sent: true, messageId: 'msg-123' };
  }
);

// Type error at compile time: missing required fields
await queue.add('send', { to: 'test@example.com' }); // Error!

// QueueEvents with typed result and progress
const events = new QueueEvents<EmailResult, number>('emails');
events.on('completed', ({ jobId, returnvalue }) => {
  // returnvalue is typed as EmailResult
  console.log(returnvalue.messageId);
});
```

:::tip[Related]
- [Queue API](/guide/queue/) - Queue usage with these types
- [Worker API](/guide/worker/) - Worker usage with these types
- [HTTP API Reference](/api/http/) - HTTP endpoints reference
:::
