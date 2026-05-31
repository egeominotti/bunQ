/**
 * Job domain types
 * Core job structure and related types
 */

/** Branded type for Job IDs (UUIDv7) */
export type JobId = string & { readonly __brand: 'JobId' };

/** Create a JobId from string */
export function jobId(id: string): JobId {
  return id as JobId;
}

/** Generate a new UUIDv7 JobId */
export function generateJobId(): JobId {
  return Bun.randomUUIDv7() as JobId;
}

/** Job state enumeration */
export const enum JobState {
  Waiting = 'waiting',
  Prioritized = 'prioritized',
  Delayed = 'delayed',
  Active = 'active',
  Completed = 'completed',
  Failed = 'failed',
}

/** Repeat configuration for repeatable jobs */
export interface RepeatConfig {
  /** Repeat every N milliseconds */
  readonly every?: number;
  /** Maximum number of repetitions (null = infinite) */
  readonly limit?: number;
  /** Cron pattern (alternative to every) */
  readonly pattern?: string;
  /** Current repeat count */
  count: number;
  /** Start date timestamp */
  readonly startDate?: number;
  /** End date timestamp */
  readonly endDate?: number;
  /** Timezone for cron pattern */
  readonly tz?: string;
  /** Execute immediately on start */
  readonly immediately?: boolean;
  /** Previous execution timestamp */
  prevMillis?: number;
  /** Offset in milliseconds */
  readonly offset?: number;
  /** Custom job ID for repeat jobs */
  readonly jobId?: string;
}

/** Backoff configuration */
export interface BackoffConfig {
  /** Backoff strategy type */
  readonly type: 'fixed' | 'exponential';
  /** Base delay in milliseconds */
  readonly delay: number;
  /** Maximum delay in milliseconds (default: 3600000 = 1 hour) */
  readonly maxDelay?: number;
}

/** Default maximum backoff delay: 1 hour */
export const DEFAULT_MAX_BACKOFF = 3_600_000;

/** Timeline entry — tracks each state transition */
export interface JobTimelineEntry {
  readonly state: string;
  readonly timestamp: number;
  readonly worker?: string;
  readonly error?: string;
  readonly attempt?: number;
}

/** Max timeline entries per job (prevents unbounded growth) */
export const MAX_TIMELINE_ENTRIES = 20;

/** Core job structure */
export interface Job {
  readonly id: JobId;
  readonly queue: string;
  readonly data: unknown;
  readonly priority: number;
  readonly createdAt: number;
  readonly lifo: boolean;

  // Scheduling
  runAt: number;
  startedAt: number | null;
  completedAt: number | null;

  // Retry config
  attempts: number;
  readonly maxAttempts: number;
  /** Backoff delay in ms or backoff configuration */
  readonly backoff: number;
  /** Backoff configuration (if using object-based backoff) */
  readonly backoffConfig: BackoffConfig | null;

  // Timeouts
  readonly ttl: number | null;
  readonly timeout: number | null;

  // Deduplication
  readonly uniqueKey: string | null;
  readonly customId: string | null;

  // Dependencies & workflows
  readonly dependsOn: JobId[];
  readonly parentId: JobId | null;
  childrenIds: JobId[];
  childrenCompleted: number;

  // Metadata
  readonly tags: string[];
  readonly groupId: string | null;

  // Progress tracking
  progress: number;
  progressMessage: string | null;

  // Cleanup config
  readonly removeOnComplete: boolean;
  readonly removeOnFail: boolean;

  // Repeat config
  readonly repeat: RepeatConfig | null;

  // Stall detection
  lastHeartbeat: number;
  readonly stallTimeout: number | null;
  stallCount: number;

  // BullMQ v5 additional options
  /** Maximum stack trace lines to store */
  readonly stackTraceLimit: number;
  /** Maximum number of log entries to keep */
  readonly keepLogs: number | null;
  /** Maximum job data size in bytes */
  readonly sizeLimit: number | null;
  /** Fail parent job if this child job fails */
  readonly failParentOnFailure: boolean;
  /** Remove dependency relationship if this job fails */
  readonly removeDependencyOnFailure: boolean;
  /** Continue parent processing even if this child fails */
  readonly continueParentOnFailure: boolean;
  /** Move job to parent's failed dependencies instead of blocking parent */
  readonly ignoreDependencyOnFailure: boolean;
  /** Deduplication TTL in milliseconds */
  readonly deduplicationTtl: number | null;
  /** Extend deduplication TTL on duplicate */
  readonly deduplicationExtend: boolean;
  /** Replace job data on duplicate while in delayed state */
  readonly deduplicationReplace: boolean;
  /** Debounce ID */
  readonly debounceId: string | null;
  /** Debounce TTL in milliseconds */
  readonly debounceTtl: number | null;

  /** State transition timeline (in-memory only, not persisted to SQLite) */
  timeline: JobTimelineEntry[];
}

/** Input for creating a new job */
export interface JobInput {
  data: unknown;
  priority?: number;
  delay?: number;
  maxAttempts?: number;
  /** Backoff delay in ms or backoff configuration */
  backoff?: number | { type: 'fixed' | 'exponential'; delay: number };
  ttl?: number;
  timeout?: number;
  uniqueKey?: string;
  customId?: string;
  dependsOn?: JobId[];
  childrenIds?: JobId[];
  parentId?: JobId;
  tags?: string[];
  groupId?: string;
  lifo?: boolean;
  removeOnComplete?: boolean;
  removeOnFail?: boolean;
  stallTimeout?: number;
  repeat?: {
    every?: number;
    limit?: number;
    pattern?: string;
    /** Current count (for internal use when re-queueing) */
    count?: number;
    /** Start date timestamp */
    startDate?: number;
    /** End date timestamp */
    endDate?: number;
    /** Timezone for cron pattern */
    tz?: string;
    /** Execute immediately on start */
    immediately?: boolean;
    /** Previous execution timestamp */
    prevMillis?: number;
    /** Offset in milliseconds */
    offset?: number;
    /** Custom job ID for repeat jobs */
    jobId?: string;
  };
  /** Advanced deduplication options */
  dedup?: {
    /** TTL for unique key in milliseconds */
    ttl?: number;
    /** Extend TTL on duplicate instead of rejecting */
    extend?: boolean;
    /** Replace job data on duplicate instead of rejecting */
    replace?: boolean;
  };
  /**
   * Force immediate persistence to disk (bypass write buffer).
   * Use for critical jobs where data loss is unacceptable.
   * Default: false (uses buffered writes for better throughput)
   */
  durable?: boolean;
  // BullMQ v5 additional options
  /** Maximum stack trace lines to store */
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
  /** Debounce ID */
  debounceId?: string;
  /** Debounce TTL in milliseconds */
  debounceTtl?: number;
  /** Job creation timestamp (default: Date.now()) */
  timestamp?: number;
}

/** Job creation defaults */
export const JOB_DEFAULTS = {
  priority: 0,
  maxAttempts: 3,
  backoff: 1000,
  lifo: false,
  removeOnComplete: false,
  removeOnFail: false,
  stackTraceLimit: 10,
} as const;

/** Parse backoff from input (can be number or object) */
function parseBackoff(
  input: number | { type: 'fixed' | 'exponential'; delay: number } | undefined
): { backoff: number; backoffConfig: BackoffConfig | null } {
  if (typeof input === 'object') {
    return {
      backoff: input.delay,
      backoffConfig: { type: input.type, delay: input.delay },
    };
  }
  return { backoff: input ?? JOB_DEFAULTS.backoff, backoffConfig: null };
}

/** Parse repeat config from input */
function parseRepeatConfig(repeat: JobInput['repeat']): RepeatConfig | null {
  if (!repeat) return null;
  return {
    every: repeat.every,
    limit: repeat.limit,
    pattern: repeat.pattern,
    count: repeat.count ?? 0,
    startDate: repeat.startDate,
    endDate: repeat.endDate,
    tz: repeat.tz,
    immediately: repeat.immediately,
    prevMillis: repeat.prevMillis,
    offset: repeat.offset,
    jobId: repeat.jobId,
  };
}

/** Parse core job options with defaults */
/** Coerce a wire value to boolean. The JobInput boundary is not type-safe at
 * runtime — a hand-crafted/legacy client can send a non-boolean. See issue #90. */
function toBoolean(value: unknown, fallback: boolean): boolean {
  return value === undefined ? fallback : Boolean(value);
}

function parseCoreOptions(input: JobInput): {
  priority: number;
  lifo: boolean;
  maxAttempts: number;
  removeOnComplete: boolean;
  removeOnFail: boolean;
} {
  return {
    priority: input.priority ?? JOB_DEFAULTS.priority,
    lifo: input.lifo ?? JOB_DEFAULTS.lifo,
    maxAttempts: input.maxAttempts ?? JOB_DEFAULTS.maxAttempts,
    // Coerce to boolean: a stray number/object forwarded over the wire must not
    // persist as a non-boolean and diverge from embedded mode. See issue #90.
    removeOnComplete: toBoolean(input.removeOnComplete, JOB_DEFAULTS.removeOnComplete),
    removeOnFail: toBoolean(input.removeOnFail, JOB_DEFAULTS.removeOnFail),
  };
}

/** Parse optional fields (nullable) */
function parseOptionalFields(input: JobInput): {
  ttl: number | null;
  timeout: number | null;
  uniqueKey: string | null;
  customId: string | null;
  parentId: JobId | null;
  groupId: string | null;
  stallTimeout: number | null;
} {
  return {
    ttl: input.ttl ?? null,
    timeout: input.timeout ?? null,
    uniqueKey: input.uniqueKey ?? null,
    customId: input.customId ?? null,
    parentId: input.parentId ?? null,
    groupId: input.groupId ?? null,
    stallTimeout: input.stallTimeout ?? null,
  };
}

/** Parse BullMQ v5 specific options */
function parseBullMQV5Options(input: JobInput): {
  stackTraceLimit: number;
  keepLogs: number | null;
  sizeLimit: number | null;
  failParentOnFailure: boolean;
  removeDependencyOnFailure: boolean;
  continueParentOnFailure: boolean;
  ignoreDependencyOnFailure: boolean;
  deduplicationTtl: number | null;
  deduplicationExtend: boolean;
  deduplicationReplace: boolean;
  debounceId: string | null;
  debounceTtl: number | null;
} {
  return {
    stackTraceLimit: input.stackTraceLimit ?? JOB_DEFAULTS.stackTraceLimit,
    keepLogs: input.keepLogs ?? null,
    sizeLimit: input.sizeLimit ?? null,
    failParentOnFailure: input.failParentOnFailure ?? false,
    removeDependencyOnFailure: input.removeDependencyOnFailure ?? false,
    continueParentOnFailure: input.continueParentOnFailure ?? false,
    ignoreDependencyOnFailure: input.ignoreDependencyOnFailure ?? false,
    deduplicationTtl: input.dedup?.ttl ?? null,
    deduplicationExtend: input.dedup?.extend ?? false,
    deduplicationReplace: input.dedup?.replace ?? false,
    debounceId: input.debounceId ?? null,
    debounceTtl: input.debounceTtl ?? null,
  };
}

/** Create a new job from input */
export function createJob(
  id: JobId,
  queue: string,
  input: JobInput,
  now: number = Date.now()
): Job {
  const { backoff, backoffConfig } = parseBackoff(input.backoff);
  const coreOpts = parseCoreOptions(input);
  const optionalFields = parseOptionalFields(input);
  const v5Opts = parseBullMQV5Options(input);
  const createdAt = input.timestamp ?? now;

  return {
    id,
    queue,
    data: input.data,
    createdAt,
    runAt: createdAt + (input.delay ?? 0),
    startedAt: null,
    completedAt: null,
    attempts: 0,
    backoff,
    backoffConfig,
    dependsOn: input.dependsOn ?? [],
    childrenIds: input.childrenIds ?? [],
    childrenCompleted: 0,
    tags: input.tags ?? [],
    progress: 0,
    progressMessage: null,
    repeat: parseRepeatConfig(input.repeat),
    lastHeartbeat: createdAt,
    stallCount: 0,
    ...coreOpts,
    ...optionalFields,
    ...v5Opts,
    timeline: [],
  };
}

/** Check if job is delayed */
export function isDelayed(job: Job, now: number = Date.now()): boolean {
  return job.runAt > now;
}

/** Check if job is ready to process */
export function isReady(job: Job, now: number = Date.now()): boolean {
  return job.runAt <= now;
}

/** Check if job is expired (TTL exceeded) */
export function isExpired(job: Job, now: number = Date.now()): boolean {
  if (job.ttl === null) return false;
  return now > job.createdAt + job.ttl;
}

/** Check if job has timed out during processing */
export function isTimedOut(job: Job, now: number = Date.now()): boolean {
  if (job.timeout === null || job.startedAt === null) return false;
  return now > job.startedAt + job.timeout;
}

/** Calculate next retry delay with backoff strategy, jitter, and max cap */
export function calculateBackoff(job: Job): number {
  const maxDelay = job.backoffConfig?.maxDelay ?? DEFAULT_MAX_BACKOFF;

  if (job.backoffConfig) {
    if (job.backoffConfig.type === 'fixed') {
      // Fixed backoff with ±20% jitter
      const base = job.backoffConfig.delay;
      const jittered = base * (0.8 + Math.random() * 0.4);
      return Math.min(jittered, maxDelay);
    } else {
      // Exponential backoff with ±50% jitter
      const base = job.backoffConfig.delay * Math.pow(2, job.attempts);
      const jittered = base * (0.5 + Math.random());
      return Math.min(jittered, maxDelay);
    }
  }

  // Default: exponential backoff with ±50% jitter and max cap
  const base = job.backoff * Math.pow(2, job.attempts);
  const jittered = base * (0.5 + Math.random());
  return Math.min(jittered, maxDelay);
}

/** Check if job can retry */
export function canRetry(job: Job): boolean {
  return job.attempts < job.maxAttempts;
}

/** Lock token type (UUID) */
export type LockToken = string & { readonly __brand: 'LockToken' };

/** Create a LockToken from string */
export function lockToken(token: string): LockToken {
  return token as LockToken;
}

/** Generate a new lock token */
export function generateLockToken(): LockToken {
  return Bun.randomUUIDv7() as LockToken;
}

/** Job lock structure - tracks ownership of job processing */
export interface JobLock {
  readonly jobId: JobId;
  readonly token: LockToken;
  readonly owner: string; // Client/Worker identifier
  readonly createdAt: number;
  expiresAt: number;
  lastRenewalAt: number;
  renewalCount: number;
  readonly ttl: number; // Lock duration in ms
}

/** Default lock TTL in milliseconds (30 seconds like BullMQ) */
export const DEFAULT_LOCK_TTL = 30_000;

/** Create a new job lock */
export function createJobLock(
  jobId: JobId,
  owner: string,
  ttl: number = DEFAULT_LOCK_TTL,
  now: number = Date.now()
): JobLock {
  return {
    jobId,
    token: generateLockToken(),
    owner,
    createdAt: now,
    expiresAt: now + ttl,
    lastRenewalAt: now,
    renewalCount: 0,
    ttl,
  };
}

/** Check if lock is expired */
export function isLockExpired(lock: JobLock, now: number = Date.now()): boolean {
  return now >= lock.expiresAt;
}

/** Renew a lock, extending its expiration */
export function renewLock(lock: JobLock, newTtl?: number, now: number = Date.now()): void {
  const ttl = newTtl ?? lock.ttl;
  lock.expiresAt = now + ttl;
  lock.lastRenewalAt = now;
  lock.renewalCount++;
}
