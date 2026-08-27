/**
 * Dead Letter Queue Types
 * Enhanced DLQ with metadata, auto-retry, and lifecycle management
 */

import type { Job } from './job';

/** Failure reason categories */
export const enum FailureReason {
  /** Job explicitly failed via fail() call */
  ExplicitFail = 'explicit_fail',
  /** Job exceeded max attempts */
  MaxAttemptsExceeded = 'max_attempts_exceeded',
  /** Job timed out during processing */
  Timeout = 'timeout',
  /** Job stalled (no heartbeat) */
  Stalled = 'stalled',
  /** Job TTL expired */
  TtlExpired = 'ttl_expired',
  /** Worker crashed/disconnected */
  WorkerLost = 'worker_lost',
  /** Unknown/other reason */
  Unknown = 'unknown',
}

/** Single attempt record */
export interface AttemptRecord {
  /** Attempt number (1-based) */
  readonly attempt: number;
  /** When this attempt started */
  readonly startedAt: number;
  /** When this attempt failed */
  readonly failedAt: number;
  /** Failure reason for this attempt */
  readonly reason: FailureReason;
  /** Error message if any */
  readonly error: string | null;
  /** Duration of this attempt in ms */
  readonly duration: number;
}

/** Internal lifecycle state carried while a DLQ auto-retry is live. */
export interface DlqRetryState {
  readonly enteredAt: number | null;
  readonly expiresAt: number | null;
  readonly attempts: AttemptRecord[];
  readonly retryCount: number;
  readonly lastRetryAt: number | null;
  readonly nextRetryAt: number | null;
}

const DLQ_RETRY_STATE = Symbol('bunqueue.dlqRetryState');

/** Read internal DLQ state without exposing it on the public Job wire shape. */
export function getDlqRetryState(job: Job): DlqRetryState | null {
  return (job as { [DLQ_RETRY_STATE]?: DlqRetryState })[DLQ_RETRY_STATE] ?? null;
}

/** Restore internal DLQ state from SQLite as a non-enumerable Job property. */
export function setDlqRetryState(job: Job, state: DlqRetryState | null): void {
  if (state === null) {
    delete (job as { [DLQ_RETRY_STATE]?: DlqRetryState })[DLQ_RETRY_STATE];
    return;
  }
  Object.defineProperty(job, DLQ_RETRY_STATE, {
    value: state,
    writable: true,
    configurable: true,
    enumerable: false,
  });
}

function createAttemptRecord(
  job: Job,
  reason: FailureReason,
  error: string | null,
  failedAt: number = Date.now()
): AttemptRecord {
  return {
    attempt: job.attempts,
    startedAt: job.startedAt ?? job.createdAt,
    failedAt,
    reason,
    error,
    duration: job.startedAt ? failedAt - job.startedAt : 0,
  };
}

/** Retain a non-terminal failed attempt so a later DLQ entry has full history. */
export function recordJobFailureAttempt(
  job: Job,
  reason: FailureReason,
  error: string | null,
  failedAt: number = Date.now()
): void {
  const state = getDlqRetryState(job) ?? {
    enteredAt: null,
    expiresAt: null,
    attempts: [],
    retryCount: 0,
    lastRetryAt: null,
    nextRetryAt: null,
  };
  state.attempts.push(createAttemptRecord(job, reason, error, failedAt));
  setDlqRetryState(job, state);
}

/** Carry a dispatched DLQ entry through its live auto-retry execution. */
export function retainDlqRetryState(job: Job, entry: DlqEntry): void {
  setDlqRetryState(job, {
    enteredAt: entry.enteredAt,
    expiresAt: entry.expiresAt,
    attempts: [...entry.attempts],
    retryCount: entry.retryCount,
    lastRetryAt: entry.lastRetryAt,
    nextRetryAt: entry.nextRetryAt,
  });
}

/** DLQ Entry with full metadata */
export interface DlqEntry {
  /** Original job */
  readonly job: Job;
  /** When first moved to DLQ */
  readonly enteredAt: number;
  /** Last failure reason */
  readonly reason: FailureReason;
  /** Last error message */
  readonly error: string | null;
  /** Full attempt history */
  readonly attempts: AttemptRecord[];
  /** Number of times retried from DLQ */
  retryCount: number;
  /** Last retry attempt from DLQ */
  lastRetryAt: number | null;
  /** Next scheduled auto-retry (null = no auto-retry) */
  nextRetryAt: number | null;
  /** Expires at (for auto-purge) */
  readonly expiresAt: number | null;
}

/** DLQ configuration per queue */
export interface DlqConfig {
  /** Enable auto-retry from DLQ */
  autoRetry: boolean;
  /** Auto-retry interval in ms (default: 1 hour) */
  autoRetryInterval: number;
  /** Max auto-retries before giving up (default: 3) */
  maxAutoRetries: number;
  /** Max age in ms before auto-purge (default: 7 days, null = never) */
  maxAge: number | null;
  /** Max entries per queue (default: 10000) */
  maxEntries: number;
}

/** Default DLQ configuration */
export const DEFAULT_DLQ_CONFIG: DlqConfig = {
  autoRetry: false,
  autoRetryInterval: 60 * 60 * 1000, // 1 hour
  maxAutoRetries: 3,
  maxAge: 7 * 24 * 60 * 60 * 1000, // 7 days
  maxEntries: 10000,
};

/** Create a DLQ entry from a failed job */
export function createDlqEntry(
  job: Job,
  reason: FailureReason,
  error: string | null,
  config: DlqConfig = DEFAULT_DLQ_CONFIG,
  now: number = Date.now()
): DlqEntry {
  const retained = getDlqRetryState(job);
  const alreadyEntered = retained?.enteredAt !== null && retained?.enteredAt !== undefined;
  const attemptRecord = createAttemptRecord(job, reason, error, now);
  setDlqRetryState(job, null);

  return {
    job,
    enteredAt: alreadyEntered ? retained.enteredAt! : now,
    reason,
    error,
    attempts: [...(retained?.attempts ?? []), attemptRecord],
    retryCount: retained?.retryCount ?? 0,
    lastRetryAt: retained?.lastRetryAt ?? null,
    nextRetryAt: alreadyEntered
      ? retained.nextRetryAt
      : config.autoRetry
        ? now + config.autoRetryInterval
        : null,
    expiresAt: alreadyEntered
      ? retained.expiresAt
      : config.maxAge !== null
        ? now + config.maxAge
        : null,
  };
}

/** Add attempt record to existing DLQ entry */
export function addAttemptRecord(
  entry: DlqEntry,
  reason: FailureReason,
  error: string | null
): void {
  const job = entry.job;
  const now = Date.now();
  entry.attempts.push({
    attempt: job.attempts,
    startedAt: job.startedAt ?? now,
    failedAt: now,
    reason,
    error,
    duration: job.startedAt ? now - job.startedAt : 0,
  });
}

/** Check if DLQ entry is expired */
export function isDlqEntryExpired(entry: DlqEntry, now: number = Date.now()): boolean {
  return entry.expiresAt !== null && now >= entry.expiresAt;
}

/** Check if DLQ entry can be auto-retried */
export function canAutoRetry(
  entry: DlqEntry,
  config: DlqConfig,
  now: number = Date.now()
): boolean {
  if (!config.autoRetry) return false;
  if (entry.retryCount >= config.maxAutoRetries) return false;
  if (entry.nextRetryAt === null) return false;
  return now >= entry.nextRetryAt;
}

/** Schedule next auto-retry */
export function scheduleNextRetry(entry: DlqEntry, config: DlqConfig): void {
  const now = Date.now();
  entry.retryCount++;
  entry.lastRetryAt = now;

  if (entry.retryCount < config.maxAutoRetries) {
    // Exponential backoff for DLQ retries
    const backoffMultiplier = Math.pow(2, entry.retryCount - 1);
    entry.nextRetryAt = now + config.autoRetryInterval * backoffMultiplier;
  } else {
    entry.nextRetryAt = null; // No more retries
  }
}

/** DLQ filter options */
export interface DlqFilter {
  /** Filter by failure reason */
  reason?: FailureReason;
  /** Filter by queue */
  queue?: string;
  /** Only entries older than this timestamp */
  olderThan?: number;
  /** Only entries newer than this timestamp */
  newerThan?: number;
  /** Only entries that can be retried */
  retriable?: boolean;
  /** Only entries that are expired */
  expired?: boolean;
  /** Limit results */
  limit?: number;
  /** Offset for pagination */
  offset?: number;
}

/** DLQ statistics */
export interface DlqStats {
  /** Total entries */
  total: number;
  /** Entries by reason */
  byReason: Record<FailureReason, number>;
  /** Entries by queue */
  byQueue: Record<string, number>;
  /** Entries awaiting auto-retry */
  pendingRetry: number;
  /** Expired entries (awaiting cleanup) */
  expired: number;
  /** Oldest entry timestamp */
  oldestEntry: number | null;
  /** Newest entry timestamp */
  newestEntry: number | null;
}
