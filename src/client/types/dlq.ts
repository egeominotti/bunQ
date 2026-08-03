import type { Job } from './job';

export interface DlqConfig {
  autoRetry?: boolean;
  autoRetryInterval?: number;
  maxAutoRetries?: number;
  maxAge?: number | null;
  maxEntries?: number;
}

export type FailureReason =
  | 'explicit_fail'
  | 'max_attempts_exceeded'
  | 'timeout'
  | 'stalled'
  | 'ttl_expired'
  | 'worker_lost'
  | 'unknown';

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

export interface DlqStats {
  total: number;
  byReason: Record<FailureReason, number>;
  byQueue: Record<string, number>;
  pendingRetry: number;
  expired: number;
  oldestEntry: number | null;
  newestEntry: number | null;
}

export interface DlqFilter {
  reason?: FailureReason;
  olderThan?: number;
  newerThan?: number;
  retriable?: boolean;
  expired?: boolean;
  limit?: number;
  offset?: number;
}
