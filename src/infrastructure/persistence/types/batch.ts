import type { Job } from '../../../domain/types/job';

export interface BatchInsertResult {
  transient: Job[];
  conflicts: Job[];
  error?: Error;
}

export type WriteBufferErrorCallback = (
  error: Error,
  jobCount: number,
  retryInfo?: { retryCount: number; nextBackoffMs: number; maxRetries: number }
) => void;

export type CriticalErrorCallback = (jobs: Job[], lastError: Error, totalAttempts: number) => void;
