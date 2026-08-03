import type { DlqEntry as InternalDlqEntry } from '../domain/types/dlq';
import type { DlqEntry, Job } from './types';
import { resolvePublicJobPayload } from './jobHelpers';
import type { CreatePublicJobOptions, PublicJobMethodContext } from './jobConversionTypes';

/** Convert authoritative DLQ metadata while retaining live public Job methods. */
export function convertDlqEntry<T>(
  entry: InternalDlqEntry,
  methods: PublicJobMethodContext,
  buildJob: (options: CreatePublicJobOptions) => Job<T>
): DlqEntry<T> {
  const { name } = resolvePublicJobPayload(entry.job);
  return {
    job: buildJob({
      job: entry.job,
      name,
      failedReason: entry.error ?? undefined,
      ...methods,
    }),
    enteredAt: entry.enteredAt,
    reason: entry.reason,
    error: entry.error,
    attempts: entry.attempts.map((attempt) => ({
      attempt: attempt.attempt,
      startedAt: attempt.startedAt,
      failedAt: attempt.failedAt,
      reason: attempt.reason,
      error: attempt.error,
      duration: attempt.duration,
    })),
    retryCount: entry.retryCount,
    lastRetryAt: entry.lastRetryAt,
    nextRetryAt: entry.nextRetryAt,
    expiresAt: entry.expiresAt,
  };
}
