import { FailureReason } from '../../domain/types/dlq';
import type { DlqStats } from '../types';

const EMPTY_REASONS = {
  [FailureReason.ExplicitFail]: 0,
  [FailureReason.MaxAttemptsExceeded]: 0,
  [FailureReason.Timeout]: 0,
  [FailureReason.Stalled]: 0,
  [FailureReason.TtlExpired]: 0,
  [FailureReason.WorkerLost]: 0,
  [FailureReason.Unknown]: 0,
};

/** Normalize one authoritative DLQ stats shape for both transports. */
export function toPublicDlqStats(stats: Partial<DlqStats> | undefined): DlqStats {
  return {
    total: stats?.total ?? 0,
    byReason: { ...EMPTY_REASONS, ...stats?.byReason },
    byQueue: { ...stats?.byQueue },
    pendingRetry: stats?.pendingRetry ?? 0,
    expired: stats?.expired ?? 0,
    oldestEntry: stats?.oldestEntry ?? null,
    newestEntry: stats?.newestEntry ?? null,
  };
}
