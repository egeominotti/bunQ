import type { TransactionSQL } from 'bun';
import { storageLog } from '../../../shared/logger';
import type { PostgresContext } from './context';

const MAX_TRANSACTION_ATTEMPTS = 2;
const RETRYABLE_SQL_STATES = new Set(['40001', '40P01', '55P03']);

function sqlState(error: unknown): string | null {
  if (typeof error !== 'object' || error === null) return null;
  const record = error as Record<string, unknown>;
  for (const field of ['errno', 'sqlState', 'code']) {
    const value = record[field];
    if (typeof value === 'string' && /^[0-9A-Z]{5}$/.test(value)) return value;
  }
  return null;
}

/** Retry only PostgreSQL failures that guarantee the current transaction was aborted. */
export function isRetryablePostgresTransactionError(error: unknown): boolean {
  const state = sqlState(error);
  return state !== null && RETRYABLE_SQL_STATES.has(state);
}

function retryDelayMs(failedAttempt: number): number {
  const base = 10 * 2 ** (failedAttempt - 1);
  return base + Math.floor(Math.random() * base);
}

/** Replay the complete callback after rollback-certain contention, never after ambiguous I/O. */
export async function runPostgresTransactionWithRetry<T>(
  ctx: PostgresContext,
  operation: (tx: TransactionSQL) => Promise<T>
): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    try {
      return await ctx.sql.begin(operation);
    } catch (error) {
      if (attempt >= MAX_TRANSACTION_ATTEMPTS || !isRetryablePostgresTransactionError(error)) {
        throw error;
      }
      storageLog.warn('Retrying PostgreSQL transaction after rollback', {
        failedAttempt: attempt,
        maxAttempts: MAX_TRANSACTION_ATTEMPTS,
        sqlState: sqlState(error),
      });
      await Bun.sleep(retryDelayMs(attempt));
    }
  }
}
