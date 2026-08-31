import type { TransactionSQL } from 'bun';
import { DEFAULT_DLQ_CONFIG } from '../../../domain/types/dlq';
import { DEFAULT_STALL_CONFIG } from '../../../domain/types/stall';
import type { GroupPullOptions } from '../../../domain/types/group';
import { claimPostgresCandidates } from './claimBatch';
import { decodePostgresValue } from './codec';
import { databaseNow, type PostgresContext } from './context';
import { expirePendingPostgresJobs } from './expiry';
import { repairResolvedPostgresDependencyConsumers } from './dependencyPromotion';
import type { ClaimedPostgresJob, PostgresQueueState } from './types';
import { lockPostgresBrokerSession } from './brokerSessions';
import { runPostgresTransactionWithRetry } from './transactionRetry';
import { hasClaimablePostgresGroup } from './claimSelection';
import { ensurePostgresGroupStates, refreshPostgresGroupRateWindows } from './groupClaims';

interface QueueStateRow {
  queue: string;
  paused: boolean;
  rate_limit: number | null;
  rate_duration_ms: number | string | bigint | null;
  rate_window_started_at: number | string | bigint | null;
  rate_expires_at: number | string | bigint | null;
  rate_count: number;
  concurrency_limit: number | null;
  stall_config: Uint8Array | null;
  dlq_config: Uint8Array | null;
  group_sequence: number | string | bigint;
}

interface ClaimQueueState extends PostgresQueueState {
  readonly groupSequence: number;
}

function toQueueState(row: QueueStateRow): ClaimQueueState {
  return {
    queue: row.queue,
    paused: row.paused,
    rateLimit: row.rate_limit,
    rateDurationMs: row.rate_duration_ms === null ? null : Number(row.rate_duration_ms),
    rateWindowStartedAt:
      row.rate_window_started_at === null ? null : Number(row.rate_window_started_at),
    rateExpiresAt: row.rate_expires_at === null ? null : Number(row.rate_expires_at),
    rateCount: row.rate_count,
    concurrencyLimit: row.concurrency_limit,
    stallConfig: decodePostgresValue(row.stall_config, DEFAULT_STALL_CONFIG, 'stallConfig'),
    dlqConfig: decodePostgresValue(row.dlq_config, DEFAULT_DLQ_CONFIG, 'dlqConfig'),
    groupSequence: Number(row.group_sequence),
  };
}

async function lockQueueState(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queue: string,
  exclusive: boolean
): Promise<ClaimQueueState> {
  const select = async () =>
    exclusive
      ? await tx<QueueStateRow[]>`
          SELECT queue, paused, rate_limit, rate_duration_ms, rate_window_started_at,
                 rate_expires_at, rate_count, concurrency_limit, stall_config, dlq_config,
                 group_sequence
          FROM bunqueue_queue_state
          WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
          FOR UPDATE
        `
      : await tx<QueueStateRow[]>`
          SELECT queue, paused, rate_limit, rate_duration_ms, rate_window_started_at,
                 rate_expires_at, rate_count, concurrency_limit, stall_config, dlq_config,
                 group_sequence
          FROM bunqueue_queue_state
          WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
          FOR SHARE
        `;
  let rows = await select();
  if (rows.length === 0) {
    await tx`
      INSERT INTO bunqueue_queue_state (namespace, queue)
      VALUES (${ctx.config.namespace}, ${queue})
      ON CONFLICT DO NOTHING
    `;
    rows = await select();
  }
  const [row] = rows;
  if (!row) throw new Error(`Missing PostgreSQL queue state for ${queue}`);
  return toQueueState(row);
}

async function refreshRateWindow(
  tx: TransactionSQL,
  ctx: PostgresContext,
  state: ClaimQueueState,
  now: number
): Promise<ClaimQueueState> {
  if (state.rateLimit === null) return state;
  if (state.rateExpiresAt !== null && state.rateExpiresAt <= now) {
    await tx`
      UPDATE bunqueue_queue_state
      SET rate_limit = NULL, rate_duration_ms = NULL, rate_window_started_at = NULL,
          rate_expires_at = NULL, rate_count = 0
      WHERE namespace = ${ctx.config.namespace} AND queue = ${state.queue}
    `;
    return {
      ...state,
      rateLimit: null,
      rateDurationMs: null,
      rateWindowStartedAt: null,
      rateExpiresAt: null,
      rateCount: 0,
    };
  }
  const duration = state.rateDurationMs ?? 1000;
  if (state.rateWindowStartedAt !== null && now < state.rateWindowStartedAt + duration) {
    return state;
  }
  await tx`
    UPDATE bunqueue_queue_state
    SET rate_window_started_at = ${now}, rate_count = 0
    WHERE namespace = ${ctx.config.namespace} AND queue = ${state.queue}
  `;
  return { ...state, rateWindowStartedAt: now, rateCount: 0 };
}

async function activeCount(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queue: string,
  now: number
): Promise<number> {
  const [row] = await tx<{ count: number | string | bigint }[]>`
    SELECT COUNT(*)::bigint AS count
    FROM bunqueue_jobs
    WHERE namespace = ${ctx.config.namespace}
      AND queue = ${queue}
      AND state = 'active'
      AND lease_until > ${now}
  `;
  return Number(row.count);
}

export async function claimPostgresJobs(
  ctx: PostgresContext,
  input: {
    queue: string;
    count: number;
    owner: string;
    leaseDurationMs: number;
    groupOptions?: GroupPullOptions;
  }
): Promise<ClaimedPostgresJob[]> {
  const { queue, count, owner, leaseDurationMs, groupOptions } = input;
  if (count <= 0) return [];
  await expirePendingPostgresJobs(ctx, queue);

  const claimWithLock = async (
    exclusive: boolean
  ): Promise<{ claimed: ClaimedPostgresJob[]; retryExclusive: boolean }> =>
    await runPostgresTransactionWithRetry(ctx, async (tx) => {
      await lockPostgresBrokerSession(tx, ctx);
      const now = await databaseNow(tx);
      let state = await lockQueueState(tx, ctx, queue, exclusive);
      if (!exclusive && (state.rateLimit !== null || state.concurrencyLimit !== null)) {
        return { claimed: [], retryExclusive: true };
      }
      if (exclusive) state = await refreshRateWindow(tx, ctx, state, now);
      if (state.paused) return { claimed: [], retryExclusive: false };
      const active = state.concurrencyLimit === null ? 0 : await activeCount(tx, ctx, queue, now);
      const concurrencyCapacity =
        state.concurrencyLimit === null ? count : Math.max(0, state.concurrencyLimit - active);
      const rateCapacity =
        state.rateLimit === null ? count : Math.max(0, state.rateLimit - state.rateCount);
      const capacity = Math.min(count, concurrencyCapacity, rateCapacity);
      if (capacity <= 0) return { claimed: [], retryExclusive: false };

      await repairResolvedPostgresDependencyConsumers(tx, ctx, queue, now);
      const hasGroups = await hasClaimablePostgresGroup(tx, ctx, queue, now);
      if (!exclusive && hasGroups) return { claimed: [], retryExclusive: true };
      let groupSequence = state.groupSequence;
      if (hasGroups) {
        groupSequence = await ensurePostgresGroupStates(tx, ctx, queue, groupSequence);
        await refreshPostgresGroupRateWindows(tx, ctx, queue, groupOptions, now);
      }
      const claimed = await claimPostgresCandidates(tx, ctx, {
        queue,
        capacity,
        owner,
        requestedLeaseMs: leaseDurationMs,
        now,
        groupOptions,
        groupSequence,
      });
      if (state.rateLimit !== null && claimed.length > 0) {
        await tx`
          UPDATE bunqueue_queue_state
          SET rate_count = rate_count + ${claimed.length}
          WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
        `;
      }
      return { claimed, retryExclusive: false };
    });

  const sharedAttempt = await claimWithLock(false);
  if (!sharedAttempt.retryExclusive) return sharedAttempt.claimed;
  return (await claimWithLock(true)).claimed;
}
