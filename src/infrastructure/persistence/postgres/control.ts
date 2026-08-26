import type { TransactionSQL } from 'bun';
import { DEFAULT_DLQ_CONFIG, type DlqConfig } from '../../../domain/types/dlq';
import { jobId } from '../../../domain/types/job';
import { DEFAULT_STALL_CONFIG, type StallConfig } from '../../../domain/types/stall';
import { decodePostgresValue, encodePostgresValue } from './codec';
import { enforcePostgresDlqLimit } from './dlqLifecycle';
import { databaseNow, recordPostgresEvent, type PostgresContext } from './context';
import { normalizePostgresRateLimit } from './rateLimit';
import type { PostgresQueueState } from './types';

export interface PostgresQueueStateRow {
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
}

function optionalNumber(value: number | string | bigint | null | undefined): number | null {
  return value === null || value === undefined ? null : Number(value);
}

export function decodePostgresQueueState(
  queue: string,
  row?: PostgresQueueStateRow
): PostgresQueueState {
  return {
    queue,
    paused: row?.paused ?? false,
    rateLimit: row?.rate_limit ?? null,
    rateDurationMs: optionalNumber(row?.rate_duration_ms),
    rateWindowStartedAt: optionalNumber(row?.rate_window_started_at),
    rateExpiresAt: optionalNumber(row?.rate_expires_at),
    rateCount: row?.rate_count ?? 0,
    concurrencyLimit: row?.concurrency_limit ?? null,
    stallConfig: decodePostgresValue(
      row?.stall_config ?? null,
      DEFAULT_STALL_CONFIG,
      'stallConfig'
    ),
    dlqConfig: decodePostgresValue(row?.dlq_config ?? null, DEFAULT_DLQ_CONFIG, 'dlqConfig'),
  };
}

async function recordQueueInvalidation(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queue: string,
  type: string,
  occurredAt?: number
): Promise<void> {
  await recordPostgresEvent(tx, ctx, {
    queue,
    type,
    jobId: jobId(`__queue__:${queue}`),
    occurredAt: occurredAt ?? (await databaseNow(tx)),
  });
}

export async function getPostgresQueueState(
  ctx: PostgresContext,
  queue: string
): Promise<PostgresQueueState> {
  const rows = await ctx.sql<PostgresQueueStateRow[]>`
    SELECT queue, paused, rate_limit, rate_duration_ms, rate_window_started_at, rate_expires_at,
           rate_count, concurrency_limit, stall_config, dlq_config
    FROM bunqueue_queue_state
    WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
  `;
  return decodePostgresQueueState(queue, rows[0]);
}

export async function setPostgresPaused(
  ctx: PostgresContext,
  queue: string,
  paused: boolean
): Promise<void> {
  await ctx.sql.begin(async (tx) => {
    await tx`
      INSERT INTO bunqueue_queue_state (namespace, queue, paused)
      VALUES (${ctx.config.namespace}, ${queue}, ${paused})
      ON CONFLICT (namespace, queue) DO UPDATE SET paused = excluded.paused
    `;
    await recordQueueInvalidation(tx, ctx, queue, paused ? 'queue-paused' : 'queue-resumed');
  });
}

export async function setPostgresRateLimit(
  ctx: PostgresContext,
  queue: string,
  limit: number | null,
  durationMs: number | null,
  ttlMs?: number | null
): Promise<void> {
  const normalized = normalizePostgresRateLimit(durationMs, ttlMs);
  await ctx.sql.begin(async (tx) => {
    const now = await databaseNow(tx);
    const rateExpiresAt =
      limit !== null && normalized.ttlMs !== null ? now + normalized.ttlMs : null;
    await tx`
      INSERT INTO bunqueue_queue_state
        (namespace, queue, rate_limit, rate_duration_ms, rate_window_started_at,
         rate_expires_at, rate_count)
      VALUES
        (${ctx.config.namespace}, ${queue}, ${limit}, ${normalized.durationMs}, ${now},
         ${rateExpiresAt}, 0)
      ON CONFLICT (namespace, queue) DO UPDATE SET
        rate_limit = excluded.rate_limit,
        rate_duration_ms = excluded.rate_duration_ms,
        rate_window_started_at = excluded.rate_window_started_at,
        rate_expires_at = excluded.rate_expires_at,
        rate_count = 0
    `;
    await recordQueueInvalidation(tx, ctx, queue, 'queue-rate-limit-changed', now);
  });
}

export async function setPostgresConcurrency(
  ctx: PostgresContext,
  queue: string,
  limit: number | null
): Promise<void> {
  await ctx.sql.begin(async (tx) => {
    await tx`
      INSERT INTO bunqueue_queue_state (namespace, queue, concurrency_limit)
      VALUES (${ctx.config.namespace}, ${queue}, ${limit})
      ON CONFLICT (namespace, queue) DO UPDATE SET concurrency_limit = excluded.concurrency_limit
    `;
    await recordQueueInvalidation(tx, ctx, queue, 'queue-concurrency-changed');
  });
}

export async function setPostgresStallConfig(
  ctx: PostgresContext,
  queue: string,
  config: StallConfig
): Promise<void> {
  await ctx.sql.begin(async (tx) => {
    await tx`
      INSERT INTO bunqueue_queue_state (namespace, queue, stall_config)
      VALUES (${ctx.config.namespace}, ${queue}, ${encodePostgresValue(config)})
      ON CONFLICT (namespace, queue) DO UPDATE SET stall_config = excluded.stall_config
    `;
    await recordQueueInvalidation(tx, ctx, queue, 'queue-stall-config-changed');
  });
}

export async function setPostgresDlqConfig(
  ctx: PostgresContext,
  queue: string,
  config: DlqConfig
): Promise<void> {
  await ctx.sql.begin(async (tx) => {
    const now = await databaseNow(tx);
    await tx`
      INSERT INTO bunqueue_queue_state (namespace, queue, dlq_config)
      VALUES (${ctx.config.namespace}, ${queue}, ${encodePostgresValue(config)})
      ON CONFLICT (namespace, queue) DO UPDATE SET dlq_config = excluded.dlq_config
    `;
    await recordQueueInvalidation(tx, ctx, queue, 'queue-dlq-config-changed', now);
  });
  await enforcePostgresDlqLimit(ctx, queue, config.maxEntries);
}
