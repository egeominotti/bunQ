import {
  assertGroupId,
  assertPositiveSafeInteger,
  type GroupRateLimitOverride,
} from '../../../domain/types/group';
import type { TransactionSQL } from 'bun';
import { postgresAdvisoryLockName } from './advisoryLocks';
import { databaseNow, type PostgresContext } from './context';

interface GroupStateRow {
  rate_limit: number | string | bigint | null;
  rate_duration_ms: number | string | bigint | null;
  rate_window_started_at: number | string | bigint | null;
  rate_count: number | string | bigint;
  rate_effective_duration_ms: number | string | bigint | null;
  concurrency_limit: number | string | bigint | null;
  manual_rate_limit_until: number | string | bigint | null;
}

export async function lockPostgresGroupCapacity(
  tx: TransactionSQL,
  ctx: PostgresContext,
  groups: ReadonlyArray<{ queue: string; groupId: string }>
): Promise<void> {
  const lockNames = [
    ...new Set(
      groups.map(({ queue, groupId }) =>
        postgresAdvisoryLockName('group-capacity', ctx.config.namespace, queue, groupId)
      )
    ),
  ];
  if (lockNames.length === 0) return;
  await tx`
    WITH requested AS MATERIALIZED (
      SELECT DISTINCT hashtextextended(lock_name, 0) AS lock_key
      FROM unnest(${tx.array(lockNames, 'TEXT')}) AS locks(lock_name)
      ORDER BY lock_key
    ), acquired AS MATERIALIZED (
      SELECT lock_key, pg_advisory_xact_lock(lock_key)
      FROM requested ORDER BY lock_key
    )
    SELECT COUNT(*) FROM acquired
  `;
}

export async function getPostgresGroupJobsCount(
  ctx: PostgresContext,
  queue: string,
  groupId?: string
): Promise<number> {
  if (groupId !== undefined) assertGroupId(groupId);
  const [row] =
    groupId === undefined
      ? await ctx.sql<{ count: number | string | bigint }[]>`
        SELECT COUNT(*)::bigint AS count FROM bunqueue_jobs
        WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
          AND group_id IS NOT NULL AND state IN ('waiting', 'prioritized', 'delayed')
      `
      : await ctx.sql<{ count: number | string | bigint }[]>`
        SELECT COUNT(*)::bigint AS count FROM bunqueue_jobs
        WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
          AND group_id = ${groupId} AND state IN ('waiting', 'prioritized', 'delayed')
      `;
  return Number(row.count);
}

export async function getPostgresGroupActiveCount(
  ctx: PostgresContext,
  queue: string,
  groupId: string
): Promise<number> {
  assertGroupId(groupId);
  const now = await databaseNow(ctx.sql);
  const [row] = await ctx.sql<{ count: number | string | bigint }[]>`
    SELECT COUNT(*)::bigint AS count FROM bunqueue_jobs
    WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
      AND group_id = ${groupId} AND state = 'active' AND lease_until > ${now}
  `;
  return Number(row.count);
}

export async function setPostgresGroupRateLimit(
  ctx: PostgresContext,
  queue: string,
  groupId: string,
  max: number,
  duration: number
): Promise<void> {
  assertGroupId(groupId);
  assertPositiveSafeInteger(max, 'max');
  assertPositiveSafeInteger(duration, 'duration');
  await ctx.sql`
    INSERT INTO bunqueue_group_state (
      namespace, queue, group_id, rate_limit, rate_duration_ms
    ) VALUES (${ctx.config.namespace}, ${queue}, ${groupId}, ${max}, ${duration})
    ON CONFLICT (namespace, queue, group_id) DO UPDATE
    SET rate_limit = EXCLUDED.rate_limit,
        rate_duration_ms = EXCLUDED.rate_duration_ms,
        rate_window_started_at = NULL,
        rate_count = 0,
        rate_effective_max = NULL,
        rate_effective_duration_ms = NULL
  `;
}

export async function getPostgresGroupRateLimit(
  ctx: PostgresContext,
  queue: string,
  groupId: string
): Promise<GroupRateLimitOverride | null> {
  assertGroupId(groupId);
  const rows = await ctx.sql<GroupStateRow[]>`
    SELECT rate_limit, rate_duration_ms, rate_window_started_at, rate_count,
           rate_effective_duration_ms, concurrency_limit, manual_rate_limit_until
    FROM bunqueue_group_state
    WHERE namespace = ${ctx.config.namespace} AND queue = ${queue} AND group_id = ${groupId}
  `;
  const row = rows[0];
  if (!row || row.rate_limit === null || row.rate_duration_ms === null) return null;
  return { max: Number(row.rate_limit), duration: Number(row.rate_duration_ms) };
}

export async function removePostgresGroupRateLimit(
  ctx: PostgresContext,
  queue: string,
  groupId: string
): Promise<number> {
  assertGroupId(groupId);
  const rows = await ctx.sql<{ group_id: string }[]>`
    UPDATE bunqueue_group_state
    SET rate_limit = NULL, rate_duration_ms = NULL, rate_window_started_at = NULL,
        rate_count = 0, rate_effective_max = NULL, rate_effective_duration_ms = NULL
    WHERE namespace = ${ctx.config.namespace} AND queue = ${queue} AND group_id = ${groupId}
      AND rate_limit IS NOT NULL
    RETURNING group_id
  `;
  return rows.length;
}

export async function getPostgresGroupRateLimitTtl(
  ctx: PostgresContext,
  queue: string,
  groupId: string,
  maxJobs?: number
): Promise<number> {
  assertGroupId(groupId);
  const rows = await ctx.sql<GroupStateRow[]>`
    SELECT rate_limit, rate_duration_ms, rate_window_started_at, rate_count,
           rate_effective_duration_ms, concurrency_limit, manual_rate_limit_until
    FROM bunqueue_group_state
    WHERE namespace = ${ctx.config.namespace} AND queue = ${queue} AND group_id = ${groupId}
  `;
  const row = rows[0];
  if (!row) return -2;
  const now = await databaseNow(ctx.sql);
  const manualUntil =
    row.manual_rate_limit_until === null ? null : Number(row.manual_rate_limit_until);
  if (manualUntil !== null && manualUntil > now) return manualUntil - now;
  if (row.rate_window_started_at === null || row.rate_effective_duration_ms === null) return -2;
  if (maxJobs !== undefined && Number(row.rate_count) < maxJobs) return 0;
  const remaining =
    Number(row.rate_window_started_at) + Number(row.rate_effective_duration_ms) - now;
  return remaining > 0 ? remaining : -2;
}

export async function setPostgresGroupConcurrency(
  ctx: PostgresContext,
  queue: string,
  groupId: string,
  concurrency: number
): Promise<void> {
  assertGroupId(groupId);
  assertPositiveSafeInteger(concurrency, 'concurrency');
  await ctx.sql`
    INSERT INTO bunqueue_group_state (namespace, queue, group_id, concurrency_limit)
    VALUES (${ctx.config.namespace}, ${queue}, ${groupId}, ${concurrency})
    ON CONFLICT (namespace, queue, group_id) DO UPDATE
    SET concurrency_limit = EXCLUDED.concurrency_limit
  `;
}

export async function getPostgresGroupConcurrency(
  ctx: PostgresContext,
  queue: string,
  groupId: string
): Promise<number | null> {
  assertGroupId(groupId);
  const rows = await ctx.sql<{ concurrency_limit: number | null }[]>`
    SELECT concurrency_limit FROM bunqueue_group_state
    WHERE namespace = ${ctx.config.namespace} AND queue = ${queue} AND group_id = ${groupId}
  `;
  const value = rows[0]?.concurrency_limit;
  return value === undefined || value === null ? null : Number(value);
}

export async function removePostgresGroupConcurrency(
  ctx: PostgresContext,
  queue: string,
  groupId: string
): Promise<number> {
  assertGroupId(groupId);
  const rows = await ctx.sql<{ group_id: string }[]>`
    UPDATE bunqueue_group_state SET concurrency_limit = NULL
    WHERE namespace = ${ctx.config.namespace} AND queue = ${queue} AND group_id = ${groupId}
      AND concurrency_limit IS NOT NULL
    RETURNING group_id
  `;
  return rows.length;
}

export async function setPostgresGroupPaused(
  ctx: PostgresContext,
  queue: string,
  groupId: string,
  paused: boolean
): Promise<boolean> {
  assertGroupId(groupId);
  const rows = await ctx.sql<{ changed: boolean }[]>`
    INSERT INTO bunqueue_group_state (namespace, queue, group_id, paused)
    VALUES (${ctx.config.namespace}, ${queue}, ${groupId}, ${paused})
    ON CONFLICT (namespace, queue, group_id) DO UPDATE
    SET paused = EXCLUDED.paused
    WHERE bunqueue_group_state.paused IS DISTINCT FROM EXCLUDED.paused
    RETURNING TRUE AS changed
  `;
  return rows.length > 0;
}

export async function getPostgresGroupPaused(
  ctx: PostgresContext,
  queue: string,
  groupId: string
): Promise<boolean> {
  assertGroupId(groupId);
  const rows = await ctx.sql<{ paused: boolean }[]>`
    SELECT paused FROM bunqueue_group_state
    WHERE namespace = ${ctx.config.namespace} AND queue = ${queue} AND group_id = ${groupId}
  `;
  return rows[0]?.paused ?? false;
}

export async function rateLimitPostgresGroup(
  ctx: PostgresContext,
  queue: string,
  groupId: string,
  duration: number
): Promise<void> {
  assertGroupId(groupId);
  assertPositiveSafeInteger(duration, 'duration');
  const now = await databaseNow(ctx.sql);
  await ctx.sql`
    INSERT INTO bunqueue_group_state (
      namespace, queue, group_id, manual_rate_limit_until
    ) VALUES (${ctx.config.namespace}, ${queue}, ${groupId}, ${now + duration})
    ON CONFLICT (namespace, queue, group_id) DO UPDATE
    SET manual_rate_limit_until = EXCLUDED.manual_rate_limit_until
  `;
}
