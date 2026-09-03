import type { TransactionSQL } from 'bun';
import { postgresAdvisoryLockName } from './advisoryLocks';
import { numeric } from './codec';
import type { PostgresContext } from './context';

export interface PostgresPrunedEventRow {
  readonly id: number | string | bigint;
  readonly commit_seq: number | string | bigint | null;
}

async function synchronizedRetainedEventCount(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queue: string
): Promise<number> {
  await tx`
    INSERT INTO bunqueue_event_retention_state
      (namespace, queue, retained_count, last_event_id)
    VALUES (${ctx.config.namespace}, ${queue}, 0, 0)
    ON CONFLICT (namespace, queue) DO NOTHING
  `;
  const [row] = await tx<Array<{ retained_count: number | string | bigint }>>`
    WITH candidates AS MATERIALIZED (
      SELECT delta.transaction_id
      FROM bunqueue_event_retention_deltas AS delta
      WHERE delta.namespace = ${ctx.config.namespace} AND delta.queue = ${queue}
      ORDER BY delta.transaction_id
      FOR UPDATE OF delta SKIP LOCKED
    ), consumed AS (
      DELETE FROM bunqueue_event_retention_deltas AS delta
      USING candidates
      WHERE delta.namespace = ${ctx.config.namespace} AND delta.queue = ${queue}
        AND delta.transaction_id = candidates.transaction_id
      RETURNING delta.delta_count, delta.last_event_id
    ), aggregate AS (
      SELECT
        COALESCE(SUM(delta_count), 0)::BIGINT AS delta_count,
        COALESCE(MAX(last_event_id), 0)::BIGINT AS last_event_id
      FROM consumed
    )
    UPDATE bunqueue_event_retention_state AS state
    SET retained_count = GREATEST(0, state.retained_count + aggregate.delta_count),
        last_event_id = GREATEST(state.last_event_id, aggregate.last_event_id)
    FROM aggregate
    WHERE state.namespace = ${ctx.config.namespace} AND state.queue = ${queue}
    RETURNING state.retained_count
  `;
  return numeric(row?.retained_count ?? null) ?? 0;
}

/** Serialize one autonomous queue-retention sweep without joining job lock domains. */
export async function lockPostgresEventRetention(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queue: string
): Promise<void> {
  const lockName = postgresAdvisoryLockName('event-retention', ctx.config.namespace, queue);
  await tx`
    SELECT pg_advisory_xact_lock(hashtextextended(${lockName}, 0))
  `;
}

/** Never wait for a late inline retention lock after job locks are already held. */
export async function tryLockPostgresEventRetention(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queue: string
): Promise<boolean> {
  const lockName = postgresAdvisoryLockName('event-retention', ctx.config.namespace, queue);
  const [row] = await tx<{ acquired: boolean }[]>`
    SELECT pg_try_advisory_xact_lock(hashtextextended(${lockName}, 0)) AS acquired
  `;
  return row.acquired;
}

/**
 * Retain the newest queue events while locking overlapping prune candidates in
 * one deterministic order. The materialized lock step prevents concurrent
 * brokers from deleting the same physical tuples in conflicting scan orders.
 */
export async function prunePostgresQueueEvents(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queue: string,
  maxLength: number
): Promise<PostgresPrunedEventRow[]> {
  const retainedCount = await synchronizedRetainedEventCount(tx, ctx, queue);
  const excess = Math.max(0, retainedCount - maxLength);
  if (excess === 0) return [];
  const removed = await tx<PostgresPrunedEventRow[]>`
    WITH candidates AS MATERIALIZED (
      SELECT event.id
      FROM bunqueue_events AS event
      WHERE event.namespace = ${ctx.config.namespace} AND event.queue = ${queue}
      ORDER BY event.id
      LIMIT ${excess}
      FOR UPDATE OF event
    ), removed AS (
      DELETE FROM bunqueue_events AS event
      USING candidates
      WHERE event.id = candidates.id
      RETURNING event.id, event.namespace, event.transaction_id
    )
    SELECT removed.id, journal.commit_seq
    FROM removed
    LEFT JOIN bunqueue_event_commits AS journal
      ON journal.namespace = removed.namespace
     AND journal.transaction_id = removed.transaction_id
    ORDER BY removed.id
  `;
  await synchronizedRetainedEventCount(tx, ctx, queue);
  return removed;
}

/** Remove a queue's old outbox history in the same tuple order used by retention. */
export async function deletePostgresQueueEventHistory(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queue: string
): Promise<number> {
  const removed = await tx<{ id: number | string | bigint }[]>`
    WITH candidates AS MATERIALIZED (
      SELECT event.id
      FROM bunqueue_events AS event
      WHERE event.namespace = ${ctx.config.namespace}
        AND (
          event.queue = ${queue}
          OR (
            event.event_type = 'queue-batch-overflow'
            AND event.job_id = ${`__queue__:${queue}`}
          )
        )
      ORDER BY event.id
      FOR UPDATE OF event
    )
    DELETE FROM bunqueue_events AS event
    USING candidates
    WHERE event.id = candidates.id
    RETURNING event.id
  `;
  await tx`
    DELETE FROM bunqueue_event_retention_state
    WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
  `;
  await tx`
    DELETE FROM bunqueue_event_retention_deltas
    WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
  `;
  return removed.length;
}
