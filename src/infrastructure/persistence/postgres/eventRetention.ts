import type { TransactionSQL } from 'bun';
import type { PostgresContext } from './context';

export interface PostgresPrunedEventRow {
  readonly id: number | string | bigint;
  readonly commit_seq: number | string | bigint | null;
}

/** Serialize one autonomous queue-retention sweep without joining job lock domains. */
export async function lockPostgresEventRetention(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queue: string
): Promise<void> {
  await tx`
    SELECT pg_advisory_xact_lock(
      hashtextextended(
        ${`bunqueue:event-retention:${ctx.config.namespace}:${queue}`},
        0
      )
    )
  `;
}

/** Never wait for a late inline retention lock after job locks are already held. */
export async function tryLockPostgresEventRetention(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queue: string
): Promise<boolean> {
  const [row] = await tx<{ acquired: boolean }[]>`
    SELECT pg_try_advisory_xact_lock(
      hashtextextended(
        ${`bunqueue:event-retention:${ctx.config.namespace}:${queue}`},
        0
      )
    ) AS acquired
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
  return await tx<PostgresPrunedEventRow[]>`
    WITH cutoff AS MATERIALIZED (
      SELECT id
      FROM bunqueue_events
      WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
      ORDER BY id DESC
      OFFSET ${maxLength}
      LIMIT 1
    ), candidates AS MATERIALIZED (
      SELECT event.id
      FROM bunqueue_events AS event
      CROSS JOIN cutoff
      WHERE event.namespace = ${ctx.config.namespace} AND event.queue = ${queue}
        AND event.id <= cutoff.id
      ORDER BY event.id
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
  return removed.length;
}
