import { decodePostgresEvent, numeric } from './codec';
import type { PostgresContext } from './context';
import type { PostgresStoreEvent } from './types';

export interface PostgresEventJournalCursor {
  readonly commitSeq: number;
  readonly eventId: number;
}

export interface PostgresJournalEvent {
  readonly commitSeq: number;
  readonly event: PostgresStoreEvent;
}

interface JournalEventRow {
  readonly id: number | string | bigint;
  readonly queue: string;
  readonly event_type: string;
  readonly job_id: string;
  readonly occurred_at: number | string | bigint;
  readonly payload: Uint8Array | null;
  readonly commit_seq: number | string | bigint;
}

export async function latestPostgresEventJournalBaseline(
  ctx: PostgresContext
): Promise<{ commitSeq: number; latestEventId: number }> {
  const [row] = await ctx.sql<
    Array<{
      commit_seq: number | string | bigint | null;
      latest_event_id: number | string | bigint | null;
    }>
  >`
    SELECT
      (
        SELECT MAX(commit_seq) FROM bunqueue_event_commits
        WHERE namespace = ${ctx.config.namespace} AND commit_seq IS NOT NULL
      ) AS commit_seq,
      (
        SELECT MAX(id) FROM bunqueue_events
        WHERE namespace = ${ctx.config.namespace}
      ) AS latest_event_id
  `;
  return {
    commitSeq: numeric(row.commit_seq) ?? 0,
    latestEventId: numeric(row.latest_event_id) ?? 0,
  };
}

/** Load events in commit order, independent of physical BIGSERIAL allocation. */
export async function loadPostgresJournalEventsAfter(
  ctx: PostgresContext,
  cursor: PostgresEventJournalCursor,
  limit = 1000
): Promise<PostgresJournalEvent[]> {
  const rows = await ctx.sql<JournalEventRow[]>`
    SELECT event.id, event.queue, event.event_type, event.job_id,
           event.occurred_at, event.payload, journal.commit_seq
    FROM bunqueue_event_commits AS journal
    JOIN bunqueue_events AS event
      ON event.namespace = journal.namespace
     AND event.transaction_id = journal.transaction_id
    WHERE journal.namespace = ${ctx.config.namespace}
      AND journal.commit_seq IS NOT NULL
      AND (journal.commit_seq, event.id) > (${cursor.commitSeq}, ${cursor.eventId})
    ORDER BY journal.commit_seq, event.id
    LIMIT ${Math.max(1, limit)}
  `;
  return rows.map((row) => ({
    commitSeq: numeric(row.commit_seq) ?? 0,
    event: decodePostgresEvent(row),
  }));
}

/** Remove commit envelopes after every referenced event and watermark is gone. */
export async function prunePostgresEventCommits(
  ctx: PostgresContext,
  limit = 10_000
): Promise<number> {
  const deleted = await ctx.sql<{ transaction_id: number | string | bigint }[]>`
    WITH candidates AS MATERIALIZED (
      SELECT journal.namespace, journal.transaction_id
      FROM bunqueue_event_commits AS journal
      WHERE journal.namespace = ${ctx.config.namespace}
        AND journal.commit_seq IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM bunqueue_events AS event
          WHERE event.namespace = journal.namespace
            AND event.transaction_id = journal.transaction_id
        )
        AND NOT EXISTS (
          SELECT 1 FROM bunqueue_event_prune_watermarks AS watermark
          WHERE watermark.namespace = journal.namespace
            AND watermark.transaction_id = journal.transaction_id
        )
      ORDER BY journal.commit_seq
      LIMIT ${Math.max(1, limit)}
      FOR UPDATE SKIP LOCKED
    )
    DELETE FROM bunqueue_event_commits AS journal
    USING candidates
    WHERE journal.namespace = candidates.namespace
      AND journal.transaction_id = candidates.transaction_id
    RETURNING journal.transaction_id
  `;
  return deleted.length;
}
