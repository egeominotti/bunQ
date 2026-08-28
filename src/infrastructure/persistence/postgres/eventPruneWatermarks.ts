import type { TransactionSQL } from 'bun';
import { numeric } from './codec';
import type { PostgresContext } from './context';

export interface PostgresEventPruneWatermarkInput {
  readonly queue: string;
  readonly sourceEventId: number;
  readonly prunedThrough: number;
  readonly prunedCommitSeq?: number;
  readonly prunesCurrentTransaction?: boolean;
}

export interface PostgresEventPruneWatermark extends PostgresEventPruneWatermarkInput {
  readonly commitSeq: number;
  readonly prunedCommitSeq: number;
  /** Newest commit that pruned part of its own queue events, 0 when none did. */
  readonly selfPrunedCommitSeq: number;
}

interface WatermarkRow {
  queue: string;
  source_event_id: number | string | bigint;
  pruned_through: number | string | bigint;
  commit_seq: number | string | bigint;
  pruned_commit_seq: number | string | bigint;
  self_pruned_commit_seq: number | string | bigint;
}

export function maxPostgresEventId(rows: readonly { id: number | string | bigint }[]): number {
  return rows.reduce((latest, row) => Math.max(latest, numeric(row.id) ?? 0), 0);
}

export function summarizePostgresPrunedEvents(
  rows: readonly {
    id: number | string | bigint;
    commit_seq: number | string | bigint | null;
  }[]
): Pick<
  PostgresEventPruneWatermarkInput,
  'prunedThrough' | 'prunedCommitSeq' | 'prunesCurrentTransaction'
> {
  return {
    prunedThrough: maxPostgresEventId(rows),
    prunedCommitSeq: rows.reduce(
      (latest, row) => Math.max(latest, numeric(row.commit_seq) ?? 0),
      0
    ),
    prunesCurrentTransaction: rows.some((row) => row.commit_seq === null),
  };
}

function normalize(
  watermarks: readonly PostgresEventPruneWatermarkInput[]
): PostgresEventPruneWatermarkInput[] {
  const byQueue = new Map<string, PostgresEventPruneWatermarkInput>();
  for (const watermark of watermarks) {
    const current = byQueue.get(watermark.queue);
    byQueue.set(
      watermark.queue,
      current
        ? {
            queue: watermark.queue,
            sourceEventId: Math.max(current.sourceEventId, watermark.sourceEventId),
            prunedThrough: Math.max(current.prunedThrough, watermark.prunedThrough),
            prunedCommitSeq: Math.max(current.prunedCommitSeq ?? 0, watermark.prunedCommitSeq ?? 0),
            prunesCurrentTransaction:
              current.prunesCurrentTransaction === true ||
              watermark.prunesCurrentTransaction === true,
          }
        : watermark
    );
  }
  return [...byQueue.values()].sort((left, right) => left.queue.localeCompare(right.queue));
}

/** Record durable evidence that queue event history was pruned in this transaction. */
export async function recordPostgresEventPruneWatermarks(
  tx: TransactionSQL,
  ctx: PostgresContext,
  inputs: readonly PostgresEventPruneWatermarkInput[]
): Promise<void> {
  const watermarks = normalize(inputs).filter(
    ({ sourceEventId, prunedThrough }) => sourceEventId > 0 && prunedThrough > 0
  );
  if (watermarks.length === 0) return;
  await tx`
    WITH incoming AS (
      SELECT batch.queue, batch.source_event_id, batch.pruned_through,
             batch.pruned_commit_seq, batch.prunes_current_transaction
      FROM unnest(
        ${tx.array(
          watermarks.map(({ queue }) => queue),
          'TEXT'
        )},
        ${tx.array(
          watermarks.map(({ sourceEventId }) => sourceEventId),
          'BIGINT'
        )},
        ${tx.array(
          watermarks.map(({ prunedThrough }) => prunedThrough),
          'BIGINT'
        )},
        ${tx.array(
          watermarks.map(({ prunedCommitSeq }) => prunedCommitSeq ?? 0),
          'BIGINT'
        )},
        ${tx.array(
          watermarks.map(({ prunesCurrentTransaction }) => prunesCurrentTransaction ?? false),
          'BOOLEAN'
        )}
      ) AS batch(
        queue, source_event_id, pruned_through, pruned_commit_seq,
        prunes_current_transaction
      )
    ), inserted AS (
      INSERT INTO bunqueue_event_prune_watermarks
        (namespace, queue, source_event_id, pruned_through, pruned_commit_seq,
         self_pruned_commit_seq, prunes_current_transaction)
      SELECT ${ctx.config.namespace}, queue, source_event_id, pruned_through,
             GREATEST(
               pruned_commit_seq,
               COALESCE((
                 SELECT MAX(existing.pruned_commit_seq)
                 FROM bunqueue_event_prune_watermarks AS existing
                 WHERE existing.namespace = ${ctx.config.namespace}
                   AND existing.queue = incoming.queue
               ), 0)
             ),
             COALESCE((
               SELECT MAX(existing.self_pruned_commit_seq)
               FROM bunqueue_event_prune_watermarks AS existing
               WHERE existing.namespace = ${ctx.config.namespace}
                 AND existing.queue = incoming.queue
             ), 0),
             prunes_current_transaction OR COALESCE((
               SELECT bool_or(existing.prunes_current_transaction)
               FROM bunqueue_event_prune_watermarks AS existing
               WHERE existing.namespace = ${ctx.config.namespace}
                 AND existing.queue = incoming.queue
                 AND existing.commit_seq IS NULL
             ), FALSE)
      FROM incoming
      ON CONFLICT (namespace, queue, source_event_id) DO UPDATE SET
        pruned_through = GREATEST(
          bunqueue_event_prune_watermarks.pruned_through,
          excluded.pruned_through
        ),
        pruned_commit_seq = GREATEST(
          COALESCE(bunqueue_event_prune_watermarks.pruned_commit_seq, 0),
          COALESCE(excluded.pruned_commit_seq, 0)
        ),
        self_pruned_commit_seq = GREATEST(
          COALESCE(bunqueue_event_prune_watermarks.self_pruned_commit_seq, 0),
          COALESCE(excluded.self_pruned_commit_seq, 0)
        ),
        prunes_current_transaction =
          bunqueue_event_prune_watermarks.prunes_current_transaction
          OR excluded.prunes_current_transaction,
        transaction_id = pg_current_xact_id()::text::bigint,
        commit_seq = NULL
      RETURNING namespace, queue, source_event_id, pruned_through
    )
    DELETE FROM bunqueue_event_prune_watermarks AS previous
    USING inserted AS latest
    WHERE previous.namespace = latest.namespace
      AND previous.queue = latest.queue
      AND previous.source_event_id <> latest.source_event_id
      AND previous.pruned_through <= latest.pruned_through
  `;
}

export async function loadPostgresEventPruneWatermarks(
  ctx: PostgresContext
): Promise<PostgresEventPruneWatermark[]> {
  const rows = await ctx.sql<WatermarkRow[]>`
    SELECT DISTINCT ON (queue)
           queue, source_event_id, pruned_through, commit_seq,
           MAX(pruned_commit_seq) OVER (PARTITION BY queue) AS pruned_commit_seq,
           MAX(self_pruned_commit_seq) OVER (PARTITION BY queue) AS self_pruned_commit_seq
    FROM bunqueue_event_prune_watermarks
    WHERE namespace = ${ctx.config.namespace}
      AND commit_seq IS NOT NULL
    ORDER BY queue, commit_seq DESC, source_event_id DESC
  `;
  return rows.map((row) => ({
    queue: row.queue,
    sourceEventId: numeric(row.source_event_id) ?? 0,
    prunedThrough: numeric(row.pruned_through) ?? 0,
    commitSeq: numeric(row.commit_seq) ?? 0,
    prunedCommitSeq: numeric(row.pruned_commit_seq) ?? 0,
    selfPrunedCommitSeq: numeric(row.self_pruned_commit_seq) ?? 0,
  }));
}

export async function deletePostgresEventPruneWatermark(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queue: string
): Promise<void> {
  await tx`
    DELETE FROM bunqueue_event_prune_watermarks
    WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
  `;
}
