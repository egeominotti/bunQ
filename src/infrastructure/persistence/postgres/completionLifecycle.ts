import type { TransactionSQL } from 'bun';
import { jobId, type JobId } from '../../../domain/types/job';
import { databaseNow, recordPostgresEvent, type PostgresContext } from './context';
import { lockPostgresDependencyCompletions } from './dependencyPromotion';

const LIVE_CONSUMER_STATES = [
  'waiting',
  'prioritized',
  'delayed',
  'waiting-children',
  'active',
] as const;
const COMPLETION_PRUNE_BATCH_SIZE = 1000;

function uniqueIds(ids: readonly JobId[]): string[] {
  return [...new Set(ids.map(String))].sort();
}

/** Reject retirement while any non-terminal consumer still owns the old generation. */
export async function assertNoLivePostgresCompletionConsumers(
  tx: TransactionSQL,
  ctx: PostgresContext,
  ids: readonly JobId[],
  deletingConsumerIds: readonly JobId[] = []
): Promise<void> {
  const candidates = uniqueIds(ids);
  if (candidates.length === 0) return;
  const deleting = uniqueIds(deletingConsumerIds);
  const rows = await tx<{ dependency_id: string }[]>`
    SELECT dependency.dependency_id
    FROM bunqueue_dependencies AS dependency
    JOIN bunqueue_jobs AS consumer
      ON consumer.namespace = dependency.namespace AND consumer.id = dependency.job_id
    WHERE dependency.namespace = ${ctx.config.namespace}
      AND dependency.dependency_id = ANY(${tx.array(candidates, 'TEXT')})
      AND consumer.state = ANY(${tx.array([...LIVE_CONSUMER_STATES], 'TEXT')})
      AND (
        ${deleting.length} = 0
        OR consumer.id <> ALL(${tx.array(deleting, 'TEXT')})
      )
    ORDER BY dependency.dependency_id, consumer.id
    LIMIT 1
  `;
  if (rows[0]) {
    throw new Error(`Custom ID ${rows[0].dependency_id} still has unresolved dependency consumers`);
  }
}

/** Remove completion evidence for IDs that are about to start a new generation. */
export async function retirePostgresCompletionGenerations(
  tx: TransactionSQL,
  ctx: PostgresContext,
  ids: readonly JobId[],
  deletingConsumerIds: readonly JobId[] = []
): Promise<string[]> {
  const candidates = uniqueIds(ids);
  if (candidates.length === 0) return [];
  const existing = await tx<{ job_id: string }[]>`
    SELECT job_id FROM bunqueue_completions
    WHERE namespace = ${ctx.config.namespace}
      AND job_id = ANY(${tx.array(candidates, 'TEXT')})
    ORDER BY job_id
  `;
  if (existing.length === 0) return [];
  await assertNoLivePostgresCompletionConsumers(
    tx,
    ctx,
    existing.map((row) => jobId(row.job_id)),
    deletingConsumerIds
  );
  const rows = await tx<{ job_id: string }[]>`
    DELETE FROM bunqueue_completions
    WHERE namespace = ${ctx.config.namespace}
      AND job_id = ANY(${tx.array(
        existing.map((row) => row.job_id),
        'TEXT'
      )})
    RETURNING job_id
  `;
  return rows.map((row) => row.job_id);
}

/** Delete queue-owned completion-only rows inside an already serialized obliterate. */
export async function deletePostgresQueueCompletions(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queue: string
): Promise<JobId[]> {
  const rows = await tx<{ job_id: string }[]>`
    DELETE FROM bunqueue_completions
    WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
    RETURNING job_id
  `;
  return rows.map((row) => jobId(row.job_id));
}

interface CompletionPruneBatch {
  readonly selected: number;
  readonly removed: number;
}

async function prunePostgresCompletionTombstoneBatch(
  ctx: PostgresContext
): Promise<CompletionPruneBatch> {
  return await ctx.sql.begin(async (tx) => {
    await tx`
      SELECT pg_advisory_xact_lock(
        hashtext(${`${ctx.config.namespace}:completion-retention`})
      )
    `;
    const candidates = await tx<{ job_id: string }[]>`
      SELECT completion.job_id
      FROM bunqueue_completions AS completion
      WHERE completion.namespace = ${ctx.config.namespace}
        AND NOT EXISTS (
          SELECT 1 FROM bunqueue_jobs AS retained
          WHERE retained.namespace = completion.namespace
            AND retained.id = completion.job_id
        )
        AND NOT EXISTS (
          SELECT 1
          FROM bunqueue_dependencies AS dependency
          JOIN bunqueue_jobs AS consumer
            ON consumer.namespace = dependency.namespace AND consumer.id = dependency.job_id
          WHERE dependency.namespace = completion.namespace
            AND dependency.dependency_id = completion.job_id
            AND consumer.state = ANY(${tx.array([...LIVE_CONSUMER_STATES], 'TEXT')})
      )
      ORDER BY completion.completed_at DESC, completion.job_id DESC
      OFFSET ${ctx.config.maxCompletedJobs}
      LIMIT ${COMPLETION_PRUNE_BATCH_SIZE}
    `;
    const ids = candidates.map((row) => jobId(row.job_id));
    if (ids.length === 0) return { selected: 0, removed: 0 };
    await lockPostgresDependencyCompletions(tx, ctx, ids);
    const removed = await tx<{ job_id: string; queue: string }[]>`
      DELETE FROM bunqueue_completions AS completion
      WHERE completion.namespace = ${ctx.config.namespace}
        AND completion.job_id = ANY(${tx.array(ids.map(String), 'TEXT')})
        AND NOT EXISTS (
          SELECT 1 FROM bunqueue_jobs AS retained
          WHERE retained.namespace = completion.namespace
            AND retained.id = completion.job_id
        )
        AND NOT EXISTS (
          SELECT 1
          FROM bunqueue_dependencies AS dependency
          JOIN bunqueue_jobs AS consumer
            ON consumer.namespace = dependency.namespace AND consumer.id = dependency.job_id
          WHERE dependency.namespace = completion.namespace
            AND dependency.dependency_id = completion.job_id
            AND consumer.state = ANY(${tx.array([...LIVE_CONSUMER_STATES], 'TEXT')})
        )
      RETURNING completion.job_id, completion.queue
    `;
    const now = await databaseNow(tx);
    for (const queue of [...new Set(removed.map((row) => row.queue))].sort()) {
      await recordPostgresEvent(tx, ctx, {
        queue,
        type: 'queue-completions-pruned',
        jobId: jobId(`__queue__:${queue}`),
        occurredAt: now,
      });
    }
    return { selected: ids.length, removed: removed.length };
  });
}

/** Bound unreferenced removeOnComplete tombstones without pruning live dependency evidence. */
export async function prunePostgresCompletionTombstones(ctx: PostgresContext): Promise<number> {
  let removed = 0;
  while (true) {
    const batch = await prunePostgresCompletionTombstoneBatch(ctx);
    removed += batch.removed;
    if (batch.selected === 0) return removed;
  }
}
