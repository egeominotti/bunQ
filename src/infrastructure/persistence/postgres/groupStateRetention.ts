import type { TransactionSQL } from 'bun';
import { databaseNow, type PostgresContext } from './context';

interface InactiveGroup {
  queue: string;
  group_id: string;
}

interface PruneBatchResult {
  deleted: number;
  selected: number;
}

async function pruneBatch(
  tx: TransactionSQL,
  ctx: PostgresContext,
  batchSize: number
): Promise<PruneBatchResult> {
  const now = await databaseNow(tx);
  const rows = await tx<InactiveGroup[]>`
    SELECT groups.queue, groups.group_id
    FROM bunqueue_group_state AS groups
    WHERE groups.namespace = ${ctx.config.namespace}
      AND NOT EXISTS (
        SELECT 1 FROM bunqueue_jobs AS job
        WHERE job.namespace = groups.namespace
          AND job.queue = groups.queue
          AND job.group_id = groups.group_id
          AND job.state IN (
            'waiting', 'prioritized', 'delayed', 'waiting-children', 'active'
          )
      )
      AND (
        groups.last_served IS NOT NULL
        OR (
          groups.rate_limit IS NULL
          AND groups.rate_duration_ms IS NULL
          AND groups.concurrency_limit IS NULL
          AND (
            groups.rate_window_started_at IS NULL
            OR (
              groups.rate_effective_duration_ms IS NOT NULL
              AND groups.rate_window_started_at + groups.rate_effective_duration_ms <= ${now}
            )
          )
        )
      )
    ORDER BY groups.queue, groups.group_id
    LIMIT ${batchSize}
    FOR UPDATE OF groups SKIP LOCKED
  `;
  if (rows.length === 0) return { deleted: 0, selected: 0 };
  const queues = rows.map(({ queue }) => queue);
  const groupIds = rows.map(({ group_id }) => group_id);

  await tx`
    UPDATE bunqueue_group_state AS groups
    SET last_served = NULL
    FROM unnest(
      ${tx.array(queues, 'TEXT')},
      ${tx.array(groupIds, 'TEXT')}
    ) AS target(queue, group_id)
    WHERE groups.namespace = ${ctx.config.namespace}
      AND groups.queue = target.queue
      AND groups.group_id = target.group_id
  `;
  const deleted = await tx<{ group_id: string }[]>`
    DELETE FROM bunqueue_group_state AS groups
    USING unnest(
      ${tx.array(queues, 'TEXT')},
      ${tx.array(groupIds, 'TEXT')}
    ) AS target(queue, group_id)
    WHERE groups.namespace = ${ctx.config.namespace}
      AND groups.queue = target.queue
      AND groups.group_id = target.group_id
      AND groups.rate_limit IS NULL
      AND groups.rate_duration_ms IS NULL
      AND groups.concurrency_limit IS NULL
      AND (
        groups.rate_window_started_at IS NULL
        OR (
          groups.rate_effective_duration_ms IS NOT NULL
          AND groups.rate_window_started_at + groups.rate_effective_duration_ms <= ${now}
        )
      )
      AND NOT EXISTS (
        SELECT 1 FROM bunqueue_jobs AS job
        WHERE job.namespace = groups.namespace
          AND job.queue = groups.queue
          AND job.group_id = groups.group_id
          AND job.state IN (
            'waiting', 'prioritized', 'delayed', 'waiting-children', 'active'
          )
      )
    RETURNING groups.group_id
  `;
  return { deleted: deleted.length, selected: rows.length };
}

/** Reclaim bounded batches of inactive policyless group scheduler state. */
export async function prunePostgresGroupStates(
  ctx: PostgresContext,
  batchSize = 1000,
  maxBatches = 10
): Promise<number> {
  let deleted = 0;
  for (let batch = 0; batch < maxBatches; batch++) {
    const result = await ctx.sql.begin((tx) => pruneBatch(tx, ctx, batchSize));
    deleted += result.deleted;
    if (result.selected < batchSize) break;
  }
  return deleted;
}
