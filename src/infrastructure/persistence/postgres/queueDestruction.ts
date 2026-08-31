import type { TransactionSQL } from 'bun';
import { jobId } from '../../../domain/types/job';
import { databaseNow, recordPostgresEvent, type PostgresContext } from './context';
import {
  lockPostgresDestructionCandidates,
  lockPostgresDestructionIdentities,
  lockPostgresDestructionRows,
  partitionPostgresDestructionCandidates,
} from './dependencyDestruction';
import { deletePostgresEventPruneWatermark } from './eventPruneWatermarks';
import { lockPostgresQueueLifecycleExclusive } from './queueLifecycle';
import { deletePostgresQueueEventHistory, lockPostgresEventRetention } from './eventRetention';

async function recordQueueInvalidation(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queue: string,
  type: string
): Promise<void> {
  await recordPostgresEvent(tx, ctx, {
    queue,
    type,
    jobId: jobId(`__queue__:${queue}`),
    occurredAt: await databaseNow(tx),
  });
}

export async function drainPostgresQueue(ctx: PostgresContext, queue: string): Promise<number> {
  return await ctx.sql.begin(async (tx) => {
    const candidates = await tx<{ id: string }[]>`
      SELECT id FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
        AND state IN ('waiting', 'prioritized', 'delayed', 'waiting-children')
      ORDER BY id
    `;
    const candidateIds = candidates.map((row) => jobId(row.id));
    if (candidateIds.length === 0) return 0;
    await lockPostgresDestructionCandidates(tx, ctx, candidateIds);
    const rows = await tx<{ id: string }[]>`
      SELECT id FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
        AND state IN ('waiting', 'prioritized', 'delayed', 'waiting-children')
        AND id = ANY(${tx.array(candidateIds.map(String), 'TEXT')})
      ORDER BY id
    `;
    const plannedIds = rows.map((row) => jobId(row.id));
    const { deletableIds } = await partitionPostgresDestructionCandidates(
      tx,
      ctx,
      plannedIds,
      plannedIds
    );
    const ids = deletableIds.map(String);
    if (ids.length === 0) return 0;
    const values = tx.array(ids, 'TEXT');
    await tx`
      DELETE FROM bunqueue_job_logs
      WHERE namespace = ${ctx.config.namespace} AND job_id = ANY(${values})
    `;
    await tx`
      DELETE FROM bunqueue_flow_failures
      WHERE namespace = ${ctx.config.namespace} AND parent_id = ANY(${values})
    `;
    await tx`
      DELETE FROM bunqueue_dependencies
      WHERE namespace = ${ctx.config.namespace} AND job_id = ANY(${values})
    `;
    await tx`
      DELETE FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace} AND id = ANY(${values})
    `;
    await recordQueueInvalidation(tx, ctx, queue, 'queue-drained');
    return ids.length;
  });
}

export async function obliteratePostgresQueue(
  ctx: PostgresContext,
  queue: string
): Promise<number> {
  return await ctx.sql.begin(async (tx) => {
    await lockPostgresQueueLifecycleExclusive(tx, ctx, queue);
    await lockPostgresEventRetention(tx, ctx, queue);
    await tx`
      INSERT INTO bunqueue_queue_state (namespace, queue)
      VALUES (${ctx.config.namespace}, ${queue})
      ON CONFLICT (namespace, queue) DO NOTHING
    `;
    await tx`
      SELECT queue FROM bunqueue_queue_state
      WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
      FOR UPDATE
    `;
    const candidates = await tx<{ id: string }[]>`
      SELECT id FROM (
        SELECT id FROM bunqueue_jobs
        WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
        UNION
        SELECT job_id AS id FROM bunqueue_completions
        WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
      ) AS queue_ids
      ORDER BY id
    `;
    const candidateIds = candidates.map((row) => jobId(row.id));
    await lockPostgresDestructionIdentities(tx, ctx, candidateIds);
    await lockPostgresDestructionRows(tx, ctx, candidateIds);
    const jobs = await tx<{ id: string }[]>`
      SELECT id FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
        AND id = ANY(${tx.array(candidateIds.map(String), 'TEXT')})
      ORDER BY id
    `;
    const completions = await tx<{ job_id: string }[]>`
      SELECT job_id FROM bunqueue_completions
      WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
        AND job_id = ANY(${tx.array(candidateIds.map(String), 'TEXT')})
      ORDER BY job_id
    `;
    const jobIds = jobs.map((row) => jobId(row.id));
    const targetIds = [
      ...new Set([...jobIds.map(String), ...completions.map((row) => row.job_id)]),
    ].map(jobId);
    const { protectedIds } = await partitionPostgresDestructionCandidates(
      tx,
      ctx,
      targetIds,
      jobIds
    );
    if (protectedIds.length > 0) {
      throw new Error(
        `Cannot obliterate queue ${queue}: ${protectedIds.length} job(s) have live dependents`
      );
    }
    const targets = tx.array(targetIds.map(String), 'TEXT');
    const jobValues = tx.array(jobIds.map(String), 'TEXT');
    if (targetIds.length > 0) {
      await tx`
        DELETE FROM bunqueue_job_logs
        WHERE namespace = ${ctx.config.namespace} AND job_id = ANY(${targets})
      `;
      await tx`
        DELETE FROM bunqueue_repeat_links
        WHERE namespace = ${ctx.config.namespace}
          AND (original_id = ANY(${targets}) OR successor_id = ANY(${targets}))
      `;
      await tx`
        DELETE FROM bunqueue_flow_failures
        WHERE namespace = ${ctx.config.namespace} AND parent_id = ANY(${targets})
      `;
      await tx`
        DELETE FROM bunqueue_dependencies
        WHERE namespace = ${ctx.config.namespace}
          AND (job_id = ANY(${jobValues}) OR dependency_id = ANY(${targets}))
      `;
      await tx`
        DELETE FROM bunqueue_completions
        WHERE namespace = ${ctx.config.namespace} AND job_id = ANY(${targets})
      `;
      await tx`
        DELETE FROM bunqueue_jobs
        WHERE namespace = ${ctx.config.namespace} AND id = ANY(${jobValues})
      `;
    }
    await tx`
      DELETE FROM bunqueue_group_state
      WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
    `;
    await tx`
      DELETE FROM bunqueue_queue_state
      WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
    `;
    await tx`
      DELETE FROM bunqueue_metric_buckets
      WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
    `;
    await tx`
      DELETE FROM bunqueue_metric_totals
      WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
    `;
    await deletePostgresQueueEventHistory(tx, ctx, queue);
    await deletePostgresEventPruneWatermark(tx, ctx, queue);
    await recordQueueInvalidation(tx, ctx, queue, 'queue-obliterated');
    return jobIds.length;
  });
}
