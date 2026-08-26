import type { TransactionSQL } from 'bun';
import { jobId, type JobId } from '../../../domain/types/job';
import { decodePostgresJob } from './codec';
import { databaseNow, recordPostgresEvent, type PostgresContext } from './context';
import type { PostgresJobRow } from './types';
import {
  lockPostgresDestructionCandidates,
  partitionPostgresDestructionCandidates,
} from './dependencyDestruction';

async function prepareDeletableRows(
  tx: TransactionSQL,
  ctx: PostgresContext,
  candidateIds: readonly JobId[],
  load: (ids: readonly JobId[]) => Promise<PostgresJobRow[]>
): Promise<PostgresJobRow[]> {
  if (candidateIds.length === 0) return [];
  await lockPostgresDestructionCandidates(tx, ctx, candidateIds);
  const rows = await load(candidateIds);
  const plannedIds = rows.map((row) => jobId(row.id));
  const { deletableIds } = await partitionPostgresDestructionCandidates(
    tx,
    ctx,
    plannedIds,
    plannedIds
  );
  const deletable = new Set(deletableIds.map(String));
  return rows.filter((row) => deletable.has(row.id));
}

async function deleteRows(
  tx: TransactionSQL,
  ctx: PostgresContext,
  rows: readonly PostgresJobRow[]
): Promise<JobId[]> {
  if (rows.length === 0) return [];
  const ids = rows.map((row) => row.id);
  await tx`
    DELETE FROM bunqueue_job_logs
    WHERE namespace = ${ctx.config.namespace} AND job_id = ANY(${tx.array(ids, 'TEXT')})
  `;
  await tx`
    DELETE FROM bunqueue_flow_failures
    WHERE namespace = ${ctx.config.namespace}
      AND parent_id = ANY(${tx.array(ids, 'TEXT')})
  `;
  await tx`
    DELETE FROM bunqueue_dependencies
    WHERE namespace = ${ctx.config.namespace} AND job_id = ANY(${tx.array(ids, 'TEXT')})
  `;
  await tx`
    DELETE FROM bunqueue_completions
    WHERE namespace = ${ctx.config.namespace} AND job_id = ANY(${tx.array(ids, 'TEXT')})
  `;
  await tx`
    DELETE FROM bunqueue_jobs
    WHERE namespace = ${ctx.config.namespace} AND id = ANY(${tx.array(ids, 'TEXT')})
  `;
  const now = await databaseNow(tx);
  for (const row of rows) {
    const job = decodePostgresJob(row).job;
    await recordPostgresEvent(tx, ctx, {
      queue: job.queue,
      type: 'removed',
      jobId: job.id,
      occurredAt: now,
      job,
      state: row.state,
      removed: true,
    });
  }
  return ids.map(jobId);
}

/** Remove old jobs with the same state grouping used by Queue.clean(). */
export async function cleanPostgresQueue(
  ctx: PostgresContext,
  queue: string,
  graceMs: number,
  state?: string,
  limit = 1000
): Promise<JobId[]> {
  const cap = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : 0;
  if (cap === 0 || state === 'active') return [];
  return await ctx.sql.begin(async (tx) => {
    const threshold = (await databaseNow(tx)) - graceMs;
    let rows: PostgresJobRow[];
    if (state === 'completed') {
      const candidates = await tx<{ id: string }[]>`
        SELECT id FROM bunqueue_jobs
        WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
          AND state = 'completed' AND COALESCE(completed_at, created_at) <= ${threshold}
        ORDER BY COALESCE(completed_at, created_at), id
        LIMIT ${cap}
      `;
      rows = await prepareDeletableRows(
        tx,
        ctx,
        candidates.map((row) => jobId(row.id)),
        (ids) =>
          tx<PostgresJobRow[]>`
            SELECT * FROM bunqueue_jobs
            WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
              AND state = 'completed' AND COALESCE(completed_at, created_at) <= ${threshold}
              AND id = ANY(${tx.array(ids.map(String), 'TEXT')})
            ORDER BY COALESCE(completed_at, created_at), id
          `
      );
    } else if (state === 'failed') {
      const candidates = await tx<{ id: string }[]>`
        SELECT id FROM bunqueue_jobs
        WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
          AND state = 'failed' AND COALESCE(completed_at, created_at) <= ${threshold}
        ORDER BY COALESCE(completed_at, created_at), id
        LIMIT ${cap}
      `;
      rows = await prepareDeletableRows(
        tx,
        ctx,
        candidates.map((row) => jobId(row.id)),
        (ids) =>
          tx<PostgresJobRow[]>`
            SELECT * FROM bunqueue_jobs
            WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
              AND state = 'failed' AND COALESCE(completed_at, created_at) <= ${threshold}
              AND id = ANY(${tx.array(ids.map(String), 'TEXT')})
            ORDER BY COALESCE(completed_at, created_at), id
          `
      );
    } else if (
      state === undefined ||
      state === 'wait' ||
      state === 'waiting' ||
      state === 'delayed' ||
      state === 'prioritized' ||
      state === 'paused'
    ) {
      const candidates = await tx<{ id: string }[]>`
        SELECT id FROM bunqueue_jobs
        WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
          AND state IN ('waiting', 'prioritized', 'delayed') AND created_at <= ${threshold}
        ORDER BY created_at, id
        LIMIT ${cap}
      `;
      rows = await prepareDeletableRows(
        tx,
        ctx,
        candidates.map((row) => jobId(row.id)),
        (ids) =>
          tx<PostgresJobRow[]>`
            SELECT * FROM bunqueue_jobs
            WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
              AND state IN ('waiting', 'prioritized', 'delayed') AND created_at <= ${threshold}
              AND id = ANY(${tx.array(ids.map(String), 'TEXT')})
            ORDER BY created_at, id
          `
      );
    } else {
      return [];
    }
    return await deleteRows(tx, ctx, rows);
  });
}

/** Permanently remove every failed job currently visible in one queue. */
export async function purgePostgresDlq(ctx: PostgresContext, queue: string): Promise<JobId[]> {
  return await ctx.sql.begin(async (tx) => {
    const candidates = await tx<{ id: string }[]>`
      SELECT id FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace} AND queue = ${queue} AND state = 'failed'
      ORDER BY completed_at, id
    `;
    const rows = await prepareDeletableRows(
      tx,
      ctx,
      candidates.map((row) => jobId(row.id)),
      (ids) =>
        tx<PostgresJobRow[]>`
          SELECT * FROM bunqueue_jobs
          WHERE namespace = ${ctx.config.namespace} AND queue = ${queue} AND state = 'failed'
            AND id = ANY(${tx.array(ids.map(String), 'TEXT')})
          ORDER BY completed_at, id
        `
    );
    return await deleteRows(tx, ctx, rows);
  });
}
