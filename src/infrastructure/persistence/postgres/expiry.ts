import { jobId } from '../../../domain/types/job';
import { decodePostgresJob } from './codec';
import { databaseNow, recordPostgresEvent, type PostgresContext } from './context';
import {
  lockPostgresDestructionCandidates,
  partitionPostgresDestructionCandidates,
} from './dependencyDestruction';
import type { PostgresJobRow } from './types';

/** Delete pending jobs whose TTL elapsed, matching the existing SQLite pull semantics. */
export async function expirePendingPostgresJobs(
  ctx: PostgresContext,
  queue: string,
  requestedNow?: number
): Promise<number> {
  return await ctx.sql.begin(async (tx) => {
    const now = requestedNow ?? (await databaseNow(tx));
    const candidates = await tx<{ id: string }[]>`
      SELECT id FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace}
        AND queue = ${queue}
        AND state IN ('waiting', 'prioritized', 'delayed')
        AND ttl IS NOT NULL
        AND created_at + ttl < ${now}
      ORDER BY created_at, id
      LIMIT 1000
    `;
    const candidateIds = candidates.map((row) => jobId(row.id));
    if (candidateIds.length === 0) return 0;
    await lockPostgresDestructionCandidates(tx, ctx, candidateIds);
    const rows = await tx<PostgresJobRow[]>`
      SELECT * FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace}
        AND queue = ${queue}
        AND state IN ('waiting', 'prioritized', 'delayed')
        AND ttl IS NOT NULL
        AND created_at + ttl < ${now}
        AND id = ANY(${tx.array(candidateIds.map(String), 'TEXT')})
      ORDER BY created_at, id
    `;
    const plannedIds = rows.map((row) => jobId(row.id));
    const { deletableIds } = await partitionPostgresDestructionCandidates(
      tx,
      ctx,
      plannedIds,
      plannedIds
    );
    const deletable = new Set(deletableIds.map(String));
    const removedRows = rows.filter((row) => deletable.has(row.id));
    if (removedRows.length === 0) return 0;
    const ids = removedRows.map((row) => row.id);
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
      WHERE namespace = ${ctx.config.namespace}
        AND job_id = ANY(${tx.array(ids, 'TEXT')})
    `;
    await tx`
      DELETE FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace}
        AND id = ANY(${tx.array(ids, 'TEXT')})
    `;
    for (const row of removedRows) {
      const job = decodePostgresJob(row).job;
      await recordPostgresEvent(tx, ctx, {
        queue,
        type: 'removed',
        jobId: job.id,
        occurredAt: now,
        job,
        state: row.state,
        error: 'Job TTL expired',
        removed: true,
      });
    }
    return removedRows.length;
  });
}
