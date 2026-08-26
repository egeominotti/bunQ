import type { TransactionSQL } from 'bun';
import { jobId, type JobId } from '../../../domain/types/job';
import { decodePostgresJob, encodePostgresValue } from './codec';
import { databaseNow, recordPostgresEvent, type PostgresContext } from './context';
import type { PostgresJobRow } from './types';
import {
  lockPostgresDestructionIdentities,
  partitionPostgresDestructionCandidates,
} from './dependencyDestruction';

function isProtectedCron(row: PostgresJobRow): boolean {
  return decodePostgresJob(row).job.uniqueKey?.startsWith('cron:') === true;
}

export async function discardPostgresProtectedCronLease(
  tx: TransactionSQL,
  ctx: PostgresContext,
  row: PostgresJobRow,
  now: number
): Promise<boolean> {
  if (!isProtectedCron(row)) return false;
  const id = jobId(row.id);
  const { protectedIds } = await partitionPostgresDestructionCandidates(tx, ctx, [id], [id]);
  if (protectedIds.length > 0) return false;
  const job = decodePostgresJob(row).job;
  await tx`
    DELETE FROM bunqueue_job_logs
    WHERE namespace = ${ctx.config.namespace} AND job_id = ${row.id}
  `;
  await tx`
    DELETE FROM bunqueue_flow_failures
    WHERE namespace = ${ctx.config.namespace} AND parent_id = ${row.id}
  `;
  await tx`
    DELETE FROM bunqueue_dependencies
    WHERE namespace = ${ctx.config.namespace} AND job_id = ${row.id}
  `;
  await tx`
    DELETE FROM bunqueue_jobs
    WHERE namespace = ${ctx.config.namespace} AND id = ${row.id}
  `;
  await recordPostgresEvent(tx, ctx, {
    queue: job.queue,
    type: 'removed',
    jobId: job.id,
    occurredAt: now,
    job,
    state: 'active',
    removed: true,
  });
  return true;
}

async function releaseLeaseRow(
  tx: TransactionSQL,
  ctx: PostgresContext,
  row: PostgresJobRow,
  now: number
): Promise<void> {
  if (await discardPostgresProtectedCronLease(tx, ctx, row, now)) return;
  const job = decodePostgresJob(row).job;
  job.startedAt = null;
  job.runAt = Math.min(job.runAt, now);
  const state = job.priority > 0 ? 'prioritized' : 'waiting';
  await tx`
    UPDATE bunqueue_jobs
    SET payload = ${encodePostgresValue(job)}, state = ${state}, run_at = ${job.runAt},
        started_at = NULL, lease_owner = NULL, lease_token = NULL, lease_until = NULL,
        lease_broker_id = NULL, lease_renewals = 0, version = version + 1
    WHERE namespace = ${ctx.config.namespace} AND id = ${row.id}
  `;
  await recordPostgresEvent(tx, ctx, {
    queue: job.queue,
    type: 'retried',
    jobId: job.id,
    occurredAt: now,
    job,
    state,
  });
}

/** Release a never-renewed connection lease without racing pooled heartbeats. */
export async function releasePostgresClientLease(
  ctx: PostgresContext,
  id: JobId,
  token: string
): Promise<boolean> {
  return await ctx.sql.begin(async (tx) => {
    await lockPostgresDestructionIdentities(tx, ctx, [id]);
    const now = await databaseNow(tx);
    const rows = await tx<PostgresJobRow[]>`
      SELECT * FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace} AND id = ${String(id)}
      FOR UPDATE
    `;
    const row = rows[0];
    if (
      row?.state !== 'active' ||
      row.lease_token !== token ||
      Number(row.lease_until ?? 0) <= now ||
      row.lease_renewals > 0
    ) {
      return false;
    }
    await releaseLeaseRow(tx, ctx, row, now);
    return true;
  });
}

/** Return or discard every active lease currently coordinated by this broker. */
export async function releasePostgresBrokerLeases(ctx: PostgresContext): Promise<number> {
  return await ctx.sql.begin(async (tx) => {
    const now = await databaseNow(tx);
    const candidates = await tx<{ id: string }[]>`
      SELECT id FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace}
        AND state = 'active' AND lease_broker_id = ${ctx.config.brokerId}
      ORDER BY id
    `;
    const ids = candidates.map((row) => jobId(row.id));
    if (ids.length === 0) return 0;
    await lockPostgresDestructionIdentities(tx, ctx, ids);
    const rows = await tx<PostgresJobRow[]>`
      SELECT * FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace}
        AND state = 'active'
        AND lease_broker_id = ${ctx.config.brokerId}
        AND id = ANY(${tx.array(ids.map(String), 'TEXT')})
      ORDER BY id
      FOR UPDATE
    `;
    for (const row of rows) await releaseLeaseRow(tx, ctx, row, now);
    return rows.length;
  });
}
