import type { TransactionSQL } from 'bun';
import { createDlqEntry, FailureReason } from '../../../domain/types/dlq';
import type { JobId } from '../../../domain/types/job';
import { decodePostgresJob, encodePostgresValue } from './codec';
import {
  databaseNow,
  recordPostgresEvent,
  runPostgresPostCommitMaintenance,
  type PostgresContext,
} from './context';
import { enforcePostgresDlqLimit } from './dlqLifecycle';
import { getPostgresQueuePolicies } from './policies';
import type { PostgresJobRow } from './types';
import {
  lockPostgresDestructionCandidates,
  partitionPostgresDestructionCandidates,
} from './dependencyDestruction';

async function lockJob(
  tx: TransactionSQL,
  ctx: PostgresContext,
  id: JobId
): Promise<PostgresJobRow | null> {
  const rows = await tx<PostgresJobRow[]>`
    SELECT * FROM bunqueue_jobs
    WHERE namespace = ${ctx.config.namespace} AND id = ${String(id)}
    FOR UPDATE
  `;
  return rows[0] ?? null;
}

async function deleteJobDependencies(
  tx: TransactionSQL,
  ctx: PostgresContext,
  id: JobId
): Promise<void> {
  await tx`
    DELETE FROM bunqueue_dependencies
    WHERE namespace = ${ctx.config.namespace} AND job_id = ${String(id)}
  `;
}

async function deleteParentFailureValues(
  tx: TransactionSQL,
  ctx: PostgresContext,
  id: JobId
): Promise<void> {
  await tx`
    DELETE FROM bunqueue_flow_failures
    WHERE namespace = ${ctx.config.namespace} AND parent_id = ${String(id)}
  `;
}

/** Cancel only a pending job, matching the existing QueueManager contract. */
export async function cancelPostgresJob(ctx: PostgresContext, id: JobId): Promise<boolean> {
  return await ctx.sql.begin(async (tx) => {
    await lockPostgresDestructionCandidates(tx, ctx, [id]);
    const row = await lockJob(tx, ctx, id);
    if (!row || !['waiting', 'prioritized', 'delayed', 'waiting-children'].includes(row.state)) {
      return false;
    }
    const { protectedIds } = await partitionPostgresDestructionCandidates(tx, ctx, [id], [id]);
    if (protectedIds.length > 0) return false;
    const job = decodePostgresJob(row).job;
    await deleteJobDependencies(tx, ctx, id);
    await tx`
      DELETE FROM bunqueue_job_logs
      WHERE namespace = ${ctx.config.namespace} AND job_id = ${String(id)}
    `;
    await deleteParentFailureValues(tx, ctx, id);
    await tx`
      DELETE FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace} AND id = ${String(id)}
    `;
    await recordPostgresEvent(tx, ctx, {
      queue: job.queue,
      type: 'removed',
      jobId: id,
      occurredAt: await databaseNow(tx),
      job,
      state: row.state,
      removed: true,
    });
    return true;
  });
}

/** Move a runnable pending or actively leased job to the DLQ. */
export async function discardPostgresJob(
  ctx: PostgresContext,
  id: JobId,
  token?: string
): Promise<boolean> {
  const request = await ctx.sql.begin(async (tx) => {
    const row = await lockJob(tx, ctx, id);
    if (!row || !['waiting', 'prioritized', 'delayed', 'active'].includes(row.state)) return null;
    const now = await databaseNow(tx);
    if (
      row.state === 'active' &&
      (row.lease_token !== token || Number(row.lease_until ?? 0) <= now)
    ) {
      return null;
    }
    const job = decodePostgresJob(row).job;
    job.completedAt = now;
    const { dlq } = await getPostgresQueuePolicies(tx, ctx, job.queue);
    const entry = createDlqEntry(job, FailureReason.Unknown, null, dlq);
    await deleteJobDependencies(tx, ctx, id);
    await deleteParentFailureValues(tx, ctx, id);
    await tx`
      UPDATE bunqueue_jobs
      SET payload = ${encodePostgresValue(job)}, state = 'failed', completed_at = ${now},
          lease_owner = NULL, lease_broker_id = NULL, lease_token = NULL, lease_until = NULL,
          error = NULL, failure_reason = ${FailureReason.Unknown},
          dlq_entry = ${encodePostgresValue(entry)}, dlq_retry_state = NULL,
          version = version + 1
      WHERE namespace = ${ctx.config.namespace} AND id = ${String(id)}
    `;
    await recordPostgresEvent(tx, ctx, {
      queue: job.queue,
      type: 'failed',
      jobId: id,
      occurredAt: now,
      job,
      state: 'failed',
      dlqEntry: entry,
      dlqRetryState: null,
    });
    return { queue: job.queue, maxEntries: dlq.maxEntries };
  });
  if (!request) return false;
  await runPostgresPostCommitMaintenance(ctx, `dlq-limit:${request.queue}`, () =>
    enforcePostgresDlqLimit(ctx, request.queue, request.maxEntries)
  );
  return true;
}

/** Permanently remove exactly one failed job from its queue. */
export async function removePostgresDlqJob(
  ctx: PostgresContext,
  queue: string,
  id: JobId
): Promise<boolean> {
  return await ctx.sql.begin(async (tx) => {
    await lockPostgresDestructionCandidates(tx, ctx, [id]);
    const row = await lockJob(tx, ctx, id);
    if (!row || row.queue !== queue || row.state !== 'failed') return false;
    const { protectedIds } = await partitionPostgresDestructionCandidates(tx, ctx, [id], [id]);
    if (protectedIds.length > 0) return false;
    const job = decodePostgresJob(row).job;
    await deleteJobDependencies(tx, ctx, id);
    await tx`
      DELETE FROM bunqueue_job_logs
      WHERE namespace = ${ctx.config.namespace} AND job_id = ${String(id)}
    `;
    await deleteParentFailureValues(tx, ctx, id);
    await tx`
      DELETE FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace} AND id = ${String(id)}
    `;
    await recordPostgresEvent(tx, ctx, {
      queue,
      type: 'removed',
      jobId: id,
      occurredAt: await databaseNow(tx),
      job,
      state: 'failed',
      removed: true,
    });
    return true;
  });
}
