import type { TransactionSQL } from 'bun';
import { MAX_TIMELINE_ENTRIES, type Job, type JobId } from '../../../domain/types/job';
import { decodePostgresJob, encodePostgresValue } from './codec';
import { databaseNow, recordPostgresEvent, type PostgresContext } from './context';
import {
  lockPostgresDestructionCandidates,
  partitionPostgresDestructionCandidates,
} from './dependencyDestruction';
import type { PostgresJobRow, PostgresJobState } from './types';

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

function pendingState(job: Job, now: number): PostgresJobState {
  if (job.dependsOn.length > 0) return 'waiting-children';
  if (job.runAt > now) return 'delayed';
  return job.priority > 0 ? 'prioritized' : 'waiting';
}

export async function removePostgresJob(
  ctx: PostgresContext,
  id: JobId,
  token?: string
): Promise<boolean> {
  return await ctx.sql.begin(async (tx) => {
    await lockPostgresDestructionCandidates(tx, ctx, [id]);
    const row = await lockJob(tx, ctx, id);
    if (!row || (row.state === 'active' && row.lease_token !== token)) return false;
    const { protectedIds } = await partitionPostgresDestructionCandidates(tx, ctx, [id], [id]);
    if (protectedIds.length > 0) return false;
    const job = decodePostgresJob(row).job;
    const now = await databaseNow(tx);
    await tx`
      DELETE FROM bunqueue_job_logs
      WHERE namespace = ${ctx.config.namespace} AND job_id = ${String(id)}
    `;
    await tx`
      DELETE FROM bunqueue_flow_failures
      WHERE namespace = ${ctx.config.namespace} AND parent_id = ${String(id)}
    `;
    await tx`
      DELETE FROM bunqueue_dependencies
      WHERE namespace = ${ctx.config.namespace} AND job_id = ${String(id)}
    `;
    await tx`
      DELETE FROM bunqueue_completions
      WHERE namespace = ${ctx.config.namespace} AND job_id = ${String(id)}
    `;
    await tx`
      DELETE FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace} AND id = ${String(id)}
    `;
    await recordPostgresEvent(tx, ctx, {
      queue: job.queue,
      type: 'removed',
      jobId: job.id,
      occurredAt: now,
      job,
      state: row.state,
    });
    return true;
  });
}

export async function retryPostgresTerminalJob(
  ctx: PostgresContext,
  id: JobId,
  requestedTimestamp?: number
): Promise<boolean> {
  return await ctx.sql.begin(async (tx) => {
    await lockPostgresDestructionCandidates(tx, ctx, [id]);
    const timestamp = requestedTimestamp ?? (await databaseNow(tx));
    const row = await lockJob(tx, ctx, id);
    if (!row || (row.state !== 'failed' && row.state !== 'completed')) return false;
    if (row.state === 'completed') {
      const { protectedIds } = await partitionPostgresDestructionCandidates(tx, ctx, [id], [id]);
      if (protectedIds.length > 0) return false;
    }
    const job = decodePostgresJob(row).job;
    job.attempts = 0;
    job.stallCount = 0;
    job.completedAt = null;
    job.startedAt = null;
    job.runAt = timestamp;
    job.progress = 0;
    job.progressMessage = null;
    job.lastHeartbeat = timestamp;
    if (job.timeline.length < MAX_TIMELINE_ENTRIES) {
      job.timeline.push({ state: 'waiting', timestamp });
    }
    const state = pendingState(job, timestamp);
    await tx`
      DELETE FROM bunqueue_completions
      WHERE namespace = ${ctx.config.namespace} AND job_id = ${String(id)}
    `;
    await tx`
      UPDATE bunqueue_jobs
      SET payload = ${encodePostgresValue(job)}, state = ${state}, run_at = ${timestamp},
          started_at = NULL, completed_at = NULL, result = NULL, dlq_entry = NULL,
          error = NULL, failure_reason = NULL, lease_owner = NULL, lease_token = NULL,
          lease_broker_id = NULL, lease_until = NULL, unique_key = NULL,
          unique_expires_at = NULL, dlq_retry_state = NULL, version = version + 1
      WHERE namespace = ${ctx.config.namespace} AND id = ${String(id)}
    `;
    await recordPostgresEvent(tx, ctx, {
      queue: job.queue,
      type: 'retried',
      jobId: job.id,
      occurredAt: timestamp,
      job,
      state,
    });
    return true;
  });
}
