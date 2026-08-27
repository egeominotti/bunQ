import type { TransactionSQL } from 'bun';
import { MAX_TIMELINE_ENTRIES, type Job, type JobId } from '../../../domain/types/job';
import { decodePostgresJob, encodePostgresValue } from './codec';
import { databaseNow, recordPostgresEvent, type PostgresContext } from './context';
import { updatedPostgresJobData } from './jobData';
import { updatePostgresRepeatSuccessorData } from './repeatMutations';
import type { PostgresJobRow, PostgresJobState, PostgresStoredJob } from './types';

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

async function saveJob(
  tx: TransactionSQL,
  ctx: PostgresContext,
  input: { job: Job; state: PostgresJobState; eventType: string; now: number }
): Promise<PostgresStoredJob> {
  const { job, state, eventType, now } = input;
  const rows = await tx<PostgresJobRow[]>`
    UPDATE bunqueue_jobs
    SET payload = ${encodePostgresValue(job)}, state = ${state}, priority = ${job.priority},
        lifo = ${job.lifo}, run_at = ${job.runAt}, started_at = ${job.startedAt},
        completed_at = ${job.completedAt}, attempts = ${job.attempts},
        lease_owner = CASE WHEN ${state} = 'active' THEN lease_owner ELSE NULL END,
        lease_broker_id = CASE WHEN ${state} = 'active' THEN lease_broker_id ELSE NULL END,
        lease_broker_session_id = CASE
          WHEN ${state} = 'active' THEN lease_broker_session_id ELSE NULL
        END,
        lease_token = CASE WHEN ${state} = 'active' THEN lease_token ELSE NULL END,
        lease_until = CASE WHEN ${state} = 'active' THEN lease_until ELSE NULL END,
        version = version + 1
    WHERE namespace = ${ctx.config.namespace} AND id = ${String(job.id)}
    RETURNING *
  `;
  await recordPostgresEvent(tx, ctx, {
    queue: job.queue,
    type: eventType,
    jobId: job.id,
    occurredAt: now,
    job,
    state,
  });
  return decodePostgresJob(rows[0]);
}

export async function updatePostgresJobData(
  ctx: PostgresContext,
  id: JobId,
  data: unknown
): Promise<boolean> {
  return await ctx.sql.begin(async (tx) => {
    const row = await lockJob(tx, ctx, id);
    if (!row || row.state === 'completed' || row.state === 'failed') {
      return await updatePostgresRepeatSuccessorData(tx, ctx, id, data);
    }
    const stored = decodePostgresJob(row);
    const job = { ...stored.job, data: updatedPostgresJobData(stored.job, data) };
    const now = await databaseNow(tx);
    await saveJob(tx, ctx, {
      job,
      state: stored.state,
      eventType: 'updated',
      now,
    });
    return true;
  });
}

export async function updatePostgresProgress(
  ctx: PostgresContext,
  id: JobId,
  progress: number,
  message?: string
): Promise<boolean> {
  return await ctx.sql.begin(async (tx) => {
    const row = await lockJob(tx, ctx, id);
    if (row?.state !== 'active') return false;
    const stored = decodePostgresJob(row);
    const now = await databaseNow(tx);
    stored.job.progress = Math.max(0, Math.min(100, progress));
    stored.job.progressMessage = message === undefined ? stored.job.progressMessage : message;
    stored.job.lastHeartbeat = now;
    await saveJob(tx, ctx, {
      job: stored.job,
      state: stored.state,
      eventType: 'progress',
      now,
    });
    return true;
  });
}

export async function changePostgresPriority(
  ctx: PostgresContext,
  id: JobId,
  priority: number,
  lifo?: boolean
): Promise<boolean> {
  return await ctx.sql.begin(async (tx) => {
    const row = await lockJob(tx, ctx, id);
    if (!row || !['waiting', 'prioritized', 'delayed'].includes(row.state)) return false;
    const stored = decodePostgresJob(row);
    const job: Job = { ...stored.job, priority, lifo: lifo ?? stored.job.lifo };
    const now = await databaseNow(tx);
    const state = pendingState(job, now);
    await saveJob(tx, ctx, { job, state, eventType: 'priority', now });
    return true;
  });
}

export async function changePostgresDelay(
  ctx: PostgresContext,
  id: JobId,
  runAt: number,
  token?: string
): Promise<boolean> {
  return await ctx.sql.begin(async (tx) => {
    const row = await lockJob(tx, ctx, id);
    if (!row || !['waiting', 'prioritized', 'delayed', 'active'].includes(row.state)) return false;
    const now = await databaseNow(tx);
    if (
      row.state === 'active' &&
      (row.lease_token !== token || Number(row.lease_until ?? 0) <= now)
    ) {
      return false;
    }
    const stored = decodePostgresJob(row);
    stored.job.runAt = runAt;
    stored.job.startedAt = null;
    const state = pendingState(stored.job, now);
    if (stored.job.timeline.length < MAX_TIMELINE_ENTRIES) {
      stored.job.timeline.push({ state, timestamp: now });
    }
    await saveJob(tx, ctx, { job: stored.job, state, eventType: 'delayed', now });
    return true;
  });
}

export async function promotePostgresJob(ctx: PostgresContext, id: JobId): Promise<boolean> {
  return await changePostgresDelay(ctx, id, await databaseNow(ctx.sql));
}

export async function movePostgresJobToWaitingChildren(
  ctx: PostgresContext,
  id: JobId,
  token?: string
): Promise<boolean> {
  return await ctx.sql.begin(async (tx) => {
    const row = await lockJob(tx, ctx, id);
    if (!row || !['waiting', 'prioritized', 'delayed', 'active'].includes(row.state)) return false;
    const now = await databaseNow(tx);
    if (
      row.state === 'active' &&
      (row.lease_token !== token || Number(row.lease_until ?? 0) <= now)
    ) {
      return false;
    }
    const job = decodePostgresJob(row).job;
    job.startedAt = null;
    if (job.timeline.length < MAX_TIMELINE_ENTRIES) {
      job.timeline.push({ state: 'waiting-children', timestamp: now });
    }
    await saveJob(tx, ctx, {
      job,
      state: 'waiting-children',
      eventType: 'waiting-children',
      now,
    });
    return true;
  });
}

export async function clearPostgresJobUniqueKey(ctx: PostgresContext, id: JobId): Promise<boolean> {
  return await ctx.sql.begin(async (tx) => {
    const row = await lockJob(tx, ctx, id);
    if (!row?.unique_key) return false;
    const job = decodePostgresJob(row).job;
    const updated = { ...job, uniqueKey: null };
    const now = await databaseNow(tx);
    await tx`
      UPDATE bunqueue_jobs
      SET payload = ${encodePostgresValue(updated)}, unique_key = NULL,
          unique_expires_at = NULL, version = version + 1
      WHERE namespace = ${ctx.config.namespace} AND id = ${String(id)}
    `;
    await recordPostgresEvent(tx, ctx, {
      queue: job.queue,
      type: 'updated',
      jobId: id,
      occurredAt: now,
      job: updated,
      state: row.state,
    });
    return true;
  });
}
