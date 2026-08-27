import type { TransactionSQL } from 'bun';
import {
  canAutoRetry,
  getDlqRetryState,
  isDlqEntryExpired,
  retainDlqRetryState,
  scheduleNextRetry,
  type DlqConfig,
} from '../../../domain/types/dlq';
import { MAX_TIMELINE_ENTRIES, jobId, type Job, type JobId } from '../../../domain/types/job';
import { decodePostgresJob, encodePostgresValue } from './codec';
import { databaseNow, recordPostgresEvent, type PostgresContext } from './context';
import {
  lockPostgresDestructionCandidates,
  partitionPostgresDestructionCandidates,
} from './dependencyDestruction';
import { postgresDependenciesSatisfied } from './dependencyPromotion';
import { discoverPostgresDlqRetryPlan, lockPostgresDlqRetryBatch } from './dlqRetryPlan';
import type { PostgresJobRow, PostgresJobState } from './types';

type TerminalRow = Pick<PostgresJobRow, 'id' | 'queue'>;

export async function recordPostgresDlqInvalidation(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queue: string,
  occurredAt: number
): Promise<void> {
  await recordPostgresEvent(tx, ctx, {
    queue,
    type: 'queue-dlq-pruned',
    jobId: jobId(`__queue__:${queue}`),
    occurredAt,
  });
}

async function deleteTerminalRows(
  tx: TransactionSQL,
  ctx: PostgresContext,
  rows: readonly TerminalRow[],
  occurredAt: number
): Promise<void> {
  if (rows.length === 0) return;
  const ids = rows.map((row) => row.id);
  const values = tx.array([...ids], 'TEXT');
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
  const queues = [...new Set(rows.map((row) => row.queue))].sort();
  for (const queue of queues) {
    await recordPostgresDlqInvalidation(tx, ctx, queue, occurredAt);
  }
}

/** Enforce the same oldest-first bounded DLQ policy as the SQLite engine. */
export async function enforcePostgresDlqLimit(
  ctx: PostgresContext,
  queue: string,
  maxEntries: number
): Promise<JobId[]> {
  return await ctx.sql.begin((tx) =>
    enforcePostgresDlqLimitInTransaction(tx, ctx, queue, maxEntries)
  );
}

export async function enforcePostgresDlqLimitInTransaction(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queue: string,
  maxEntries: number
): Promise<JobId[]> {
  const retained = Math.max(1, maxEntries);
  const candidates = await tx<{ id: string }[]>`
    SELECT id FROM bunqueue_jobs
    WHERE namespace = ${ctx.config.namespace} AND queue = ${queue} AND state = 'failed'
    ORDER BY completed_at DESC NULLS LAST, id DESC
    OFFSET ${retained}
  `;
  const candidateIds = candidates.map((row) => jobId(row.id));
  if (candidateIds.length === 0) return [];
  await lockPostgresDestructionCandidates(tx, ctx, candidateIds);
  const rows = await tx<TerminalRow[]>`
    WITH ranked AS (
      SELECT id, queue,
             ROW_NUMBER() OVER (ORDER BY completed_at DESC NULLS LAST, id DESC) AS position
      FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace} AND queue = ${queue} AND state = 'failed'
    )
    SELECT id, queue FROM ranked
    WHERE position > ${retained}
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
  const deletable = new Set(deletableIds.map(String));
  const removed = rows.filter((row) => deletable.has(row.id));
  await deleteTerminalRows(tx, ctx, removed, await databaseNow(tx));
  return removed.map((row) => jobId(row.id));
}

function pendingState(job: Job, resolved: boolean, now: number): PostgresJobState {
  if (!resolved) return 'waiting-children';
  if (job.runAt > now) return 'delayed';
  return job.priority > 0 ? 'prioritized' : 'waiting';
}

interface RetryEntryOptions {
  readonly config: DlqConfig;
  readonly now: number;
  readonly dependencyIds: readonly JobId[];
}

async function retryEntry(
  tx: TransactionSQL,
  ctx: PostgresContext,
  row: PostgresJobRow,
  options: RetryEntryOptions
): Promise<void> {
  const { config, now, dependencyIds } = options;
  const stored = decodePostgresJob(row);
  const entry = stored.dlqEntry;
  if (!entry) return;
  scheduleNextRetry(entry, config);
  const job = stored.job;
  retainDlqRetryState(job, entry);
  job.attempts = 0;
  job.runAt = now;
  job.startedAt = null;
  job.completedAt = null;
  job.stallCount = 0;
  job.lastHeartbeat = now;
  if (job.timeline.length < MAX_TIMELINE_ENTRIES) {
    job.timeline.push({ state: 'waiting', timestamp: now });
  }
  const state = pendingState(job, await postgresDependenciesSatisfied(tx, ctx, dependencyIds), now);
  await tx`
    UPDATE bunqueue_jobs
    SET payload = ${encodePostgresValue(job)}, state = ${state}, attempts = 0,
        run_at = ${now}, started_at = NULL, completed_at = NULL, result = NULL,
        dlq_entry = NULL, dlq_retry_state = ${encodePostgresValue(getDlqRetryState(job))},
        error = NULL, failure_reason = NULL, lease_owner = NULL, lease_broker_id = NULL,
        lease_broker_session_id = NULL,
        lease_token = NULL, lease_until = NULL, unique_key = NULL,
        unique_expires_at = NULL, version = version + 1
    WHERE namespace = ${ctx.config.namespace} AND id = ${row.id}
  `;
  await recordPostgresEvent(tx, ctx, {
    queue: job.queue,
    type: 'retried',
    jobId: job.id,
    occurredAt: now,
    job,
    state,
    dlqEntry: null,
    dlqRetryState: getDlqRetryState(job),
  });
}

export interface PostgresDlqMaintenanceResult {
  readonly retried: number;
  readonly purged: number;
}

async function purgeExpiredEntries(
  ctx: PostgresContext,
  ids: readonly JobId[],
  requestedNow: number
): Promise<number> {
  if (ids.length === 0) return 0;
  return await ctx.sql.begin(async (tx) => {
    await lockPostgresDestructionCandidates(tx, ctx, ids);
    const rows = await tx<PostgresJobRow[]>`
      SELECT * FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace}
        AND state = 'failed' AND dlq_entry IS NOT NULL
        AND id = ANY(${tx.array(ids.map(String), 'TEXT')})
      ORDER BY id
    `;
    const expired = rows.filter((row) => {
      const entry = decodePostgresJob(row).dlqEntry;
      return entry !== null && isDlqEntryExpired(entry, requestedNow);
    });
    const plannedIds = expired.map((row) => jobId(row.id));
    const { deletableIds } = await partitionPostgresDestructionCandidates(
      tx,
      ctx,
      plannedIds,
      plannedIds
    );
    const deletable = new Set(deletableIds.map(String));
    const removed = expired.filter((row) => deletable.has(row.id));
    await deleteTerminalRows(tx, ctx, removed, requestedNow);
    return removed.length;
  });
}

/** Claim due DLQ work with row locks so concurrent brokers execute it once. */
export async function maintainPostgresDlq(
  ctx: PostgresContext,
  limit = 1000
): Promise<PostgresDlqMaintenanceResult> {
  const plan = await discoverPostgresDlqRetryPlan(ctx, limit);
  const phase = await ctx.sql.begin(async (tx) => {
    const batch = await lockPostgresDlqRetryBatch(tx, ctx, plan);
    const now = await databaseNow(tx);
    const expired: JobId[] = [];
    let retried = 0;
    for (const row of batch.rows) {
      const stored = decodePostgresJob(row);
      const entry = stored.dlqEntry;
      if (!entry) continue;
      if (isDlqEntryExpired(entry, now)) {
        expired.push(jobId(row.id));
        continue;
      }
      const dlq = batch.policies.get(row.queue);
      if (!dlq) continue;
      if (!canAutoRetry(entry, dlq, now)) continue;
      await retryEntry(tx, ctx, row, {
        config: dlq,
        now,
        dependencyIds: batch.dependencies.get(row.id) ?? [],
      });
      retried++;
    }
    return { retried, expired, now };
  });
  return {
    retried: phase.retried,
    purged: await purgeExpiredEntries(ctx, phase.expired, phase.now),
  };
}
