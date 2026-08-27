import type { TransactionSQL } from 'bun';
import { createDlqEntry, FailureReason } from '../../../domain/types/dlq';
import type { FlowFailureMode } from '../../../domain/types/flow';
import { MAX_TIMELINE_ENTRIES, type Job, type JobId } from '../../../domain/types/job';
import { postgresAdvisoryLockName } from './advisoryLocks';
import { decodePostgresJob, encodePostgresValue } from './codec';
import { recordPostgresEvent, type PostgresContext } from './context';
import { getPostgresQueuePolicies } from './policies';
import { lockPostgresDependencyCompletions } from './dependencyPromotion';
import type { PostgresJobRow, PostgresJobState } from './types';

const LINKABLE_STATES: readonly PostgresJobState[] = [
  'waiting',
  'prioritized',
  'delayed',
  'waiting-children',
];

function failureMode(job: Job): FlowFailureMode | null {
  if (!job.parentId) return null;
  if (job.failParentOnFailure) return 'fail';
  if (job.continueParentOnFailure) return 'continue';
  if (job.ignoreDependencyOnFailure) return 'ignore';
  if (job.removeDependencyOnFailure) return 'remove';
  return null;
}

function readyState(job: Job, now: number): PostgresJobState {
  if (job.runAt > now) return 'delayed';
  return job.priority > 0 ? 'prioritized' : 'waiting';
}

interface FlowFailureMutation {
  readonly tx: TransactionSQL;
  readonly ctx: PostgresContext;
  readonly parent: Job;
  readonly child: Job;
  readonly error: string;
  readonly now: number;
}

export interface PostgresDlqLimitRequest {
  readonly queue: string;
  readonly maxEntries: number;
}

async function storeFailureValue(
  input: FlowFailureMutation,
  mode: 'ignore' | 'continue'
): Promise<void> {
  const { tx, ctx, child, error, now } = input;
  await tx`
    INSERT INTO bunqueue_flow_failures
      (namespace, parent_id, child_id, child_queue, mode, error, created_at)
    VALUES
      (${ctx.config.namespace}, ${String(child.parentId)}, ${String(child.id)}, ${child.queue},
       ${mode}, ${error}, ${now})
    ON CONFLICT (namespace, parent_id, child_id) DO UPDATE SET
      child_queue = excluded.child_queue,
      mode = excluded.mode,
      error = excluded.error,
      created_at = excluded.created_at
  `;
}

async function unresolvedCount(
  tx: TransactionSQL,
  ctx: PostgresContext,
  parentId: JobId
): Promise<number> {
  const [row] = await tx<{ count: number | string | bigint }[]>`
    SELECT COUNT(*)::bigint AS count
    FROM bunqueue_dependencies AS dependency
    WHERE dependency.namespace = ${ctx.config.namespace}
      AND dependency.job_id = ${String(parentId)}
      AND NOT EXISTS (
        SELECT 1 FROM bunqueue_completions AS completion
        WHERE completion.namespace = dependency.namespace
          AND completion.job_id = dependency.dependency_id
      )
  `;
  return Number(row.count);
}

async function failParent(input: FlowFailureMutation): Promise<PostgresDlqLimitRequest> {
  const { tx, ctx, parent, child, error, now } = input;
  const message = `Child job ${String(child.id)} failed: ${error}`;
  parent.completedAt = now;
  if (parent.timeline.length < MAX_TIMELINE_ENTRIES) {
    parent.timeline.push({ state: 'failed', timestamp: now, error: message });
  }
  const { dlq } = await getPostgresQueuePolicies(tx, ctx, parent.queue);
  const entry = createDlqEntry(parent, FailureReason.Unknown, message, dlq, now);
  await tx`
    DELETE FROM bunqueue_dependencies
    WHERE namespace = ${ctx.config.namespace} AND job_id = ${String(parent.id)}
  `;
  await tx`
    DELETE FROM bunqueue_flow_failures
    WHERE namespace = ${ctx.config.namespace} AND parent_id = ${String(parent.id)}
  `;
  await tx`
    UPDATE bunqueue_jobs
    SET payload = ${encodePostgresValue(parent)}, state = 'failed', completed_at = ${now},
        error = ${message}, failure_reason = ${FailureReason.Unknown},
        dlq_entry = ${encodePostgresValue(entry)}, dlq_retry_state = NULL,
        version = version + 1
    WHERE namespace = ${ctx.config.namespace} AND id = ${String(parent.id)}
  `;
  await recordPostgresEvent(tx, ctx, {
    queue: parent.queue,
    type: 'failed',
    jobId: parent.id,
    occurredAt: now,
    job: parent,
    state: 'failed',
    error: message,
    dlqEntry: entry,
    dlqRetryState: null,
  });
  return { queue: parent.queue, maxEntries: dlq.maxEntries };
}

async function releaseDependencies(
  input: FlowFailureMutation,
  mode: 'remove' | 'ignore' | 'continue'
): Promise<void> {
  const { tx, ctx, parent, child, now } = input;
  if (mode === 'ignore' || mode === 'continue') {
    await storeFailureValue(input, mode);
  }
  const released = mode === 'continue' ? [...parent.dependsOn] : [child.id];
  const remaining = parent.dependsOn.filter((id) => !released.includes(id));
  await tx`
    DELETE FROM bunqueue_dependencies
    WHERE namespace = ${ctx.config.namespace}
      AND job_id = ${String(parent.id)}
      AND dependency_id = ANY(${tx.array(released.map(String), 'TEXT')})
  `;
  const resolved = (await unresolvedCount(tx, ctx, parent.id)) === 0;
  const updated = { ...parent, dependsOn: remaining };
  const state = resolved ? readyState(updated, now) : 'waiting-children';
  await tx`
    UPDATE bunqueue_jobs
    SET payload = ${encodePostgresValue(updated)}, state = ${state}, version = version + 1
    WHERE namespace = ${ctx.config.namespace} AND id = ${String(parent.id)}
  `;
  if (resolved) {
    await recordPostgresEvent(tx, ctx, {
      queue: parent.queue,
      type: 'retried',
      jobId: parent.id,
      occurredAt: now,
      job: updated,
      state,
    });
  }
}

/** Serialize mutations involving one flow parent across brokers. */
export async function lockPostgresFlowParent(
  tx: TransactionSQL,
  ctx: PostgresContext,
  parentId: JobId
): Promise<void> {
  await lockPostgresFlowParents(tx, ctx, [parentId]);
}

/** Acquire a flow-parent batch in canonical order with one database round trip. */
export async function lockPostgresFlowParents(
  tx: TransactionSQL,
  ctx: PostgresContext,
  parentIds: readonly JobId[]
): Promise<void> {
  const lockNames = [...new Set(parentIds.map(String))].map((id) =>
    postgresAdvisoryLockName('flow', ctx.config.namespace, id)
  );
  if (lockNames.length === 0) return;
  await tx`
    WITH ordered_ids AS MATERIALIZED (
      SELECT DISTINCT hashtextextended(lock_name, 0) AS lock_key
      FROM unnest(${tx.array(lockNames, 'TEXT')}) AS parent(lock_name)
      ORDER BY lock_key
    ), locked_ids AS MATERIALIZED (
      SELECT lock_key, pg_advisory_xact_lock(lock_key) AS acquired
      FROM ordered_ids
      ORDER BY lock_key
    )
    SELECT COUNT(*) FROM locked_ids
  `;
}

/** Lock child attachment, then its current parent, before a terminal row mutation. */
export async function lockPostgresJobFlowParent(
  tx: TransactionSQL,
  ctx: PostgresContext,
  childId: JobId
): Promise<void> {
  await lockPostgresDependencyCompletions(tx, ctx, [childId]);
  const [row] = await tx<{ parent_id: string | null }[]>`
    SELECT parent_id FROM bunqueue_jobs
    WHERE namespace = ${ctx.config.namespace} AND id = ${String(childId)}
  `;
  if (row?.parent_id) await lockPostgresFlowParent(tx, ctx, row.parent_id as JobId);
}

/** Apply a terminal child's configured parent policy in the same transaction. */
export async function applyPostgresFlowFailure(
  tx: TransactionSQL,
  ctx: PostgresContext,
  child: Job,
  errorValue: string | undefined,
  now: number
): Promise<PostgresDlqLimitRequest | null> {
  const mode = failureMode(child);
  if (!mode || !child.parentId) return null;
  const rows = await tx<PostgresJobRow[]>`
    SELECT * FROM bunqueue_jobs
    WHERE namespace = ${ctx.config.namespace} AND id = ${String(child.parentId)}
    FOR UPDATE
  `;
  const row = rows[0];
  if (!row || !LINKABLE_STATES.includes(row.state)) return null;
  const parent = decodePostgresJob(row).job;
  const error = errorValue ?? 'unknown error';
  const input = { tx, ctx, parent, child, error, now };
  if (mode === 'fail') return await failParent(input);
  await releaseDependencies(input, mode);
  return null;
}

export async function clearPostgresParentFlowFailures(
  tx: TransactionSQL,
  ctx: PostgresContext,
  parentId: JobId
): Promise<void> {
  await tx`
    DELETE FROM bunqueue_flow_failures
    WHERE namespace = ${ctx.config.namespace} AND parent_id = ${String(parentId)}
  `;
}

export async function getPostgresFlowFailureValues(
  ctx: PostgresContext,
  parentId: JobId,
  mode: 'ignore' | 'continue'
): Promise<Record<string, string>> {
  const rows = await ctx.sql<Array<{ child_id: string; child_queue: string; error: string }>>`
    SELECT child_id, child_queue, error
    FROM bunqueue_flow_failures
    WHERE namespace = ${ctx.config.namespace}
      AND parent_id = ${String(parentId)} AND mode = ${mode}
    ORDER BY created_at, child_id
  `;
  return Object.fromEntries(rows.map((row) => [`${row.child_queue}:${row.child_id}`, row.error]));
}
