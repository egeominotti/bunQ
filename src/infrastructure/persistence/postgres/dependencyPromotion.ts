import type { TransactionSQL } from 'bun';
import { jobId, MAX_TIMELINE_ENTRIES, type Job, type JobId } from '../../../domain/types/job';
import { postgresAdvisoryLockName } from './advisoryLocks';
import { recordPostgresJobEvents } from './batchEvents';
import { decodePostgresJob, encodePostgresValue, postgresByteaBase64 } from './codec';
import type { PostgresContext } from './context';
import { lockPostgresFlowParents } from './flowFailures';
import type { PostgresJobRow, PostgresJobState } from './types';

type DependencyReadyState = Extract<PostgresJobState, 'waiting' | 'prioritized' | 'delayed'>;

interface PreparedDependencyPromotion {
  readonly job: Job;
  readonly state: DependencyReadyState;
  readonly payload: Uint8Array;
}

function uniqueIds(ids: readonly JobId[]): string[] {
  return [...new Set(ids.map(String))].sort();
}

/** Serialize completion evidence readers and writers for the supplied generations. */
export async function lockPostgresDependencyCompletions(
  tx: TransactionSQL,
  ctx: PostgresContext,
  dependencyIds: readonly JobId[]
): Promise<void> {
  const lockNames = uniqueIds(dependencyIds).map((id) =>
    postgresAdvisoryLockName('dependency-completion', ctx.config.namespace, id)
  );
  if (lockNames.length === 0) return;
  await tx`
    WITH ordered_ids AS MATERIALIZED (
      SELECT DISTINCT hashtextextended(lock_name, 0) AS lock_key
      FROM unnest(${tx.array(lockNames, 'TEXT')}) AS dependency(lock_name)
      ORDER BY lock_key
    ), locked_ids AS MATERIALIZED (
      SELECT lock_key, pg_advisory_xact_lock(lock_key) AS acquired
      FROM ordered_ids
      ORDER BY lock_key
    )
    SELECT COUNT(*) FROM locked_ids
  `;
}

/** Safely probe a late-discovered identity without inverting row/advisory lock order. */
export async function tryLockPostgresDependencyCompletions(
  tx: TransactionSQL,
  ctx: PostgresContext,
  dependencyIds: readonly JobId[]
): Promise<boolean> {
  const lockNames = uniqueIds(dependencyIds).map((id) =>
    postgresAdvisoryLockName('dependency-completion', ctx.config.namespace, id)
  );
  if (lockNames.length === 0) return true;
  const [row] = await tx<{ acquired: boolean }[]>`
    WITH ordered_ids AS MATERIALIZED (
      SELECT DISTINCT hashtextextended(lock_name, 0) AS lock_key
      FROM unnest(${tx.array(lockNames, 'TEXT')}) AS dependency(lock_name)
      ORDER BY lock_key
    ), locked_ids AS MATERIALIZED (
      SELECT lock_key, pg_try_advisory_xact_lock(lock_key) AS acquired
      FROM ordered_ids
      ORDER BY lock_key
    )
    SELECT COALESCE(BOOL_AND(acquired), TRUE) AS acquired FROM locked_ids
  `;
  return row.acquired;
}

/** Reject dependency references that have no job row or durable completion evidence. */
export async function assertPostgresDependenciesExist(
  tx: TransactionSQL,
  ctx: PostgresContext,
  dependencyIds: readonly JobId[]
): Promise<void> {
  const ids = [...new Set(dependencyIds.map(String))];
  if (ids.length === 0) return;
  const rows = await tx<{ dependency_id: string }[]>`
    WITH requested AS MATERIALIZED (
      SELECT dependency_id, ordinal
      FROM unnest(${tx.array(ids, 'TEXT')}) WITH ORDINALITY
        AS dependency(dependency_id, ordinal)
    )
    SELECT requested.dependency_id
    FROM requested
    WHERE NOT EXISTS (
      SELECT 1 FROM bunqueue_jobs AS job
      WHERE job.namespace = ${ctx.config.namespace} AND job.id = requested.dependency_id
    ) AND NOT EXISTS (
      SELECT 1 FROM bunqueue_completions AS completion
      WHERE completion.namespace = ${ctx.config.namespace}
        AND completion.job_id = requested.dependency_id
    )
    ORDER BY requested.ordinal
    LIMIT 1
  `;
  if (rows[0]) throw new Error(`Dependency job not found: ${rows[0].dependency_id}`);
}

/** Read dependency evidence while the caller holds every corresponding completion lock. */
export async function postgresDependenciesSatisfied(
  tx: TransactionSQL,
  ctx: PostgresContext,
  dependencyIds: readonly JobId[]
): Promise<boolean> {
  const ids = uniqueIds(dependencyIds);
  if (ids.length === 0) return true;
  const [row] = await tx<{ count: number | string | bigint }[]>`
    SELECT COUNT(DISTINCT dependency_id)::bigint AS count
    FROM (
      SELECT job_id AS dependency_id
      FROM bunqueue_completions
      WHERE namespace = ${ctx.config.namespace}
        AND job_id = ANY(${tx.array(ids, 'TEXT')})
      UNION
      SELECT id AS dependency_id
      FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace}
        AND state = 'completed'
        AND id = ANY(${tx.array(ids, 'TEXT')})
    ) completed_dependencies
  `;
  return Number(row.count) === ids.length;
}

/** Pure scheduling policy shared by completion-time promotion and pull-time repair. */
export function postgresDependencyReadyState(job: Job, now: number): DependencyReadyState {
  if (job.runAt > now) return 'delayed';
  return job.priority > 0 ? 'prioritized' : 'waiting';
}

function preparePromotion(row: PostgresJobRow, now: number): PreparedDependencyPromotion {
  const job = decodePostgresJob(row).job;
  const state = postgresDependencyReadyState(job, now);
  if (job.timeline.at(-1)?.state !== state && job.timeline.length < MAX_TIMELINE_ENTRIES) {
    job.timeline.push({ state, timestamp: now });
  }
  return { job, state, payload: encodePostgresValue(job) };
}

async function consumerIdsForDependencies(
  tx: TransactionSQL,
  ctx: PostgresContext,
  dependencyIds: readonly JobId[]
): Promise<JobId[]> {
  const ids = uniqueIds(dependencyIds);
  if (ids.length === 0) return [];
  const rows = await tx<{ parent_id: string }[]>`
    SELECT DISTINCT dependency.job_id AS parent_id
    FROM bunqueue_dependencies AS dependency
    JOIN bunqueue_jobs AS parent
      ON parent.namespace = dependency.namespace AND parent.id = dependency.job_id
    WHERE dependency.namespace = ${ctx.config.namespace}
      AND dependency.dependency_id = ANY(${tx.array(ids, 'TEXT')})
      AND parent.state = 'waiting-children'
    ORDER BY dependency.job_id
  `;
  return rows.map((row) => jobId(row.parent_id));
}

async function resolvedRows(
  tx: TransactionSQL,
  ctx: PostgresContext,
  parentIds: readonly JobId[]
): Promise<PostgresJobRow[]> {
  const ids = uniqueIds(parentIds);
  if (ids.length === 0) return [];
  return await tx<PostgresJobRow[]>`
    SELECT parent.*
    FROM bunqueue_jobs AS parent
    WHERE parent.namespace = ${ctx.config.namespace}
      AND parent.id = ANY(${tx.array(ids, 'TEXT')})
      AND parent.state = 'waiting-children'
      AND NOT EXISTS (
        SELECT 1
        FROM bunqueue_dependencies AS dependency
        WHERE dependency.namespace = parent.namespace
          AND dependency.job_id = parent.id
          AND NOT EXISTS (
            SELECT 1 FROM bunqueue_completions AS completion
            WHERE completion.namespace = dependency.namespace
              AND completion.job_id = dependency.dependency_id
          )
      )
    ORDER BY parent.id
    FOR UPDATE OF parent
  `;
}

async function persistPromotions(
  tx: TransactionSQL,
  ctx: PostgresContext,
  prepared: readonly PreparedDependencyPromotion[]
): Promise<void> {
  if (prepared.length === 0) return;
  await tx`
    UPDATE bunqueue_jobs AS parent
    SET payload = decode(batch.payload_base64, 'base64'),
        state = batch.state,
        version = parent.version + 1
    FROM unnest(
      ${tx.array(
        prepared.map(({ job }) => String(job.id)),
        'TEXT'
      )},
      ${tx.array(
        prepared.map(({ payload }) => postgresByteaBase64(payload)),
        'TEXT'
      )},
      ${tx.array(
        prepared.map(({ state }) => state),
        'TEXT'
      )}
    ) AS batch(id, payload_base64, state)
    WHERE parent.namespace = ${ctx.config.namespace} AND parent.id = batch.id
  `;
}

/**
 * Discover and serialize every dependency consumer before completion locks child rows.
 * The returned immutable plan is safe to execute after completion evidence is stored.
 */
export async function lockPostgresCompletionConsumers(
  tx: TransactionSQL,
  ctx: PostgresContext,
  dependencyIds: readonly JobId[]
): Promise<readonly JobId[]> {
  const parentIds = await consumerIdsForDependencies(tx, ctx, dependencyIds);
  await lockPostgresFlowParents(tx, ctx, parentIds);
  return parentIds;
}

/** Build a completion-scoped command after acquiring its parent locks. */
export async function planPostgresDependencyCompletion(
  tx: TransactionSQL,
  ctx: PostgresContext,
  dependencyIds: readonly JobId[]
): Promise<{ promote(now: number): Promise<JobId[]> }> {
  await lockPostgresDependencyCompletions(tx, ctx, dependencyIds);
  const parentIds = await lockPostgresCompletionConsumers(tx, ctx, dependencyIds);
  return {
    promote: (now) => promotePostgresCompletionConsumers(tx, ctx, parentIds, now),
  };
}

/** Promote all resolved members of a previously locked completion-consumer plan. */
export async function promotePostgresCompletionConsumers(
  tx: TransactionSQL,
  ctx: PostgresContext,
  parentIds: readonly JobId[],
  now: number
): Promise<JobId[]> {
  const prepared = (await resolvedRows(tx, ctx, parentIds)).map((row) =>
    preparePromotion(row, now)
  );
  await persistPromotions(tx, ctx, prepared);
  await recordPostgresJobEvents(
    tx,
    ctx,
    prepared.map(({ job, state }) => ({ job, type: 'retried', state })),
    now
  );
  return prepared.map(({ job }) => job.id);
}

/** Pull-time convergence path for rows left behind by interrupted or legacy writers. */
export async function repairResolvedPostgresDependencyConsumers(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queue: string,
  now: number
): Promise<JobId[]> {
  const rows = await tx<{ parent_id: string }[]>`
    SELECT parent.id AS parent_id
    FROM bunqueue_jobs AS parent
    WHERE parent.namespace = ${ctx.config.namespace}
      AND parent.queue = ${queue}
      AND parent.state = 'waiting-children'
      AND NOT EXISTS (
        SELECT 1 FROM bunqueue_dependencies AS dependency
        WHERE dependency.namespace = parent.namespace AND dependency.job_id = parent.id
          AND NOT EXISTS (
            SELECT 1 FROM bunqueue_completions AS completion
            WHERE completion.namespace = dependency.namespace
              AND completion.job_id = dependency.dependency_id
          )
      )
    ORDER BY parent.id
  `;
  const parentIds = rows.map((row) => jobId(row.parent_id));
  await lockPostgresFlowParents(tx, ctx, parentIds);
  return await promotePostgresCompletionConsumers(tx, ctx, parentIds, now);
}
