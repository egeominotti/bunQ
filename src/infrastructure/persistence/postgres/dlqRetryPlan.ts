import type { TransactionSQL } from 'bun';
import type { DlqConfig } from '../../../domain/types/dlq';
import { jobId, type JobId } from '../../../domain/types/job';
import { lockPostgresDependencyCompletions } from './dependencyPromotion';
import { decodePostgresDlqConfig } from './policies';
import type { PostgresContext } from './context';
import type { PostgresJobRow } from './types';

interface DiscoveryRow {
  readonly id: string;
  readonly queue: string;
  readonly dependency_id: string | null;
}

interface PolicyRow {
  readonly queue: string;
  readonly dlq_config: Uint8Array | null;
}

interface DependencyRow {
  readonly job_id: string;
  readonly dependency_id: string;
}

export interface PostgresDlqRetryPlan {
  readonly candidates: ReadonlyArray<{ id: JobId; queue: string }>;
  readonly identityIds: readonly JobId[];
}

export interface LockedPostgresDlqRetryBatch {
  readonly rows: readonly PostgresJobRow[];
  readonly policies: ReadonlyMap<string, DlqConfig>;
  readonly dependencies: ReadonlyMap<string, readonly JobId[]>;
}

/** Discover identities before taking any row lock so advisory locks stay first. */
export async function discoverPostgresDlqRetryPlan(
  ctx: PostgresContext,
  limit: number
): Promise<PostgresDlqRetryPlan> {
  const rows = await ctx.sql<DiscoveryRow[]>`
    WITH candidates AS MATERIALIZED (
      SELECT id, queue, completed_at
      FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace}
        AND state = 'failed' AND dlq_entry IS NOT NULL
      ORDER BY completed_at, id
      LIMIT ${Math.max(1, Math.floor(limit))}
    )
    SELECT candidate.id, candidate.queue, dependency.dependency_id
    FROM candidates AS candidate
    LEFT JOIN bunqueue_dependencies AS dependency
      ON dependency.namespace = ${ctx.config.namespace}
      AND dependency.job_id = candidate.id
    ORDER BY candidate.completed_at, candidate.id, dependency.dependency_id
  `;
  const candidates = new Map<string, { id: JobId; queue: string }>();
  const identities = new Set<string>();
  for (const row of rows) {
    if (!candidates.has(row.id)) {
      candidates.set(row.id, { id: jobId(row.id), queue: row.queue });
      identities.add(row.id);
    }
    if (row.dependency_id !== null) identities.add(row.dependency_id);
  }
  return {
    candidates: [...candidates.values()],
    identityIds: [...identities].sort().map(jobId),
  };
}

async function lockPolicies(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queues: readonly string[]
): Promise<Map<string, DlqConfig>> {
  const uniqueQueues = [...new Set(queues)].sort();
  if (uniqueQueues.length === 0) return new Map();
  const rows = await tx<PolicyRow[]>`
    SELECT queue, dlq_config
    FROM bunqueue_queue_state
    WHERE namespace = ${ctx.config.namespace}
      AND queue = ANY(${tx.array(uniqueQueues, 'TEXT')})
    ORDER BY queue
    FOR SHARE
  `;
  const encoded = new Map(rows.map((row) => [row.queue, row.dlq_config]));
  return new Map(
    uniqueQueues.map((queue) => [queue, decodePostgresDlqConfig(queue, encoded.get(queue) ?? null)])
  );
}

async function currentDependencies(
  tx: TransactionSQL,
  ctx: PostgresContext,
  ids: readonly string[]
): Promise<Map<string, JobId[]>> {
  if (ids.length === 0) return new Map();
  const rows = await tx<DependencyRow[]>`
    SELECT job_id, dependency_id
    FROM bunqueue_dependencies
    WHERE namespace = ${ctx.config.namespace}
      AND job_id = ANY(${tx.array([...ids], 'TEXT')})
    ORDER BY job_id, dependency_id
  `;
  const result = new Map<string, JobId[]>();
  for (const row of rows) {
    const values = result.get(row.job_id) ?? [];
    values.push(jobId(row.dependency_id));
    result.set(row.job_id, values);
  }
  return result;
}

/** Lock policies and identities before rows, then reject any late dependency edge. */
export async function lockPostgresDlqRetryBatch(
  tx: TransactionSQL,
  ctx: PostgresContext,
  plan: PostgresDlqRetryPlan
): Promise<LockedPostgresDlqRetryBatch> {
  const policies = await lockPolicies(
    tx,
    ctx,
    plan.candidates.map((candidate) => candidate.queue)
  );
  await lockPostgresDependencyCompletions(tx, ctx, plan.identityIds);
  const ids = plan.candidates.map((candidate) => String(candidate.id));
  if (ids.length === 0) return { rows: [], policies, dependencies: new Map() };
  const plannedQueues = new Map(
    plan.candidates.map((candidate) => [String(candidate.id), candidate.queue])
  );
  const rows = await tx<PostgresJobRow[]>`
    SELECT job.*
    FROM bunqueue_jobs AS job
    WHERE job.namespace = ${ctx.config.namespace}
      AND job.id = ANY(${tx.array(ids, 'TEXT')})
      AND job.state = 'failed' AND job.dlq_entry IS NOT NULL
    ORDER BY array_position(${tx.array(ids, 'TEXT')}, job.id)
    FOR UPDATE OF job SKIP LOCKED
  `;
  const dependencies = await currentDependencies(
    tx,
    ctx,
    rows.map((row) => row.id)
  );
  const locked = new Set(plan.identityIds.map(String));
  const revalidated = rows.filter(
    (row) =>
      plannedQueues.get(row.id) === row.queue &&
      (dependencies.get(row.id) ?? []).every((dependencyId) => locked.has(String(dependencyId)))
  );
  return { rows: revalidated, policies, dependencies };
}
