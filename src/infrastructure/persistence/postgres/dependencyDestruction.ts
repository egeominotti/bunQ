import type { TransactionSQL } from 'bun';
import { jobId, type JobId } from '../../../domain/types/job';
import type { PostgresContext } from './context';
import { lockPostgresDependencyCompletions } from './dependencyPromotion';

const LIVE_CONSUMER_STATES = [
  'waiting',
  'prioritized',
  'delayed',
  'waiting-children',
  'active',
] as const;

function uniqueIds(ids: readonly JobId[]): string[] {
  return [...new Set(ids.map(String))].sort();
}

/** Acquire candidate identity locks in the same order as dependency admission. */
export async function lockPostgresDestructionIdentities(
  tx: TransactionSQL,
  ctx: PostgresContext,
  ids: readonly JobId[]
): Promise<void> {
  const candidates = uniqueIds(ids);
  if (candidates.length === 0) return;
  await lockPostgresDependencyCompletions(tx, ctx, candidates.map(jobId));
}

/** Lock candidate rows and every live consumer row in deterministic order. */
export async function lockPostgresDestructionRows(
  tx: TransactionSQL,
  ctx: PostgresContext,
  ids: readonly JobId[]
): Promise<void> {
  const candidates = uniqueIds(ids);
  if (candidates.length === 0) return;
  const consumers = await tx<{ id: string }[]>`
    SELECT DISTINCT consumer.id
    FROM bunqueue_dependencies AS dependency
    JOIN bunqueue_jobs AS consumer
      ON consumer.namespace = dependency.namespace AND consumer.id = dependency.job_id
    WHERE dependency.namespace = ${ctx.config.namespace}
      AND dependency.dependency_id = ANY(${tx.array(candidates, 'TEXT')})
      AND consumer.state = ANY(${tx.array([...LIVE_CONSUMER_STATES], 'TEXT')})
    ORDER BY consumer.id
  `;
  const rowIds = [...new Set([...candidates, ...consumers.map((row) => row.id)])].sort();
  await tx`
    SELECT id FROM bunqueue_jobs
    WHERE namespace = ${ctx.config.namespace}
      AND id = ANY(${tx.array(rowIds, 'TEXT')})
    ORDER BY id
    FOR UPDATE
  `;
}

/** Acquire identity locks before any target or consumer row lock. */
export async function lockPostgresDestructionCandidates(
  tx: TransactionSQL,
  ctx: PostgresContext,
  ids: readonly JobId[]
): Promise<void> {
  await lockPostgresDestructionIdentities(tx, ctx, ids);
  await lockPostgresDestructionRows(tx, ctx, ids);
}

/** Split a revalidated delete set without orphaning consumers that survive the command. */
export async function partitionPostgresDestructionCandidates(
  tx: TransactionSQL,
  ctx: PostgresContext,
  ids: readonly JobId[],
  deletingConsumerIds: readonly JobId[] = ids
): Promise<{ deletableIds: JobId[]; protectedIds: JobId[] }> {
  const candidates = uniqueIds(ids);
  if (candidates.length === 0) return { deletableIds: [], protectedIds: [] };
  const deleting = uniqueIds(deletingConsumerIds);
  const rows = await tx<{ dependency_id: string }[]>`
    SELECT DISTINCT dependency.dependency_id
    FROM bunqueue_dependencies AS dependency
    JOIN bunqueue_jobs AS consumer
      ON consumer.namespace = dependency.namespace AND consumer.id = dependency.job_id
    WHERE dependency.namespace = ${ctx.config.namespace}
      AND dependency.dependency_id = ANY(${tx.array(candidates, 'TEXT')})
      AND consumer.state = ANY(${tx.array([...LIVE_CONSUMER_STATES], 'TEXT')})
      AND (
        ${deleting.length} = 0
        OR consumer.id <> ALL(${tx.array(deleting, 'TEXT')})
      )
    ORDER BY dependency.dependency_id
  `;
  const protectedSet = new Set(rows.map((row) => row.dependency_id));
  return {
    deletableIds: candidates.filter((id) => !protectedSet.has(id)).map(jobId),
    protectedIds: candidates.filter((id) => protectedSet.has(id)).map(jobId),
  };
}
