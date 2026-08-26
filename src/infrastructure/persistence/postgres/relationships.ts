import type { TransactionSQL } from 'bun';
import type { Job, JobId } from '../../../domain/types/job';
import { decodePostgresJob, encodePostgresValue } from './codec';
import { databaseNow, recordPostgresEvent, type PostgresContext } from './context';
import { enforcePostgresDlqLimit } from './dlqLifecycle';
import {
  applyPostgresFlowFailure,
  lockPostgresFlowParent,
  type PostgresDlqLimitRequest,
} from './flowFailures';
import type { PostgresJobRow, PostgresJobState } from './types';
import { lockPostgresDependencyCompletions } from './dependencyPromotion';

function recordData(value: unknown): Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? { ...(value as Record<string, unknown>) }
    : {};
}

function attachChild(child: Job, parent: Job): Job {
  return {
    ...child,
    parentId: parent.id,
    data: { ...recordData(child.data), __parentId: String(parent.id), __parentQueue: parent.queue },
  };
}

function detachChild(child: Job): Job {
  const data = recordData(child.data);
  delete data.__parentId;
  delete data.__parentQueue;
  return { ...child, parentId: null, data };
}

function withChildren(parent: Job, childrenIds: JobId[], dependsOn: JobId[]): Job {
  const data = recordData(parent.data);
  if (childrenIds.length > 0) data.__childrenIds = childrenIds.map(String);
  else delete data.__childrenIds;
  return { ...parent, childrenIds, dependsOn, data };
}

async function lockJobs(
  tx: TransactionSQL,
  ctx: PostgresContext,
  ids: readonly JobId[]
): Promise<Map<string, PostgresJobRow>> {
  const values = [...new Set(ids.map(String))].sort();
  const rows = await tx<PostgresJobRow[]>`
    SELECT * FROM bunqueue_jobs
    WHERE namespace = ${ctx.config.namespace} AND id = ANY(${tx.array(values, 'TEXT')})
    ORDER BY id
    FOR UPDATE
  `;
  return new Map(rows.map((row) => [row.id, row]));
}

async function hasDependencyCycle(
  tx: TransactionSQL,
  ctx: PostgresContext,
  childId: JobId,
  parentId: JobId
): Promise<boolean> {
  const rows = await tx<{ found: number }[]>`
    WITH RECURSIVE reachable(id) AS (
      SELECT dependency_id
      FROM bunqueue_dependencies
      WHERE namespace = ${ctx.config.namespace} AND job_id = ${String(childId)}
      UNION
      SELECT dependency.dependency_id
      FROM bunqueue_dependencies AS dependency
      JOIN reachable ON reachable.id = dependency.job_id
      WHERE dependency.namespace = ${ctx.config.namespace}
    )
    SELECT 1 AS found FROM reachable WHERE id = ${String(parentId)} LIMIT 1
  `;
  return rows.length > 0;
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
      AND NOT EXISTS (
        SELECT 1 FROM bunqueue_jobs AS completed
        WHERE completed.namespace = dependency.namespace
          AND completed.id = dependency.dependency_id AND completed.state = 'completed'
      )
  `;
  return Number(row.count);
}

function readyState(job: Job, now: number): PostgresJobState {
  if (job.runAt > now) return 'delayed';
  return job.priority > 0 ? 'prioritized' : 'waiting';
}

/** Atomically attach an existing child to a linkable parent. */
export async function updatePostgresJobParentInTransaction(
  tx: TransactionSQL,
  ctx: PostgresContext,
  childId: JobId,
  parentId: JobId,
  dependencyLockHeld = false
): Promise<PostgresDlqLimitRequest | null> {
  if (childId === parentId) throw new Error('A flow job cannot be its own parent');
  if (!dependencyLockHeld) await lockPostgresDependencyCompletions(tx, ctx, [childId]);
  await lockPostgresFlowParent(tx, ctx, parentId);
  const rows = await lockJobs(tx, ctx, [childId, parentId]);
  const childRow = rows.get(String(childId));
  const parentRow = rows.get(String(parentId));
  if (!childRow) throw new Error(`Child job not found: ${String(childId)}`);
  if (!parentRow) throw new Error(`Parent job not found: ${String(parentId)}`);
  if (['active', 'completed', 'failed'].includes(parentRow.state)) {
    throw new Error(`Parent job ${String(parentId)} is not linkable`);
  }
  const child = decodePostgresJob(childRow).job;
  const parent = decodePostgresJob(parentRow).job;
  if (child.parentId && child.parentId !== parentId) {
    throw new Error(`Child job ${String(childId)} already belongs to another parent`);
  }
  if (await hasDependencyCycle(tx, ctx, childId, parentId)) {
    throw new Error('Flow dependency cycle detected');
  }
  const linkedChild = attachChild(child, parent);
  const linkedParent = withChildren(
    parent,
    [...new Set([...parent.childrenIds, childId])],
    [...new Set([...parent.dependsOn, childId])]
  );
  await tx`
    INSERT INTO bunqueue_dependencies (namespace, job_id, dependency_id)
    VALUES (${ctx.config.namespace}, ${String(parentId)}, ${String(childId)})
    ON CONFLICT DO NOTHING
  `;
  const now = await databaseNow(tx);
  const state =
    (await unresolvedCount(tx, ctx, parentId)) > 0
      ? 'waiting-children'
      : readyState(linkedParent, now);
  await tx`
    UPDATE bunqueue_jobs
    SET payload = ${encodePostgresValue(linkedChild)}, parent_id = ${String(parentId)},
        version = version + 1
    WHERE namespace = ${ctx.config.namespace} AND id = ${String(childId)}
  `;
  await tx`
    UPDATE bunqueue_jobs
    SET payload = ${encodePostgresValue(linkedParent)}, state = ${state}, version = version + 1
    WHERE namespace = ${ctx.config.namespace} AND id = ${String(parentId)}
  `;
  await recordPostgresEvent(tx, ctx, {
    queue: parent.queue,
    type: state === 'waiting-children' ? 'waiting-children' : 'retried',
    jobId: parentId,
    occurredAt: now,
    job: linkedParent,
    state,
  });
  if (childRow.state === 'failed') {
    return await applyPostgresFlowFailure(tx, ctx, linkedChild, childRow.error ?? undefined, now);
  }
  return null;
}

export async function updatePostgresJobParent(
  ctx: PostgresContext,
  childId: JobId,
  parentId: JobId
): Promise<void> {
  const request = await ctx.sql.begin((tx) =>
    updatePostgresJobParentInTransaction(tx, ctx, childId, parentId)
  );
  if (request) await enforcePostgresDlqLimit(ctx, request.queue, request.maxEntries);
}

/** Detach a child and promote the parent when no unresolved edge remains. */
export async function removePostgresChildDependency(
  ctx: PostgresContext,
  childId: JobId
): Promise<boolean> {
  return await ctx.sql.begin(async (tx) => {
    await lockPostgresDependencyCompletions(tx, ctx, [childId]);
    const [relation] = await tx<{ parent_id: string | null }[]>`
      SELECT parent_id FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace} AND id = ${String(childId)}
    `;
    if (!relation) throw new Error(`Job not found: ${String(childId)}`);
    if (!relation.parent_id) throw new Error(`Job ${String(childId)} has no parent`);
    const parentId = relation.parent_id as JobId;
    await lockPostgresFlowParent(tx, ctx, parentId);
    const rows = await lockJobs(tx, ctx, [childId, parentId]);
    const childRow = rows.get(String(childId));
    const parentRow = rows.get(String(parentId));
    if (!childRow || !parentRow) return false;
    const child = decodePostgresJob(childRow).job;
    const parent = decodePostgresJob(parentRow).job;
    if (child.parentId !== parentId || !parent.dependsOn.includes(childId)) return false;
    const detachedChild = detachChild(child);
    const detachedParent = withChildren(
      parent,
      parent.childrenIds.filter((id) => id !== childId),
      parent.dependsOn.filter((id) => id !== childId)
    );
    await tx`
      DELETE FROM bunqueue_dependencies
      WHERE namespace = ${ctx.config.namespace} AND job_id = ${String(parentId)}
        AND dependency_id = ${String(childId)}
    `;
    const now = await databaseNow(tx);
    const state =
      (await unresolvedCount(tx, ctx, parentId)) > 0
        ? 'waiting-children'
        : readyState(detachedParent, now);
    await tx`
      UPDATE bunqueue_jobs
      SET payload = ${encodePostgresValue(detachedChild)}, parent_id = NULL, version = version + 1
      WHERE namespace = ${ctx.config.namespace} AND id = ${String(childId)}
    `;
    await tx`
      UPDATE bunqueue_jobs
      SET payload = ${encodePostgresValue(detachedParent)}, state = ${state}, version = version + 1
      WHERE namespace = ${ctx.config.namespace} AND id = ${String(parentId)}
    `;
    await recordPostgresEvent(tx, ctx, {
      queue: parent.queue,
      type: state === 'waiting-children' ? 'waiting-children' : 'retried',
      jobId: parentId,
      occurredAt: now,
      job: detachedParent,
      state,
    });
    return true;
  });
}
