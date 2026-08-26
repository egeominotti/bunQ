import type { TransactionSQL } from 'bun';
import { jobId, MAX_TIMELINE_ENTRIES, type Job, type JobId } from '../../../domain/types/job';
import { recordPostgresJobEvents } from './batchEvents';
import { decodePostgresJob, encodePostgresValue, postgresByteaBase64 } from './codec';
import { databaseNow, type PostgresContext } from './context';
import { lockPostgresDependencyCompletions } from './dependencyPromotion';
import { lockPostgresFlowParent } from './flowFailures';
import type { PostgresJobRow, PostgresJobState } from './types';

const REMOVABLE_STATES = new Set<PostgresJobState>([
  'waiting',
  'prioritized',
  'delayed',
  'waiting-children',
]);
const LIVE_CONSUMER_STATES: readonly PostgresJobState[] = [
  'waiting',
  'prioritized',
  'delayed',
  'waiting-children',
  'active',
];

function recordData(value: unknown): Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? { ...(value as Record<string, unknown>) }
    : {};
}

function uniqueIds(ids: readonly (JobId | string)[]): string[] {
  return [...new Set(ids.map(String))].sort();
}

function detachChild(job: Job): Job {
  const data = recordData(job.data);
  delete data.__parentId;
  delete data.__parentQueue;
  return { ...job, parentId: null, data };
}

function updateParent(job: Job, removedIds: ReadonlySet<string>): Job {
  const childrenIds = job.childrenIds.filter((id) => !removedIds.has(String(id)));
  const dependsOn = job.dependsOn.filter((id) => !removedIds.has(String(id)));
  const data = recordData(job.data);
  if (childrenIds.length > 0) data.__childrenIds = childrenIds.map(String);
  else delete data.__childrenIds;
  return { ...job, childrenIds, dependsOn, data };
}

function readyState(job: Job, now: number): PostgresJobState {
  if (job.runAt > now) return 'delayed';
  return job.priority > 0 ? 'prioritized' : 'waiting';
}

async function discoverChildren(
  tx: TransactionSQL,
  ctx: PostgresContext,
  parentId: JobId
): Promise<string[]> {
  const [parent] = await tx<PostgresJobRow[]>`
    SELECT * FROM bunqueue_jobs
    WHERE namespace = ${ctx.config.namespace} AND id = ${String(parentId)}
  `;
  if (!parent) return [];
  const linked = await tx<{ child_id: string }[]>`
    SELECT id AS child_id
    FROM bunqueue_jobs
    WHERE namespace = ${ctx.config.namespace} AND parent_id = ${String(parentId)}
    UNION
    SELECT dependency_id AS child_id
    FROM bunqueue_dependencies
    WHERE namespace = ${ctx.config.namespace} AND job_id = ${String(parentId)}
  `;
  return uniqueIds([
    ...decodePostgresJob(parent).job.childrenIds,
    ...linked.map((row) => row.child_id),
  ]);
}

async function lockRows(
  tx: TransactionSQL,
  ctx: PostgresContext,
  parentId: JobId,
  candidates: readonly string[]
): Promise<Map<string, PostgresJobRow>> {
  const consumers = await tx<{ id: string }[]>`
    SELECT DISTINCT consumer.id
    FROM bunqueue_dependencies AS dependency
    JOIN bunqueue_jobs AS consumer
      ON consumer.namespace = dependency.namespace AND consumer.id = dependency.job_id
    WHERE dependency.namespace = ${ctx.config.namespace}
      AND dependency.dependency_id = ANY(${tx.array([...candidates], 'TEXT')})
      AND consumer.state = ANY(${tx.array([...LIVE_CONSUMER_STATES], 'TEXT')})
    ORDER BY consumer.id
  `;
  const ids = uniqueIds([parentId, ...candidates, ...consumers.map((row) => row.id)]);
  const rows = await tx<PostgresJobRow[]>`
    SELECT * FROM bunqueue_jobs
    WHERE namespace = ${ctx.config.namespace} AND id = ANY(${tx.array(ids, 'TEXT')})
    ORDER BY id
    FOR UPDATE
  `;
  return new Map(rows.map((row) => [row.id, row]));
}

async function protectedCandidates(
  tx: TransactionSQL,
  ctx: PostgresContext,
  parentId: JobId,
  removable: ReadonlySet<string>
): Promise<Set<string>> {
  if (removable.size === 0) return new Set();
  const edges = await tx<{ consumer_id: string; dependency_id: string }[]>`
    SELECT dependency.job_id AS consumer_id, dependency.dependency_id
    FROM bunqueue_dependencies AS dependency
    JOIN bunqueue_jobs AS consumer
      ON consumer.namespace = dependency.namespace AND consumer.id = dependency.job_id
    WHERE dependency.namespace = ${ctx.config.namespace}
      AND dependency.dependency_id = ANY(${tx.array([...removable], 'TEXT')})
      AND consumer.state = ANY(${tx.array([...LIVE_CONSUMER_STATES], 'TEXT')})
    ORDER BY dependency.job_id, dependency.dependency_id
  `;
  const protectedIds = new Set<string>();
  for (const edge of edges) {
    if (edge.consumer_id !== String(parentId) && !removable.has(edge.consumer_id)) {
      protectedIds.add(edge.dependency_id);
    }
  }
  let changed = true;
  while (changed) {
    changed = false;
    for (const edge of edges) {
      if (!protectedIds.has(edge.consumer_id) || protectedIds.has(edge.dependency_id)) continue;
      protectedIds.add(edge.dependency_id);
      changed = true;
    }
  }
  return protectedIds;
}

async function deleteJobs(
  tx: TransactionSQL,
  ctx: PostgresContext,
  ids: readonly string[]
): Promise<void> {
  if (ids.length === 0) return;
  await tx`
    DELETE FROM bunqueue_dependencies
    WHERE namespace = ${ctx.config.namespace} AND job_id = ANY(${tx.array([...ids], 'TEXT')})
  `;
  await tx`
    DELETE FROM bunqueue_job_logs
    WHERE namespace = ${ctx.config.namespace} AND job_id = ANY(${tx.array([...ids], 'TEXT')})
  `;
  await tx`
    DELETE FROM bunqueue_flow_failures
    WHERE namespace = ${ctx.config.namespace} AND parent_id = ANY(${tx.array([...ids], 'TEXT')})
  `;
  await tx`
    DELETE FROM bunqueue_jobs
    WHERE namespace = ${ctx.config.namespace} AND id = ANY(${tx.array([...ids], 'TEXT')})
  `;
}

async function detachJobs(
  tx: TransactionSQL,
  ctx: PostgresContext,
  jobs: readonly Job[]
): Promise<void> {
  if (jobs.length === 0) return;
  await tx`
    UPDATE bunqueue_jobs AS child
    SET payload = decode(batch.payload_base64, 'base64'), parent_id = NULL,
        version = child.version + 1
    FROM unnest(
      ${tx.array(
        jobs.map((job) => String(job.id)),
        'TEXT'
      )},
      ${tx.array(
        jobs.map((job) => postgresByteaBase64(encodePostgresValue(job))),
        'TEXT'
      )}
    ) AS batch(id, payload_base64)
    WHERE child.namespace = ${ctx.config.namespace} AND child.id = batch.id
  `;
}

async function unresolvedParentDependencies(
  tx: TransactionSQL,
  ctx: PostgresContext,
  parentId: JobId
): Promise<boolean> {
  const [row] = await tx<{ unresolved: boolean }[]>`
    SELECT EXISTS (
      SELECT 1 FROM bunqueue_dependencies AS dependency
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
    ) AS unresolved
  `;
  return row.unresolved;
}

/** Atomically remove only direct, pending children without orphaning live consumers. */
export async function removePostgresUnprocessedChildren(
  ctx: PostgresContext,
  parentId: JobId
): Promise<JobId[]> {
  return await ctx.sql.begin(async (tx) => {
    const candidates = await discoverChildren(tx, ctx, parentId);
    if (candidates.length === 0) return [];
    await lockPostgresDependencyCompletions(tx, ctx, candidates.map(jobId));
    await lockPostgresFlowParent(tx, ctx, parentId);
    const rows = await lockRows(tx, ctx, parentId, candidates);
    const parentRow = rows.get(String(parentId));
    if (!parentRow) return [];

    const removable = new Set(
      candidates.filter((id) => {
        const row = rows.get(id);
        return row?.parent_id === String(parentId) && REMOVABLE_STATES.has(row.state);
      })
    );
    if (removable.size === 0) return [];
    const protectedIds = await protectedCandidates(tx, ctx, parentId, removable);
    const deletedIds = [...removable].filter((id) => !protectedIds.has(id));
    const detachedRows = [...protectedIds]
      .map((id) => rows.get(id))
      .filter((row): row is PostgresJobRow => row !== undefined);
    const deletedRows = deletedIds
      .map((id) => rows.get(id))
      .filter((row): row is PostgresJobRow => row !== undefined);
    const detachedJobs = detachedRows.map((row) => detachChild(decodePostgresJob(row).job));
    const removed = new Set(removable);

    await tx`
      DELETE FROM bunqueue_dependencies
      WHERE namespace = ${ctx.config.namespace} AND job_id = ${String(parentId)}
        AND dependency_id = ANY(${tx.array([...removed], 'TEXT')})
    `;
    await deleteJobs(tx, ctx, deletedIds);
    await detachJobs(tx, ctx, detachedJobs);

    const now = await databaseNow(tx);
    const parent = updateParent(decodePostgresJob(parentRow).job, removed);
    const promote =
      parentRow.state === 'waiting-children' &&
      !(await unresolvedParentDependencies(tx, ctx, parentId));
    const parentState = promote ? readyState(parent, now) : parentRow.state;
    if (promote && parent.timeline.at(-1)?.state !== parentState) {
      if (parent.timeline.length < MAX_TIMELINE_ENTRIES) {
        parent.timeline.push({ state: parentState, timestamp: now });
      }
    }
    await tx`
      UPDATE bunqueue_jobs
      SET payload = ${encodePostgresValue(parent)}, state = ${parentState}, version = version + 1
      WHERE namespace = ${ctx.config.namespace} AND id = ${String(parentId)}
    `;
    await recordPostgresJobEvents(
      tx,
      ctx,
      [
        ...deletedRows.map((row) => ({
          job: decodePostgresJob(row).job,
          type: 'removed',
          state: row.state,
          removed: true,
        })),
        ...detachedRows.map((row, index) => ({
          job: detachedJobs[index],
          type: 'updated',
          state: row.state,
        })),
        { job: parent, type: promote ? 'retried' : 'updated', state: parentState },
      ],
      now
    );
    return [...removed].map(jobId);
  });
}
