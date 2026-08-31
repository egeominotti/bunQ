import type { TransactionSQL } from 'bun';
import { MAX_TIMELINE_ENTRIES, type Job, type JobId } from '../../../domain/types/job';
import { decodePostgresJob, encodePostgresValue, postgresStateForJob } from './codec';
import { databaseNow, recordPostgresEvent, type PostgresContext } from './context';
import {
  clearPostgresDeduplication,
  extendPostgresDeduplication,
  recordPostgresDeduplicationRemoval,
} from './deduplication';
import { updatePostgresJobParentInTransaction } from './relationships';
import {
  assertPostgresDependenciesExist,
  lockPostgresDependencyCompletions,
  postgresDependenciesSatisfied,
  tryLockPostgresDependencyCompletions,
} from './dependencyPromotion';
import {
  assertNoLivePostgresCompletionConsumers,
  retirePostgresCompletionGenerations,
} from './completionLifecycle';
import type { PostgresJobRow, PostgresStoredJob } from './types';
import { lockPostgresAdmissionKeys } from './batchAdmission';
import type { PostgresAdmissionResult } from './admissionResult';
import { linkedPostgresParentQueue, lockPostgresAdmissionParent } from './admissionParent';
import { lockPostgresAdmissionQueues, registerPostgresAdmissionQueues } from './queueLifecycle';
import { lockPostgresGroupCapacity } from './groups';

function isLive(state: string): boolean {
  return ['waiting', 'prioritized', 'delayed', 'waiting-children', 'active'].includes(state);
}

async function findById(
  tx: TransactionSQL,
  ctx: PostgresContext,
  id: string
): Promise<PostgresStoredJob | null> {
  const rows = await tx<PostgresJobRow[]>`
    SELECT * FROM bunqueue_jobs
    WHERE namespace = ${ctx.config.namespace} AND id = ${id}
    FOR UPDATE
  `;
  return rows[0] ? decodePostgresJob(rows[0]) : null;
}

async function findByUniqueKey(
  tx: TransactionSQL,
  ctx: PostgresContext,
  job: Job
): Promise<PostgresJobRow | null> {
  if (!job.uniqueKey) return null;
  const rows = await tx<PostgresJobRow[]>`
    SELECT * FROM bunqueue_jobs
    WHERE namespace = ${ctx.config.namespace}
      AND queue = ${job.queue}
      AND unique_key = ${job.uniqueKey}
      AND state IN ('waiting', 'prioritized', 'delayed', 'waiting-children', 'active')
    ORDER BY created_at, id
    LIMIT 1
    FOR UPDATE
  `;
  return rows[0] ?? null;
}

async function deleteTerminalGeneration(
  tx: TransactionSQL,
  ctx: PostgresContext,
  id: string
): Promise<void> {
  await tx`
    DELETE FROM bunqueue_job_logs
    WHERE namespace = ${ctx.config.namespace} AND job_id = ${id}
  `;
  await tx`
    DELETE FROM bunqueue_repeat_links
    WHERE namespace = ${ctx.config.namespace}
      AND (original_id = ${id} OR successor_id = ${id})
  `;
  await tx`
    DELETE FROM bunqueue_flow_failures
    WHERE namespace = ${ctx.config.namespace} AND parent_id = ${id}
  `;
  await tx`
    DELETE FROM bunqueue_dependencies
    WHERE namespace = ${ctx.config.namespace} AND job_id = ${id}
  `;
  await tx`
    DELETE FROM bunqueue_completions
    WHERE namespace = ${ctx.config.namespace} AND job_id = ${id}
  `;
  await tx`
    DELETE FROM bunqueue_jobs
    WHERE namespace = ${ctx.config.namespace} AND id = ${id}
  `;
}

async function insertDependencies(
  tx: TransactionSQL,
  ctx: PostgresContext,
  job: Job
): Promise<void> {
  for (const dependencyId of new Set(job.dependsOn.map(String))) {
    await tx`
      INSERT INTO bunqueue_dependencies (namespace, job_id, dependency_id)
      VALUES (${ctx.config.namespace}, ${String(job.id)}, ${dependencyId})
      ON CONFLICT DO NOTHING
    `;
  }
}

async function insertRow(
  tx: TransactionSQL,
  ctx: PostgresContext,
  job: Job,
  state: 'waiting' | 'prioritized' | 'delayed' | 'waiting-children',
  now: number
): Promise<void> {
  await tx`
    INSERT INTO bunqueue_jobs (
      namespace, id, queue, payload, state, priority, lifo, run_at, created_at,
      started_at, completed_at, attempts, max_attempts, ttl, timeout, unique_key,
      unique_expires_at, custom_id, group_id, group_order, parent_id
    ) VALUES (
      ${ctx.config.namespace}, ${String(job.id)}, ${job.queue}, ${encodePostgresValue(job)},
      ${state}, ${job.priority}, ${job.lifo}, ${job.runAt}, ${job.createdAt},
      ${job.startedAt}, ${job.completedAt}, ${job.attempts}, ${job.maxAttempts}, ${job.ttl},
      ${job.timeout}, ${job.uniqueKey},
      ${job.uniqueKey && job.deduplicationTtl !== null ? now + job.deduplicationTtl : null},
      ${job.customId}, ${job.groupId},
      CASE WHEN CAST(${job.groupId} AS TEXT) IS NULL
        THEN NULL ELSE nextval('bunqueue_group_order_seq') END,
      ${job.parentId}
    )
  `;
  await insertDependencies(tx, ctx, job);
}

export interface AdmitPostgresJobOptions {
  readonly rejectExisting?: boolean;
  readonly linkParent?: boolean;
  readonly dependencyLocksHeld?: boolean;
  readonly queueLifecycleLocksHeld?: boolean;
  readonly dependencyExistenceCheckDeferred?: boolean;
  readonly completionConsumerExemptions?: readonly JobId[];
  /** Flow caller already holds identity locks and batches completion, queue, and event writes. */
  readonly preparedFlow?: { readonly now: number };
  readonly groupMaxSize?: number;
  readonly groupCapacityLocksHeld?: boolean;
}

/** Insert one job inside the caller's transaction and return its authoritative generation. */
export async function admitPostgresJob(
  tx: TransactionSQL,
  ctx: PostgresContext,
  job: Job,
  options: AdmitPostgresJobOptions = {}
): Promise<PostgresAdmissionResult> {
  const parentQueue = options.linkParent === false ? null : linkedPostgresParentQueue(job);
  if (!options.queueLifecycleLocksHeld) {
    await lockPostgresAdmissionQueues(tx, ctx, [job.queue]);
  }
  if (job.groupId && options.groupMaxSize !== undefined && !options.groupCapacityLocksHeld) {
    await lockPostgresGroupCapacity(tx, ctx, [{ queue: job.queue, groupId: job.groupId }]);
  }
  if (!options.dependencyLocksHeld) {
    await lockPostgresDependencyCompletions(tx, ctx, [job.id, ...job.dependsOn]);
  }
  if (parentQueue) await lockPostgresAdmissionParent(tx, ctx, job, parentQueue);
  if (!options.preparedFlow) await lockPostgresAdmissionKeys(tx, ctx, [job]);
  const now = options.preparedFlow?.now ?? (await databaseNow(tx));

  const existingById = await findById(tx, ctx, String(job.id));
  if (existingById) {
    if (options.rejectExisting) throw new Error(`Flow job ${String(job.id)} already exists`);
    if (isLive(existingById.state)) {
      return { job: existingById.job, inserted: false, state: existingById.state };
    }
  }

  let existingByKeyRow = await findByUniqueKey(tx, ctx, job);
  if (
    existingByKeyRow &&
    existingByKeyRow.unique_expires_at !== null &&
    Number(existingByKeyRow.unique_expires_at) <= now
  ) {
    await clearPostgresDeduplication(tx, ctx, existingByKeyRow, now);
    existingByKeyRow = null;
  }
  if (existingByKeyRow) {
    let existingByKey = decodePostgresJob(existingByKeyRow);
    if (job.deduplicationReplace) {
      if (existingByKeyRow.state === 'active') {
        await clearPostgresDeduplication(tx, ctx, existingByKeyRow, now);
      } else {
        const acquired = await tryLockPostgresDependencyCompletions(tx, ctx, [
          existingByKey.job.id,
        ]);
        if (!acquired) {
          await recordPostgresEvent(tx, ctx, {
            queue: existingByKey.job.queue,
            type: 'duplicated',
            jobId: existingByKey.job.id,
            occurredAt: now,
            job: existingByKey.job,
            state: existingByKey.state,
          });
          return { job: existingByKey.job, inserted: false, state: existingByKey.state };
        }
        await assertNoLivePostgresCompletionConsumers(tx, ctx, [existingByKey.job.id]);
        await deleteTerminalGeneration(tx, ctx, existingByKeyRow.id);
        await recordPostgresDeduplicationRemoval(tx, ctx, existingByKeyRow, now);
      }
    } else {
      if (job.deduplicationExtend && job.deduplicationTtl !== null) {
        existingByKey = await extendPostgresDeduplication(
          tx,
          ctx,
          existingByKeyRow,
          job.deduplicationTtl,
          now
        );
      }
      await recordPostgresEvent(tx, ctx, {
        queue: existingByKey.job.queue,
        type: 'duplicated',
        jobId: existingByKey.job.id,
        occurredAt: now,
        job: existingByKey.job,
        state: existingByKey.state,
      });
      return { job: existingByKey.job, inserted: false, state: existingByKey.state };
    }
  }

  if (existingById) {
    await assertNoLivePostgresCompletionConsumers(
      tx,
      ctx,
      [job.id],
      options.completionConsumerExemptions
    );
    await deleteTerminalGeneration(tx, ctx, String(job.id));
  } else if (!options.preparedFlow) {
    await retirePostgresCompletionGenerations(
      tx,
      ctx,
      [job.id],
      options.completionConsumerExemptions
    );
  }

  if (!options.dependencyExistenceCheckDeferred) {
    await assertPostgresDependenciesExist(tx, ctx, job.dependsOn);
  }
  if (job.groupId && options.groupMaxSize !== undefined) {
    const [group] = await tx<{ count: number | string | bigint }[]>`
      SELECT COUNT(*)::bigint AS count FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace} AND queue = ${job.queue}
        AND group_id = ${job.groupId} AND state IN ('waiting', 'prioritized', 'delayed')
    `;
    if (Number(group.count) >= options.groupMaxSize) {
      throw new Error(
        `Group ${job.groupId} has reached its maximum size of ${options.groupMaxSize}`
      );
    }
  }
  if (job.runAt === job.createdAt) job.runAt = now;
  const state = postgresStateForJob(
    job,
    await postgresDependenciesSatisfied(tx, ctx, job.dependsOn),
    now
  );
  const timelineIndex =
    job.timeline.length < MAX_TIMELINE_ENTRIES ? job.timeline.length : undefined;
  if (timelineIndex !== undefined) job.timeline.push({ state, timestamp: now });
  await insertRow(tx, ctx, job, state, now);
  if (!options.preparedFlow) await registerPostgresAdmissionQueues(tx, ctx, [job.queue]);
  const pushedEventId = options.preparedFlow
    ? undefined
    : await recordPostgresEvent(tx, ctx, {
        queue: job.queue,
        type: 'pushed',
        jobId: job.id,
        occurredAt: now,
        job,
        state,
      });
  if (parentQueue && job.parentId) {
    await updatePostgresJobParentInTransaction(tx, ctx, job.id, job.parentId, true);
  }
  return { job, inserted: true, state, pushedEventId, timelineIndex };
}
