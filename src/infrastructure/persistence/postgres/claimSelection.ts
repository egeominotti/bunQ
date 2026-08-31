import type { TransactionSQL } from 'bun';
import type { GroupPullOptions } from '../../../domain/types/group';
import type { PostgresContext } from './context';
import type { PostgresJobRow } from './types';

export async function hasClaimablePostgresGroup(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queue: string,
  now: number
): Promise<boolean> {
  const [row] = await tx<{ exists: boolean }[]>`
    SELECT EXISTS (
      SELECT 1 FROM bunqueue_jobs AS job
      WHERE job.namespace = ${ctx.config.namespace} AND job.queue = ${queue}
        AND job.group_id IS NOT NULL
        AND job.state IN ('waiting', 'prioritized', 'delayed')
        AND job.run_at <= ${now}
        AND (job.ttl IS NULL OR job.created_at + job.ttl >= ${now})
        AND NOT EXISTS (
          SELECT 1 FROM bunqueue_dependencies AS dependency
          WHERE dependency.namespace = job.namespace AND dependency.job_id = job.id
            AND NOT EXISTS (
              SELECT 1 FROM bunqueue_completions AS completion
              WHERE completion.namespace = dependency.namespace
                AND completion.job_id = dependency.dependency_id
            )
        )
    ) AS exists
  `;
  return row.exists;
}

async function selectUngroupedCandidates(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queue: string,
  now: number,
  capacity: number
): Promise<PostgresJobRow[]> {
  return await tx<PostgresJobRow[]>`
    WITH selected AS (
      SELECT job.id, job.priority, job.lifo, job.run_at
      FROM bunqueue_jobs AS job
      WHERE job.namespace = ${ctx.config.namespace} AND job.queue = ${queue}
        AND job.group_id IS NULL
        AND job.state IN ('waiting', 'prioritized', 'delayed')
        AND job.run_at <= ${now}
        AND (job.ttl IS NULL OR job.created_at + job.ttl >= ${now})
        AND NOT EXISTS (
          SELECT 1 FROM bunqueue_dependencies AS dependency
          WHERE dependency.namespace = job.namespace AND dependency.job_id = job.id
            AND NOT EXISTS (
              SELECT 1 FROM bunqueue_completions AS completion
              WHERE completion.namespace = dependency.namespace
                AND completion.job_id = dependency.dependency_id
            )
        )
      ORDER BY job.priority DESC,
        CASE WHEN job.lifo THEN 0 ELSE 1 END,
        CASE WHEN job.lifo THEN job.id END DESC,
        CASE WHEN NOT job.lifo THEN job.run_at END,
        job.id
      LIMIT ${capacity}
      FOR UPDATE OF job SKIP LOCKED
    )
    SELECT job.* FROM bunqueue_jobs AS job
    JOIN selected ON selected.id = job.id
    WHERE job.namespace = ${ctx.config.namespace}
    ORDER BY selected.priority DESC,
      CASE WHEN selected.lifo THEN 0 ELSE 1 END,
      CASE WHEN selected.lifo THEN selected.id END DESC,
      CASE WHEN NOT selected.lifo THEN selected.run_at END,
      selected.id
  `;
}

async function selectGroupedCandidates(
  tx: TransactionSQL,
  ctx: PostgresContext,
  input: {
    queue: string;
    now: number;
    capacity: number;
    options?: GroupPullOptions;
  }
): Promise<PostgresJobRow[]> {
  const { queue, now, capacity, options } = input;
  const concurrency = options?.concurrency ?? null;
  const rateMax = options?.limit?.max ?? null;
  return await tx<PostgresJobRow[]>`
    WITH ready AS (
      SELECT job.id, job.group_id,
        row_number() OVER (
          PARTITION BY job.group_id ORDER BY job.run_at, job.group_order, job.id
        ) AS group_position
      FROM bunqueue_jobs AS job
      WHERE job.namespace = ${ctx.config.namespace} AND job.queue = ${queue}
        AND job.group_id IS NOT NULL
        AND job.state IN ('waiting', 'prioritized', 'delayed')
        AND job.run_at <= ${now}
        AND (job.ttl IS NULL OR job.created_at + job.ttl >= ${now})
        AND NOT EXISTS (
          SELECT 1 FROM bunqueue_dependencies AS dependency
          WHERE dependency.namespace = job.namespace AND dependency.job_id = job.id
            AND NOT EXISTS (
              SELECT 1 FROM bunqueue_completions AS completion
              WHERE completion.namespace = dependency.namespace
                AND completion.job_id = dependency.dependency_id
            )
        )
    ), active AS (
      SELECT group_id, COUNT(*)::integer AS count
      FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
        AND group_id IS NOT NULL AND state = 'active' AND lease_until > ${now}
      GROUP BY group_id
    ), capacities AS (
      SELECT groups.group_id, groups.last_served,
        LEAST(
          ${capacity},
          CASE WHEN CAST(${concurrency} AS BIGINT) IS NULL THEN ${capacity}
            ELSE GREATEST(
              0,
              COALESCE(groups.concurrency_limit, CAST(${concurrency} AS BIGINT))
                - COALESCE(active.count, 0)
            )
          END,
          CASE WHEN CAST(${rateMax} AS BIGINT) IS NULL THEN ${capacity}
            ELSE GREATEST(
              0,
              COALESCE(groups.rate_limit, CAST(${rateMax} AS BIGINT)) - groups.rate_count
            )
          END
        ) AS available
      FROM bunqueue_group_state AS groups
      LEFT JOIN active ON active.group_id = groups.group_id
      WHERE groups.namespace = ${ctx.config.namespace} AND groups.queue = ${queue}
    ), selected AS (
      SELECT ready.id, ready.group_id, ready.group_position, capacities.last_served
      FROM ready JOIN capacities ON capacities.group_id = ready.group_id
      WHERE ready.group_position <= capacities.available
      ORDER BY ready.group_position, capacities.last_served NULLS FIRST, ready.group_id
      LIMIT ${capacity}
    )
    SELECT job.* FROM bunqueue_jobs AS job
    JOIN selected ON selected.id = job.id
    WHERE job.namespace = ${ctx.config.namespace}
      AND job.state IN ('waiting', 'prioritized', 'delayed')
      AND job.run_at <= ${now}
    ORDER BY selected.group_position, selected.last_served NULLS FIRST, selected.group_id
    FOR UPDATE OF job SKIP LOCKED
  `;
}

export async function selectPostgresClaimCandidates(
  tx: TransactionSQL,
  ctx: PostgresContext,
  input: {
    queue: string;
    now: number;
    capacity: number;
    options?: GroupPullOptions;
  }
): Promise<PostgresJobRow[]> {
  const { queue, now, capacity, options } = input;
  const ungrouped = await selectUngroupedCandidates(tx, ctx, queue, now, capacity);
  if (ungrouped.length >= capacity) return ungrouped;
  const grouped = await selectGroupedCandidates(tx, ctx, {
    queue,
    now,
    capacity: capacity - ungrouped.length,
    options,
  });
  return [...ungrouped, ...grouped];
}
