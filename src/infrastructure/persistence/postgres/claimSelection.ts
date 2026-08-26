import type { TransactionSQL } from 'bun';
import type { PostgresContext } from './context';
import type { PostgresJobRow } from './types';

async function hasClaimableGroupedJob(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queue: string,
  now: number
): Promise<boolean> {
  const [row] = await tx<{ exists: boolean }[]>`
    SELECT EXISTS (
      SELECT 1
      FROM bunqueue_jobs AS job
      WHERE job.namespace = ${ctx.config.namespace}
        AND job.queue = ${queue}
        AND job.group_id IS NOT NULL
        AND job.state IN ('waiting', 'prioritized', 'delayed')
        AND job.run_at <= ${now}
        AND (job.ttl IS NULL OR job.created_at + job.ttl >= ${now})
        AND NOT EXISTS (
          SELECT 1
          FROM bunqueue_dependencies AS dependency
          WHERE dependency.namespace = job.namespace
            AND dependency.job_id = job.id
            AND NOT EXISTS (
              SELECT 1 FROM bunqueue_completions AS completion
              WHERE completion.namespace = dependency.namespace
                AND completion.job_id = dependency.dependency_id
            )
        )
        AND NOT EXISTS (
          SELECT 1 FROM bunqueue_jobs AS active_group
          WHERE active_group.namespace = job.namespace
            AND active_group.queue = job.queue
            AND active_group.group_id = job.group_id
            AND active_group.state = 'active'
            AND active_group.lease_until > ${now}
        )
    ) AS exists
  `;
  return row.exists;
}

async function hasClaimableLifoJob(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queue: string,
  now: number
): Promise<boolean> {
  const [row] = await tx<{ exists: boolean }[]>`
    SELECT EXISTS (
      SELECT 1 FROM bunqueue_jobs AS job
      WHERE job.namespace = ${ctx.config.namespace}
        AND job.queue = ${queue}
        AND job.group_id IS NULL
        AND job.lifo
        AND job.state IN ('waiting', 'prioritized', 'delayed')
        AND job.run_at <= ${now}
        AND (job.ttl IS NULL OR job.created_at + job.ttl >= ${now})
    ) AS exists
  `;
  return row.exists;
}

async function selectFifoCandidates(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queue: string,
  now: number,
  capacity: number
): Promise<PostgresJobRow[]> {
  return await tx<PostgresJobRow[]>`
    WITH selected AS (
      SELECT job.id, job.priority, job.run_at
      FROM bunqueue_jobs AS job
      WHERE job.namespace = ${ctx.config.namespace}
        AND job.queue = ${queue}
        AND job.group_id IS NULL
        AND NOT job.lifo
        AND job.state IN ('waiting', 'prioritized', 'delayed')
        AND job.run_at <= ${now}
        AND (job.ttl IS NULL OR job.created_at + job.ttl >= ${now})
        AND NOT EXISTS (
          SELECT 1 FROM bunqueue_dependencies AS dependency
          WHERE dependency.namespace = job.namespace
            AND dependency.job_id = job.id
            AND NOT EXISTS (
              SELECT 1 FROM bunqueue_completions AS completion
              WHERE completion.namespace = dependency.namespace
                AND completion.job_id = dependency.dependency_id
            )
        )
      ORDER BY job.priority DESC, job.run_at ASC, job.id ASC
      LIMIT ${capacity}
      FOR UPDATE OF job SKIP LOCKED
    )
    SELECT job.*
    FROM bunqueue_jobs AS job
    JOIN selected ON selected.id = job.id
    WHERE job.namespace = ${ctx.config.namespace}
    ORDER BY selected.priority DESC, selected.run_at ASC, selected.id ASC
  `;
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
      WHERE job.namespace = ${ctx.config.namespace}
        AND job.queue = ${queue}
        AND job.group_id IS NULL
        AND job.state IN ('waiting', 'prioritized', 'delayed')
        AND job.run_at <= ${now}
        AND (job.ttl IS NULL OR job.created_at + job.ttl >= ${now})
        AND NOT EXISTS (
          SELECT 1 FROM bunqueue_dependencies AS dependency
          WHERE dependency.namespace = job.namespace
            AND dependency.job_id = job.id
            AND NOT EXISTS (
              SELECT 1 FROM bunqueue_completions AS completion
              WHERE completion.namespace = dependency.namespace
                AND completion.job_id = dependency.dependency_id
            )
        )
      ORDER BY
        job.priority DESC,
        CASE WHEN job.lifo THEN 0 ELSE 1 END ASC,
        CASE WHEN job.lifo THEN job.id END DESC,
        CASE WHEN NOT job.lifo THEN job.run_at END ASC,
        job.id ASC
      LIMIT ${capacity}
      FOR UPDATE OF job SKIP LOCKED
    )
    SELECT job.*
    FROM bunqueue_jobs AS job
    JOIN selected ON selected.id = job.id
    WHERE job.namespace = ${ctx.config.namespace}
    ORDER BY
      selected.priority DESC,
      CASE WHEN selected.lifo THEN 0 ELSE 1 END ASC,
      CASE WHEN selected.lifo THEN selected.id END DESC,
      CASE WHEN NOT selected.lifo THEN selected.run_at END ASC,
      selected.id ASC
  `;
}

async function selectGroupedCandidates(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queue: string,
  now: number,
  capacity: number
): Promise<PostgresJobRow[]> {
  return await tx<PostgresJobRow[]>`
    WITH eligible AS (
      SELECT job.id, job.priority, job.lifo, job.run_at,
        row_number() OVER (
          PARTITION BY CASE
            WHEN job.group_id IS NULL THEN 'job:' || job.id ELSE 'group:' || job.group_id
          END
          ORDER BY job.priority DESC,
            CASE WHEN job.lifo THEN 0 ELSE 1 END ASC,
            CASE WHEN job.lifo THEN job.id END DESC,
            CASE WHEN NOT job.lifo THEN job.run_at END ASC, job.id ASC
        ) AS group_position
      FROM bunqueue_jobs AS job
      WHERE job.namespace = ${ctx.config.namespace}
        AND job.queue = ${queue}
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
        AND (job.group_id IS NULL OR NOT EXISTS (
          SELECT 1 FROM bunqueue_jobs AS active_group
          WHERE active_group.namespace = job.namespace AND active_group.queue = job.queue
            AND active_group.group_id = job.group_id AND active_group.state = 'active'
            AND active_group.lease_until > ${now}
        ))
    )
    SELECT job.*
    FROM bunqueue_jobs AS job
    JOIN eligible ON eligible.id = job.id
    WHERE job.namespace = ${ctx.config.namespace}
      AND eligible.group_position = 1
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
      AND (job.group_id IS NULL OR NOT EXISTS (
        SELECT 1 FROM bunqueue_jobs AS active_group
        WHERE active_group.namespace = job.namespace AND active_group.queue = job.queue
          AND active_group.group_id = job.group_id AND active_group.state = 'active'
          AND active_group.lease_until > ${now}
      ))
    ORDER BY job.priority DESC,
      CASE WHEN job.lifo THEN 0 ELSE 1 END ASC,
      CASE WHEN job.lifo THEN job.id END DESC,
      CASE WHEN NOT job.lifo THEN job.run_at END ASC, job.id ASC
    LIMIT ${capacity}
    FOR UPDATE OF job SKIP LOCKED
  `;
}

export async function selectPostgresClaimCandidates(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queue: string,
  now: number,
  capacity: number
): Promise<PostgresJobRow[]> {
  if (await hasClaimableGroupedJob(tx, ctx, queue, now)) {
    return await selectGroupedCandidates(tx, ctx, queue, now, capacity);
  }
  return (await hasClaimableLifoJob(tx, ctx, queue, now))
    ? await selectUngroupedCandidates(tx, ctx, queue, now, capacity)
    : await selectFifoCandidates(tx, ctx, queue, now, capacity);
}
