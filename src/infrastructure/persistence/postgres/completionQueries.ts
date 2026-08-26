import { jobId, type Job, type JobId } from '../../../domain/types/job';
import { decodePostgresValue } from './codec';
import type { PostgresContext } from './context';
import type { PostgresReadSql } from './context';
import type { PostgresCompletionResult } from './types';

interface CompletionRow {
  job_id: string;
  queue: string;
  result: Uint8Array | null;
  pinned: boolean;
}

const LIVE_CONSUMER_STATES = [
  'waiting',
  'prioritized',
  'delayed',
  'waiting-children',
  'active',
] as const;

export async function getPostgresCompletionResult(
  ctx: PostgresContext,
  id: JobId
): Promise<{ found: boolean; result: unknown }> {
  const rows = await ctx.sql<{ result: Uint8Array | null }[]>`
    SELECT result FROM bunqueue_completions
    WHERE namespace = ${ctx.config.namespace} AND job_id = ${String(id)}
  `;
  if (!rows[0]) return { found: false, result: undefined };
  return {
    found: true,
    result: decodePostgresValue(rows[0].result, null, `postgresCompletion:${String(id)}`),
  };
}

async function loadCompletionRows(
  ctx: PostgresContext,
  queue?: string,
  sql: PostgresReadSql = ctx.sql
): Promise<CompletionRow[]> {
  const queueFilter = queue === undefined ? null : queue;
  return await sql<CompletionRow[]>`
    WITH pinned AS MATERIALIZED (
      SELECT DISTINCT dependency.dependency_id AS job_id
      FROM bunqueue_dependencies AS dependency
      JOIN bunqueue_jobs AS consumer
        ON consumer.namespace = dependency.namespace AND consumer.id = dependency.job_id
      WHERE dependency.namespace = ${ctx.config.namespace}
        AND consumer.state = ANY(${ctx.sql.array([...LIVE_CONSUMER_STATES], 'TEXT')})
    ), recent AS MATERIALIZED (
      SELECT completion.job_id, completion.queue, completion.result,
             FALSE AS pinned, completion.completed_at
      FROM bunqueue_completions AS completion
      WHERE completion.namespace = ${ctx.config.namespace}
        AND (${queueFilter}::text IS NULL OR completion.queue = ${queueFilter})
        AND NOT EXISTS (
          SELECT 1 FROM pinned WHERE pinned.job_id = completion.job_id
        )
      ORDER BY completion.completed_at DESC, completion.job_id DESC
      LIMIT ${ctx.config.maxJobResults}
    )
    SELECT completion.job_id, completion.queue, completion.result, TRUE AS pinned
    FROM bunqueue_completions AS completion
    JOIN pinned ON pinned.job_id = completion.job_id
    WHERE completion.namespace = ${ctx.config.namespace}
      AND (${queueFilter}::text IS NULL OR completion.queue = ${queueFilter})
    UNION ALL
    SELECT recent.job_id, recent.queue, recent.result, recent.pinned
    FROM recent
    ORDER BY job_id
  `;
}

export async function loadPostgresCompletionResults(
  ctx: PostgresContext,
  queue?: string,
  sql: PostgresReadSql = ctx.sql
): Promise<PostgresCompletionResult[]> {
  const rows = await loadCompletionRows(ctx, queue, sql);
  return rows.map((row) => ({
    jobId: jobId(row.job_id),
    queue: row.queue,
    result: decodePostgresValue(row.result, null, `postgresCompletion:${row.job_id}`),
    pinned: row.pinned,
  }));
}

export async function getPostgresChildrenValues(
  ctx: PostgresContext,
  parentId: JobId
): Promise<Record<string, unknown>> {
  const parents = await ctx.sql<{ payload: Uint8Array }[]>`
    SELECT payload FROM bunqueue_jobs
    WHERE namespace = ${ctx.config.namespace} AND id = ${String(parentId)}
  `;
  if (!parents[0]) return {};
  const parent = decodePostgresValue<Job | null>(
    parents[0].payload,
    null,
    `postgresChildrenParent:${String(parentId)}`
  );
  const childIds = [
    ...new Set([...(parent?.childrenIds ?? []), ...(parent?.dependsOn ?? [])].map(String)),
  ];
  if (childIds.length === 0) return {};
  const rows = await ctx.sql<{ job_id: string; queue: string; result: Uint8Array | null }[]>`
    SELECT job_id, queue, result FROM bunqueue_completions
    WHERE namespace = ${ctx.config.namespace}
      AND job_id = ANY(${ctx.sql.array(childIds, 'TEXT')})
    ORDER BY job_id
  `;
  return Object.fromEntries(
    rows.map((row) => [
      `${row.queue}:${row.job_id}`,
      decodePostgresValue(row.result, null, `postgresChildResult:${row.job_id}`),
    ])
  );
}
