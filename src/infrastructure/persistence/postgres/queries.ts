import type { DlqEntry } from '../../../domain/types/dlq';
import type { Job, JobId } from '../../../domain/types/job';
import { jobId } from '../../../domain/types/job';
import { decodePostgresJob, decodePostgresValue, numeric } from './codec';
import type { PostgresContext } from './context';
import type { PostgresCounts, PostgresJobRow, PostgresJobState, PostgresStoredJob } from './types';

export async function getPostgresJob(
  ctx: PostgresContext,
  id: JobId
): Promise<PostgresStoredJob | null> {
  const rows = await ctx.sql<PostgresJobRow[]>`
    SELECT * FROM bunqueue_jobs
    WHERE namespace = ${ctx.config.namespace} AND id = ${String(id)}
  `;
  return rows[0] ? decodePostgresJob(rows[0]) : null;
}

export async function getPostgresJobs(
  ctx: PostgresContext,
  ids: readonly JobId[]
): Promise<PostgresStoredJob[]> {
  if (ids.length === 0) return [];
  const rows = await ctx.sql<PostgresJobRow[]>`
    SELECT * FROM bunqueue_jobs
    WHERE namespace = ${ctx.config.namespace}
      AND id = ANY(${ctx.sql.array([...new Set(ids.map(String))], 'TEXT')})
  `;
  return rows.map(decodePostgresJob);
}

/** Return dependency ids that have neither a job row nor durable completion evidence. */
export async function findMissingPostgresDependencies(
  ctx: PostgresContext,
  ids: readonly JobId[]
): Promise<JobId[]> {
  const requested = [...new Set(ids.map(String))];
  if (requested.length === 0) return [];
  const rows = await ctx.sql<{ id: string }[]>`
    WITH requested(id) AS MATERIALIZED (
      SELECT DISTINCT id
      FROM unnest(${ctx.sql.array(requested, 'TEXT')}) AS dependency(id)
    )
    SELECT requested.id
    FROM requested
    WHERE NOT EXISTS (
      SELECT 1 FROM bunqueue_jobs AS job
      WHERE job.namespace = ${ctx.config.namespace} AND job.id = requested.id
    ) AND NOT EXISTS (
      SELECT 1 FROM bunqueue_completions AS completion
      WHERE completion.namespace = ${ctx.config.namespace}
        AND completion.job_id = requested.id
    )
  `;
  return rows.map((row) => jobId(row.id));
}

export interface ListPostgresJobsOptions {
  readonly states?: readonly PostgresJobState[];
  readonly offset?: number;
  readonly limit?: number;
  readonly asc?: boolean;
}

export async function listPostgresJobs(
  ctx: PostgresContext,
  queue: string,
  options: ListPostgresJobsOptions = {}
): Promise<PostgresStoredJob[]> {
  const states = options.states ?? [
    'waiting',
    'prioritized',
    'delayed',
    'waiting-children',
    'active',
    'completed',
    'failed',
  ];
  if (states.length === 0) return [];
  const stateArray = ctx.sql.array([...states], 'TEXT');
  const limit = Math.max(0, options.limit ?? 1000);
  const offset = Math.max(0, options.offset ?? 0);
  const rows = options.asc
    ? await ctx.sql<PostgresJobRow[]>`
        SELECT * FROM bunqueue_jobs
        WHERE namespace = ${ctx.config.namespace}
          AND queue = ${queue}
          AND state = ANY(${stateArray})
        ORDER BY created_at ASC, id ASC
        LIMIT ${limit} OFFSET ${offset}
      `
    : await ctx.sql<PostgresJobRow[]>`
        SELECT * FROM bunqueue_jobs
        WHERE namespace = ${ctx.config.namespace}
          AND queue = ${queue}
          AND state = ANY(${stateArray})
        ORDER BY created_at DESC, id DESC
        LIMIT ${limit} OFFSET ${offset}
      `;
  return rows.map(decodePostgresJob);
}

export async function loadAllPostgresJobs(ctx: PostgresContext): Promise<PostgresStoredJob[]> {
  const rows = await ctx.sql<PostgresJobRow[]>`
    WITH retained_completed AS MATERIALIZED (
      SELECT * FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace} AND state = 'completed'
      ORDER BY completed_at DESC NULLS LAST, id DESC
      LIMIT ${ctx.config.maxCompletedJobs}
    )
    SELECT * FROM (
      SELECT * FROM bunqueue_jobs
      WHERE namespace = ${ctx.config.namespace} AND state <> 'completed'
      UNION ALL
      SELECT * FROM retained_completed
    ) AS retained_jobs
    ORDER BY created_at, id
  `;
  return rows.map(decodePostgresJob);
}

export async function getPostgresCounts(
  ctx: PostgresContext,
  queue?: string
): Promise<PostgresCounts> {
  const rows = queue
    ? await ctx.sql<{ state: PostgresJobState; count: number | string | bigint }[]>`
        SELECT state, COUNT(*)::bigint AS count
        FROM bunqueue_jobs
        WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
        GROUP BY state
      `
    : await ctx.sql<{ state: PostgresJobState; count: number | string | bigint }[]>`
        SELECT state, COUNT(*)::bigint AS count
        FROM bunqueue_jobs
        WHERE namespace = ${ctx.config.namespace}
        GROUP BY state
      `;
  const byState = new Map(rows.map((row) => [row.state, Number(row.count)]));
  return {
    waiting: byState.get('waiting') ?? 0,
    active: byState.get('active') ?? 0,
    completed: byState.get('completed') ?? 0,
    failed: byState.get('failed') ?? 0,
    delayed: byState.get('delayed') ?? 0,
    prioritized: byState.get('prioritized') ?? 0,
    waitingChildren: byState.get('waiting-children') ?? 0,
  };
}

export async function listPostgresQueues(ctx: PostgresContext): Promise<string[]> {
  const rows = await ctx.sql<{ queue: string }[]>`
    SELECT queue FROM bunqueue_jobs WHERE namespace = ${ctx.config.namespace}
    UNION
    SELECT queue FROM bunqueue_queue_state WHERE namespace = ${ctx.config.namespace}
    ORDER BY queue
  `;
  return rows.map((row) => row.queue);
}

export async function listPostgresDlq(
  ctx: PostgresContext,
  queue: string,
  limit = 1000
): Promise<DlqEntry[]> {
  const rows = await ctx.sql<{ id: string; dlq_entry: Uint8Array }[]>`
    SELECT id, dlq_entry FROM bunqueue_jobs
    WHERE namespace = ${ctx.config.namespace}
      AND queue = ${queue}
      AND state = 'failed'
      AND dlq_entry IS NOT NULL
    ORDER BY completed_at DESC, id DESC
    LIMIT ${Math.max(0, limit)}
  `;
  return rows.flatMap((row) => {
    const entry = decodePostgresValue<DlqEntry | null>(
      row.dlq_entry,
      null,
      `postgresDlq:${row.id}`
    );
    return entry ? [entry] : [];
  });
}

export async function latestPostgresEventId(ctx: PostgresContext): Promise<number> {
  const [row] = await ctx.sql<{ id: number | string | bigint | null }[]>`
    SELECT MAX(id) AS id FROM bunqueue_events WHERE namespace = ${ctx.config.namespace}
  `;
  return numeric(row.id) ?? 0;
}

export function jobsOnly(rows: readonly PostgresStoredJob[]): Job[] {
  return rows.map((row) => row.job);
}
