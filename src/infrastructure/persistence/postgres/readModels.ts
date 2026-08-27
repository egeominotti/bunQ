import type { CronJob } from '../../../domain/types/cron';
import type { JobId } from '../../../domain/types/job';
import { decodePostgresValue } from './codec';
import { loadPostgresCompletionResults } from './completionQueries';
import { getPostgresQueueState } from './control';
import { listPostgresCrons } from './crons';
import type { PostgresContext } from './context';
import {
  loadPostgresLifetimeMetrics,
  type PostgresLifetimeMetricsSnapshot,
} from './lifetimeMetrics';
import {
  getPostgresJobs,
  listPostgresJobs,
  listPostgresQueues,
  loadAllPostgresJobs,
} from './queries';
import type { PostgresCompletionResult, PostgresQueueState, PostgresStoredJob } from './types';
import { assertPostgresSnapshotBudget } from './snapshotBudget';

export interface PostgresManagerSnapshot {
  readonly rows: readonly PostgresStoredJob[];
  readonly results: readonly PostgresCompletionResult[];
  readonly crons: readonly CronJob[];
  readonly states: readonly PostgresQueueState[];
  readonly lifetimeMetrics: PostgresLifetimeMetricsSnapshot;
}

export interface PostgresQueueReadModel {
  readonly rows: readonly PostgresStoredJob[];
  readonly results: readonly PostgresCompletionResult[];
  readonly state: PostgresQueueState;
  readonly exists: boolean;
}

export interface PostgresProjectionRequest {
  readonly id: JobId;
  readonly queue: string;
}

export interface PostgresJobProjection {
  readonly row: PostgresStoredJob | null;
  readonly completion: PostgresCompletionResult | null;
}

/** Load one transactionally coherent manager bootstrap projection. */
export async function loadPostgresManagerSnapshot(
  ctx: PostgresContext
): Promise<PostgresManagerSnapshot> {
  return await ctx.sql.begin('isolation level repeatable read read only', async (tx) => {
    await assertPostgresSnapshotBudget(ctx, tx);
    const [rows, results, crons, queues, lifetimeMetrics] = await Promise.all([
      loadAllPostgresJobs(ctx, tx),
      loadPostgresCompletionResults(ctx, undefined, tx),
      listPostgresCrons(ctx, tx),
      listPostgresQueues(ctx, tx),
      loadPostgresLifetimeMetrics(ctx, tx),
    ]);
    const states = await Promise.all(queues.map((queue) => getPostgresQueueState(ctx, queue, tx)));
    return { rows, results, crons, states, lifetimeMetrics };
  });
}

/** Load jobs, results, and policy for one queue from the same MVCC snapshot. */
export async function loadPostgresQueueReadModel(
  ctx: PostgresContext,
  queue: string
): Promise<PostgresQueueReadModel> {
  return await ctx.sql.begin('isolation level repeatable read read only', async (tx) => {
    await assertPostgresSnapshotBudget(ctx, tx, queue);
    const [rows, results, state, [presence]] = await Promise.all([
      listPostgresJobs(ctx, queue, { limit: 2_147_483_647, asc: true }, tx),
      loadPostgresCompletionResults(ctx, queue, tx),
      getPostgresQueueState(ctx, queue, tx),
      tx<{ exists: boolean }[]>`
        SELECT EXISTS (
          SELECT 1 FROM bunqueue_jobs
          WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
          UNION ALL
          SELECT 1 FROM bunqueue_queue_state
          WHERE namespace = ${ctx.config.namespace} AND queue = ${queue}
        ) AS exists
      `,
    ]);
    return { rows, results, state, exists: presence.exists };
  });
}

/** Batch current rows and consult completion evidence only for removed jobs. */
export async function loadPostgresJobProjections(
  ctx: PostgresContext,
  requests: readonly PostgresProjectionRequest[]
): Promise<ReadonlyMap<JobId, PostgresJobProjection>> {
  if (requests.length === 0) return new Map();
  const rows = await getPostgresJobs(
    ctx,
    requests.map(({ id }) => id)
  );
  const byId = new Map(rows.map((row) => [row.job.id, row]));
  const missing = requests.filter(({ id }) => !byId.has(id));
  const completions =
    missing.length === 0
      ? []
      : await ctx.sql<{ job_id: string; queue: string; result: Uint8Array | null }[]>`
          SELECT job_id, queue, result FROM bunqueue_completions
          WHERE namespace = ${ctx.config.namespace}
            AND job_id = ANY(${ctx.sql.array(
              missing.map(({ id }) => String(id)),
              'TEXT'
            )})
        `;
  const completionById = new Map(completions.map((row) => [row.job_id, row]));
  return new Map(
    requests.map(({ id }) => {
      const row = byId.get(id) ?? null;
      const completionRow = completionById.get(String(id));
      const completion =
        !row && completionRow !== undefined
          ? {
              jobId: id,
              queue: completionRow.queue,
              result: decodePostgresValue(
                completionRow.result,
                null,
                `postgresCompletion:${String(id)}`
              ),
              pinned: false,
            }
          : null;
      return [id, { row, completion }];
    })
  );
}
