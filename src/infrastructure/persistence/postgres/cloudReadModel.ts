import type { CronJob } from '../../../domain/types/cron';
import type { JobLogEntry, Worker } from '../../../domain/types/worker';
import { decodePostgresJob, decodePostgresValue } from './codec';
import type { PostgresContext } from './context';
import { decodePostgresQueueState, type PostgresQueueStateRow } from './control';
import { decodePostgresCron, type PostgresCronRow } from './crons';
import type {
  PostgresCompletionResult,
  PostgresJobRow,
  PostgresJobState,
  PostgresQueueState,
  PostgresStoredJob,
} from './types';
import { jobId } from '../../../domain/types/job';

export interface PostgresCloudQueueCount {
  readonly queue: string;
  readonly state: PostgresJobState;
  readonly count: number;
}

export interface PostgresCloudQueueTotal {
  readonly queue: string;
  readonly metricType: 'completed' | 'failed';
  readonly count: bigint;
}

export interface PostgresCloudReadModel {
  readonly jobs: readonly PostgresStoredJob[];
  readonly queueStates: readonly PostgresQueueState[];
  readonly counts: readonly PostgresCloudQueueCount[];
  readonly totals: readonly PostgresCloudQueueTotal[];
  readonly results: readonly PostgresCompletionResult[];
  readonly logs: ReadonlyMap<string, readonly JobLogEntry[]>;
  readonly workers: readonly Worker[];
  readonly crons: readonly CronJob[];
}

interface CountRow {
  queue: string;
  state: PostgresJobState;
  count: number | string | bigint;
}

interface TotalRow {
  queue: string;
  metric_type: 'completed' | 'failed';
  total_count: number | string | bigint;
}

interface ResultRow {
  job_id: string;
  queue: string;
  result: Uint8Array | null;
}

interface LogRow extends JobLogEntry {
  job_id: string;
}

interface WorkerRow {
  id: string;
  payload: Uint8Array;
  last_seen: number | string | bigint;
}

/** Read one coherent, bounded Cloud view from the durable PostgreSQL source. */
export async function loadPostgresCloudReadModel(
  ctx: PostgresContext
): Promise<PostgresCloudReadModel> {
  return await ctx.sql.begin('isolation level repeatable read read only', async (tx) => {
    const [jobRows, stateRows, countRows, totalRows, resultRows, workerRows, cronRows] =
      await Promise.all([
        tx<PostgresJobRow[]>`
          WITH ranked_live AS MATERIALIZED (
            SELECT job.*, row_number() OVER (
              PARTITION BY queue, state ORDER BY created_at, id
            ) AS cloud_rank
            FROM bunqueue_jobs AS job
            WHERE namespace = ${ctx.config.namespace} AND state <> 'completed'
          ), retained_completed AS MATERIALIZED (
            SELECT job.*, 1::bigint AS cloud_rank
            FROM bunqueue_jobs AS job
            WHERE namespace = ${ctx.config.namespace} AND state = 'completed'
            ORDER BY completed_at DESC NULLS LAST, id DESC
            LIMIT ${ctx.config.maxCompletedJobs}
          )
          SELECT cloud_jobs.*
          FROM (
            SELECT * FROM ranked_live WHERE cloud_rank <= 1000
            UNION ALL SELECT * FROM retained_completed
          ) AS cloud_jobs
          ORDER BY created_at, id
        `,
        tx<PostgresQueueStateRow[]>`
          SELECT queue, paused, rate_limit, rate_duration_ms, rate_window_started_at,
                 rate_expires_at, rate_count, concurrency_limit, stall_config, dlq_config
          FROM bunqueue_queue_state
          WHERE namespace = ${ctx.config.namespace}
          ORDER BY queue
        `,
        tx<CountRow[]>`
          SELECT queue, state, COUNT(*)::bigint AS count
          FROM bunqueue_jobs
          WHERE namespace = ${ctx.config.namespace}
          GROUP BY queue, state ORDER BY queue, state
        `,
        tx<TotalRow[]>`
          SELECT queue, metric_type, total_count
          FROM bunqueue_metric_totals
          WHERE namespace = ${ctx.config.namespace}
          ORDER BY queue, metric_type
        `,
        tx<ResultRow[]>`
          SELECT job_id, queue, result
          FROM bunqueue_completions
          WHERE namespace = ${ctx.config.namespace}
          ORDER BY completed_at DESC, job_id DESC
          LIMIT ${ctx.config.maxJobResults}
        `,
        tx<WorkerRow[]>`
          SELECT id, payload, last_seen FROM bunqueue_workers
          WHERE namespace = ${ctx.config.namespace}
          ORDER BY id
        `,
        tx<PostgresCronRow[]>`
          SELECT name, payload, next_run, executions, max_limit
          FROM bunqueue_crons
          WHERE namespace = ${ctx.config.namespace}
          ORDER BY name
        `,
      ]);
    const ids = jobRows.map((row) => row.id);
    const logRows =
      ids.length === 0
        ? []
        : await tx<LogRow[]>`
            SELECT job_id, timestamp, level, message
            FROM bunqueue_job_logs
            WHERE namespace = ${ctx.config.namespace}
              AND job_id = ANY(${tx.array(ids, 'TEXT')})
            ORDER BY job_id, id
          `;
    return decodeCloudReadModel({
      jobRows,
      stateRows,
      countRows,
      totalRows,
      resultRows,
      logRows,
      workerRows,
      cronRows,
    });
  });
}

interface CloudReadRows {
  readonly jobRows: readonly PostgresJobRow[];
  readonly stateRows: readonly PostgresQueueStateRow[];
  readonly countRows: readonly CountRow[];
  readonly totalRows: readonly TotalRow[];
  readonly resultRows: readonly ResultRow[];
  readonly logRows: readonly LogRow[];
  readonly workerRows: readonly WorkerRow[];
  readonly cronRows: readonly PostgresCronRow[];
}

function decodeCloudReadModel(rows: CloudReadRows): PostgresCloudReadModel {
  const { jobRows, stateRows, countRows, totalRows, resultRows, logRows, workerRows, cronRows } =
    rows;
  const logs = new Map<string, JobLogEntry[]>();
  for (const { job_id, timestamp, level, message } of logRows) {
    const entries = logs.get(job_id) ?? [];
    entries.push({ timestamp: Number(timestamp), level, message });
    logs.set(job_id, entries);
  }
  return {
    jobs: jobRows.map(decodePostgresJob),
    queueStates: stateRows.map((row) => decodePostgresQueueState(row.queue, row)),
    counts: countRows.map((row) => ({ ...row, count: Number(row.count) })),
    totals: totalRows.map((row) => ({
      queue: row.queue,
      metricType: row.metric_type,
      count: BigInt(row.total_count),
    })),
    results: resultRows.map((row) => ({
      jobId: jobId(row.job_id),
      queue: row.queue,
      result: decodePostgresValue(row.result, null, `postgresCloudResult:${row.job_id}`),
      pinned: false,
    })),
    logs,
    workers: workerRows.map((row) => {
      const worker = decodePostgresValue<Worker | null>(
        row.payload,
        null,
        `postgresCloudWorker:${row.id}`
      );
      if (!worker) throw new Error(`Corrupt PostgreSQL worker payload for ${row.id}`);
      worker.lastSeen = Number(row.last_seen);
      return worker;
    }),
    crons: cronRows.map(decodePostgresCron),
  };
}
