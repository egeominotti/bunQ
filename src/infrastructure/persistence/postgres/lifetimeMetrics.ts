import type { PostgresContext, PostgresReadSql } from './context';

export interface PostgresQueueLifetimeTotals {
  readonly queue: string;
  readonly totalCompleted: bigint;
  readonly totalFailed: bigint;
}

export interface PostgresLifetimeMetricsSnapshot {
  readonly baselineEventId: number;
  readonly baselineCommitSeq: number;
  readonly totalCompleted: bigint;
  readonly totalFailed: bigint;
  readonly queues: readonly PostgresQueueLifetimeTotals[];
}

interface MetricTotalRow {
  readonly queue: string;
  readonly metric_type: 'completed' | 'failed';
  readonly total_count: number | string | bigint;
}

/** Load terminal lifetime counters and their matching event boundary atomically. */
export async function loadPostgresLifetimeMetrics(
  ctx: PostgresContext,
  sql?: PostgresReadSql
): Promise<PostgresLifetimeMetricsSnapshot> {
  const load = async (readSql: PostgresReadSql) => {
    const [[baseline], rows] = await Promise.all([
      readSql<
        Array<{
          event_id: number | string | bigint | null;
          commit_seq: number | string | bigint | null;
        }>
      >`
        SELECT
          (SELECT MAX(id) FROM bunqueue_events
           WHERE namespace = ${ctx.config.namespace}) AS event_id,
          (SELECT MAX(commit_seq) FROM bunqueue_event_commits
           WHERE namespace = ${ctx.config.namespace}) AS commit_seq
      `,
      readSql<MetricTotalRow[]>`
        SELECT queue, metric_type, total_count
        FROM bunqueue_metric_totals
        WHERE namespace = ${ctx.config.namespace}
        ORDER BY queue, metric_type
      `,
    ]);
    const queues = new Map<string, { totalCompleted: bigint; totalFailed: bigint }>();
    let totalCompleted = 0n;
    let totalFailed = 0n;
    for (const row of rows) {
      const count = BigInt(row.total_count);
      const totals = queues.get(row.queue) ?? { totalCompleted: 0n, totalFailed: 0n };
      if (row.metric_type === 'completed') {
        totals.totalCompleted = count;
        totalCompleted += count;
      } else {
        totals.totalFailed = count;
        totalFailed += count;
      }
      queues.set(row.queue, totals);
    }
    return {
      baselineEventId: Number(baseline.event_id ?? 0),
      baselineCommitSeq: Number(baseline.commit_seq ?? 0),
      totalCompleted,
      totalFailed,
      queues: [...queues].map(([queue, totals]) => ({ queue, ...totals })),
    };
  };
  if (sql) return await load(sql);
  return await ctx.sql.begin('isolation level repeatable read read only', load);
}
