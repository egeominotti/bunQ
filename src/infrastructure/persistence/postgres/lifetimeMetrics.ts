import type { PostgresContext } from './context';

export interface PostgresQueueLifetimeTotals {
  readonly queue: string;
  readonly totalCompleted: bigint;
  readonly totalFailed: bigint;
}

export interface PostgresLifetimeMetricsSnapshot {
  readonly baselineEventId: number;
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
  ctx: PostgresContext
): Promise<PostgresLifetimeMetricsSnapshot> {
  return await ctx.sql.begin('isolation level repeatable read read only', async (tx) => {
    const [[event], rows] = await Promise.all([
      tx<{ id: number | string | bigint | null }[]>`
        SELECT MAX(id) AS id FROM bunqueue_events WHERE namespace = ${ctx.config.namespace}
      `,
      tx<MetricTotalRow[]>`
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
      baselineEventId: Number(event.id ?? 0),
      totalCompleted,
      totalFailed,
      queues: [...queues].map(([queue, totals]) => ({ queue, ...totals })),
    };
  });
}
