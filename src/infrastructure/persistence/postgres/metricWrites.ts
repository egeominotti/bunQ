import type { TransactionSQL } from 'bun';
import type { QueueMetricType } from '../../../domain/types/metrics';
import type { PostgresContext } from './context';

export interface PostgresMetricAddition {
  readonly queue: string;
  readonly count: number;
}

function normalizeAdditions(inputs: readonly PostgresMetricAddition[]): PostgresMetricAddition[] {
  const counts = new Map<string, number>();
  for (const input of inputs) {
    if (input.count <= 0) continue;
    counts.set(input.queue, (counts.get(input.queue) ?? 0) + input.count);
  }
  return [...counts]
    .map(([queue, count]) => ({ queue, count }))
    .sort((left, right) => left.queue.localeCompare(right.queue));
}

async function pruneMetricBuckets(
  tx: TransactionSQL,
  ctx: PostgresContext,
  queues: readonly string[],
  type: QueueMetricType,
  minute: number
): Promise<void> {
  const metricLimit = ctx.config.maxMetricDataPoints;
  await tx`
    DELETE FROM bunqueue_metric_buckets
    WHERE namespace = ${ctx.config.namespace}
      AND queue = ANY(${tx.array([...queues], 'TEXT')})
      AND metric_type = ${type}
      ${metricLimit === 0 ? tx`` : tx`AND minute < ${minute - metricLimit + 1}`}
  `;
}

async function updateTotalsWithoutBuckets(
  tx: TransactionSQL,
  ctx: PostgresContext,
  type: QueueMetricType,
  additions: readonly PostgresMetricAddition[],
  now: number
): Promise<void> {
  await tx`
    INSERT INTO bunqueue_metric_totals
      (namespace, queue, metric_type, total_count, prev_ts, prev_count)
    SELECT ${ctx.config.namespace}, batch.queue, ${type}, batch.addition, ${now},
           batch.addition
    FROM unnest(
      ${tx.array(
        additions.map(({ queue }) => queue),
        'TEXT'
      )},
      ${tx.array(
        additions.map(({ count }) => count),
        'INTEGER'
      )}
    ) AS batch(queue, addition)
    ORDER BY batch.queue
    ON CONFLICT (namespace, queue, metric_type) DO UPDATE SET
      total_count = bunqueue_metric_totals.total_count + excluded.total_count,
      prev_ts = GREATEST(bunqueue_metric_totals.prev_ts, excluded.prev_ts),
      prev_count = CASE
        WHEN excluded.prev_ts / 60000 > bunqueue_metric_totals.prev_ts / 60000
          THEN excluded.prev_count
        WHEN excluded.prev_ts / 60000 = bunqueue_metric_totals.prev_ts / 60000
          THEN bunqueue_metric_totals.prev_count + excluded.prev_count
        ELSE bunqueue_metric_totals.prev_count
      END
  `;
}

async function updateBucketsAndTotals(
  tx: TransactionSQL,
  ctx: PostgresContext,
  type: QueueMetricType,
  additions: readonly PostgresMetricAddition[],
  now: number
): Promise<void> {
  const minute = Math.floor(now / 60_000);
  await tx`
    WITH incoming AS MATERIALIZED (
      SELECT batch.queue, batch.addition
      FROM unnest(
        ${tx.array(
          additions.map(({ queue }) => queue),
          'TEXT'
        )},
        ${tx.array(
          additions.map(({ count }) => count),
          'INTEGER'
        )}
      ) AS batch(queue, addition)
      ORDER BY batch.queue
    ), buckets AS (
      INSERT INTO bunqueue_metric_buckets
        (namespace, queue, metric_type, minute, count)
      SELECT ${ctx.config.namespace}, incoming.queue, ${type}, ${minute}, incoming.addition
      FROM incoming
      ORDER BY incoming.queue
      ON CONFLICT (namespace, queue, metric_type, minute)
      DO UPDATE SET count = bunqueue_metric_buckets.count + excluded.count
      RETURNING queue, count
    )
    INSERT INTO bunqueue_metric_totals
      (namespace, queue, metric_type, total_count, prev_ts, prev_count)
    SELECT ${ctx.config.namespace}, incoming.queue, ${type}, incoming.addition, ${now},
           buckets.count
    FROM incoming
    JOIN buckets USING (queue)
    ORDER BY incoming.queue
    ON CONFLICT (namespace, queue, metric_type) DO UPDATE SET
      total_count = bunqueue_metric_totals.total_count + excluded.total_count,
      prev_ts = GREATEST(bunqueue_metric_totals.prev_ts, excluded.prev_ts),
      prev_count = CASE
        WHEN excluded.prev_ts / 60000 >= bunqueue_metric_totals.prev_ts / 60000
          THEN excluded.prev_count
        ELSE bunqueue_metric_totals.prev_count
      END
  `;
}

/** Update exact durable metrics while minimizing time spent holding shared hot rows. */
export async function recordPostgresMetricAdditions(
  tx: TransactionSQL,
  ctx: PostgresContext,
  type: QueueMetricType,
  inputs: readonly PostgresMetricAddition[],
  now: number
): Promise<void> {
  const additions = normalizeAdditions(inputs);
  if (additions.length === 0) return;
  const minute = Math.floor(now / 60_000);
  await pruneMetricBuckets(
    tx,
    ctx,
    additions.map(({ queue }) => queue),
    type,
    minute
  );
  if (ctx.config.maxMetricDataPoints === 0) {
    await updateTotalsWithoutBuckets(tx, ctx, type, additions, now);
    return;
  }
  await updateBucketsAndTotals(tx, ctx, type, additions, now);
}
