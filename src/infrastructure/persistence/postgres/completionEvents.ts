import type { TransactionSQL } from 'bun';
import type { Job } from '../../../domain/types/job';
import { recordPostgresJobEvents } from './batchEvents';
import type { PostgresContext } from './context';

interface CompletionEventInput {
  readonly job: Job;
  readonly result: unknown;
  readonly removed: boolean;
}

interface MetricBucketRow {
  readonly queue: string;
  readonly count: number;
}

async function updateMetrics(
  tx: TransactionSQL,
  ctx: PostgresContext,
  inputs: readonly CompletionEventInput[],
  now: number
): Promise<void> {
  const counts = new Map<string, number>();
  for (const input of inputs) counts.set(input.job.queue, (counts.get(input.job.queue) ?? 0) + 1);
  const queues = [...counts.keys()];
  const additions = queues.map((queue) => counts.get(queue) ?? 0);
  const minute = Math.floor(now / 60_000);
  const buckets = await tx<MetricBucketRow[]>`
    INSERT INTO bunqueue_metric_buckets
      (namespace, queue, metric_type, minute, count)
    SELECT ${ctx.config.namespace}, batch.queue, 'completed', ${minute}, batch.addition
    FROM unnest(
      ${tx.array(queues, 'TEXT')},
      ${tx.array(additions, 'INTEGER')}
    ) AS batch(queue, addition)
    ON CONFLICT (namespace, queue, metric_type, minute)
    DO UPDATE SET count = bunqueue_metric_buckets.count + excluded.count
    RETURNING queue, count
  `;
  const bucketCounts = new Map(buckets.map((bucket) => [bucket.queue, bucket.count]));
  await tx`
    INSERT INTO bunqueue_metric_totals
      (namespace, queue, metric_type, total_count, prev_ts, prev_count)
    SELECT
      ${ctx.config.namespace}, batch.queue, 'completed', batch.addition, ${now},
      batch.bucket_count
    FROM unnest(
      ${tx.array(queues, 'TEXT')},
      ${tx.array(additions, 'BIGINT')},
      ${tx.array(
        queues.map((queue) => bucketCounts.get(queue) ?? 0),
        'INTEGER'
      )}
    ) AS batch(queue, addition, bucket_count)
    ON CONFLICT (namespace, queue, metric_type) DO UPDATE SET
      total_count = bunqueue_metric_totals.total_count + excluded.total_count,
      prev_ts = excluded.prev_ts,
      prev_count = excluded.prev_count
  `;
  const metricLimit = ctx.config.maxMetricDataPoints;
  if (metricLimit === 0) {
    await tx`
      DELETE FROM bunqueue_metric_buckets
      WHERE namespace = ${ctx.config.namespace}
        AND queue = ANY(${tx.array(queues, 'TEXT')})
        AND metric_type = 'completed'
    `;
  } else {
    await tx`
      DELETE FROM bunqueue_metric_buckets
      WHERE namespace = ${ctx.config.namespace}
        AND queue = ANY(${tx.array(queues, 'TEXT')})
        AND metric_type = 'completed'
        AND minute < ${minute - metricLimit + 1}
    `;
  }
}

/** Persist one completion event per job while amortizing queue-level bookkeeping. */
export async function recordPostgresCompletionEvents(
  tx: TransactionSQL,
  ctx: PostgresContext,
  inputs: readonly CompletionEventInput[],
  now: number
): Promise<void> {
  if (inputs.length === 0) return;
  await recordPostgresJobEvents(
    tx,
    ctx,
    inputs.map((input) => ({
      job: input.job,
      type: 'completed',
      state: 'completed',
      result: input.result,
      removed: input.removed,
    })),
    now
  );
  await updateMetrics(tx, ctx, inputs, now);
}
