import type { QueueMetrics, QueueMetricType } from '../../../domain/types/metrics';
import type { PostgresContext } from './context';
import {
  recordPostgresEventPruneWatermarks,
  summarizePostgresPrunedEvents,
} from './eventPruneWatermarks';
import { lockPostgresEventRetention, prunePostgresQueueEvents } from './eventRetention';
import { POSTGRES_EVENT_CHANNEL } from './schema';

function assertNonNegativeInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`${name} must be a non-negative safe integer`);
  }
}

export async function getPostgresQueueMetrics(
  ctx: PostgresContext,
  queue: string,
  type: QueueMetricType,
  start = 0,
  end = -1
): Promise<QueueMetrics> {
  assertNonNegativeInteger(start, 'start');
  if (end !== -1) assertNonNegativeInteger(end, 'end');
  const [metaRows, buckets] = await Promise.all([
    ctx.sql<
      Array<{
        total_count: number | string | bigint;
        prev_ts: number | string | bigint;
        prev_count: number;
      }>
    >`
      SELECT total_count, prev_ts, prev_count
      FROM bunqueue_metric_totals
      WHERE namespace = ${ctx.config.namespace} AND queue = ${queue} AND metric_type = ${type}
    `,
    ctx.sql<Array<{ minute: number | string | bigint; count: number }>>`
      SELECT minute, count FROM bunqueue_metric_buckets
      WHERE namespace = ${ctx.config.namespace} AND queue = ${queue} AND metric_type = ${type}
      ORDER BY minute DESC
      LIMIT ${ctx.config.maxMetricDataPoints}
    `,
  ]);
  const data: number[] = [];
  if (buckets.length > 0) {
    const newest = Number(buckets[0].minute);
    const oldest = Math.max(
      Number(buckets.at(-1)!.minute),
      newest - ctx.config.maxMetricDataPoints + 1
    );
    data.push(...new Array<number>(newest - oldest + 1).fill(0));
    for (const bucket of buckets) data[newest - Number(bucket.minute)] = bucket.count;
  }
  const count = data.length;
  const exclusiveEnd = end === -1 ? count : Math.min(count, end + 1);
  const meta = metaRows[0];
  return {
    meta: {
      count: Number(meta?.total_count ?? 0),
      prevTS: Number(meta?.prev_ts ?? 0),
      prevCount: meta?.prev_count ?? 0,
    },
    data: start >= exclusiveEnd ? [] : data.slice(start, exclusiveEnd),
    count,
  };
}

export async function trimPostgresQueueEvents(
  ctx: PostgresContext,
  queue: string,
  maxLength: number
): Promise<number> {
  assertNonNegativeInteger(maxLength, 'maxLength');
  return await ctx.sql.begin(async (tx) => {
    await lockPostgresEventRetention(tx, ctx, queue);
    const rows = await prunePostgresQueueEvents(tx, ctx, queue, maxLength);
    const prune = summarizePostgresPrunedEvents(rows);
    const { prunedThrough } = prune;
    if (prunedThrough > 0) {
      await recordPostgresEventPruneWatermarks(tx, ctx, [
        {
          queue,
          sourceEventId: prunedThrough,
          ...prune,
          prunesCurrentTransaction: false,
        },
      ]);
      await tx.notify(
        POSTGRES_EVENT_CHANNEL,
        JSON.stringify({
          namespace: ctx.config.namespace,
          queue,
          brokerId: ctx.config.brokerId,
          prunedThrough,
        })
      );
    }
    return rows.length;
  });
}

/** Converge retained windows after missed notifications or an unclean broker exit. */
export async function sweepPostgresEventRetention(ctx: PostgresContext): Promise<number> {
  const queues = await ctx.sql<{ queue: string }[]>`
    SELECT queue
    FROM bunqueue_events
    WHERE namespace = ${ctx.config.namespace}
    GROUP BY queue
    HAVING COUNT(*) > ${ctx.config.maxQueueEvents}
    ORDER BY queue
  `;
  let removed = 0;
  for (const { queue } of queues) {
    removed += await trimPostgresQueueEvents(ctx, queue, ctx.config.maxQueueEvents);
  }
  return removed;
}
