import type { QueueMetricsMeta, QueueMetricType } from '../../../domain/types/metrics';
import { EventType, type JobEvent } from '../../../domain/types/queue';
import { pack } from '../sqliteSerializer';
import { SqliteControl } from './control';

interface MetricBucketRow {
  minute: number;
  count: number;
}

interface MetricMetaRow {
  total_count: number;
  prev_ts: number;
  prev_count: number;
}

export interface PersistedQueueMetrics {
  meta: QueueMetricsMeta;
  data: number[];
}

function terminalMetricType(event: JobEvent): QueueMetricType | null {
  if (event.eventType === EventType.Completed) return 'completed';
  if (event.eventType === EventType.Failed && event.terminal !== false) return 'failed';
  return null;
}

function eventPayload(event: JobEvent): Uint8Array {
  return pack({
    data: event.data,
    error: event.error,
    progress: event.progress,
    prev: event.prev,
    delay: event.delay,
    terminal: event.terminal,
  });
}

/** Durable lifecycle journal and minute-bucket metrics. */
export abstract class SqliteTelemetry extends SqliteControl {
  recordQueueEvent(event: JobEvent, maxEvents: number, maxMetricDataPoints: number): void {
    const eventLimit = Math.max(0, Math.floor(maxEvents));
    const metricLimit = Math.max(0, Math.floor(maxMetricDataPoints));
    const metricType = terminalMetricType(event);
    const minute = Math.floor(event.timestamp / 60_000);
    const payload = eventPayload(event);

    this.safeWrite(() => {
      const transaction = this.db.transaction(() => {
        this.db
          .prepare(
            'INSERT INTO queue_events (queue, event_type, job_id, occurred_at, payload) VALUES (?, ?, ?, ?, ?)'
          )
          .run(event.queue, event.eventType, String(event.jobId), event.timestamp, payload);
        this.db
          .prepare(
            'DELETE FROM queue_events WHERE queue = ? AND id IN (SELECT id FROM queue_events WHERE queue = ? ORDER BY id DESC LIMIT -1 OFFSET ?)'
          )
          .run(event.queue, event.queue, eventLimit);

        if (!metricType) return;
        this.db
          .prepare(
            `INSERT INTO queue_metric_buckets (queue, type, minute, count)
             VALUES (?, ?, ?, 1)
             ON CONFLICT(queue, type, minute) DO UPDATE SET count = count + 1`
          )
          .run(event.queue, metricType, minute);
        const bucketCount =
          this.db
            .query<{ count: number }, [string, QueueMetricType, number]>(
              'SELECT count FROM queue_metric_buckets WHERE queue = ? AND type = ? AND minute = ?'
            )
            .get(event.queue, metricType, minute)?.count ?? 1;
        this.db
          .prepare(
            `INSERT INTO queue_metrics_meta (queue, type, total_count, prev_ts, prev_count)
             VALUES (?, ?, 1, ?, ?)
             ON CONFLICT(queue, type) DO UPDATE SET
               total_count = total_count + 1,
               prev_ts = excluded.prev_ts,
               prev_count = excluded.prev_count`
          )
          .run(event.queue, metricType, event.timestamp, bucketCount);

        if (metricLimit === 0) {
          this.db
            .prepare('DELETE FROM queue_metric_buckets WHERE queue = ? AND type = ?')
            .run(event.queue, metricType);
          return;
        }
        const newest =
          this.db
            .query<{ minute: number }, [string, QueueMetricType]>(
              'SELECT MAX(minute) AS minute FROM queue_metric_buckets WHERE queue = ? AND type = ?'
            )
            .get(event.queue, metricType)?.minute ?? minute;
        this.db
          .prepare('DELETE FROM queue_metric_buckets WHERE queue = ? AND type = ? AND minute < ?')
          .run(event.queue, metricType, newest - metricLimit + 1);
      });
      transaction();
    });
  }

  getQueueMetrics(
    queue: string,
    type: QueueMetricType,
    maxMetricDataPoints: number
  ): PersistedQueueMetrics {
    const metaRow = this.db
      .query<MetricMetaRow, [string, QueueMetricType]>(
        'SELECT total_count, prev_ts, prev_count FROM queue_metrics_meta WHERE queue = ? AND type = ?'
      )
      .get(queue, type);
    const limit = Math.max(0, Math.floor(maxMetricDataPoints));
    const rows =
      limit === 0
        ? []
        : this.db
            .query<MetricBucketRow, [string, QueueMetricType, number]>(
              'SELECT minute, count FROM queue_metric_buckets WHERE queue = ? AND type = ? ORDER BY minute DESC LIMIT ?'
            )
            .all(queue, type, limit);
    return {
      meta: {
        count: metaRow?.total_count ?? 0,
        prevTS: metaRow?.prev_ts ?? 0,
        prevCount: metaRow?.prev_count ?? 0,
      },
      data: this.expandMetricBuckets(rows, limit),
    };
  }

  trimQueueEvents(queue: string, maxLength: number): number {
    let removed = 0;
    this.safeWrite(() => {
      removed = this.db
        .prepare(
          'DELETE FROM queue_events WHERE queue = ? AND id IN (SELECT id FROM queue_events WHERE queue = ? ORDER BY id DESC LIMIT -1 OFFSET ?)'
        )
        .run(queue, queue, maxLength).changes;
    });
    return removed;
  }

  countQueueEvents(queue: string): number {
    return (
      this.db
        .query<{ count: number }, [string]>(
          'SELECT COUNT(*) AS count FROM queue_events WHERE queue = ?'
        )
        .get(queue)?.count ?? 0
    );
  }

  clearQueueTelemetry(queue: string): void {
    this.safeWrite(() => {
      const transaction = this.db.transaction(() => {
        this.db.prepare('DELETE FROM queue_events WHERE queue = ?').run(queue);
        this.db.prepare('DELETE FROM queue_metrics_meta WHERE queue = ?').run(queue);
        this.db.prepare('DELETE FROM queue_metric_buckets WHERE queue = ?').run(queue);
      });
      transaction();
    });
  }

  private expandMetricBuckets(rows: MetricBucketRow[], limit: number): number[] {
    if (rows.length === 0 || limit === 0) return [];
    const newest = rows[0].minute;
    const oldest = Math.max(rows[rows.length - 1].minute, newest - limit + 1);
    const data = new Array<number>(newest - oldest + 1).fill(0);
    for (const row of rows) {
      const index = newest - row.minute;
      if (index >= 0 && index < data.length) data[index] = row.count;
    }
    return data;
  }
}
