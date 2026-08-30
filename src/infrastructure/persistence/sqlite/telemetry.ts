import type { QueueMetricsMeta, QueueMetricType } from '../../../domain/types/metrics';
import type { JobEvent } from '../../../domain/types/queue';
import { SqliteControl } from './control';
import { writeQueueEvents } from './telemetryWrites';

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

/** Durable lifecycle journal and minute-bucket metrics. */
export abstract class SqliteTelemetry extends SqliteControl {
  recordQueueEvent(event: JobEvent, maxEvents: number, maxMetricDataPoints: number): void {
    this.safeWrite(() => writeQueueEvents(this.db, [event], maxEvents, maxMetricDataPoints));
  }

  recordQueueEventsBatch(
    events: readonly JobEvent[],
    maxEvents: number,
    maxMetricDataPoints: number
  ): void {
    this.safeWrite(() => writeQueueEvents(this.db, events, maxEvents, maxMetricDataPoints));
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
