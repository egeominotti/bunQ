import type { QueueMetricsMeta, QueueMetricType } from '../../../domain/types/metrics';
import type { JobEvent } from '../../../domain/types/queue';
import { SqliteControl } from './control';
import type { MetricBucketRow } from './telemetryStore';

export interface PersistedQueueMetrics {
  meta: QueueMetricsMeta;
  data: number[];
}

/** Durable lifecycle journal and minute-bucket metrics. */
export abstract class SqliteTelemetry extends SqliteControl {
  recordQueueEvent(event: JobEvent, maxEvents: number, maxMetricDataPoints: number): void {
    this.safeWrite(() =>
      this.telemetryStore.writeQueueEvents([event], maxEvents, maxMetricDataPoints)
    );
  }

  recordQueueEventsBatch(
    events: readonly JobEvent[],
    maxEvents: number,
    maxMetricDataPoints: number
  ): void {
    this.safeWrite(() =>
      this.telemetryStore.writeQueueEvents(events, maxEvents, maxMetricDataPoints)
    );
  }

  getQueueMetrics(
    queue: string,
    type: QueueMetricType,
    maxMetricDataPoints: number
  ): PersistedQueueMetrics {
    const metaRow = this.telemetryStore.getMetricMeta(queue, type);
    const limit = Math.max(0, Math.floor(maxMetricDataPoints));
    const rows = limit === 0 ? [] : this.telemetryStore.getMetricBuckets(queue, type, limit);
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
      removed = this.telemetryStore.trimQueueEvents(queue, maxLength);
    });
    return removed;
  }

  countQueueEvents(queue: string): number {
    return this.telemetryStore.countQueueEvents(queue);
  }

  clearQueueTelemetry(queue: string): void {
    this.safeWrite(() => {
      this.telemetryStore.clearQueueTelemetry(queue);
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
