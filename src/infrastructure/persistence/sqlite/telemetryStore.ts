import type { Database } from 'bun:sqlite';
import type { QueueMetricType } from '../../../domain/types/metrics';
import type { JobEvent } from '../../../domain/types/queue';
import {
  applyQueueEvents,
  groupMetricEvents,
  prepareQueueEvents,
  type MetricGroup,
  type PreparedQueueEvent,
  type QueueEventCountUpdate,
  type TelemetryWriteStatements,
} from './telemetryWrites';

export interface MetricBucketRow {
  minute: number;
  count: number;
}

export interface MetricMetaRow {
  total_count: number;
  prev_ts: number;
  prev_count: number;
}

interface EventCountRow {
  queue: string;
  count: number;
}

function prepareTelemetryStatements(db: Database) {
  return {
    insertEvent: db.prepare(
      'INSERT INTO queue_events (queue, event_type, job_id, occurred_at, payload) VALUES (?, ?, ?, ?, ?)'
    ),
    trimEvents: db.prepare(
      'DELETE FROM queue_events WHERE queue = ? AND id IN (SELECT id FROM queue_events WHERE queue = ? ORDER BY id DESC LIMIT -1 OFFSET ?)'
    ),
    evictOldestEvents: db.prepare(
      'DELETE FROM queue_events WHERE queue = ? AND id IN (SELECT id FROM queue_events WHERE queue = ? ORDER BY id ASC LIMIT ?)'
    ),
    incrementBucket: db.prepare(
      `INSERT INTO queue_metric_buckets (queue, type, minute, count)
       VALUES (?, ?, ?, ?)
       ON CONFLICT(queue, type, minute) DO UPDATE SET count = count + excluded.count`
    ),
    readBucket: db.prepare(
      'SELECT count FROM queue_metric_buckets WHERE queue = ? AND type = ? AND minute = ?'
    ),
    updateMeta: db.prepare(
      `INSERT INTO queue_metrics_meta (queue, type, total_count, prev_ts, prev_count)
       VALUES (?, ?, ?, ?, ?)
       ON CONFLICT(queue, type) DO UPDATE SET
         total_count = total_count + excluded.total_count,
         prev_ts = excluded.prev_ts,
         prev_count = excluded.prev_count`
    ),
    clearBuckets: db.prepare('DELETE FROM queue_metric_buckets WHERE queue = ? AND type = ?'),
    readNewest: db.prepare(
      'SELECT MAX(minute) AS minute FROM queue_metric_buckets WHERE queue = ? AND type = ?'
    ),
    trimBuckets: db.prepare(
      'DELETE FROM queue_metric_buckets WHERE queue = ? AND type = ? AND minute < ?'
    ),
    loadEventCounts: db.prepare('SELECT queue, COUNT(*) AS count FROM queue_events GROUP BY queue'),
    countEvents: db.prepare('SELECT COUNT(*) AS count FROM queue_events WHERE queue = ?'),
    readMetricMeta: db.prepare(
      'SELECT total_count, prev_ts, prev_count FROM queue_metrics_meta WHERE queue = ? AND type = ?'
    ),
    readMetricBuckets: db.prepare(
      'SELECT minute, count FROM queue_metric_buckets WHERE queue = ? AND type = ? ORDER BY minute DESC LIMIT ?'
    ),
    clearQueueEvents: db.prepare('DELETE FROM queue_events WHERE queue = ?'),
    clearQueueMetricMeta: db.prepare('DELETE FROM queue_metrics_meta WHERE queue = ?'),
    clearQueueMetricBuckets: db.prepare('DELETE FROM queue_metric_buckets WHERE queue = ?'),
  };
}

type TelemetryStatements = ReturnType<typeof prepareTelemetryStatements>;

/** Owns SQLite telemetry statements, transactions, and exact event retention counts. */
export class SqliteTelemetryStore {
  private readonly statements: TelemetryStatements;
  private readonly eventCounts = new Map<string, number>();
  private readonly writeTransaction: (
    batch: readonly PreparedQueueEvent[],
    metricGroups: readonly MetricGroup[],
    eventLimit: number,
    metricLimit: number
  ) => QueueEventCountUpdate[];
  private readonly clearTransaction: (queue: string) => void;

  constructor(db: Database) {
    this.statements = prepareTelemetryStatements(db);
    const rows = this.statements.loadEventCounts.all() as EventCountRow[];
    for (const row of rows) this.eventCounts.set(row.queue, row.count);

    this.writeTransaction = db.transaction((batch, metricGroups, eventLimit, metricLimit) =>
      applyQueueEvents({
        statements: this.statements as TelemetryWriteStatements,
        batch,
        metricGroups,
        eventLimit,
        metricLimit,
        eventCounts: this.eventCounts,
      })
    );
    this.clearTransaction = db.transaction((queue) => this.deleteQueueTelemetryRows(queue));
  }

  writeQueueEvents(
    events: readonly JobEvent[],
    maxEvents: number,
    maxMetricDataPoints: number
  ): void {
    if (events.length === 0) return;
    const preparedEvents = prepareQueueEvents(events);
    const updates = this.writeTransaction(
      preparedEvents,
      groupMetricEvents(preparedEvents),
      Math.max(0, Math.floor(maxEvents)),
      Math.max(0, Math.floor(maxMetricDataPoints))
    );
    for (const update of updates) {
      if (update.count === 0) this.eventCounts.delete(update.queue);
      else this.eventCounts.set(update.queue, update.count);
    }
  }

  getMetricMeta(queue: string, type: QueueMetricType): MetricMetaRow | null {
    return this.statements.readMetricMeta.get(queue, type) as MetricMetaRow | null;
  }

  getMetricBuckets(queue: string, type: QueueMetricType, limit: number): MetricBucketRow[] {
    return this.statements.readMetricBuckets.all(queue, type, limit) as MetricBucketRow[];
  }

  trimQueueEvents(queue: string, maxLength: number): number {
    const removed = this.statements.trimEvents.run(queue, queue, maxLength).changes;
    this.refreshEventCount(queue);
    return removed;
  }

  countQueueEvents(queue: string): number {
    return this.refreshEventCount(queue);
  }

  clearQueueTelemetry(queue: string): void {
    this.clearTransaction(queue);
    this.eventCounts.delete(queue);
  }

  /** Run cached queue deletes inside the caller's transaction. */
  deleteQueueTelemetryRows(queue: string): void {
    this.statements.clearQueueEvents.run(queue);
    this.statements.clearQueueMetricMeta.run(queue);
    this.statements.clearQueueMetricBuckets.run(queue);
  }

  markQueueTelemetryCleared(queue: string): void {
    this.eventCounts.delete(queue);
  }

  private refreshEventCount(queue: string): number {
    const row = this.statements.countEvents.get(queue) as { count: number } | null;
    const count = row?.count ?? 0;
    if (count === 0) this.eventCounts.delete(queue);
    else this.eventCounts.set(queue, count);
    return count;
  }
}
