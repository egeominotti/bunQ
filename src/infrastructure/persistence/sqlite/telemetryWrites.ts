import type { Database } from 'bun:sqlite';
import type { QueueMetricType } from '../../../domain/types/metrics';
import { EventType, type JobEvent } from '../../../domain/types/queue';
import { pack } from '../sqliteSerializer';

interface PreparedQueueEvent {
  event: JobEvent;
  metricType: QueueMetricType | null;
  minute: number;
  payload: Uint8Array;
}

interface MetricEvent {
  minute: number;
  timestamp: number;
}

interface MetricGroup {
  queue: string;
  type: QueueMetricType;
  events: MetricEvent[];
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

function prepareQueueEvents(events: readonly JobEvent[]): PreparedQueueEvent[] {
  return events.map((event) => ({
    event,
    metricType: terminalMetricType(event),
    minute: Math.floor(event.timestamp / 60_000),
    payload: eventPayload(event),
  }));
}

function groupMetricEvents(events: readonly PreparedQueueEvent[]): MetricGroup[] {
  const byQueue = new Map<string, Map<QueueMetricType, MetricGroup>>();
  for (const entry of events) {
    if (!entry.metricType) continue;
    let byType = byQueue.get(entry.event.queue);
    if (!byType) {
      byType = new Map();
      byQueue.set(entry.event.queue, byType);
    }
    let group = byType.get(entry.metricType);
    if (!group) {
      group = { queue: entry.event.queue, type: entry.metricType, events: [] };
      byType.set(entry.metricType, group);
    }
    group.events.push({ minute: entry.minute, timestamp: entry.event.timestamp });
  }
  return [...byQueue.values()].flatMap((byType) => [...byType.values()]);
}

/** Apply events in input order inside one transaction, preserving scalar semantics. */
export function writeQueueEvents(
  db: Database,
  events: readonly JobEvent[],
  maxEvents: number,
  maxMetricDataPoints: number
): void {
  if (events.length === 0) return;
  const preparedEvents = prepareQueueEvents(events);
  const metricGroups = groupMetricEvents(preparedEvents);
  const eventLimit = Math.max(0, Math.floor(maxEvents));
  const metricLimit = Math.max(0, Math.floor(maxMetricDataPoints));
  const insertEvent = db.prepare(
    'INSERT INTO queue_events (queue, event_type, job_id, occurred_at, payload) VALUES (?, ?, ?, ?, ?)'
  );
  const trimEvents = db.prepare(
    'DELETE FROM queue_events WHERE queue = ? AND id IN (SELECT id FROM queue_events WHERE queue = ? ORDER BY id DESC LIMIT -1 OFFSET ?)'
  );
  const incrementBucket = db.prepare(
    `INSERT INTO queue_metric_buckets (queue, type, minute, count)
     VALUES (?, ?, ?, ?)
     ON CONFLICT(queue, type, minute) DO UPDATE SET count = count + excluded.count`
  );
  const readBucket = db.query<{ count: number }, [string, QueueMetricType, number]>(
    'SELECT count FROM queue_metric_buckets WHERE queue = ? AND type = ? AND minute = ?'
  );
  const updateMeta = db.prepare(
    `INSERT INTO queue_metrics_meta (queue, type, total_count, prev_ts, prev_count)
     VALUES (?, ?, ?, ?, ?)
     ON CONFLICT(queue, type) DO UPDATE SET
       total_count = total_count + excluded.total_count,
       prev_ts = excluded.prev_ts,
       prev_count = excluded.prev_count`
  );
  const clearBuckets = db.prepare('DELETE FROM queue_metric_buckets WHERE queue = ? AND type = ?');
  const readNewest = db.query<{ minute: number | null }, [string, QueueMetricType]>(
    'SELECT MAX(minute) AS minute FROM queue_metric_buckets WHERE queue = ? AND type = ?'
  );
  const trimBuckets = db.prepare(
    'DELETE FROM queue_metric_buckets WHERE queue = ? AND type = ? AND minute < ?'
  );

  const transaction = db.transaction((batch: readonly PreparedQueueEvent[]) => {
    const affectedQueues = new Set<string>();
    for (const { event, payload } of batch) {
      insertEvent.run(event.queue, event.eventType, String(event.jobId), event.timestamp, payload);
      affectedQueues.add(event.queue);
    }
    for (const queue of affectedQueues) trimEvents.run(queue, queue, eventLimit);

    for (const group of metricGroups) {
      const eventMinutes = new Set(group.events.map((event) => event.minute));
      const counts = new Map<number, number>();
      for (const minute of eventMinutes) {
        const count = readBucket.get(group.queue, group.type, minute)?.count;
        if (count !== undefined) counts.set(minute, count);
      }

      let newest = readNewest.get(group.queue, group.type)?.minute ?? null;
      const increments = new Map<number, number>();
      let lastTimestamp = 0;
      let lastCount = 0;
      let cutoff: number | null = null;

      for (const event of group.events) {
        lastCount = (counts.get(event.minute) ?? 0) + 1;
        lastTimestamp = event.timestamp;
        counts.set(event.minute, lastCount);
        increments.set(event.minute, (increments.get(event.minute) ?? 0) + 1);

        if (metricLimit === 0) {
          counts.clear();
          increments.clear();
          newest = null;
          continue;
        }

        newest = newest === null ? event.minute : Math.max(newest, event.minute);
        cutoff = newest - metricLimit + 1;
        for (const minute of counts.keys()) {
          if (minute >= cutoff) continue;
          counts.delete(minute);
          increments.delete(minute);
        }
      }

      updateMeta.run(group.queue, group.type, group.events.length, lastTimestamp, lastCount);
      if (metricLimit === 0) {
        clearBuckets.run(group.queue, group.type);
        continue;
      }
      for (const [minute, count] of increments) {
        incrementBucket.run(group.queue, group.type, minute, count);
      }
      if (cutoff !== null) trimBuckets.run(group.queue, group.type, cutoff);
    }
  });
  transaction(preparedEvents);
}
