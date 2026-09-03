import type { Database } from 'bun:sqlite';
import type { QueueMetricType } from '../../../domain/types/metrics';
import { EventType, type JobEvent } from '../../../domain/types/queue';
import { pack } from '../sqliteSerializer';

export interface PreparedQueueEvent {
  event: JobEvent;
  metricType: QueueMetricType | null;
  minute: number;
  payload: Uint8Array;
}

interface MetricEvent {
  minute: number;
  timestamp: number;
}

export interface MetricGroup {
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

export function prepareQueueEvents(events: readonly JobEvent[]): PreparedQueueEvent[] {
  return events.map((event) => ({
    event,
    metricType: terminalMetricType(event),
    minute: Math.floor(event.timestamp / 60_000),
    payload: eventPayload(event),
  }));
}

export function groupMetricEvents(events: readonly PreparedQueueEvent[]): MetricGroup[] {
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

type TelemetryStatement = ReturnType<Database['prepare']>;

export interface TelemetryWriteStatements {
  insertEvent: TelemetryStatement;
  trimEvents: TelemetryStatement;
  evictOldestEvents: TelemetryStatement;
  incrementBucket: TelemetryStatement;
  readBucket: TelemetryStatement;
  updateMeta: TelemetryStatement;
  clearBuckets: TelemetryStatement;
  readNewest: TelemetryStatement;
  trimBuckets: TelemetryStatement;
}

export interface QueueEventCountUpdate {
  queue: string;
  count: number;
}

export interface QueueEventWrite {
  statements: TelemetryWriteStatements;
  batch: readonly PreparedQueueEvent[];
  metricGroups: readonly MetricGroup[];
  eventLimit: number;
  metricLimit: number;
  eventCounts: ReadonlyMap<string, number>;
}

/** Apply prepared events inside an existing transaction, preserving scalar semantics. */
export function applyQueueEvents(write: QueueEventWrite): QueueEventCountUpdate[] {
  const { statements, batch, metricGroups, eventLimit, metricLimit, eventCounts } = write;
  const insertedByQueue = new Map<string, number>();
  const affectedQueues = new Set<string>();
  for (const { event, payload } of batch) {
    affectedQueues.add(event.queue);
    if (eventLimit === 0) continue;
    const inserted = statements.insertEvent.run(
      event.queue,
      event.eventType,
      String(event.jobId),
      event.timestamp,
      payload
    ).changes;
    insertedByQueue.set(event.queue, (insertedByQueue.get(event.queue) ?? 0) + inserted);
  }

  const eventCountUpdates: QueueEventCountUpdate[] = [];
  for (const queue of affectedQueues) {
    let count = (eventCounts.get(queue) ?? 0) + (insertedByQueue.get(queue) ?? 0);
    if (count > eventLimit) {
      count -= statements.evictOldestEvents.run(queue, queue, count - eventLimit).changes;
    }
    eventCountUpdates.push({ queue, count });
  }

  for (const group of metricGroups) {
    const eventMinutes = new Set(group.events.map((event) => event.minute));
    const counts = new Map<number, number>();
    for (const minute of eventMinutes) {
      const row = statements.readBucket.get(group.queue, group.type, minute) as {
        count: number;
      } | null;
      const count = row?.count;
      if (count !== undefined) counts.set(minute, count);
    }

    const newestRow = statements.readNewest.get(group.queue, group.type) as {
      minute: number | null;
    } | null;
    let newest = newestRow?.minute ?? null;
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

    statements.updateMeta.run(
      group.queue,
      group.type,
      group.events.length,
      lastTimestamp,
      lastCount
    );
    if (metricLimit === 0) {
      statements.clearBuckets.run(group.queue, group.type);
      continue;
    }
    for (const [minute, count] of increments) {
      statements.incrementBucket.run(group.queue, group.type, minute, count);
    }
    if (cutoff !== null) statements.trimBuckets.run(group.queue, group.type, cutoff);
  }

  return eventCountUpdates;
}
