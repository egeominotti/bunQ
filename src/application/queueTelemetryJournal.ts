import type { QueueMetrics, QueueMetricType } from '../domain/types/metrics';
import { EventType, type JobEvent } from '../domain/types/queue';
import type { SqliteStorage } from '../infrastructure/persistence/sqlite';

interface MetricState {
  total: number;
  prevTS: number;
  prevCount: number;
  buckets: Map<number, number>;
}

function metricType(event: JobEvent): QueueMetricType | null {
  if (event.eventType === EventType.Completed) return 'completed';
  if (event.eventType === EventType.Failed && event.terminal !== false) return 'failed';
  return null;
}

function assertNonNegativeInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new RangeError(`${name} must be a non-negative safe integer`);
  }
}

/** Per-queue bounded event journal and cumulative minute metrics. */
export class QueueTelemetryJournal {
  private readonly events = new Map<string, JobEvent[]>();
  private readonly metrics = new Map<string, Map<QueueMetricType, MetricState>>();
  private readonly maxEvents: number;
  private readonly maxMetricDataPoints: number;

  constructor(
    private readonly storage: SqliteStorage | null,
    maxEvents: number,
    maxMetricDataPoints: number
  ) {
    assertNonNegativeInteger(maxEvents, 'maxQueueEvents');
    assertNonNegativeInteger(maxMetricDataPoints, 'maxMetricDataPoints');
    this.maxEvents = maxEvents;
    this.maxMetricDataPoints = maxMetricDataPoints;
  }

  record(event: JobEvent): void {
    if (this.storage) {
      this.storage.recordQueueEvent(event, this.maxEvents, this.maxMetricDataPoints);
      return;
    }

    const queueEvents = this.events.get(event.queue) ?? [];
    queueEvents.push(event);
    if (queueEvents.length > this.maxEvents) {
      queueEvents.splice(0, queueEvents.length - this.maxEvents);
    }
    this.events.set(event.queue, queueEvents);

    const type = metricType(event);
    if (!type) return;
    const queueMetrics = this.metrics.get(event.queue) ?? new Map();
    const state = queueMetrics.get(type) ?? {
      total: 0,
      prevTS: 0,
      prevCount: 0,
      buckets: new Map<number, number>(),
    };
    const minute = Math.floor(event.timestamp / 60_000);
    const bucketCount = (state.buckets.get(minute) ?? 0) + 1;
    state.buckets.set(minute, bucketCount);
    state.total++;
    state.prevTS = event.timestamp;
    state.prevCount = bucketCount;
    this.pruneBuckets(state.buckets);
    queueMetrics.set(type, state);
    this.metrics.set(event.queue, queueMetrics);
  }

  getMetrics(queue: string, type: QueueMetricType, start = 0, end = -1): QueueMetrics {
    assertNonNegativeInteger(start, 'start');
    if (end !== -1) assertNonNegativeInteger(end, 'end');

    const full = this.storage
      ? this.storage.getQueueMetrics(queue, type, this.maxMetricDataPoints)
      : this.getMemoryMetrics(queue, type);
    const count = full.data.length;
    const exclusiveEnd = end === -1 ? count : Math.min(count, end + 1);
    return {
      meta: full.meta,
      data: start >= exclusiveEnd ? [] : full.data.slice(start, exclusiveEnd),
      count,
    };
  }

  trimEvents(queue: string, maxLength: number): number {
    assertNonNegativeInteger(maxLength, 'maxLength');
    if (this.storage) return this.storage.trimQueueEvents(queue, maxLength);
    const events = this.events.get(queue);
    if (!events || events.length <= maxLength) return 0;
    const removed = events.length - maxLength;
    events.splice(0, removed);
    if (events.length === 0) this.events.delete(queue);
    return removed;
  }

  clearQueue(queue: string): void {
    this.events.delete(queue);
    this.metrics.delete(queue);
    this.storage?.clearQueueTelemetry(queue);
  }

  clearMemory(): void {
    this.events.clear();
    this.metrics.clear();
  }

  private getMemoryMetrics(queue: string, type: QueueMetricType) {
    const state = this.metrics.get(queue)?.get(type);
    if (!state) {
      return { meta: { count: 0, prevTS: 0, prevCount: 0 }, data: [] as number[] };
    }
    return {
      meta: { count: state.total, prevTS: state.prevTS, prevCount: state.prevCount },
      data: this.expandBuckets(state.buckets),
    };
  }

  private pruneBuckets(buckets: Map<number, number>): void {
    if (this.maxMetricDataPoints <= 0) {
      buckets.clear();
      return;
    }
    let newest = Number.NEGATIVE_INFINITY;
    for (const minute of buckets.keys()) newest = Math.max(newest, minute);
    const cutoff = newest - this.maxMetricDataPoints + 1;
    for (const minute of buckets.keys()) {
      if (minute < cutoff) buckets.delete(minute);
    }
  }

  private expandBuckets(buckets: Map<number, number>): number[] {
    if (buckets.size === 0 || this.maxMetricDataPoints <= 0) return [];
    const minutes = [...buckets.keys()].sort((a, b) => b - a);
    const newest = minutes[0];
    const oldest = Math.max(minutes[minutes.length - 1], newest - this.maxMetricDataPoints + 1);
    const data = new Array<number>(newest - oldest + 1).fill(0);
    for (const [minute, count] of buckets) {
      const index = newest - minute;
      if (index >= 0 && index < data.length) data[index] = count;
    }
    return data;
  }
}
