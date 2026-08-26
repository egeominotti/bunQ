import { EventType } from '../../domain/types/queue';
import type { PostgresStoreEvent } from '../../infrastructure/persistence/postgres';
import type { PostgresLifetimeMetricsSnapshot } from '../../infrastructure/persistence/postgres/lifetimeMetrics';

interface PostgresMetricCounters {
  readonly totalPushed: { value: bigint };
  readonly totalPulled: { value: bigint };
  readonly totalCompleted: { value: bigint };
  readonly totalFailed: { value: bigint };
}

interface PostgresPerQueueMetrics {
  clear(): void;
  get(queue: string): { totalCompleted: bigint; totalFailed: bigint } | undefined;
  set(queue: string, totals: { totalCompleted: bigint; totalFailed: bigint }): unknown;
}

export function postgresEventType(value: string): EventType | null {
  switch (value) {
    case EventType.Pushed:
    case EventType.Pulled:
    case EventType.Completed:
    case EventType.Failed:
    case EventType.Progress:
    case EventType.Stalled:
    case EventType.Removed:
    case EventType.Delayed:
    case EventType.Duplicated:
    case EventType.Retried:
    case EventType.WaitingChildren:
    case EventType.Drained:
    case EventType.Paused:
    case EventType.Resumed:
      return value;
    default:
      return null;
  }
}

export function hydratePostgresLifetimeMetrics(
  metrics: PostgresMetricCounters,
  perQueue: PostgresPerQueueMetrics,
  snapshot: PostgresLifetimeMetricsSnapshot
): void {
  metrics.totalCompleted.value = snapshot.totalCompleted;
  metrics.totalFailed.value = snapshot.totalFailed;
  perQueue.clear();
  for (const totals of snapshot.queues) {
    perQueue.set(totals.queue, {
      totalCompleted: totals.totalCompleted,
      totalFailed: totals.totalFailed,
    });
  }
}

export function applyPostgresEventMetrics(
  metrics: PostgresMetricCounters,
  perQueue: PostgresPerQueueMetrics,
  event: PostgresStoreEvent,
  includeTerminal: boolean
): void {
  const type = postgresEventType(event.type);
  if (type === EventType.Pushed) metrics.totalPushed.value++;
  if (type === EventType.Pulled) metrics.totalPulled.value++;
  const completed = includeTerminal && type === EventType.Completed;
  const failed = includeTerminal && type === EventType.Failed && event.state === 'failed';
  if (!completed && !failed) return;
  const totals = perQueue.get(event.queue) ?? { totalCompleted: 0n, totalFailed: 0n };
  if (completed) {
    metrics.totalCompleted.value++;
    totals.totalCompleted++;
  }
  if (failed) {
    metrics.totalFailed.value++;
    totals.totalFailed++;
  }
  perQueue.set(event.queue, totals);
}
