/**
 * Metrics Exporter
 * Prometheus metrics generation
 */

import type { WorkerManager } from './workerManager';
import type { WebhookManager } from './webhookManager';
import type { PerQueueStats } from './statsManager';
import { latencyTracker } from './latencyTracker';
import { appendOperationalMetrics, type SystemMetrics } from './prometheusOperationalMetrics';

export type { OperationalMetrics, SystemMetrics } from './prometheusOperationalMetrics';

function escapeLabel(value: string): string {
  return value.replaceAll('\\', '\\\\').replaceAll('\n', '\\n').replaceAll('"', '\\"');
}

/** Stats data structure */
export interface QueueStats {
  waiting: number;
  prioritized: number;
  delayed: number;
  active: number;
  dlq: number;
  completed: number;
  totalPushed: bigint;
  totalPulled: bigint;
  totalCompleted: bigint;
  totalFailed: bigint;
  uptime: number;
  cronJobs: number;
  cronPending: number;
}

/** Generate Prometheus metrics */
export function generatePrometheusMetrics(
  stats: QueueStats,
  workerManager: WorkerManager,
  webhookManager: WebhookManager,
  perQueueStats?: Map<string, PerQueueStats>,
  systemMetrics?: SystemMetrics
): string {
  const workerStats = workerManager.getStats();
  const webhookStats = webhookManager.getStats();
  const memory = process.memoryUsage();
  const system = systemMetrics ?? {
    storageDegraded: false,
    storageDiskFull: false,
    processHeapUsedBytes: memory.heapUsed,
    processHeapTotalBytes: memory.heapTotal,
    processResidentMemoryBytes: memory.rss,
  };

  const lines: string[] = [
    '# HELP bunqueue_jobs_waiting Number of jobs waiting in queue',
    '# TYPE bunqueue_jobs_waiting gauge',
    `bunqueue_jobs_waiting ${stats.waiting}`,
    '',
    '# HELP bunqueue_jobs_prioritized Number of prioritized jobs (priority > 0)',
    '# TYPE bunqueue_jobs_prioritized gauge',
    `bunqueue_jobs_prioritized ${stats.prioritized}`,
    '',
    '# HELP bunqueue_jobs_delayed Number of delayed jobs',
    '# TYPE bunqueue_jobs_delayed gauge',
    `bunqueue_jobs_delayed ${stats.delayed}`,
    '',
    '# HELP bunqueue_jobs_active Number of jobs being processed',
    '# TYPE bunqueue_jobs_active gauge',
    `bunqueue_jobs_active ${stats.active}`,
    '',
    '# HELP bunqueue_jobs_dlq Number of jobs in dead letter queue',
    '# TYPE bunqueue_jobs_dlq gauge',
    `bunqueue_jobs_dlq ${stats.dlq}`,
    '',
    '# HELP bunqueue_jobs_completed Number of completed jobs',
    '# TYPE bunqueue_jobs_completed gauge',
    `bunqueue_jobs_completed ${stats.completed}`,
    '',
    '# HELP bunqueue_jobs_pushed_total Total jobs pushed',
    '# TYPE bunqueue_jobs_pushed_total counter',
    `bunqueue_jobs_pushed_total ${stats.totalPushed}`,
    '',
    '# HELP bunqueue_jobs_pulled_total Total jobs pulled',
    '# TYPE bunqueue_jobs_pulled_total counter',
    `bunqueue_jobs_pulled_total ${stats.totalPulled}`,
    '',
    '# HELP bunqueue_jobs_completed_total Total jobs completed',
    '# TYPE bunqueue_jobs_completed_total counter',
    `bunqueue_jobs_completed_total ${stats.totalCompleted}`,
    '',
    '# HELP bunqueue_jobs_failed_total Total jobs failed',
    '# TYPE bunqueue_jobs_failed_total counter',
    `bunqueue_jobs_failed_total ${stats.totalFailed}`,
    '',
    '# HELP bunqueue_uptime_seconds Server uptime in seconds',
    '# TYPE bunqueue_uptime_seconds gauge',
    `bunqueue_uptime_seconds ${Math.floor(stats.uptime / 1000)}`,
    '',
    '# HELP bunqueue_cron_jobs_registered Number of registered cron jobs',
    '# TYPE bunqueue_cron_jobs_registered gauge',
    `bunqueue_cron_jobs_registered ${stats.cronJobs}`,
    '',
    '# HELP bunqueue_workers_registered Number of registered workers',
    '# TYPE bunqueue_workers_registered gauge',
    `bunqueue_workers_registered ${workerStats.total}`,
    '',
    '# HELP bunqueue_workers_active Number of active workers',
    '# TYPE bunqueue_workers_active gauge',
    `bunqueue_workers_active ${workerStats.active}`,
    '',
    '# HELP bunqueue_worker_active_jobs Number of jobs currently processed by workers',
    '# TYPE bunqueue_worker_active_jobs gauge',
    `bunqueue_worker_active_jobs ${workerStats.activeJobs}`,
    '',
    '# HELP bunqueue_worker_concurrency_slots Configured worker concurrency capacity',
    '# TYPE bunqueue_worker_concurrency_slots gauge',
    `bunqueue_worker_concurrency_slots ${workerStats.concurrencySlots}`,
    '',
    '# HELP bunqueue_workers_processed_total Total jobs processed by workers',
    '# TYPE bunqueue_workers_processed_total counter',
    `bunqueue_workers_processed_total ${workerStats.totalProcessed}`,
    '',
    '# HELP bunqueue_workers_failed_total Total jobs failed by workers',
    '# TYPE bunqueue_workers_failed_total counter',
    `bunqueue_workers_failed_total ${workerStats.totalFailed}`,
    '',
    '# HELP bunqueue_webhooks_registered Number of registered webhooks',
    '# TYPE bunqueue_webhooks_registered gauge',
    `bunqueue_webhooks_registered ${webhookStats.total}`,
    '',
    '# HELP bunqueue_webhooks_enabled Number of enabled webhooks',
    '# TYPE bunqueue_webhooks_enabled gauge',
    `bunqueue_webhooks_enabled ${webhookStats.enabled}`,
    '',
    '# HELP bunqueue_storage_degraded Whether persistent storage is degraded',
    '# TYPE bunqueue_storage_degraded gauge',
    `bunqueue_storage_degraded ${system.storageDegraded ? 1 : 0}`,
    '',
    '# HELP bunqueue_storage_disk_full Whether persistent storage reported a full disk',
    '# TYPE bunqueue_storage_disk_full gauge',
    `bunqueue_storage_disk_full ${system.storageDiskFull ? 1 : 0}`,
    '',
    '# HELP bunqueue_process_heap_used_bytes Process heap bytes currently used',
    '# TYPE bunqueue_process_heap_used_bytes gauge',
    `bunqueue_process_heap_used_bytes ${system.processHeapUsedBytes}`,
    '',
    '# HELP bunqueue_process_heap_total_bytes Total process heap bytes',
    '# TYPE bunqueue_process_heap_total_bytes gauge',
    `bunqueue_process_heap_total_bytes ${system.processHeapTotalBytes}`,
    '',
    '# HELP bunqueue_process_resident_memory_bytes Process resident memory size in bytes',
    '# TYPE bunqueue_process_resident_memory_bytes gauge',
    `bunqueue_process_resident_memory_bytes ${system.processResidentMemoryBytes}`,
  ];

  if (system.sqliteDatabaseSizeBytes !== undefined) {
    lines.push(
      '',
      '# HELP bunqueue_sqlite_database_size_bytes SQLite database file size in bytes',
      '# TYPE bunqueue_sqlite_database_size_bytes gauge',
      `bunqueue_sqlite_database_size_bytes ${system.sqliteDatabaseSizeBytes}`
    );
  }

  // Per-queue metrics
  if (perQueueStats && perQueueStats.size > 0) {
    lines.push('');
    lines.push('# HELP bunqueue_queue_jobs_waiting Number of waiting jobs per queue');
    lines.push('# TYPE bunqueue_queue_jobs_waiting gauge');
    for (const [queue, qs] of perQueueStats) {
      lines.push(`bunqueue_queue_jobs_waiting{queue="${escapeLabel(queue)}"} ${qs.waiting}`);
    }

    lines.push('');
    lines.push('# HELP bunqueue_queue_jobs_prioritized Number of prioritized jobs per queue');
    lines.push('# TYPE bunqueue_queue_jobs_prioritized gauge');
    for (const [queue, qs] of perQueueStats) {
      lines.push(
        `bunqueue_queue_jobs_prioritized{queue="${escapeLabel(queue)}"} ${qs.prioritized}`
      );
    }

    lines.push('');
    lines.push('# HELP bunqueue_queue_jobs_delayed Number of delayed jobs per queue');
    lines.push('# TYPE bunqueue_queue_jobs_delayed gauge');
    for (const [queue, qs] of perQueueStats) {
      lines.push(`bunqueue_queue_jobs_delayed{queue="${escapeLabel(queue)}"} ${qs.delayed}`);
    }

    lines.push('');
    lines.push('# HELP bunqueue_queue_jobs_active Number of active jobs per queue');
    lines.push('# TYPE bunqueue_queue_jobs_active gauge');
    for (const [queue, qs] of perQueueStats) {
      lines.push(`bunqueue_queue_jobs_active{queue="${escapeLabel(queue)}"} ${qs.active}`);
    }

    lines.push('');
    lines.push('# HELP bunqueue_queue_jobs_dlq Number of DLQ jobs per queue');
    lines.push('# TYPE bunqueue_queue_jobs_dlq gauge');
    for (const [queue, qs] of perQueueStats) {
      lines.push(`bunqueue_queue_jobs_dlq{queue="${escapeLabel(queue)}"} ${qs.dlq}`);
    }
  }

  appendOperationalMetrics(lines, {
    ...system,
    perQueueMetricsExported: system.perQueueMetricsExported ?? perQueueStats?.size ?? 0,
  });

  // Append latency histograms
  const histogramOutput = latencyTracker.toPrometheus();
  if (histogramOutput) {
    lines.push('');
    lines.push(histogramOutput);
  }

  return `${lines.join('\n')}\n`;
}
