import type { JobId } from '../../domain/types/job';
import type { JobLogEntry } from '../../domain/types/worker';
import * as logsOps from '../jobLogsManager';
import { generatePrometheusMetrics, type OperationalMetrics } from '../metricsExporter';
import { selectPrometheusQueues } from '../prometheusOperationalMetrics';
import * as statsMgr from '../statsManager';
import type { QueueMetricType } from '../../domain/types/metrics';
import { isStorageDegraded } from '../../shared/storageHealth';
import { QueueManagerStats } from './stats';

export class QueueManagerObservability extends QueueManagerStats {
  getQueueMetrics(queue: string, type: QueueMetricType, start = 0, end = -1) {
    return this.telemetryJournal.getMetrics(queue, type, start, end);
  }

  trimQueueEvents(queue: string, maxLength: number): number {
    return this.telemetryJournal.trimEvents(queue, maxLength);
  }

  addLog(jobId: JobId, message: string, level: 'info' | 'warn' | 'error' = 'info'): boolean {
    return logsOps.addJobLog(jobId, message, this.contextFactory.getLogsContext(), level);
  }

  getLogs(jobId: JobId): JobLogEntry[] {
    return logsOps.getJobLogs(jobId, this.contextFactory.getLogsContext());
  }

  clearLogs(jobId: JobId, keepLogs?: number): void {
    logsOps.clearJobLogs(jobId, this.contextFactory.getLogsContext(), keepLogs);
  }

  getPerQueueStats() {
    return statsMgr.getPerQueueStats(this.contextFactory.getStatsContext(), this.queueNamesCache);
  }

  getPrometheusMetrics(): string {
    const storageStatus = this.getStorageStatus();
    const memory = process.memoryUsage();
    const queueSelection = selectPrometheusQueues(
      this.queueNamesCache,
      this.config.maxPrometheusQueues
    );
    return generatePrometheusMetrics(
      this.getStats(),
      this.workerManager,
      this.webhookManager,
      statsMgr.getPerQueueStats(this.contextFactory.getStatsContext(), queueSelection.selected),
      {
        storageDegraded: isStorageDegraded(storageStatus),
        storageDiskFull: storageStatus.diskFull,
        ...(this.storage && { sqliteDatabaseSizeBytes: this.storage.getSize() }),
        processHeapUsedBytes: memory.heapUsed,
        processHeapTotalBytes: memory.heapTotal,
        processResidentMemoryBytes: memory.rss,
        perQueueMetricsExported: queueSelection.selected.size,
        perQueueMetricsOmitted: queueSelection.omitted,
        operational: this.operationalMetricsProvider?.(),
      }
    );
  }

  setOperationalMetricsProvider(provider: () => OperationalMetrics): void {
    this.operationalMetricsProvider = provider;
  }
}
