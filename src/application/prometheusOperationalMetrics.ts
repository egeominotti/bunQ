import { VERSION } from '../shared/version';
import { EMPTY_BACKUP_METRICS, type BackupMetrics } from '../infrastructure/backup/backupTelemetry';

const PROCESS_START_TIME_SECONDS = Date.now() / 1000 - process.uptime();

/** Runtime/storage gauges which are not part of queue state. */
export interface SystemMetrics {
  storageDegraded: boolean;
  storageDiskFull: boolean;
  sqliteDatabaseSizeBytes?: number;
  processHeapUsedBytes: number;
  processHeapTotalBytes: number;
  processResidentMemoryBytes: number;
  processCpuSeconds?: number;
  processStartTimeSeconds?: number;
  perQueueMetricsExported?: number;
  perQueueMetricsOmitted?: number;
  operational?: OperationalMetrics;
}

/** Server components that are owned outside QueueManager. */
export interface OperationalMetrics {
  backup?: BackupMetrics;
  connections?: {
    tcp: number;
    websocket: number;
    sse: number;
  };
}

/** Select a deterministic bounded prefix and report exact conservation. */
export function selectPrometheusQueues(
  queueNames: Iterable<string>,
  requestedLimit: number
): { selected: Set<string>; omitted: number } {
  const names = [...queueNames];
  const limit = Number.isFinite(requestedLimit) ? Math.max(0, Math.floor(requestedLimit)) : 0;
  const selected = new Set(names.slice(0, limit));
  return { selected, omitted: names.length - selected.size };
}

function escapeLabel(value: string): string {
  return value.replaceAll('\\', '\\\\').replaceAll('\n', '\\n').replaceAll('"', '\\"');
}

function metric(
  lines: string[],
  name: string,
  help: string,
  type: 'counter' | 'gauge',
  value: number
) {
  lines.push('', `# HELP ${name} ${help}`, `# TYPE ${name} ${type}`, `${name} ${value}`);
}

/** Append bounded operational and standard process collectors. */
export function appendOperationalMetrics(lines: string[], system: SystemMetrics): void {
  const cpu = system.processCpuSeconds ?? cpuSeconds();
  const processStart = system.processStartTimeSeconds ?? PROCESS_START_TIME_SECONDS;
  const backup = system.operational?.backup ?? EMPTY_BACKUP_METRICS;
  const connections = system.operational?.connections ?? { tcp: 0, websocket: 0, sse: 0 };
  const bunVersion = typeof Bun === 'undefined' ? 'unknown' : Bun.version;

  lines.push(
    '',
    '# HELP bunqueue_build_info Build and runtime identity',
    '# TYPE bunqueue_build_info gauge',
    `bunqueue_build_info{bun_version="${escapeLabel(bunVersion)}",version="${escapeLabel(VERSION)}"} 1`
  );
  metric(lines, 'process_cpu_seconds_total', 'Total process CPU time in seconds', 'counter', cpu);
  metric(
    lines,
    'process_start_time_seconds',
    'Start time of the process since the Unix epoch in seconds',
    'gauge',
    processStart
  );
  metric(
    lines,
    'process_resident_memory_bytes',
    'Resident memory size in bytes',
    'gauge',
    system.processResidentMemoryBytes
  );
  metric(
    lines,
    'process_heap_bytes',
    'Process heap size in bytes',
    'gauge',
    system.processHeapTotalBytes
  );

  lines.push(
    '',
    '# HELP bunqueue_connections Current server connections by bounded transport',
    '# TYPE bunqueue_connections gauge',
    `bunqueue_connections{transport="tcp"} ${connections.tcp}`,
    `bunqueue_connections{transport="websocket"} ${connections.websocket}`,
    `bunqueue_connections{transport="sse"} ${connections.sse}`
  );
  metric(
    lines,
    'bunqueue_queue_metrics_exported',
    'Queue names included in labelled Prometheus series',
    'gauge',
    system.perQueueMetricsExported ?? 0
  );
  metric(
    lines,
    'bunqueue_queue_metrics_omitted',
    'Queue names omitted by the Prometheus cardinality limit',
    'gauge',
    system.perQueueMetricsOmitted ?? 0
  );
  appendBackupMetrics(lines, backup);
}

function appendBackupMetrics(lines: string[], backup: BackupMetrics): void {
  metric(
    lines,
    'bunqueue_backup_enabled',
    'Whether scheduled S3 backup is enabled',
    'gauge',
    +backup.enabled
  );
  metric(
    lines,
    'bunqueue_backup_scheduler_running',
    'Whether the scheduled S3 backup timer is running',
    'gauge',
    +backup.schedulerRunning
  );
  metric(
    lines,
    'bunqueue_backup_in_progress',
    'Whether an S3 backup is in progress',
    'gauge',
    +backup.inProgress
  );
  metric(
    lines,
    'bunqueue_backup_interval_seconds',
    'Configured S3 backup interval in seconds',
    'gauge',
    backup.intervalSeconds
  );
  metric(
    lines,
    'bunqueue_backup_retention',
    'Configured number of S3 backups to retain',
    'gauge',
    backup.retention
  );
  metric(
    lines,
    'bunqueue_backup_attempts_total',
    'S3 backup attempts started',
    'counter',
    backup.attemptsTotal
  );
  metric(
    lines,
    'bunqueue_backup_successes_total',
    'Successful S3 backup attempts',
    'counter',
    backup.successesTotal
  );
  metric(
    lines,
    'bunqueue_backup_failures_total',
    'Failed S3 backup attempts',
    'counter',
    backup.failuresTotal
  );
  metric(
    lines,
    'bunqueue_backup_overlap_rejections_total',
    'S3 backup requests rejected because another backup was active',
    'counter',
    backup.overlapRejectionsTotal
  );
  metric(
    lines,
    'bunqueue_backup_consecutive_failures',
    'Consecutive failed S3 backup attempts',
    'gauge',
    backup.consecutiveFailures
  );
  metric(
    lines,
    'bunqueue_backup_last_success_timestamp_seconds',
    'Unix timestamp of the last successful S3 backup, or zero',
    'gauge',
    backup.lastSuccessTimestampSeconds
  );
  metric(
    lines,
    'bunqueue_backup_last_failure_timestamp_seconds',
    'Unix timestamp of the last failed S3 backup, or zero',
    'gauge',
    backup.lastFailureTimestampSeconds
  );
  metric(
    lines,
    'bunqueue_backup_last_duration_seconds',
    'Duration of the last S3 backup attempt',
    'gauge',
    backup.lastDurationSeconds
  );
  metric(
    lines,
    'bunqueue_backup_last_size_bytes',
    'Compressed size of the last successful S3 backup',
    'gauge',
    backup.lastSizeBytes
  );
}

function cpuSeconds(): number {
  const usage = process.cpuUsage();
  return (usage.user + usage.system) / 1_000_000;
}
