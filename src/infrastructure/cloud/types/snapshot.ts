import type { CloudEvent } from './event';
import type {
  SnapshotCollections,
  SnapshotConnections,
  SnapshotCron,
  SnapshotLatency,
  SnapshotMemory,
  SnapshotQueue,
  SnapshotRuntime,
  SnapshotSqliteStats,
  SnapshotStats,
  SnapshotStorage,
  SnapshotTaskErrors,
  SnapshotThroughput,
  SnapshotWorkers,
} from './snapshotCore';
import type {
  SnapshotActiveLock,
  SnapshotDlqEntry,
  SnapshotJob,
  SnapshotJobLogEntry,
  SnapshotStallDetail,
  SnapshotWorkerDetail,
} from './snapshotJobs';
import type {
  SnapshotDurationHistogram,
  SnapshotMcpOperation,
  SnapshotMcpSummary,
  SnapshotQueueBacklogVelocity,
  SnapshotQueueConfig,
  SnapshotQueueExtended,
  SnapshotQueueRetryRate,
  SnapshotQueueThroughput,
  SnapshotQueueWaitTime,
  SnapshotS3Backup,
  SnapshotTopError,
  SnapshotWebhook,
  SnapshotWorkerUtilization,
} from './snapshotTelemetry';

export interface CloudSnapshot {
  instanceId: string;
  instanceName: string;
  version: string;
  hostname: string;
  pid: number;
  startedAt: number;
  timestamp: number;
  sequenceId: number;
  shutdown?: boolean;
  stats: SnapshotStats;
  throughput: SnapshotThroughput;
  latency: SnapshotLatency;
  memory: SnapshotMemory;
  collections: SnapshotCollections;
  queues: SnapshotQueue[];
  workers: SnapshotWorkers;
  crons: SnapshotCron[];
  storage: SnapshotStorage;
  taskErrors: SnapshotTaskErrors;
  recentJobs: SnapshotJob[];
  dlqEntries: SnapshotDlqEntry[];
  workerDetails: SnapshotWorkerDetail[];
  queueConfigs: Record<string, SnapshotQueueConfig>;
  connections: SnapshotConnections;
  webhooks: SnapshotWebhook[];
  topErrors: SnapshotTopError[];
  queueThroughput: Record<string, SnapshotQueueThroughput>;
  durationHistogram: SnapshotDurationHistogram;
  workerUtilization: SnapshotWorkerUtilization[];
  sqliteStats: SnapshotSqliteStats | null;
  runtime: SnapshotRuntime;
  queuePriorityDistribution: Record<string, Record<number, number>>;
  queueWaitTime: Record<string, SnapshotQueueWaitTime>;
  queueRetryRate: Record<string, SnapshotQueueRetryRate>;
  queueBacklogVelocity: Record<string, SnapshotQueueBacklogVelocity>;
  stallDetails: SnapshotStallDetail[];
  events?: CloudEvent[];
  mcpOperations?: SnapshotMcpOperation[];
  mcpSummary?: SnapshotMcpSummary;
  s3Backup: SnapshotS3Backup | null;
  jobResults: Record<string, unknown>;
  jobLogEntries: Record<string, SnapshotJobLogEntry[]>;
  activeLocks: SnapshotActiveLock[];
  queueExtended: Record<string, SnapshotQueueExtended>;
  eventSubscribers: number;
  pendingDepChecks: number;
}
