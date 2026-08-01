export interface SnapshotStats {
  waiting: number;
  prioritized: number;
  delayed: number;
  active: number;
  dlq: number;
  completed: number;
  'waiting-children': number;
  totalPushed: string;
  totalPulled: string;
  totalCompleted: string;
  totalFailed: string;
  stalled: number;
  paused: number;
  uptime: number;
  cronJobs: number;
  cronPending: number;
}

export interface SnapshotThroughput {
  pushPerSec: number;
  pullPerSec: number;
  completePerSec: number;
  failPerSec: number;
}

export interface SnapshotLatency {
  averages: { pushMs: number; pullMs: number; ackMs: number };
  percentiles: {
    push: { p50: number; p95: number; p99: number };
    pull: { p50: number; p95: number; p99: number };
    ack: { p50: number; p95: number; p99: number };
  };
}

export interface SnapshotMemory {
  heapUsed: number;
  heapTotal: number;
  rss: number;
  external: number;
}

export interface SnapshotCollections {
  jobIndex: number;
  completedJobs: number;
  jobResults: number;
  jobLogs: number;
  customIdMap: number;
  jobLocks: number;
  processingTotal: number;
  queuedTotal: number;
  temporalIndexTotal: number;
  delayedHeapTotal: number;
}

export interface SnapshotQueue {
  name: string;
  waiting: number;
  prioritized: number;
  delayed: number;
  active: number;
  completed: number;
  failed: number;
  'waiting-children': number;
  paused: boolean;
  totalCompleted: number;
  totalFailed: number;
}

export interface SnapshotWorkers {
  total: number;
  active: number;
  totalProcessed: number;
  totalFailed: number;
  activeJobs: number;
}

export interface SnapshotCron {
  name: string;
  queue: string;
  schedule: string | null;
  repeatEvery: number | null;
  nextRun: number;
  executions: number;
  maxLimit: number | null;
  lastRun: number | null;
  priority: number;
  timezone: string | null;
  data: unknown;
  uniqueKey: string | null;
  dedup: unknown;
}

export interface SnapshotStorage {
  diskFull: boolean;
  error: string | null;
}

export type SnapshotTaskErrors = Record<
  string,
  { consecutiveFailures: number; lastError?: string; lastFailureAt?: number }
>;

export interface SnapshotConnections {
  tcp: number;
  ws: number;
  sse: number;
}

export interface SnapshotSqliteStats {
  dbSizeBytes: number;
  writeBufferPending: number;
}

export interface SnapshotRuntime {
  bunVersion: string;
  os: string;
  arch: string;
  cpus: number;
}
