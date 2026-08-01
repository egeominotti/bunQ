export interface SnapshotQueueConfig {
  paused: boolean;
  rateLimit: number | null;
  concurrencyLimit: number | null;
  concurrencyActive: number;
  stallConfig?: {
    enabled: boolean;
    stallInterval: number;
    maxStalls: number;
    gracePeriod: number;
  };
  dlqConfig?: {
    autoRetry: boolean;
    autoRetryInterval: number;
    maxRetries: number;
    maxAge: number;
    maxEntries: number;
  };
}

export interface SnapshotWebhook {
  id: string;
  url: string;
  events: string[];
  queue: string | null;
  enabled: boolean;
  successCount: number;
  failureCount: number;
  lastTriggered: number | null;
}

export interface SnapshotTopError {
  message: string;
  count: number;
  queue: string;
  lastSeen: number;
}

export interface SnapshotQueueThroughput {
  pushPerSec: number;
  completePerSec: number;
  failPerSec: number;
  errorRate: number;
}

export interface SnapshotDurationHistogram {
  lt100ms: number;
  lt1s: number;
  lt10s: number;
  lt60s: number;
  gt60s: number;
}

export interface SnapshotWorkerUtilization {
  id: string;
  name: string;
  utilization: number;
}

export interface SnapshotQueueWaitTime {
  avgMs: number;
  maxMs: number;
  minMs: number;
}

export interface SnapshotQueueRetryRate {
  retryRate: number;
  retrying: number;
  total: number;
}

export interface SnapshotQueueBacklogVelocity {
  deltaWaiting: number;
  deltaPerMin: number;
  trend: 'growing' | 'shrinking' | 'stable';
}

export interface SnapshotMcpOperation {
  tool: string;
  queue: string | null;
  timestamp: number;
  durationMs: number;
  success: boolean;
  error: string | null;
}

export interface SnapshotMcpSummary {
  totalInvocations: number;
  successCount: number;
  failureCount: number;
  avgDurationMs: number;
  topTools: Array<{ tool: string; count: number }>;
}

export interface SnapshotS3Backup {
  enabled: boolean;
  bucket: string;
  endpoint: string;
  intervalMs: number;
  retention: number;
  isRunning: boolean;
}

export interface SnapshotQueueExtended {
  uniqueKeys: number;
  activeGroups: number;
  waitingDeps: number;
  waitingChildren: number;
}
