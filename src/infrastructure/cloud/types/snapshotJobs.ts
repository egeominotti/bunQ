export interface SnapshotTimelineEntry {
  state: string;
  timestamp: number;
  worker?: string;
  error?: string;
  attempt?: number;
}

export interface SnapshotJob {
  id: string;
  name: string;
  queue: string;
  state: string;
  data?: unknown;
  priority: number;
  createdAt: number;
  startedAt?: number;
  completedAt?: number;
  runAt: number;
  failedReason?: string;
  attempts: number;
  maxAttempts: number;
  backoff: number;
  timeout?: number;
  ttl?: number;
  duration?: number;
  waitTime?: number;
  totalDuration?: number;
  progress?: number;
  progressMessage?: string;
  customId?: string;
  uniqueKey?: string;
  tags?: string[];
  groupId?: string;
  parentId?: string;
  childrenIds?: string[];
  dependsOn?: string[];
  childrenCompleted?: number;
  lastHeartbeat?: number;
  stallCount?: number;
  stallTimeout?: number;
  removeOnComplete?: boolean;
  removeOnFail?: boolean;
  lifo?: boolean;
  backoffConfig?: unknown;
  repeat?: unknown;
  stackTraceLimit: number;
  keepLogs?: number;
  sizeLimit?: number;
  failParentOnFailure?: boolean;
  removeDependencyOnFailure?: boolean;
  continueParentOnFailure?: boolean;
  ignoreDependencyOnFailure?: boolean;
  deduplicationTtl?: number;
  deduplicationExtend?: boolean;
  deduplicationReplace?: boolean;
  debounceId?: string;
  debounceTtl?: number;
  timeline?: SnapshotTimelineEntry[];
}

export interface SnapshotDlqAttempt {
  attempt: number;
  startedAt: number;
  failedAt: number;
  reason: string;
  error: string | null;
  duration: number;
}

export interface SnapshotDlqEntry {
  jobId: string;
  queue: string;
  reason: string;
  error: string | null;
  enteredAt: number;
  retryCount: number;
  lastRetryAt?: number;
  nextRetryAt?: number;
  expiresAt?: number;
  jobAttempts: number;
  jobMaxAttempts: number;
  jobData?: unknown;
  jobCreatedAt: number;
  jobPriority: number;
  attemptHistory: SnapshotDlqAttempt[];
}

export interface SnapshotWorkerDetail {
  id: string;
  name: string;
  queues: string[];
  concurrency: number;
  hostname: string;
  pid: number;
  registeredAt: number;
  lastSeen: number;
  activeJobs: number;
  processedJobs: number;
  failedJobs: number;
  currentJob: string | null;
  uptime: number;
  status: 'active' | 'idle' | 'stalled';
  errorRate: number;
  utilization: number;
}

export interface SnapshotStallDetail {
  jobId: string;
  queue: string;
  workerId: string | null;
  stalledAt: number;
  stalledForMs: number;
}

export interface SnapshotJobLogEntry {
  timestamp: number;
  level: 'info' | 'warn' | 'error';
  message: string;
}

export interface SnapshotActiveLock {
  jobId: string;
  owner: string;
  token: string;
  createdAt: number;
  expiresAt: number;
  lastRenewalAt: number;
  renewalCount: number;
  ttl: number;
}
