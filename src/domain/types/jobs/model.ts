export type JobId = string & { readonly __brand: 'JobId' };
export type LockToken = string & { readonly __brand: 'LockToken' };

export const enum JobState {
  Waiting = 'waiting',
  Prioritized = 'prioritized',
  Delayed = 'delayed',
  Active = 'active',
  Completed = 'completed',
  Failed = 'failed',
}

export interface RepeatConfig {
  readonly every?: number;
  readonly limit?: number;
  readonly pattern?: string;
  count: number;
  readonly startDate?: number;
  readonly endDate?: number;
  readonly tz?: string;
  readonly immediately?: boolean;
  prevMillis?: number;
  readonly offset?: number;
  readonly jobId?: string;
}

export interface BackoffConfig {
  readonly type: 'fixed' | 'exponential';
  readonly delay: number;
  readonly maxDelay?: number;
}

export interface JobTimelineEntry {
  readonly state: string;
  readonly timestamp: number;
  readonly worker?: string;
  readonly error?: string;
  readonly attempt?: number;
}

export interface Job {
  readonly id: JobId;
  readonly queue: string;
  readonly name: string;
  readonly data: unknown;
  readonly priority: number;
  readonly createdAt: number;
  readonly lifo: boolean;
  runAt: number;
  startedAt: number | null;
  completedAt: number | null;
  attempts: number;
  readonly maxAttempts: number;
  readonly backoff: number;
  readonly backoffConfig: BackoffConfig | null;
  readonly ttl: number | null;
  readonly timeout: number | null;
  readonly uniqueKey: string | null;
  readonly customId: string | null;
  readonly dependsOn: JobId[];
  readonly parentId: JobId | null;
  childrenIds: JobId[];
  childrenCompleted: number;
  readonly tags: string[];
  readonly groupId: string | null;
  progress: number;
  progressMessage: string | null;
  stacktrace: string[] | null;
  readonly removeOnComplete: boolean;
  readonly removeOnFail: boolean;
  readonly repeat: RepeatConfig | null;
  lastHeartbeat: number;
  readonly stallTimeout: number | null;
  stallCount: number;
  readonly stackTraceLimit: number;
  readonly keepLogs: number | null;
  readonly sizeLimit: number | null;
  readonly failParentOnFailure: boolean;
  readonly removeDependencyOnFailure: boolean;
  readonly continueParentOnFailure: boolean;
  readonly ignoreDependencyOnFailure: boolean;
  readonly deduplicationTtl: number | null;
  readonly deduplicationExtend: boolean;
  readonly deduplicationReplace: boolean;
  readonly debounceId: string | null;
  readonly debounceTtl: number | null;
  /** Whether each generation bypasses the persistence write buffer. */
  readonly durable?: boolean;
  timeline: JobTimelineEntry[];
}

export interface JobInput {
  name?: string;
  data: unknown;
  priority?: number;
  delay?: number;
  maxAttempts?: number;
  backoff?: number | { type: 'fixed' | 'exponential'; delay: number };
  ttl?: number;
  timeout?: number;
  uniqueKey?: string;
  customId?: string;
  dependsOn?: JobId[];
  childrenIds?: JobId[];
  parentId?: JobId;
  tags?: string[];
  groupId?: string;
  lifo?: boolean;
  removeOnComplete?: boolean;
  removeOnFail?: boolean;
  stallTimeout?: number;
  repeat?: {
    every?: number;
    limit?: number;
    pattern?: string;
    count?: number;
    startDate?: number;
    endDate?: number;
    tz?: string;
    immediately?: boolean;
    prevMillis?: number;
    offset?: number;
    jobId?: string;
  };
  dedup?: { ttl?: number; extend?: boolean; replace?: boolean };
  durable?: boolean;
  stackTraceLimit?: number;
  keepLogs?: number;
  sizeLimit?: number;
  failParentOnFailure?: boolean;
  removeDependencyOnFailure?: boolean;
  continueParentOnFailure?: boolean;
  ignoreDependencyOnFailure?: boolean;
  debounceId?: string;
  debounceTtl?: number;
  timestamp?: number;
}

export interface JobLock {
  readonly jobId: JobId;
  readonly token: LockToken;
  readonly owner: string;
  readonly createdAt: number;
  expiresAt: number;
  lastRenewalAt: number;
  renewalCount: number;
  readonly ttl: number;
}
