import type { JobOptions } from './options';

export type JobStateType =
  | 'waiting'
  | 'prioritized'
  | 'delayed'
  | 'active'
  | 'completed'
  | 'failed'
  | 'waiting-children'
  | 'unknown';

export interface JobJson<T = unknown> {
  id: string;
  name: string;
  data: T;
  opts: JobOptions;
  progress: number;
  delay: number;
  timestamp: number;
  attemptsMade: number;
  stacktrace: string[] | null;
  returnvalue?: unknown;
  failedReason?: string;
  finishedOn?: number;
  processedOn?: number;
  queueQualifiedName: string;
  parentKey?: string;
}

export interface JobJsonRaw {
  id: string;
  name: string;
  data: string;
  opts: string;
  progress: string;
  delay: string;
  timestamp: string;
  attemptsMade: string;
  stacktrace: string | null;
  returnvalue?: string;
  failedReason?: string;
  finishedOn?: string;
  processedOn?: string;
  parentKey?: string;
}

export interface ChangePriorityOpts {
  priority: number;
  lifo?: boolean;
}

export interface GetDependenciesOpts {
  processed?: { cursor?: number; count?: number };
  unprocessed?: { cursor?: number; count?: number };
}

export interface JobDependencies {
  processed: Record<string, unknown>;
  unprocessed: string[];
  nextProcessedCursor?: number;
  nextUnprocessedCursor?: number;
}

export interface JobDependenciesCount {
  processed: number;
  unprocessed: number;
}

/** Public BullMQ-compatible job contract. */
export interface Job<T = unknown> {
  id: string;
  name: string;
  data: T;
  queueName: string;
  attemptsMade: number;
  timestamp: number;
  progress: number;
  returnvalue?: unknown;
  failedReason?: string;
  parent?: { id: string; queueQualifiedName: string };
  delay: number;
  processedOn?: number;
  finishedOn?: number;
  stacktrace: string[] | null;
  stalledCounter: number;
  priority: number;
  parentKey?: string;
  opts: JobOptions;
  token?: string;
  processedBy?: string;
  deduplicationId?: string;
  repeatJobKey?: string;
  attemptsStarted: number;

  updateProgress(progress: number, message?: string): Promise<void>;
  log(message: string): Promise<void>;
  getState(): Promise<JobStateType>;
  remove(): Promise<void>;
  retry(): Promise<void>;
  getChildrenValues<R = unknown>(): Promise<Record<string, R>>;
  isWaiting(): Promise<boolean>;
  isActive(): Promise<boolean>;
  isDelayed(): Promise<boolean>;
  isCompleted(): Promise<boolean>;
  isFailed(): Promise<boolean>;
  isWaitingChildren(): Promise<boolean>;
  updateData(data: T): Promise<void>;
  promote(): Promise<void>;
  changeDelay(delay: number): Promise<void>;
  changePriority(opts: ChangePriorityOpts): Promise<void>;
  extendLock(token: string, duration: number): Promise<number>;
  clearLogs(keepLogs?: number): Promise<void>;
  getDependencies(opts?: GetDependenciesOpts): Promise<JobDependencies>;
  getDependenciesCount(opts?: GetDependenciesOpts): Promise<JobDependenciesCount>;
  toJSON(): JobJson<T>;
  asJSON(): JobJsonRaw;
  moveToCompleted(returnValue: unknown, token?: string, fetchNext?: boolean): Promise<unknown>;
  moveToFailed(error: Error, token?: string, fetchNext?: boolean): Promise<void>;
  moveToWait(token?: string): Promise<boolean>;
  moveToDelayed(timestamp: number, token?: string): Promise<void>;
  moveToWaitingChildren(
    token?: string,
    opts?: { child?: { id: string; queue: string } }
  ): Promise<boolean>;
  waitUntilFinished(queueEvents: unknown, ttl?: number): Promise<unknown>;
  discard(): void;
  getFailedChildrenValues(): Promise<Record<string, string>>;
  getIgnoredChildrenFailures(): Promise<Record<string, string>>;
  removeChildDependency(): Promise<boolean>;
  removeDeduplicationKey(): Promise<boolean>;
  removeUnprocessedChildren(): Promise<void>;
  /** Available on a native batch processor's leading job. */
  getBatch?(): Job<T>[];
  /** Mark only this member as failed while allowing the rest of its batch to complete. */
  setAsFailed?(error: Error): void;
}
