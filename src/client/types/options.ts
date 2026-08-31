export interface ParentOpts {
  id: string;
  queue: string;
}

export interface BackoffOptions {
  type: 'fixed' | 'exponential';
  delay: number;
}

export interface KeepJobs {
  age?: number;
  count?: number;
}

export interface DeduplicationOptions {
  id: string;
  ttl?: number;
  extend?: boolean;
  replace?: boolean;
}

export interface DebounceOptions {
  id: string;
  ttl: number;
}

export interface RepeatOptions {
  every?: number;
  limit?: number;
  pattern?: string;
  startDate?: Date | string | number;
  endDate?: Date | string | number;
  tz?: string;
  immediately?: boolean;
  count?: number;
  prevMillis?: number;
  offset?: number;
  jobId?: string;
}

export interface GroupJobOptions {
  id: string | number;
}

/** Options accepted when adding a job. */
export interface JobOptions {
  priority?: number;
  delay?: number;
  attempts?: number;
  backoff?: number | BackoffOptions;
  timeout?: number;
  jobId?: string;
  /** Age/count retention is intentionally unsupported. */
  removeOnComplete?: boolean;
  removeOnFail?: boolean;
  stallTimeout?: number;
  repeat?: RepeatOptions;
  durable?: boolean;
  parent?: ParentOpts;
  lifo?: boolean;
  stackTraceLimit?: number;
  keepLogs?: number;
  sizeLimit?: number;
  failParentOnFailure?: boolean;
  removeDependencyOnFailure?: boolean;
  continueParentOnFailure?: boolean;
  ignoreDependencyOnFailure?: boolean;
  timestamp?: number;
  deduplication?: DeduplicationOptions;
  debounce?: DebounceOptions;
  group?: GroupJobOptions;
}
