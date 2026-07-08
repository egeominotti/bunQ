/** Shared option types + wire mapping, mirroring the official client. */

import { compact } from './frame.js';

export interface BackoffOptions {
  type: 'fixed' | 'exponential';
  delay: number;
  maxDelay?: number;
}

export interface DeduplicationOptions {
  id: string;
  ttl?: number;
  extend?: boolean;
  replace?: boolean;
}

export interface RepeatOptions {
  every?: number;
  limit?: number;
  pattern?: string;
  count?: number;
  startDate?: number;
  endDate?: number;
  tz?: string;
  immediately?: boolean;
  offset?: number;
  jobId?: string;
}

export interface JobOptions {
  priority?: number;
  delay?: number;
  /** Max attempts (server name: maxAttempts). */
  attempts?: number;
  backoff?: number | BackoffOptions;
  ttl?: number;
  timeout?: number;
  /** Custom job id (idempotent add). */
  jobId?: string;
  deduplication?: DeduplicationOptions;
  dependsOn?: string[];
  parentId?: string;
  childrenIds?: string[];
  tags?: string[];
  groupId?: string;
  lifo?: boolean;
  removeOnComplete?: boolean;
  removeOnFail?: boolean;
  stallTimeout?: number;
  durable?: boolean;
  repeat?: RepeatOptions;
  debounce?: { id: string; ttl?: number };
  stackTraceLimit?: number;
  keepLogs?: number;
  sizeLimit?: number;
  timestamp?: number;
  failParentOnFailure?: boolean;
  removeDependencyOnFailure?: boolean;
  ignoreDependencyOnFailure?: boolean;
  continueParentOnFailure?: boolean;
}

/** Map SDK JobOptions to the PUSH/PUSHB wire fields (undefined keys dropped). */
export function wireJobOptions(opts: JobOptions = {}): Record<string, unknown> {
  return compact({
    priority: opts.priority,
    delay: opts.delay,
    maxAttempts: opts.attempts,
    backoff: opts.backoff,
    ttl: opts.ttl,
    timeout: opts.timeout,
    jobId: opts.jobId,
    uniqueKey: opts.deduplication?.id,
    dedup: opts.deduplication
      ? compact({
          ttl: opts.deduplication.ttl,
          extend: opts.deduplication.extend,
          replace: opts.deduplication.replace,
        })
      : undefined,
    dependsOn: opts.dependsOn,
    parentId: opts.parentId,
    childrenIds: opts.childrenIds,
    tags: opts.tags,
    groupId: opts.groupId,
    lifo: opts.lifo,
    removeOnComplete:
      typeof opts.removeOnComplete === 'boolean' ? opts.removeOnComplete : undefined,
    removeOnFail: typeof opts.removeOnFail === 'boolean' ? opts.removeOnFail : undefined,
    stallTimeout: opts.stallTimeout,
    durable: opts.durable,
    repeat: opts.repeat,
    debounceId: opts.debounce?.id,
    debounceTtl: opts.debounce?.ttl,
    stackTraceLimit: opts.stackTraceLimit,
    keepLogs: opts.keepLogs,
    sizeLimit: opts.sizeLimit,
    timestamp: opts.timestamp,
    failParentOnFailure: opts.failParentOnFailure,
    removeDependencyOnFailure: opts.removeDependencyOnFailure,
    ignoreDependencyOnFailure: opts.ignoreDependencyOnFailure,
    continueParentOnFailure: opts.continueParentOnFailure,
  });
}

/** The job name travels inside `data`, mirroring the official client. */
export function jobPayload(name: string, data: unknown): Record<string, unknown> {
  if (data && typeof data === 'object' && !Array.isArray(data)) {
    return { name, ...(data as Record<string, unknown>) };
  }
  return data === undefined || data === null ? { name } : { name, payload: data };
}

export interface JobCounts {
  waiting: number;
  prioritized: number;
  delayed: number;
  active: number;
  completed: number;
  failed: number;
  'waiting-children': number;
  paused: number;
}

export type JobStateName =
  | 'waiting'
  | 'prioritized'
  | 'delayed'
  | 'active'
  | 'completed'
  | 'failed'
  | 'waiting-children';
