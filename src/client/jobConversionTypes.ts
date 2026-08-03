/**
 * Job Conversion Types
 * Interfaces for job conversion options
 */

import type { Job as InternalJob } from '../domain/types/job';
import type {
  JobStateType,
  ChangePriorityOpts,
  GetDependenciesOpts,
  JobDependencies,
  JobDependenciesCount,
} from './types';

/** Options for creating a public job */
export interface CreatePublicJobOptions {
  job: InternalJob;
  name: string;
  updateProgress: (id: string, progress: number, message?: string) => Promise<void>;
  log: (id: string, message: string) => Promise<void>;
  getState?: (id: string) => Promise<JobStateType>;
  remove?: (id: string) => Promise<void>;
  retry?: (id: string) => Promise<void>;
  getChildrenValues?: (id: string) => Promise<Record<string, unknown>>;
  // BullMQ v5 additional callbacks
  updateData?: (id: string, data: unknown) => Promise<void>;
  promote?: (id: string) => Promise<void>;
  changeDelay?: (id: string, delay: number) => Promise<void>;
  changePriority?: (id: string, opts: ChangePriorityOpts) => Promise<void>;
  extendLock?: (id: string, token: string, duration: number) => Promise<number>;
  clearLogs?: (id: string, keepLogs?: number) => Promise<void>;
  getDependencies?: (id: string, opts?: GetDependenciesOpts) => Promise<JobDependencies>;
  getDependenciesCount?: (id: string, opts?: GetDependenciesOpts) => Promise<JobDependenciesCount>;
  // BullMQ v5 move method callbacks
  moveToCompleted?: (id: string, returnValue: unknown, token?: string) => Promise<unknown>;
  moveToFailed?: (id: string, error: Error, token?: string) => Promise<void>;
  moveToWait?: (id: string, token?: string) => Promise<boolean>;
  moveToDelayed?: (id: string, timestamp: number, token?: string) => Promise<void>;
  moveToWaitingChildren?: (
    id: string,
    token?: string,
    opts?: { child?: { id: string; queue: string } }
  ) => Promise<boolean>;
  waitUntilFinished?: (id: string, queueEvents: unknown, ttl?: number) => Promise<unknown>;
  // BullMQ v5 additional method callbacks
  discard?: (id: string) => void;
  getFailedChildrenValues?: (id: string) => Promise<Record<string, string>>;
  getIgnoredChildrenFailures?: (id: string) => Promise<Record<string, string>>;
  removeChildDependency?: (id: string) => Promise<boolean>;
  removeDeduplicationKey?: (id: string) => Promise<boolean>;
  removeUnprocessedChildren?: (id: string) => Promise<void>;
  // Additional job metadata
  token?: string;
  processedBy?: string;
  stacktrace?: string[] | null;
  returnvalue?: unknown;
  failedReason?: string;
}

/** Complete operation set required when exposing a live job from another view. */
export type PublicJobMethodContext = Required<
  Omit<
    CreatePublicJobOptions,
    'job' | 'name' | 'token' | 'processedBy' | 'stacktrace' | 'returnvalue' | 'failedReason'
  >
>;

/** Options for creating a simple public job */
export interface ToPublicJobOptions {
  job: InternalJob;
  name: string;
  updateProgress?: (id: string, progress: number, message?: string) => Promise<void>;
  log?: (id: string, message: string) => Promise<void>;
  getState?: (id: string) => Promise<JobStateType>;
  remove?: (id: string) => Promise<void>;
  retry?: (id: string) => Promise<void>;
  getChildrenValues?: (id: string) => Promise<Record<string, unknown>>;
  // BullMQ v5 additional callbacks
  updateData?: (id: string, data: unknown) => Promise<void>;
  promote?: (id: string) => Promise<void>;
  changeDelay?: (id: string, delay: number) => Promise<void>;
  changePriority?: (id: string, opts: ChangePriorityOpts) => Promise<void>;
  extendLock?: (id: string, token: string, duration: number) => Promise<number>;
  clearLogs?: (id: string, keepLogs?: number) => Promise<void>;
  getDependencies?: (id: string, opts?: GetDependenciesOpts) => Promise<JobDependencies>;
  getDependenciesCount?: (id: string, opts?: GetDependenciesOpts) => Promise<JobDependenciesCount>;
  // BullMQ v5 move method callbacks
  moveToCompleted?: (id: string, returnValue: unknown, token?: string) => Promise<unknown>;
  moveToFailed?: (id: string, error: Error, token?: string) => Promise<void>;
  moveToWait?: (id: string, token?: string) => Promise<boolean>;
  moveToDelayed?: (id: string, timestamp: number, token?: string) => Promise<void>;
  moveToWaitingChildren?: (
    id: string,
    token?: string,
    opts?: { child?: { id: string; queue: string } }
  ) => Promise<boolean>;
  waitUntilFinished?: (id: string, queueEvents: unknown, ttl?: number) => Promise<unknown>;
  // BullMQ v5 additional method callbacks
  discard?: (id: string) => void;
  getFailedChildrenValues?: (id: string) => Promise<Record<string, string>>;
  getIgnoredChildrenFailures?: (id: string) => Promise<Record<string, string>>;
  removeChildDependency?: (id: string) => Promise<boolean>;
  removeDeduplicationKey?: (id: string) => Promise<boolean>;
  removeUnprocessedChildren?: (id: string) => Promise<void>;
  // Additional job metadata
  stacktrace?: string[] | null;
  returnvalue?: unknown;
  failedReason?: string;
}
