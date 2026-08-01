/**
 * Job Conversion Functions
 * Convert internal job types to public API types
 */

import type { DlqEntry as InternalDlqEntry } from '../domain/types/dlq';
import type { Job, JobStateType, ChangePriorityOpts, GetDependenciesOpts, DlqEntry } from './types';
import { buildJobOpts } from './jobHelpers';
import {
  buildJobProperties,
  buildStateCheckMethods,
  buildSerializationMethods,
} from './jobConversionHelpers';
import type {
  CreatePublicJobOptions,
  PublicJobMethodContext,
  ToPublicJobOptions,
} from './jobConversionTypes';

// Re-export types for backward compatibility
export type {
  CreatePublicJobOptions,
  PublicJobMethodContext,
  ToPublicJobOptions,
} from './jobConversionTypes';

/** Convert internal job to public job (with methods) */
export function createPublicJob<T>(opts: CreatePublicJobOptions): Job<T> {
  const {
    job,
    name,
    updateProgress,
    log,
    getState,
    remove,
    retry,
    getChildrenValues,
    updateData,
    promote,
    changeDelay,
    changePriority,
    extendLock,
    clearLogs,
    getDependencies,
    getDependenciesCount,
    moveToCompleted,
    moveToFailed,
    moveToWait,
    moveToDelayed,
    moveToWaitingChildren,
    waitUntilFinished,
    discard,
    getFailedChildrenValues,
    getIgnoredChildrenFailures,
    removeChildDependency,
    removeDeduplicationKey,
    removeUnprocessedChildren,
    token,
    processedBy,
    stacktrace,
  } = opts;

  const id = String(job.id);
  const jobOpts = buildJobOpts(job);
  const props = buildJobProperties<T>(job, name, stacktrace, token, processedBy);
  const stateChecks = buildStateCheckMethods(id, getState, getDependenciesCount);
  const serialization = buildSerializationMethods<T>(job, id, name, jobOpts, stacktrace);

  return {
    ...props,
    ...stateChecks,
    ...serialization,

    // Core methods
    updateProgress: (progress: number, message?: string) => updateProgress(id, progress, message),
    log: (message: string) => log(id, message),
    getState: () => (getState ? getState(id) : Promise.resolve('unknown' as JobStateType)),
    remove: () => (remove ? remove(id) : Promise.resolve()),
    retry: () => (retry ? retry(id) : Promise.resolve()),
    getChildrenValues: <R = unknown>(): Promise<Record<string, R>> =>
      getChildrenValues
        ? (getChildrenValues(id) as unknown as Promise<Record<string, R>>)
        : Promise.resolve({}),

    // BullMQ v5 mutation methods
    updateData: (data: T) => (updateData ? updateData(id, data) : Promise.resolve()),
    promote: () => (promote ? promote(id) : Promise.resolve()),
    changeDelay: (delay: number) => (changeDelay ? changeDelay(id, delay) : Promise.resolve()),
    changePriority: (prioOpts: ChangePriorityOpts) =>
      changePriority ? changePriority(id, prioOpts) : Promise.resolve(),
    extendLock: (lockToken: string, duration: number) =>
      extendLock ? extendLock(id, lockToken, duration) : Promise.resolve(0),
    clearLogs: (keepLogs?: number) => (clearLogs ? clearLogs(id, keepLogs) : Promise.resolve()),

    // BullMQ v5 dependency methods
    getDependencies: (depOpts?: GetDependenciesOpts) =>
      getDependencies
        ? getDependencies(id, depOpts)
        : Promise.resolve({ processed: {}, unprocessed: [] }),
    getDependenciesCount: (depOpts?: GetDependenciesOpts) =>
      getDependenciesCount
        ? getDependenciesCount(id, depOpts)
        : Promise.resolve({ processed: 0, unprocessed: 0 }),

    // BullMQ v5 move methods
    moveToCompleted: (returnValue: unknown, lockToken?: string, _fetchNext?: boolean) =>
      moveToCompleted ? moveToCompleted(id, returnValue, lockToken) : Promise.resolve(null),
    moveToFailed: (error: Error, lockToken?: string, _fetchNext?: boolean) =>
      moveToFailed ? moveToFailed(id, error, lockToken) : Promise.resolve(),
    moveToWait: (lockToken?: string) =>
      moveToWait ? moveToWait(id, lockToken) : Promise.resolve(false),
    moveToDelayed: (timestamp: number, lockToken?: string) =>
      moveToDelayed ? moveToDelayed(id, timestamp, lockToken) : Promise.resolve(),
    moveToWaitingChildren: (
      lockToken?: string,
      moveOpts?: { child?: { id: string; queue: string } }
    ) =>
      moveToWaitingChildren
        ? moveToWaitingChildren(id, lockToken, moveOpts)
        : Promise.resolve(false),
    waitUntilFinished: (queueEvents: unknown, ttl?: number) =>
      waitUntilFinished ? waitUntilFinished(id, queueEvents, ttl) : Promise.resolve(undefined),

    // BullMQ v5 additional methods
    discard: () => {
      if (discard) discard(id);
    },
    getFailedChildrenValues: () =>
      getFailedChildrenValues ? getFailedChildrenValues(id) : Promise.resolve({}),
    getIgnoredChildrenFailures: () =>
      getIgnoredChildrenFailures ? getIgnoredChildrenFailures(id) : Promise.resolve({}),
    removeChildDependency: () =>
      removeChildDependency ? removeChildDependency(id) : Promise.resolve(false),
    removeDeduplicationKey: () =>
      removeDeduplicationKey
        ? removeDeduplicationKey(id)
        : Promise.reject(
            new Error('removeDeduplicationKey is not implemented — no server primitive available')
          ),
    removeUnprocessedChildren: () =>
      removeUnprocessedChildren ? removeUnprocessedChildren(id) : Promise.resolve(),
  };
}

/** Simple public job without methods (for Queue.getJob) */
export function toPublicJob<T>(opts: ToPublicJobOptions): Job<T> {
  const {
    job,
    name,
    updateProgress,
    log,
    getState,
    remove,
    retry,
    getChildrenValues,
    updateData,
    promote,
    changeDelay,
    changePriority,
    extendLock,
    clearLogs,
    getDependencies,
    getDependenciesCount,
    moveToCompleted,
    moveToFailed,
    moveToWait,
    moveToDelayed,
    moveToWaitingChildren,
    waitUntilFinished,
    discard,
    getFailedChildrenValues,
    getIgnoredChildrenFailures,
    removeChildDependency,
    removeDeduplicationKey,
    removeUnprocessedChildren,
    stacktrace,
  } = opts;

  const id = String(job.id);
  const jobOpts = buildJobOpts(job);
  const props = buildJobProperties<T>(job, name, stacktrace, undefined, undefined);
  const stateChecks = buildStateCheckMethods(id, getState, getDependenciesCount);
  const serialization = buildSerializationMethods<T>(job, id, name, jobOpts, stacktrace);

  return {
    ...props,
    ...stateChecks,
    ...serialization,

    // Core methods
    updateProgress: (progress: number, message?: string) =>
      updateProgress ? updateProgress(id, progress, message) : Promise.resolve(),
    log: (message: string) => (log ? log(id, message) : Promise.resolve()),
    getState: () => (getState ? getState(id) : Promise.resolve('unknown' as JobStateType)),
    remove: () => (remove ? remove(id) : Promise.resolve()),
    retry: () => (retry ? retry(id) : Promise.resolve()),
    getChildrenValues: <R = unknown>(): Promise<Record<string, R>> =>
      getChildrenValues
        ? (getChildrenValues(id) as unknown as Promise<Record<string, R>>)
        : Promise.resolve({}),

    // BullMQ v5 mutation methods
    updateData: (data: T) => (updateData ? updateData(id, data) : Promise.resolve()),
    promote: () => (promote ? promote(id) : Promise.resolve()),
    changeDelay: (delay: number) => (changeDelay ? changeDelay(id, delay) : Promise.resolve()),
    changePriority: (prioOpts: ChangePriorityOpts) =>
      changePriority ? changePriority(id, prioOpts) : Promise.resolve(),
    extendLock: (lockToken: string, duration: number) =>
      extendLock ? extendLock(id, lockToken, duration) : Promise.resolve(0),
    clearLogs: (keepLogs?: number) => (clearLogs ? clearLogs(id, keepLogs) : Promise.resolve()),

    // BullMQ v5 dependency methods
    getDependencies: (depOpts?: GetDependenciesOpts) =>
      getDependencies
        ? getDependencies(id, depOpts)
        : Promise.resolve({ processed: {}, unprocessed: [] }),
    getDependenciesCount: (depOpts?: GetDependenciesOpts) =>
      getDependenciesCount
        ? getDependenciesCount(id, depOpts)
        : Promise.resolve({ processed: 0, unprocessed: 0 }),

    // BullMQ v5 move methods
    moveToCompleted: (returnValue: unknown, lockToken?: string, _fetchNext?: boolean) =>
      moveToCompleted ? moveToCompleted(id, returnValue, lockToken) : Promise.resolve(null),
    moveToFailed: (error: Error, lockToken?: string, _fetchNext?: boolean) =>
      moveToFailed ? moveToFailed(id, error, lockToken) : Promise.resolve(),
    moveToWait: (lockToken?: string) =>
      moveToWait ? moveToWait(id, lockToken) : Promise.resolve(false),
    moveToDelayed: (timestamp: number, lockToken?: string) =>
      moveToDelayed ? moveToDelayed(id, timestamp, lockToken) : Promise.resolve(),
    moveToWaitingChildren: (
      lockToken?: string,
      moveOpts?: { child?: { id: string; queue: string } }
    ) =>
      moveToWaitingChildren
        ? moveToWaitingChildren(id, lockToken, moveOpts)
        : Promise.resolve(false),
    waitUntilFinished: (queueEvents: unknown, ttl?: number) =>
      waitUntilFinished ? waitUntilFinished(id, queueEvents, ttl) : Promise.resolve(undefined),

    // BullMQ v5 additional methods
    discard: () => {
      if (discard) discard(id);
    },
    getFailedChildrenValues: () =>
      getFailedChildrenValues ? getFailedChildrenValues(id) : Promise.resolve({}),
    getIgnoredChildrenFailures: () =>
      getIgnoredChildrenFailures ? getIgnoredChildrenFailures(id) : Promise.resolve({}),
    removeChildDependency: () =>
      removeChildDependency ? removeChildDependency(id) : Promise.resolve(false),
    removeDeduplicationKey: () =>
      removeDeduplicationKey
        ? removeDeduplicationKey(id)
        : Promise.reject(
            new Error('removeDeduplicationKey is not implemented — no server primitive available')
          ),
    removeUnprocessedChildren: () =>
      removeUnprocessedChildren ? removeUnprocessedChildren(id) : Promise.resolve(),
  };
}

/** Convert internal DLQ entry to public DLQ entry */
export function toDlqEntry<T>(
  entry: InternalDlqEntry,
  methods: PublicJobMethodContext
): DlqEntry<T> {
  const jobData = entry.job.data as { name?: string } | null;
  return {
    job: createPublicJob<T>({
      job: entry.job,
      name: jobData?.name ?? 'default',
      ...methods,
    }),
    enteredAt: entry.enteredAt,
    reason: entry.reason,
    error: entry.error,
    attempts: entry.attempts.map((a) => ({
      attempt: a.attempt,
      startedAt: a.startedAt,
      failedAt: a.failedAt,
      reason: a.reason,
      error: a.error,
      duration: a.duration,
    })),
    retryCount: entry.retryCount,
    lastRetryAt: entry.lastRetryAt,
    nextRetryAt: entry.nextRetryAt,
    expiresAt: entry.expiresAt,
  };
}
