/**
 * Job Conversion Helpers
 * Builder functions for constructing public job properties
 */

import type { Job as InternalJob } from '../domain/types/job';
import type {
  Job,
  JobStateType,
  JobOptions,
  GetDependenciesOpts,
  JobDependenciesCount,
} from './types';
import {
  extractUserData,
  extractParent,
  buildJobOpts,
  buildParentKey,
  buildRepeatJobKey,
} from './jobHelpers';

/** Build common job properties from internal job */
export function buildJobProperties<T>(
  job: InternalJob,
  name: string,
  stacktrace?: string[] | null,
  token?: string,
  processedBy?: string
): Pick<
  Job<T>,
  | 'id'
  | 'name'
  | 'data'
  | 'queueName'
  | 'attemptsMade'
  | 'timestamp'
  | 'progress'
  | 'parent'
  | 'delay'
  | 'processedOn'
  | 'finishedOn'
  | 'stacktrace'
  | 'stalledCounter'
  | 'priority'
  | 'parentKey'
  | 'opts'
  | 'token'
  | 'processedBy'
  | 'deduplicationId'
  | 'repeatJobKey'
  | 'attemptsStarted'
> {
  const id = String(job.id);
  const parent = extractParent(job.data);
  const jobOpts = buildJobOpts(job);

  return {
    id,
    name,
    data: extractUserData(job.data) as T,
    queueName: job.queue,
    attemptsMade: job.attempts,
    timestamp: job.createdAt,
    progress: job.progress,
    parent,
    delay: job.runAt > job.createdAt ? job.runAt - job.createdAt : 0,
    processedOn: job.startedAt ?? undefined,
    finishedOn: job.completedAt ?? undefined,
    // Fall back to the stack persisted on the internal job at FAIL time (#74)
    stacktrace: stacktrace ?? job.stacktrace ?? null,
    stalledCounter: job.stallCount,
    priority: job.priority,
    parentKey: buildParentKey(job),
    opts: jobOpts,
    token,
    processedBy,
    deduplicationId: job.customId ?? undefined,
    repeatJobKey: buildRepeatJobKey(job),
    attemptsStarted: job.attempts,
  };
}

/** Build state check methods */
export function buildStateCheckMethods(
  id: string,
  getState?: (id: string) => Promise<JobStateType>,
  getDependenciesCount?: (id: string, opts?: GetDependenciesOpts) => Promise<JobDependenciesCount>
): Pick<
  Job,
  'isWaiting' | 'isActive' | 'isDelayed' | 'isCompleted' | 'isFailed' | 'isWaitingChildren'
> {
  return {
    isWaiting: async () => {
      const state = await (getState ? getState(id) : Promise.resolve('unknown'));
      return state === 'waiting';
    },
    isActive: async () => {
      const state = await (getState ? getState(id) : Promise.resolve('unknown'));
      return state === 'active';
    },
    isDelayed: async () => {
      const state = await (getState ? getState(id) : Promise.resolve('unknown'));
      return state === 'delayed';
    },
    isCompleted: async () => {
      const state = await (getState ? getState(id) : Promise.resolve('unknown'));
      return state === 'completed';
    },
    isFailed: async () => {
      const state = await (getState ? getState(id) : Promise.resolve('unknown'));
      return state === 'failed';
    },
    isWaitingChildren: async () => {
      if (getDependenciesCount) {
        const counts = await getDependenciesCount(id);
        return counts.unprocessed > 0;
      }
      return false;
    },
  };
}

/** Build serialization methods */
export function buildSerializationMethods<T>(
  job: InternalJob,
  id: string,
  name: string,
  jobOpts: JobOptions,
  stacktrace?: string[] | null
): Pick<Job<T>, 'toJSON' | 'asJSON'> {
  // Fall back to the stack persisted on the internal job at FAIL time (#74)
  const stack = stacktrace ?? job.stacktrace ?? null;
  return {
    toJSON: () => ({
      id,
      name,
      data: extractUserData(job.data) as T,
      opts: jobOpts,
      progress: job.progress,
      delay: job.runAt > job.createdAt ? job.runAt - job.createdAt : 0,
      timestamp: job.createdAt,
      attemptsMade: job.attempts,
      stacktrace: stack,
      returnvalue: undefined,
      failedReason: undefined,
      finishedOn: job.completedAt ?? undefined,
      processedOn: job.startedAt ?? undefined,
      queueQualifiedName: `bull:${job.queue}`,
      parentKey: buildParentKey(job),
    }),
    asJSON: () => ({
      id,
      name,
      data: JSON.stringify(extractUserData(job.data)),
      opts: JSON.stringify(jobOpts),
      progress: JSON.stringify(job.progress),
      delay: String(job.runAt > job.createdAt ? job.runAt - job.createdAt : 0),
      timestamp: String(job.createdAt),
      attemptsMade: String(job.attempts),
      stacktrace: stack ? JSON.stringify(stack) : null,
      returnvalue: undefined,
      failedReason: undefined,
      finishedOn: job.completedAt ? String(job.completedAt) : undefined,
      processedOn: job.startedAt ? String(job.startedAt) : undefined,
      parentKey: buildParentKey(job),
    }),
  };
}
