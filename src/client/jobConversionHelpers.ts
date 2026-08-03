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
  extractParent,
  buildJobOpts,
  buildParentKey,
  buildRepeatJobKey,
  resolvePublicJobPayload,
} from './jobHelpers';
import { serializeReturnvalue } from './queue/jobMetadata';

interface JobPropertyOptions {
  stacktrace?: string[] | null;
  token?: string;
  processedBy?: string;
  returnvalue?: unknown;
  failedReason?: string;
}

/** Build common job properties from internal job */
export function buildJobProperties<T>(
  job: InternalJob,
  name: string,
  options: JobPropertyOptions = {}
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
  | 'returnvalue'
  | 'failedReason'
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
  const { stacktrace, token, processedBy, returnvalue, failedReason } = options;
  const id = String(job.id);
  const parent = extractParent(job.data);
  const jobOpts = buildJobOpts(job);
  const payload = resolvePublicJobPayload(job);

  return {
    id,
    name,
    data: payload.data as T,
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
    returnvalue,
    failedReason,
    stalledCounter: job.stallCount,
    priority: job.priority,
    parentKey: buildParentKey(job),
    opts: jobOpts,
    token,
    processedBy,
    // deduplicationId reflects the requested jobId OR deduplication.id. customId
    // now holds ONLY an explicit jobId (the dedup id rides on uniqueKey since the
    // dedup-replace fix), so fall back to uniqueKey — mirrors the TCP proxy's
    // `opts.jobId ?? opts.deduplication?.id` (#90).
    deduplicationId: job.customId ?? job.uniqueKey ?? undefined,
    repeatJobKey: buildRepeatJobKey(job),
    attemptsStarted: job.attempts,
  };
}

/** Build state check methods */
export function buildStateCheckMethods(
  id: string,
  getState?: (id: string) => Promise<JobStateType>,
  _getDependenciesCount?: (id: string, opts?: GetDependenciesOpts) => Promise<JobDependenciesCount>
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
      const state = await (getState ? getState(id) : Promise.resolve('unknown'));
      return state === 'waiting-children';
    },
  };
}

interface JobSerializationOptions {
  id: string;
  name: string;
  jobOpts: JobOptions;
  stacktrace?: string[] | null;
  returnvalue?: unknown;
  failedReason?: string;
}

/** Build serialization methods */
export function buildSerializationMethods<T>(
  job: InternalJob,
  options: JobSerializationOptions
): Pick<Job<T>, 'toJSON' | 'asJSON'> {
  const { id, name, jobOpts, stacktrace, returnvalue, failedReason } = options;
  // Fall back to the stack persisted on the internal job at FAIL time (#74)
  const stack = stacktrace ?? job.stacktrace ?? null;
  const payload = resolvePublicJobPayload(job);
  return {
    toJSON: () => ({
      id,
      name,
      data: payload.data as T,
      opts: jobOpts,
      progress: job.progress,
      delay: job.runAt > job.createdAt ? job.runAt - job.createdAt : 0,
      timestamp: job.createdAt,
      attemptsMade: job.attempts,
      stacktrace: stack,
      returnvalue,
      failedReason,
      finishedOn: job.completedAt ?? undefined,
      processedOn: job.startedAt ?? undefined,
      queueQualifiedName: `bull:${job.queue}`,
      parentKey: buildParentKey(job),
    }),
    asJSON: () => ({
      id,
      name,
      data: JSON.stringify(payload.data),
      opts: JSON.stringify(jobOpts),
      progress: JSON.stringify(job.progress),
      delay: String(job.runAt > job.createdAt ? job.runAt - job.createdAt : 0),
      timestamp: String(job.createdAt),
      attemptsMade: String(job.attempts),
      stacktrace: stack ? JSON.stringify(stack) : null,
      returnvalue: serializeReturnvalue(returnvalue),
      failedReason,
      finishedOn: job.completedAt ? String(job.completedAt) : undefined,
      processedOn: job.startedAt ? String(job.startedAt) : undefined,
      parentKey: buildParentKey(job),
    }),
  };
}
