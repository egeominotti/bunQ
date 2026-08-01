import type { BackoffConfig, Job, JobId, JobInput, RepeatConfig } from '../types/jobs/model';
import { JOB_DEFAULTS } from './constants';

function parseBackoff(
  input: number | { type: 'fixed' | 'exponential'; delay: number } | undefined
): { backoff: number; backoffConfig: BackoffConfig | null } {
  if (typeof input === 'object') {
    return {
      backoff: input.delay,
      backoffConfig: { type: input.type, delay: input.delay },
    };
  }
  return { backoff: input ?? JOB_DEFAULTS.backoff, backoffConfig: null };
}

function parseRepeatConfig(repeat: JobInput['repeat']): RepeatConfig | null {
  if (!repeat) return null;
  return {
    every: repeat.every,
    limit: repeat.limit,
    pattern: repeat.pattern,
    count: repeat.count ?? 0,
    startDate: repeat.startDate,
    endDate: repeat.endDate,
    tz: repeat.tz,
    immediately: repeat.immediately,
    prevMillis: repeat.prevMillis,
    offset: repeat.offset,
    jobId: repeat.jobId,
  };
}

function toBoolean(value: unknown, fallback: boolean): boolean {
  return value === undefined ? fallback : Boolean(value);
}

function parseCoreOptions(input: JobInput) {
  return {
    priority: input.priority ?? JOB_DEFAULTS.priority,
    lifo: input.lifo ?? JOB_DEFAULTS.lifo,
    maxAttempts: input.maxAttempts ?? JOB_DEFAULTS.maxAttempts,
    removeOnComplete: toBoolean(input.removeOnComplete, JOB_DEFAULTS.removeOnComplete),
    removeOnFail: toBoolean(input.removeOnFail, JOB_DEFAULTS.removeOnFail),
  };
}

function parseOptionalFields(input: JobInput) {
  return {
    ttl: input.ttl ?? null,
    timeout: input.timeout ?? null,
    uniqueKey: input.uniqueKey ?? null,
    customId: input.customId ?? null,
    parentId: input.parentId ?? null,
    groupId: input.groupId ?? null,
    stallTimeout: input.stallTimeout ?? null,
  };
}

function parseBullMQV5Options(input: JobInput) {
  return {
    stackTraceLimit: input.stackTraceLimit ?? JOB_DEFAULTS.stackTraceLimit,
    keepLogs: input.keepLogs ?? null,
    sizeLimit: input.sizeLimit ?? null,
    failParentOnFailure: input.failParentOnFailure ?? false,
    removeDependencyOnFailure: input.removeDependencyOnFailure ?? false,
    continueParentOnFailure: input.continueParentOnFailure ?? false,
    ignoreDependencyOnFailure: input.ignoreDependencyOnFailure ?? false,
    deduplicationTtl: input.dedup?.ttl ?? null,
    deduplicationExtend: input.dedup?.extend ?? false,
    deduplicationReplace: input.dedup?.replace ?? false,
    debounceId: input.debounceId ?? null,
    debounceTtl: input.debounceTtl ?? null,
  };
}

export function createJob(
  id: JobId,
  queue: string,
  input: JobInput,
  now: number = Date.now()
): Job {
  const { backoff, backoffConfig } = parseBackoff(input.backoff);
  const coreOpts = parseCoreOptions(input);
  const optionalFields = parseOptionalFields(input);
  const v5Opts = parseBullMQV5Options(input);
  const createdAt = input.timestamp ?? now;

  return {
    id,
    queue,
    data: input.data,
    createdAt,
    runAt: createdAt + (input.delay ?? 0),
    startedAt: null,
    completedAt: null,
    attempts: 0,
    backoff,
    backoffConfig,
    dependsOn: input.dependsOn ?? [],
    childrenIds: input.childrenIds ?? [],
    childrenCompleted: 0,
    tags: input.tags ?? [],
    progress: 0,
    progressMessage: null,
    stacktrace: null,
    repeat: parseRepeatConfig(input.repeat),
    lastHeartbeat: createdAt,
    stallCount: 0,
    ...coreOpts,
    ...optionalFields,
    ...v5Opts,
    timeline: [],
  };
}
