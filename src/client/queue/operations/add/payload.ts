import type { JobOptions } from '../../../types';
import type { ExtendedJobOptions } from '../../types/add';

export function compact<T extends Record<string, unknown>>(object: T): T {
  const output: Record<string, unknown> = {};
  for (const key in object) {
    if (object[key] !== undefined) output[key] = object[key];
  }
  return output as T;
}

export function parseDate(date: Date | string | number | undefined): number | undefined {
  if (date instanceof Date) return date.getTime();
  if (typeof date === 'string') return new Date(date).getTime();
  return date;
}

export function buildRepeatOptions(repeat: NonNullable<JobOptions['repeat']>) {
  return {
    every: repeat.every,
    limit: repeat.limit,
    pattern: repeat.pattern,
    count: repeat.count,
    startDate: parseDate(repeat.startDate),
    endDate: parseDate(repeat.endDate),
    tz: repeat.tz,
    immediately: repeat.immediately,
    prevMillis: repeat.prevMillis,
    offset: repeat.offset,
    jobId: repeat.jobId,
  };
}

export function buildPushPayload(
  queue: string,
  name: string,
  data: unknown,
  options: ExtendedJobOptions
): Record<string, unknown> {
  return compact({
    cmd: 'PUSH',
    queue,
    name,
    data,
    priority: options.priority,
    delay: options.delay,
    maxAttempts: options.attempts,
    backoff: options.backoff,
    ttl: options.ttl,
    timeout: options.timeout,
    jobId: options.jobId,
    uniqueKey: options.deduplication?.id,
    dedup: options.deduplication
      ? {
          ttl: options.deduplication.ttl,
          extend: options.deduplication.extend,
          replace: options.deduplication.replace,
        }
      : undefined,
    dependsOn: options.dependsOn,
    tags: options.tags,
    groupId: options.groupId,
    lifo: options.lifo,
    removeOnComplete:
      typeof options.removeOnComplete === 'boolean' ? options.removeOnComplete : undefined,
    removeOnFail: typeof options.removeOnFail === 'boolean' ? options.removeOnFail : undefined,
    stallTimeout: options.stallTimeout,
    stackTraceLimit: options.stackTraceLimit,
    keepLogs: options.keepLogs,
    sizeLimit: options.sizeLimit,
    failParentOnFailure: options.failParentOnFailure,
    removeDependencyOnFailure: options.removeDependencyOnFailure,
    continueParentOnFailure: options.continueParentOnFailure,
    ignoreDependencyOnFailure: options.ignoreDependencyOnFailure,
    debounceId: options.debounce?.id,
    debounceTtl: options.debounce?.ttl,
    timestamp: options.timestamp,
    durable: options.durable,
    repeat: options.repeat,
    parentId: options.parent?.id,
  });
}

export function buildJobData(data: unknown, options: ExtendedJobOptions): unknown {
  if (!options.parent) return data;
  return {
    ...(data as object),
    __parentId: options.parent.id,
    __parentQueue: options.parent.queue,
  };
}

export function reflectionMeta(options: ExtendedJobOptions): {
  priority?: number;
  delay?: number;
  opts: JobOptions;
} {
  return { priority: options.priority, delay: options.delay, opts: options };
}
