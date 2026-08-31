/**
 * Job Helper Functions
 * Internal utilities for extracting and building job data
 */

import { normalizeLegacyJobPayload, type Job as InternalJob } from '../domain/types/job';
import type { RepeatOptions, JobOptions } from './types';

/** Decode a bare pre-name-field payload. Modern conversions use normalizeJobPayload(job). */
export function extractUserData(jobData: unknown): unknown {
  return normalizeLegacyJobPayload({ data: jobData }).data;
}

/** Resolve legacy names while preserving documented engine-owned flow metadata. */
export function resolvePublicJobPayload(job: InternalJob): { name: string; data: unknown } {
  const payload = normalizeLegacyJobPayload(job);
  return { name: payload.name, data: payload.data };
}

/** Extract parent info from job data if present */
export function extractParent(
  jobData: unknown
): { id: string; queueQualifiedName: string } | undefined {
  if (typeof jobData === 'object' && jobData !== null) {
    const data = jobData as Record<string, unknown>;
    const parentId = data.__parentId;
    const parentQueue = data.__parentQueue;
    if (
      parentId !== undefined &&
      parentQueue !== undefined &&
      (typeof parentId === 'string' || typeof parentId === 'number') &&
      (typeof parentQueue === 'string' || typeof parentQueue === 'number')
    ) {
      return {
        id: String(parentId),
        queueQualifiedName: String(parentQueue),
      };
    }
  }
  return undefined;
}

/** Build repeat options from internal job repeat config */
export function buildRepeatOpts(repeat: InternalJob['repeat']): RepeatOptions | undefined {
  if (!repeat) return undefined;
  return {
    every: repeat.every,
    limit: repeat.limit,
    pattern: repeat.pattern,
    count: repeat.count,
    startDate: repeat.startDate,
    endDate: repeat.endDate,
    tz: repeat.tz,
    immediately: repeat.immediately,
    prevMillis: repeat.prevMillis,
    offset: repeat.offset,
    jobId: repeat.jobId,
  };
}

/** Build parent reference from job data */
export function buildParentOpts(job: InternalJob): { id: string; queue: string } | undefined {
  const data = job.data as Record<string, unknown> | null;
  const rawParentId = job.parentId ?? data?.__parentId;
  const rawParentQueue = data?.__parentQueue;
  // Only create parent if both values are string-coercible
  const isStringLike = (v: unknown): v is string | number =>
    typeof v === 'string' || typeof v === 'number';
  if (isStringLike(rawParentId) && isStringLike(rawParentQueue)) {
    return { id: String(rawParentId), queue: String(rawParentQueue) };
  }
  return undefined;
}

/** Build JobOptions from internal job */
export function buildJobOpts(job: InternalJob): JobOptions {
  const backoff = job.backoffConfig
    ? { type: job.backoffConfig.type, delay: job.backoffConfig.delay }
    : job.backoff;

  return {
    priority: job.priority,
    delay: job.runAt > job.createdAt ? job.runAt - job.createdAt : 0,
    attempts: job.maxAttempts,
    backoff,
    timeout: job.timeout ?? undefined,
    jobId: job.customId ?? undefined,
    removeOnComplete: job.removeOnComplete,
    removeOnFail: job.removeOnFail,
    stallTimeout: job.stallTimeout ?? undefined,
    repeat: buildRepeatOpts(job.repeat),
    parent: buildParentOpts(job),
    lifo: job.lifo,
    stackTraceLimit: job.stackTraceLimit,
    keepLogs: job.keepLogs ?? undefined,
    sizeLimit: job.sizeLimit ?? undefined,
    failParentOnFailure: job.failParentOnFailure,
    removeDependencyOnFailure: job.removeDependencyOnFailure,
    continueParentOnFailure: job.continueParentOnFailure,
    ignoreDependencyOnFailure: job.ignoreDependencyOnFailure,
    durable: job.durable ?? false,
    deduplication:
      job.uniqueKey !== null
        ? {
            id: job.uniqueKey,
            ttl: job.deduplicationTtl ?? undefined,
            extend: job.deduplicationExtend,
            replace: job.deduplicationReplace,
          }
        : undefined,
    debounce:
      job.debounceId && job.debounceTtl !== null
        ? { id: job.debounceId, ttl: job.debounceTtl }
        : undefined,
    group: job.groupId ? { id: job.groupId } : undefined,
  };
}

/** Build parent key from job */
export function buildParentKey(job: InternalJob): string | undefined {
  if (job.parentId) {
    // Try to get parent queue from job data
    const data = job.data as Record<string, unknown> | null;
    const parentQueue = data?.__parentQueue;
    if (parentQueue && (typeof parentQueue === 'string' || typeof parentQueue === 'number')) {
      return `${String(parentQueue)}:${job.parentId}`;
    }
    return `unknown:${job.parentId}`;
  }
  return undefined;
}

/** Build repeat job key from job */
export function buildRepeatJobKey(job: InternalJob): string | undefined {
  if (job.repeat) {
    const pattern = job.repeat.pattern ?? (job.repeat.every ? `every:${job.repeat.every}` : '');
    return `${job.queue}:${job.id}:${pattern}`;
  }
  return undefined;
}
