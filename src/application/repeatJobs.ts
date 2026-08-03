import type { Job, JobInput, RepeatConfig } from '../domain/types/job';
import {
  expandCronShortcut,
  getNextCronRun,
  validateCronExpression,
} from '../infrastructure/scheduler/cronParser';

function finiteDate(value: number | undefined, field: string): void {
  if (value !== undefined && !Number.isFinite(value)) {
    throw new Error(`repeat.${field} must be a finite timestamp`);
  }
}

/** Reject repeat definitions that cannot produce an unambiguous, independent chain. */
export function validateRepeatJobInput(input: JobInput): void {
  const repeat = input.repeat;
  if (!repeat) return;

  const hasEvery = repeat.every !== undefined;
  const hasPattern = repeat.pattern !== undefined && repeat.pattern.trim().length > 0;
  if (hasEvery === hasPattern) {
    throw new Error('repeat requires exactly one of every or pattern');
  }
  if (hasEvery && (!Number.isFinite(repeat.every) || (repeat.every as number) <= 0)) {
    throw new Error('repeat.every must be a positive finite number');
  }
  if (hasPattern) {
    const expression = expandCronShortcut(repeat.pattern as string);
    const error = validateCronExpression(expression, repeat.tz);
    if (error) throw new Error(`Invalid repeat pattern: ${error}`);
  }

  finiteDate(repeat.startDate, 'startDate');
  finiteDate(repeat.endDate, 'endDate');
  finiteDate(repeat.prevMillis, 'prevMillis');
  finiteDate(repeat.offset, 'offset');

  if (input.customId) {
    throw new Error('repeat jobs cannot use JobOptions.jobId; use repeat.jobId instead');
  }
  if (input.parentId || input.dependsOn?.length || input.childrenIds?.length) {
    throw new Error('repeat jobs cannot participate in parent or dependency relationships');
  }
}

function nextIntervalRun(repeat: RepeatConfig, now: number): number {
  const every = repeat.every as number;
  const earliest = Math.max(now + 1, repeat.startDate ?? Number.NEGATIVE_INFINITY);
  let next =
    repeat.prevMillis !== undefined
      ? repeat.prevMillis + every
      : repeat.startDate !== undefined && repeat.startDate > now
        ? repeat.startDate + (repeat.offset ?? 0)
        : now + every + (repeat.offset ?? 0);
  if (next < earliest) next += Math.ceil((earliest - next) / every) * every;
  return next;
}

function nextPatternRun(repeat: RepeatConfig, now: number): number {
  const offset = repeat.offset ?? 0;
  const shiftedStart =
    repeat.startDate !== undefined ? repeat.startDate - offset - 1 : Number.NEGATIVE_INFINITY;
  const shiftedPrevious =
    repeat.prevMillis !== undefined ? repeat.prevMillis - offset : Number.NEGATIVE_INFINITY;
  const from = Math.max(now - offset, shiftedStart, shiftedPrevious);
  const expression = expandCronShortcut(repeat.pattern as string);
  let baseRun = getNextCronRun(expression, from, repeat.tz);
  let next = baseRun > 0 ? baseRun + offset : 0;

  if (baseRun > 0 && next <= now) {
    baseRun = getNextCronRun(expression, baseRun, repeat.tz);
    next = baseRun > 0 ? baseRun + offset : 0;
  }
  return next;
}

/** Return the next absolute deadline, or null when the configured window is exhausted. */
export function nextRepeatRun(repeat: RepeatConfig, now: number = Date.now()): number | null {
  const next = repeat.pattern ? nextPatternRun(repeat, now) : nextIntervalRun(repeat, now);
  if (!Number.isFinite(next) || next <= 0) return null;
  if (repeat.startDate !== undefined && next < repeat.startDate) return null;
  if (repeat.endDate !== undefined && next > repeat.endDate) return null;
  return next;
}

function repeatSuccessorConfig(repeat: RepeatConfig, nextRun: number): JobInput['repeat'] {
  return {
    every: repeat.every,
    limit: repeat.limit,
    pattern: repeat.pattern,
    count: repeat.count + 1,
    startDate: repeat.startDate,
    endDate: repeat.endDate,
    tz: repeat.tz,
    immediately: repeat.immediately,
    prevMillis: nextRun,
    offset: repeat.offset,
    jobId: repeat.jobId,
  };
}

/** Clone every repeat-compatible policy while resetting execution-owned state. */
export function buildRepeatSuccessor(job: Job, now: number = Date.now()): JobInput | null {
  if (!job.repeat) return null;
  const nextRun = nextRepeatRun(job.repeat, now);
  if (nextRun === null) return null;

  return {
    name: job.name,
    data: job.data,
    priority: job.priority,
    delay: Math.max(0, nextRun - now),
    timestamp: now,
    maxAttempts: job.maxAttempts,
    backoff: job.backoffConfig ?? job.backoff,
    ttl: job.ttl ?? undefined,
    timeout: job.timeout ?? undefined,
    uniqueKey: job.uniqueKey ?? undefined,
    tags: job.tags,
    groupId: job.groupId ?? undefined,
    lifo: job.lifo,
    removeOnComplete: job.removeOnComplete,
    removeOnFail: job.removeOnFail,
    stallTimeout: job.stallTimeout ?? undefined,
    repeat: repeatSuccessorConfig(job.repeat, nextRun),
    dedup: job.uniqueKey
      ? {
          ttl: job.deduplicationTtl ?? undefined,
          extend: job.deduplicationExtend,
          replace: job.deduplicationReplace,
        }
      : undefined,
    durable: job.durable,
    stackTraceLimit: job.stackTraceLimit,
    keepLogs: job.keepLogs ?? undefined,
    sizeLimit: job.sizeLimit ?? undefined,
    failParentOnFailure: job.failParentOnFailure,
    removeDependencyOnFailure: job.removeDependencyOnFailure,
    continueParentOnFailure: job.continueParentOnFailure,
    ignoreDependencyOnFailure: job.ignoreDependencyOnFailure,
    debounceId: job.debounceId ?? undefined,
    debounceTtl: job.debounceTtl ?? undefined,
  };
}
