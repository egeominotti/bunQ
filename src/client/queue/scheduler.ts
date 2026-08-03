/* eslint-disable @typescript-eslint/no-non-null-assertion */
/**
 * Job Scheduler Operations (BullMQ v5 repeatable jobs)
 */

import { getSharedManager } from '../manager';
import type { TcpConnectionPool } from '../tcpPool';
import type { JobOptions } from '../types';
import type { CronJobOptions } from '../../domain/types/cron';
import { paginateSchedulers } from './schedulerPagination';

interface SchedulerContext {
  /** Server-side queue key (already prefixed with `prefixKey` if set). */
  name: string;
  embedded: boolean;
  tcp: TcpConnectionPool | null;
  /**
   * Namespace prefix from QueueOptions. When set, scheduler IDs are stored
   * under `${prefixKey}${schedulerId}` so two queues with different prefixes
   * cannot collide on the global cron-name PRIMARY KEY.
   */
  prefixKey?: string;
  /** Queue-level default job options, merged under the scheduler template opts. */
  defaultJobOptions?: JobOptions;
}

/** Apply the namespace prefix to a user-facing scheduler ID. */
function toCronName(ctx: SchedulerContext, schedulerId: string): string {
  return ctx.prefixKey ? `${ctx.prefixKey}${schedulerId}` : schedulerId;
}

/** Strip the namespace prefix from a stored cron name. */
function fromCronName(ctx: SchedulerContext, cronName: string): string {
  if (ctx.prefixKey && cronName.startsWith(ctx.prefixKey)) {
    return cronName.slice(ctx.prefixKey.length);
  }
  return cronName;
}

export interface RepeatOpts {
  pattern?: string;
  every?: number;
  limit?: number;
  immediately?: boolean;
  count?: number;
  prevMillis?: number;
  offset?: number;
  jobId?: string;
  timezone?: string;
  /** Skip missed runs on restart instead of executing them (default: true) */
  skipMissedOnRestart?: boolean;
  /** Skip job push if no worker is registered for the queue (default: false) */
  skipIfNoWorker?: boolean;
  /** Prevent overlapping cron jobs via automatic dedup (default: true) */
  preventOverlap?: boolean;
}

export interface JobTemplate<T = unknown> {
  name?: string;
  data?: T;
  opts?: JobOptions;
}

export interface SchedulerInfo {
  id: string;
  name: string;
  next: number;
  pattern?: string;
  every?: number;
  /** Max number of times the scheduler will fire (RepeatOpts.limit). #111 */
  limit?: number;
}

/** Build cron job data from template */
function buildCronData(jobTemplate?: JobTemplate): unknown {
  return jobTemplate?.data ?? {};
}

/**
 * Build the job options carried by every cron-spawned job (issue #86).
 * Queue defaultJobOptions are the base; per-scheduler template opts override.
 * Returns undefined when nothing relevant is set so the server keeps its
 * own JOB_DEFAULTS fallback.
 */
function buildCronJobOptions(
  defaultJobOptions: JobOptions | undefined,
  jobTemplate?: JobTemplate
): CronJobOptions | undefined {
  const merged: JobOptions = { ...defaultJobOptions, ...jobTemplate?.opts };
  const opts: { -readonly [K in keyof CronJobOptions]: CronJobOptions[K] } = {};
  if (merged.attempts !== undefined) opts.maxAttempts = merged.attempts;
  if (merged.backoff !== undefined) opts.backoff = merged.backoff;
  if (merged.timeout !== undefined) opts.timeout = merged.timeout;
  if (merged.delay !== undefined) opts.delay = merged.delay;
  if (merged.stallTimeout !== undefined) opts.stallTimeout = merged.stallTimeout;
  if (typeof merged.removeOnComplete === 'boolean') opts.removeOnComplete = merged.removeOnComplete;
  if (typeof merged.removeOnFail === 'boolean') opts.removeOnFail = merged.removeOnFail;
  return Object.keys(opts).length > 0 ? opts : undefined;
}

/** Extract dedup config from job template */
function buildCronDedup(jobTemplate?: JobTemplate) {
  const dedup = jobTemplate?.opts?.deduplication;
  if (!dedup) return { uniqueKey: undefined, dedup: undefined };
  return {
    uniqueKey: dedup.id,
    dedup: { ttl: dedup.ttl, extend: dedup.extend, replace: dedup.replace },
  };
}

/** Create or update a job scheduler */
export async function upsertJobScheduler(
  ctx: SchedulerContext,
  schedulerId: string,
  repeatOpts: RepeatOpts,
  jobTemplate?: JobTemplate
): Promise<SchedulerInfo | null> {
  const cronPattern = repeatOpts.pattern;
  const repeatEvery = repeatOpts.every;
  const data = buildCronData(jobTemplate);
  const dedupFields = buildCronDedup(jobTemplate);
  const jobOptions = buildCronJobOptions(ctx.defaultJobOptions, jobTemplate);
  const cronName = toCronName(ctx, schedulerId);
  // Priority of spawned jobs: carried on the top-level Cron field (the handler
  // reads cmd.priority), which buildCronJobOptions does not cover.
  const priority = jobTemplate?.opts?.priority ?? ctx.defaultJobOptions?.priority;

  if (ctx.embedded) {
    const manager = getSharedManager();
    const cron = manager.addCron({
      name: cronName,
      jobName: jobTemplate?.name ?? 'default',
      queue: ctx.name,
      data,
      schedule: cronPattern,
      repeatEvery,
      priority,
      timezone: repeatOpts.timezone ?? 'UTC',
      // #111: forward the run cap; addCron ignores <=0/undefined (stored NULL).
      maxLimit: repeatOpts.limit,
      skipMissedOnRestart: repeatOpts.skipMissedOnRestart,
      immediately: repeatOpts.immediately,
      skipIfNoWorker: repeatOpts.skipIfNoWorker,
      preventOverlap: repeatOpts.preventOverlap,
      jobOptions,
      ...dedupFields,
    });
    return {
      id: schedulerId,
      name: cron.jobName,
      next: cron.nextRun,
      pattern: cron.schedule ?? undefined,
      every: cron.repeatEvery ?? undefined,
      limit: cron.maxLimit ?? undefined,
    };
  }

  const response = await ctx.tcp!.send({
    cmd: 'Cron',
    name: cronName,
    jobName: jobTemplate?.name ?? 'default',
    queue: ctx.name,
    data,
    schedule: cronPattern,
    repeatEvery,
    priority,
    timezone: repeatOpts.timezone,
    // #111: forward the run cap on the TCP path too.
    maxLimit: repeatOpts.limit,
    skipMissedOnRestart: repeatOpts.skipMissedOnRestart,
    immediately: repeatOpts.immediately,
    skipIfNoWorker: repeatOpts.skipIfNoWorker,
    preventOverlap: repeatOpts.preventOverlap,
    jobOptions,
    ...dedupFields,
  });
  if (!response.ok) return null;
  const returnedCron = (response as { cron?: { nextRun?: number; jobName?: string } }).cron;
  return {
    id: schedulerId,
    name: returnedCron?.jobName ?? jobTemplate?.name ?? 'default',
    next: returnedCron?.nextRun ?? Date.now(),
    pattern: cronPattern,
    every: repeatEvery,
    limit: repeatOpts.limit && repeatOpts.limit > 0 ? repeatOpts.limit : undefined,
  };
}

export async function removeJobScheduler(
  ctx: SchedulerContext,
  schedulerId: string
): Promise<boolean> {
  const cronName = toCronName(ctx, schedulerId);
  if (ctx.embedded) {
    getSharedManager().removeCron(cronName);
    return true;
  }
  const response = await ctx.tcp!.send({ cmd: 'CronDelete', name: cronName });
  return response.ok === true;
}

/** Get a job scheduler by ID */
export async function getJobScheduler(
  ctx: SchedulerContext,
  schedulerId: string
): Promise<SchedulerInfo | null> {
  const cronName = toCronName(ctx, schedulerId);
  if (ctx.embedded) {
    const crons = getSharedManager().listCrons();
    const cron = crons.find((c) => c.name === cronName && c.queue === ctx.name);
    if (!cron) return null;
    return {
      id: fromCronName(ctx, cron.name),
      name: cron.jobName,
      next: cron.nextRun,
      pattern: cron.schedule ?? undefined,
      every: cron.repeatEvery ?? undefined,
      limit: cron.maxLimit ?? undefined,
    };
  }

  const response = await ctx.tcp!.send({ cmd: 'CronList' });
  if (!response.ok) return null;

  type CronEntry = {
    name: string;
    jobName?: string;
    queue?: string;
    nextRun: number;
    schedule?: string;
    repeatEvery?: number;
    maxLimit?: number | null;
  };
  const crons = (response as { crons?: CronEntry[] }).crons;
  const cron = crons?.find(
    (c) => c.name === cronName && (c.queue === undefined || c.queue === ctx.name)
  );
  if (!cron) return null;

  return {
    id: fromCronName(ctx, cron.name),
    name: cron.jobName ?? fromCronName(ctx, cron.name),
    next: cron.nextRun,
    pattern: cron.schedule ?? undefined,
    every: cron.repeatEvery ?? undefined,
    limit: cron.maxLimit ?? undefined,
  };
}

export async function getJobSchedulers(
  ctx: SchedulerContext,
  start = 0,
  end = -1,
  asc = false
): Promise<SchedulerInfo[]> {
  if (ctx.embedded) {
    const schedulers = getSharedManager()
      .listCrons()
      .filter((c) => c.queue === ctx.name)
      .map((c) => ({
        id: fromCronName(ctx, c.name),
        name: c.jobName,
        next: c.nextRun,
        pattern: c.schedule ?? undefined,
        every: c.repeatEvery ?? undefined,
        limit: c.maxLimit ?? undefined,
      }));
    return paginateSchedulers(schedulers, start, end, asc);
  }

  const response = await ctx.tcp!.send({ cmd: 'CronList' });
  if (!response.ok) return [];

  type CronEntry = {
    name: string;
    jobName?: string;
    queue: string;
    nextRun: number;
    schedule?: string;
    repeatEvery?: number;
    maxLimit?: number | null;
  };
  const crons = (response as { crons?: CronEntry[] }).crons ?? [];

  const schedulers = crons
    .filter((c) => c.queue === ctx.name)
    .map((c) => ({
      id: fromCronName(ctx, c.name),
      name: c.jobName ?? fromCronName(ctx, c.name),
      next: c.nextRun,
      pattern: c.schedule ?? undefined,
      every: c.repeatEvery ?? undefined,
      limit: c.maxLimit ?? undefined,
    }));
  return paginateSchedulers(schedulers, start, end, asc);
}

export async function getJobSchedulersCount(ctx: SchedulerContext): Promise<number> {
  return (await getJobSchedulers(ctx)).length;
}
