/* eslint-disable @typescript-eslint/no-non-null-assertion, @typescript-eslint/no-unnecessary-condition, @typescript-eslint/switch-exhaustiveness-check */
/**
 * Queue Query Operations
 * getJob, getJobs, getJobState, getJobsAsync
 */

import { getSharedManager } from '../../manager';
import type { TcpConnectionPool } from '../../tcpPool';
import type {
  Job,
  JobStateType,
  GetDependenciesOpts,
  JobDependencies,
  JobDependenciesCount,
} from '../../types';
import { toPublicJob } from '../../types';
import { jobId } from '../../../domain/types/job';
import type { Job as InternalJob } from '../../../domain/types/job';
import { createSimpleJob } from '../jobProxy';
import { buildJobOpts } from '../../jobHelpers';

/** Last failure's error message from the job timeline (#74) */
function lastFailedError(timeline: InternalJob['timeline'] | undefined): string | undefined {
  if (!timeline) return undefined;
  for (let i = timeline.length - 1; i >= 0; i--) {
    const entry = timeline[i];
    if (entry.state === 'failed' && entry.error) return entry.error;
  }
  return undefined;
}

/** Build reflection meta (priority/delay/opts/stacktrace) from an internal job shape */
function metaFromJob(job: InternalJob): {
  priority: number;
  delay: number;
  opts: ReturnType<typeof buildJobOpts>;
  stacktrace: string[] | null;
  failedReason: string | undefined;
} {
  const opts = buildJobOpts(job);
  return {
    priority: job.priority ?? 0,
    delay: opts.delay ?? 0,
    opts,
    // Persisted server-side on FAIL (#74); absent on pre-#74 servers
    stacktrace: job.stacktrace ?? null,
    failedReason: lastFailedError(job.timeline),
  };
}

interface QueryContext {
  name: string;
  embedded: boolean;
  tcp: TcpConnectionPool | null;
  getJobState: (id: string) => Promise<JobStateType>;
  removeAsync: (id: string) => Promise<void>;
  retryJob: (id: string) => Promise<void>;
  getChildrenValues: (id: string) => Promise<Record<string, unknown>>;
  // Extended context for toPublicJob
  updateJobData?: (id: string, data: unknown) => Promise<void>;
  promoteJob?: (id: string) => Promise<void>;
  changeJobDelay?: (id: string, delay: number) => Promise<void>;
  changeJobPriority?: (id: string, opts: { priority: number; lifo?: boolean }) => Promise<void>;
  extendJobLock?: (id: string, token: string, duration: number) => Promise<number>;
  clearJobLogs?: (id: string, keepLogs?: number) => Promise<void>;
  getJobDependencies?: (id: string, opts?: GetDependenciesOpts) => Promise<JobDependencies>;
  getJobDependenciesCount?: (
    id: string,
    opts?: GetDependenciesOpts
  ) => Promise<JobDependenciesCount>;
  // Move / wait callbacks
  moveJobToCompleted?: (id: string, returnValue: unknown, token?: string) => Promise<unknown>;
  moveJobToFailed?: (id: string, error: Error, token?: string) => Promise<void>;
  moveJobToWait?: (id: string, token?: string) => Promise<boolean>;
  moveJobToDelayed?: (id: string, timestamp: number, token?: string) => Promise<void>;
  moveJobToWaitingChildren?: (
    id: string,
    token?: string,
    opts?: { child?: { id: string; queue: string } }
  ) => Promise<boolean>;
  waitJobUntilFinished?: (id: string, queueEvents: unknown, ttl?: number) => Promise<unknown>;
}

/** Get a single job by ID */
export async function getJob<T>(ctx: QueryContext, id: string): Promise<Job<T> | null> {
  if (ctx.embedded) {
    const job = await getSharedManager().getJob(jobId(id));
    if (!job) return null;
    const name = (job.data as { name?: string })?.name ?? 'unknown';

    // Use toPublicJob with full wiring (all callbacks route to the shared manager)
    if (ctx.updateJobData) {
      const mgr = getSharedManager();
      return toPublicJob<T>({
        job,
        name,
        getState: (jid) => ctx.getJobState(jid),
        remove: (jid) => ctx.removeAsync(jid),
        retry: (jid) => ctx.retryJob(jid),
        getChildrenValues: (jid) => ctx.getChildrenValues(jid),
        updateData: ctx.updateJobData,
        promote: ctx.promoteJob,
        changeDelay: ctx.changeJobDelay,
        changePriority: ctx.changeJobPriority,
        extendLock: ctx.extendJobLock,
        clearLogs: ctx.clearJobLogs,
        getDependencies: ctx.getJobDependencies,
        getDependenciesCount: ctx.getJobDependenciesCount,
        // Move / wait callbacks
        moveToCompleted: ctx.moveJobToCompleted,
        moveToFailed: ctx.moveJobToFailed,
        moveToWait: ctx.moveJobToWait,
        moveToDelayed: ctx.moveJobToDelayed
          ? (jid, ts, tok) => ctx.moveJobToDelayed!(jid, ts, tok)
          : undefined,
        moveToWaitingChildren: ctx.moveJobToWaitingChildren,
        waitUntilFinished: ctx.waitJobUntilFinished,
        // Direct-to-manager callbacks (no server primitive or Queue-level wrapper)
        discard: (jid) => {
          void mgr.discard(jobId(jid));
        },
        getFailedChildrenValues: (jid) => mgr.getFailedChildrenValues(jobId(jid)),
        getIgnoredChildrenFailures: (jid) => mgr.getIgnoredChildrenFailures(jobId(jid)),
        removeChildDependency: (jid) => mgr.removeChildDependency(jobId(jid)),
        removeUnprocessedChildren: async (jid) => {
          await mgr.removeUnprocessedChildren(jobId(jid));
        },
      });
    }

    // Fallback to simple job — reflect priority/delay/opts from the real job (#88)
    return createSimpleJob(String(job.id), name, job.data as T, job.createdAt, {
      queueName: ctx.name,
      embedded: ctx.embedded,
      tcp: ctx.tcp,
      getJobState: ctx.getJobState,
      removeAsync: ctx.removeAsync,
      retryJob: ctx.retryJob,
      getChildrenValues: ctx.getChildrenValues,
      meta: metaFromJob(job),
    });
  }

  const response = await ctx.tcp!.send({ cmd: 'GetJob', id });
  if (!response.ok || !response.job) return null;

  const j = response.job as InternalJob & { data: T; progress?: number };
  const name = (j.data as { name?: string })?.name ?? 'unknown';
  const result = createSimpleJob(String(j.id), name, j.data, j.createdAt ?? Date.now(), {
    queueName: ctx.name,
    embedded: ctx.embedded,
    tcp: ctx.tcp,
    getJobState: ctx.getJobState,
    removeAsync: ctx.removeAsync,
    retryJob: ctx.retryJob,
    getChildrenValues: ctx.getChildrenValues,
    meta: metaFromJob(j),
  });
  if (j.progress !== undefined) (result as { progress: number }).progress = j.progress;
  return result;
}

/** Get job state by ID */
export async function getJobState(ctx: QueryContext, id: string): Promise<JobStateType> {
  if (ctx.embedded) {
    const state = await getSharedManager().getJobState(jobId(id));
    return mapState(state);
  }

  const response = await ctx.tcp!.send({ cmd: 'GetState', id });
  if (!response.ok) return 'unknown';
  return mapState(response.state as string | undefined);
}

function mapState(state: string | undefined): JobStateType {
  switch (state) {
    case 'waiting':
    case 'prioritized':
    case 'delayed':
    case 'active':
    case 'completed':
    case 'failed':
    case 'waiting-children':
      return state;
    case 'processing':
      return 'active';
    case 'dlq':
      return 'failed';
    default:
      return 'unknown';
  }
}

/** Get children values for a job */
export async function getChildrenValues(
  ctx: QueryContext,
  id: string
): Promise<Record<string, unknown>> {
  if (ctx.embedded) {
    return getSharedManager().getChildrenValues(jobId(id));
  }
  const response = await ctx.tcp!.send({ cmd: 'GetChildrenValues', id });
  if (!response.ok) return {};
  const data = (response as { data?: { values?: Record<string, unknown> } }).data;
  return data?.values ?? {};
}

interface GetJobsOptions {
  state?: string | string[];
  start?: number;
  end?: number;
  asc?: boolean;
}

/** Get jobs with filtering (sync, embedded only) */
export function getJobs<T>(ctx: QueryContext, options: GetJobsOptions = {}): Job<T>[] {
  if (!ctx.embedded) return [];

  const manager = getSharedManager();
  const jobs = manager.getJobs(ctx.name, {
    state: options.state,
    start: options.start ?? 0,
    end: options.end !== undefined && options.end >= 0 ? options.end : 100,
  });

  return jobs.map((j) => {
    const name = (j.data as { name?: string })?.name ?? 'unknown';
    return createSimpleJob(String(j.id), name, j.data as T, j.createdAt, {
      queueName: ctx.name,
      embedded: ctx.embedded,
      tcp: ctx.tcp,
      getJobState: ctx.getJobState,
      removeAsync: ctx.removeAsync,
      retryJob: ctx.retryJob,
      getChildrenValues: ctx.getChildrenValues,
    });
  });
}

/** Get jobs with filtering (async, works with TCP) */
export async function getJobsAsync<T>(
  ctx: QueryContext,
  options: GetJobsOptions = {}
): Promise<Job<T>[]> {
  if (ctx.embedded) return getJobs(ctx, options);

  // Handle end=-1 as "get all"
  const end = options.end !== undefined && options.end >= 0 ? options.end : 1000;
  const start = options.start ?? 0;

  const response = await ctx.tcp!.send({
    cmd: 'GetJobs',
    queue: ctx.name,
    state: options.state,
    offset: start,
    limit: end - start,
  });

  if (!response.ok || !Array.isArray((response as { jobs?: unknown[] }).jobs)) {
    return [];
  }

  // handleGetJobs returns the full internal job per element ({ ...job, state }),
  // so reflect the complete opts via metaFromJob instead of a slim subset (#90).
  const jobs = (response as { jobs: Array<InternalJob & { data: T; progress?: number }> }).jobs;

  const now = Date.now();
  return jobs.map((j) => {
    const name = (j.data as { name?: string })?.name ?? 'unknown';
    const createdAt = j.createdAt ?? now;
    const result = createSimpleJob(String(j.id), name, j.data, createdAt, {
      queueName: ctx.name,
      embedded: ctx.embedded,
      tcp: ctx.tcp,
      getJobState: ctx.getJobState,
      removeAsync: ctx.removeAsync,
      retryJob: ctx.retryJob,
      getChildrenValues: ctx.getChildrenValues,
      meta: metaFromJob(j),
    });
    if (j.progress !== undefined) (result as { progress: number }).progress = j.progress;
    if (j.priority !== undefined) (result as { priority: number }).priority = j.priority;
    if (j.attempts !== undefined) (result as { attemptsMade: number }).attemptsMade = j.attempts;
    return result;
  });
}

/** Get waiting jobs */
export function getWaiting<T>(ctx: QueryContext, start = 0, end = 100): Job<T>[] {
  return getJobs(ctx, { state: 'waiting', start, end });
}

export async function getWaitingAsync<T>(
  ctx: QueryContext,
  start = 0,
  end = 100
): Promise<Job<T>[]> {
  return getJobsAsync(ctx, { state: 'waiting', start, end });
}

/** Get delayed jobs */
export function getDelayed<T>(ctx: QueryContext, start = 0, end = 100): Job<T>[] {
  return getJobs(ctx, { state: 'delayed', start, end });
}

export async function getDelayedAsync<T>(
  ctx: QueryContext,
  start = 0,
  end = 100
): Promise<Job<T>[]> {
  return getJobsAsync(ctx, { state: 'delayed', start, end });
}

/** Get active jobs */
export function getActive<T>(ctx: QueryContext, start = 0, end = 100): Job<T>[] {
  return getJobs(ctx, { state: 'active', start, end });
}

export async function getActiveAsync<T>(
  ctx: QueryContext,
  start = 0,
  end = 100
): Promise<Job<T>[]> {
  return getJobsAsync(ctx, { state: 'active', start, end });
}

/** Get completed jobs */
export function getCompleted<T>(ctx: QueryContext, start = 0, end = 100): Job<T>[] {
  return getJobs(ctx, { state: 'completed', start, end });
}

export async function getCompletedAsync<T>(
  ctx: QueryContext,
  start = 0,
  end = 100
): Promise<Job<T>[]> {
  return getJobsAsync(ctx, { state: 'completed', start, end });
}

/** Get failed jobs */
export function getFailed<T>(ctx: QueryContext, start = 0, end = 100): Job<T>[] {
  return getJobs(ctx, { state: 'failed', start, end });
}

export async function getFailedAsync<T>(
  ctx: QueryContext,
  start = 0,
  end = 100
): Promise<Job<T>[]> {
  return getJobsAsync(ctx, { state: 'failed', start, end });
}
