/* eslint-disable @typescript-eslint/no-non-null-assertion, @typescript-eslint/no-unnecessary-condition, @typescript-eslint/switch-exhaustiveness-check */

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
import { createSimpleJob } from '../jobProxy';
import { resolvePublicJobPayload } from '../../jobHelpers';
import { removeJobDeduplicationKey } from '../../jobDeduplication';
import { fetchTcpJobRows } from './queryTcpPages';
import { metadataFromJob, type TerminalJobView } from '../jobMetadata';

export interface QueryContext {
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
    const manager = getSharedManager();
    const job = await manager.getJob(jobId(id));
    if (!job) return null;
    const { name, data } = resolvePublicJobPayload(job);
    const meta = metadataFromJob(job, manager.getResult(job.id));

    // Use toPublicJob with full wiring (all callbacks route to the shared manager)
    if (ctx.updateJobData) {
      const mgr = manager;
      return toPublicJob<T>({
        job,
        name,
        returnvalue: meta.returnvalue,
        failedReason: meta.failedReason,
        updateProgress: async (jid, progress, message) => {
          await mgr.updateProgress(jobId(jid), progress, message);
        },
        log: (jid, message) => Promise.resolve(void mgr.addLog(jobId(jid), message)),
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
        removeDeduplicationKey: (jid) => removeJobDeduplicationKey(jid, true, null),
        removeUnprocessedChildren: async (jid) => {
          await mgr.removeUnprocessedChildren(jobId(jid));
        },
      });
    }

    // Fallback to simple job — reflect priority/delay/opts from the real job (#88)
    return createSimpleJob(String(job.id), name, data as T, job.createdAt, {
      queueName: ctx.name,
      embedded: ctx.embedded,
      tcp: ctx.tcp,
      getJobState: ctx.getJobState,
      removeAsync: ctx.removeAsync,
      retryJob: ctx.retryJob,
      getChildrenValues: ctx.getChildrenValues,
      meta,
    });
  }

  const response = await ctx.tcp!.send({ cmd: 'GetJob', id });
  if (!response.ok || !response.job) return null;

  const j = response.job as TerminalJobView & { data: T; progress?: number };
  const { name, data } = resolvePublicJobPayload(j);
  return createSimpleJob(String(j.id), name, data as T, j.createdAt ?? Date.now(), {
    queueName: ctx.name,
    embedded: ctx.embedded,
    tcp: ctx.tcp,
    getJobState: ctx.getJobState,
    removeAsync: ctx.removeAsync,
    retryJob: ctx.retryJob,
    getChildrenValues: ctx.getChildrenValues,
    meta: metadataFromJob(j),
  });
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
    asc: options.asc,
    end:
      options.end === -1
        ? Number.MAX_SAFE_INTEGER
        : options.end !== undefined && options.end >= 0
          ? options.end
          : 100,
  });

  return jobs.map((j) => {
    const { name, data } = resolvePublicJobPayload(j);
    return createSimpleJob(String(j.id), name, data as T, j.createdAt, {
      queueName: ctx.name,
      embedded: ctx.embedded,
      tcp: ctx.tcp,
      getJobState: ctx.getJobState,
      removeAsync: ctx.removeAsync,
      retryJob: ctx.retryJob,
      getChildrenValues: ctx.getChildrenValues,
      meta: metadataFromJob(j, manager.getResult(j.id)),
    });
  });
}

/** Get jobs with filtering (async, works with TCP) */
export async function getJobsAsync<T>(
  ctx: QueryContext,
  options: GetJobsOptions = {}
): Promise<Job<T>[]> {
  if (ctx.embedded) return getJobs(ctx, options);

  if (!ctx.tcp) return [];
  const rows = await fetchTcpJobRows({ name: ctx.name, tcp: ctx.tcp }, options);
  if (!rows) return [];

  // handleGetJobs returns the full internal job per element ({ ...job, state }),
  // so reflect the complete opts via metaFromJob instead of a slim subset (#90).
  const jobs = rows as unknown as Array<TerminalJobView & { data: T; progress?: number }>;

  const now = Date.now();
  return jobs.map((j) => {
    const { name, data } = resolvePublicJobPayload(j);
    const createdAt = j.createdAt ?? now;
    return createSimpleJob(String(j.id), name, data as T, createdAt, {
      queueName: ctx.name,
      embedded: ctx.embedded,
      tcp: ctx.tcp,
      getJobState: ctx.getJobState,
      removeAsync: ctx.removeAsync,
      retryJob: ctx.retryJob,
      getChildrenValues: ctx.getChildrenValues,
      meta: metadataFromJob(j),
    });
  });
}
