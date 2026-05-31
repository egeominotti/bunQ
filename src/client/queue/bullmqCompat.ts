/* eslint-disable @typescript-eslint/require-await */
/**
 * BullMQ v5 Compatibility Operations
 */

import { getSharedManager } from '../manager';
import type { TcpConnectionPool } from '../tcpPool';
import type { Job, JobDependencies, JobDependenciesCount, GetDependenciesOpts } from '../types';
import { toPublicJob } from '../types';
import { jobId } from '../../domain/types/job';

interface BullMQContext<T = unknown> {
  name: string;
  embedded: boolean;
  tcp: TcpConnectionPool | null;
  getJobState: (
    id: string
  ) => Promise<'waiting' | 'delayed' | 'active' | 'completed' | 'failed' | 'unknown'>;
  removeAsync: (id: string) => Promise<void>;
  retryJob: (id: string) => Promise<void>;
  getChildrenValues: (id: string) => Promise<Record<string, unknown>>;
  updateJobData: (id: string, data: unknown) => Promise<void>;
  promoteJob: (id: string) => Promise<void>;
  changeJobDelay: (id: string, delay: number) => Promise<void>;
  changeJobPriority: (id: string, opts: { priority: number; lifo?: boolean }) => Promise<void>;
  extendJobLock: (id: string, token: string, duration: number) => Promise<number>;
  clearJobLogs: (id: string, keepLogs?: number) => Promise<void>;
  getJobDependencies: (id: string, opts?: GetDependenciesOpts) => Promise<JobDependencies>;
  getJobDependenciesCount: (
    id: string,
    opts?: GetDependenciesOpts
  ) => Promise<JobDependenciesCount>;
  getWaitingAsync: (start?: number, end?: number) => Promise<Job<T>[]>;
  getPrioritizedAsync: (start?: number, end?: number) => Promise<Job<T>[]>;
}

/** Get jobs in the "prioritized" state (priority > 0, BullMQ v5) */
export function getPrioritized<T>(ctx: BullMQContext<T>, start = 0, end = -1): Promise<Job<T>[]> {
  return ctx.getPrioritizedAsync(start, end);
}

/** Get count of prioritized jobs (priority > 0) */
export async function getPrioritizedCount<T>(ctx: BullMQContext<T>): Promise<number> {
  const jobs = await ctx.getPrioritizedAsync(0, 1000);
  return jobs.length;
}

/** Get jobs waiting for parent to complete */
export async function getWaitingChildren<T>(
  ctx: BullMQContext<T>,
  start = 0,
  end = -1
): Promise<Job<T>[]> {
  if (ctx.embedded) {
    const jobs = getSharedManager().getJobs(ctx.name, {
      state: 'delayed',
      start,
      end: end === -1 ? 1000 : end,
    });

    return jobs
      .filter((j) => {
        const data = j.data as { _waitingParent?: boolean } | null;
        return data?._waitingParent === true;
      })
      .map((job) => {
        const jobData = job.data as { name?: string } | null;
        return toPublicJob<T>({
          job,
          name: jobData?.name ?? 'default',
          getState: ctx.getJobState,
          remove: ctx.removeAsync,
          retry: ctx.retryJob,
          getChildrenValues: ctx.getChildrenValues,
          updateData: ctx.updateJobData,
          promote: ctx.promoteJob,
          changeDelay: ctx.changeJobDelay,
          changePriority: ctx.changeJobPriority,
          extendLock: ctx.extendJobLock,
          clearLogs: ctx.clearJobLogs,
          getDependencies: ctx.getJobDependencies,
          getDependenciesCount: ctx.getJobDependenciesCount,
        });
      });
  }
  return [];
}

/** Get count of jobs waiting for parent */
export async function getWaitingChildrenCount<T>(ctx: BullMQContext<T>): Promise<number> {
  const children = await getWaitingChildren(ctx);
  return children.length;
}

/** Get dependencies of a parent job */
export function getDependencies(
  _ctx: BullMQContext,
  _parentId: string,
  _type?: 'processed' | 'unprocessed',
  _start?: number,
  _end?: number
): Promise<{
  processed?: Record<string, unknown>;
  unprocessed?: string[];
  nextProcessedCursor?: number;
  nextUnprocessedCursor?: number;
}> {
  return Promise.resolve({ processed: {}, unprocessed: [] });
}

/** Get job dependencies (for Job instance method) */
export async function getJobDependencies(
  ctx: BullMQContext,
  id: string,
  _opts?: GetDependenciesOpts
): Promise<JobDependencies> {
  if (ctx.embedded) {
    const manager = getSharedManager();
    const job = await manager.getJob(jobId(id));
    if (!job) return { processed: {}, unprocessed: [] };

    const childIds = job.childrenIds;
    const processed: Record<string, unknown> = {};
    const unprocessed: string[] = [];

    for (const childId of childIds) {
      const result = manager.getResult(childId);
      if (result !== undefined) {
        const childJob = await manager.getJob(childId);
        const key = childJob ? `${childJob.queue}:${childId}` : String(childId);
        processed[key] = result;
      } else {
        unprocessed.push(String(childId));
      }
    }

    return { processed, unprocessed };
  }
  return { processed: {}, unprocessed: [] };
}

/** Get job dependencies count */
export async function getJobDependenciesCount(
  ctx: BullMQContext,
  id: string,
  _opts?: GetDependenciesOpts
): Promise<JobDependenciesCount> {
  if (ctx.embedded) {
    const deps = await getJobDependencies(ctx, id);
    return {
      processed: Object.keys(deps.processed).length,
      unprocessed: deps.unprocessed.length,
    };
  }
  return { processed: 0, unprocessed: 0 };
}
