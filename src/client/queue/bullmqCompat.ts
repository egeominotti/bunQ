/* eslint-disable @typescript-eslint/require-await */
/**
 * BullMQ v5 Compatibility Operations
 */

import type { TcpConnectionPool } from '../tcpPool';
import type { Job, JobDependencies, JobDependenciesCount, GetDependenciesOpts } from '../types';
import { getFlowDependencies } from '../flowJobDependencies';

interface CompatibilityContext<T = unknown> {
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
  getJobCountsAsync: () => Promise<{ prioritized: number; 'waiting-children': number }>;
  getWaitingAsync: (start?: number, end?: number) => Promise<Job<T>[]>;
  getPrioritizedAsync: (start?: number, end?: number) => Promise<Job<T>[]>;
  getJobsAsync: (options: {
    state?: string | string[];
    start?: number;
    end?: number;
    asc?: boolean;
  }) => Promise<Job<T>[]>;
}

/** Get jobs in the "prioritized" state (priority > 0, BullMQ v5) */
export function getPrioritized<T>(
  ctx: CompatibilityContext<T>,
  start = 0,
  end = -1
): Promise<Job<T>[]> {
  return ctx.getPrioritizedAsync(start, end);
}

/** Get count of prioritized jobs (priority > 0) */
export async function getPrioritizedCount<T>(ctx: CompatibilityContext<T>): Promise<number> {
  return (await ctx.getJobCountsAsync()).prioritized;
}

/** Get jobs waiting for parent to complete */
export async function getWaitingChildren<T>(
  ctx: CompatibilityContext<T>,
  start = 0,
  end = -1
): Promise<Job<T>[]> {
  return ctx.getJobsAsync({
    state: 'waiting-children',
    start,
    end: end === -1 ? -1 : end + 1,
    asc: true,
  });
}

/** Get count of jobs waiting for parent */
export async function getWaitingChildrenCount<T>(ctx: CompatibilityContext<T>): Promise<number> {
  return (await ctx.getJobCountsAsync())['waiting-children'];
}

/** Get dependencies of a parent job */
export function getDependencies(
  ctx: CompatibilityContext,
  parentId: string,
  type?: 'processed' | 'unprocessed',
  start = 0,
  end = -1
): Promise<{
  processed?: Record<string, unknown>;
  unprocessed?: string[];
  nextProcessedCursor?: number;
  nextUnprocessedCursor?: number;
}> {
  return getFlowDependencies(parentId, ctx.name, ctx.embedded, ctx.tcp).then((dependencies) => {
    const unprocessed = [...dependencies.unprocessed].sort();
    const processedEntries = Object.entries(dependencies.processed).sort(([a], [b]) =>
      a.localeCompare(b)
    );
    const stop = end < 0 ? undefined : end + 1;
    return {
      processed:
        type === 'unprocessed' ? {} : Object.fromEntries(processedEntries.slice(start, stop)),
      unprocessed: type === 'processed' ? [] : unprocessed.slice(start, stop),
    };
  });
}

/** Get job dependencies (for Job instance method) */
export async function getJobDependencies(
  ctx: CompatibilityContext,
  id: string,
  _opts?: GetDependenciesOpts
): Promise<JobDependencies> {
  const dependencies = await getFlowDependencies(id, ctx.name, ctx.embedded, ctx.tcp);
  const processedEntries = Object.entries(dependencies.processed).sort(([a], [b]) =>
    a.localeCompare(b)
  );
  const unprocessed = [...dependencies.unprocessed].sort();
  const processedCursor = _opts?.processed?.cursor ?? 0;
  const processedCount = _opts?.processed?.count ?? processedEntries.length;
  const unprocessedCursor = _opts?.unprocessed?.cursor ?? 0;
  const unprocessedCount = _opts?.unprocessed?.count ?? unprocessed.length;
  const processedEnd = Math.min(processedEntries.length, processedCursor + processedCount);
  const unprocessedEnd = Math.min(unprocessed.length, unprocessedCursor + unprocessedCount);
  return {
    processed: Object.fromEntries(processedEntries.slice(processedCursor, processedEnd)),
    unprocessed: unprocessed.slice(unprocessedCursor, unprocessedEnd),
    nextProcessedCursor: processedEnd < processedEntries.length ? processedEnd : 0,
    nextUnprocessedCursor: unprocessedEnd < unprocessed.length ? unprocessedEnd : 0,
  };
}

/** Get job dependencies count */
export async function getJobDependenciesCount(
  ctx: CompatibilityContext,
  id: string,
  _opts?: GetDependenciesOpts
): Promise<JobDependenciesCount> {
  const deps = await getFlowDependencies(id, ctx.name, ctx.embedded, ctx.tcp);
  return {
    processed: Object.keys(deps.processed).length,
    unprocessed: deps.unprocessed.length,
  };
}
