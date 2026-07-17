/* eslint-disable @typescript-eslint/no-non-null-assertion */
/**
 * Queue Count Operations
 * getJobCounts, getWaitingCount, getDelayedCount, etc.
 */

import { getSharedManager } from '../../manager';
import type { TcpConnectionPool } from '../../tcpPool';
import { pausedView } from '../../../shared/pausedView';

interface CountsContext {
  name: string;
  embedded: boolean;
  tcp: TcpConnectionPool | null;
}

export interface JobCounts {
  waiting: number;
  prioritized: number;
  active: number;
  completed: number;
  failed: number;
  delayed: number;
  paused: number;
  'waiting-children': number;
}

/**
 * Get job counts.
 * Embedded mode returns synchronously. TCP mode delegates to the async path so
 * callers receive the REAL server-side counts (awaitable Promise) instead of
 * hardcoded zeros — defect: getjobcounts-tcp-zero.
 */
export function getJobCounts(ctx: CountsContext): JobCounts | Promise<JobCounts> {
  if (!ctx.embedded) {
    return getJobCountsAsync(ctx);
  }

  const manager = getSharedManager();
  // Use queue-specific counts
  const counts = manager.getQueueJobCounts(ctx.name);
  const isPaused = manager.isPaused(ctx.name);
  // When paused, ready jobs (waiting + prioritized) are reported under `paused` —
  // not in their own buckets, which would double-count (#92, BullMQ semantics).
  const pv = pausedView(counts.waiting, counts.prioritized, isPaused);
  return {
    waiting: pv.waiting,
    prioritized: pv.prioritized,
    active: counts.active,
    completed: counts.completed,
    failed: counts.failed,
    delayed: counts.delayed,
    paused: pv.paused,
    'waiting-children': counts['waiting-children'],
  };
}

/** Get job counts (async, works with TCP) */
export async function getJobCountsAsync(ctx: CountsContext): Promise<JobCounts> {
  if (ctx.embedded) return getJobCounts(ctx);

  // Use GetJobCounts for queue-specific counts
  const response = await ctx.tcp!.send({ cmd: 'GetJobCounts', queue: ctx.name });
  if (!response.ok) {
    return {
      waiting: 0,
      prioritized: 0,
      active: 0,
      completed: 0,
      failed: 0,
      delayed: 0,
      paused: 0,
      'waiting-children': 0,
    };
  }

  const counts = response.counts as
    | {
        waiting?: number;
        prioritized?: number;
        active?: number;
        completed?: number;
        failed?: number;
        delayed?: number;
        paused?: number;
        'waiting-children'?: number;
      }
    | undefined;

  return {
    waiting: counts?.waiting ?? 0,
    prioritized: counts?.prioritized ?? 0,
    active: counts?.active ?? 0,
    completed: counts?.completed ?? 0,
    failed: counts?.failed ?? 0,
    delayed: counts?.delayed ?? 0,
    paused: counts?.paused ?? 0,
    'waiting-children': counts?.['waiting-children'] ?? 0,
  };
}

/** Get waiting job count */
export async function getWaitingCount(ctx: CountsContext): Promise<number> {
  const counts = await getJobCountsAsync(ctx);
  return counts.waiting;
}

/** Get active job count */
export async function getActiveCount(ctx: CountsContext): Promise<number> {
  const counts = await getJobCountsAsync(ctx);
  return counts.active;
}

/** Get completed job count */
export async function getCompletedCount(ctx: CountsContext): Promise<number> {
  const counts = await getJobCountsAsync(ctx);
  return counts.completed;
}

/** Get failed job count */
export async function getFailedCount(ctx: CountsContext): Promise<number> {
  const counts = await getJobCountsAsync(ctx);
  return counts.failed;
}

/** Get delayed job count */
export async function getDelayedCount(ctx: CountsContext): Promise<number> {
  if (ctx.embedded) {
    const jobs = getSharedManager().getJobs(ctx.name, { state: 'delayed' });
    return jobs.length;
  }
  // Use GetJobCounts for queue-specific counts
  const response = await ctx.tcp!.send({ cmd: 'GetJobCounts', queue: ctx.name });
  if (!response.ok) return 0;
  return (response.counts as { delayed?: number } | undefined)?.delayed ?? 0;
}

/** Get total job count for the queue */
export function count(ctx: CountsContext): number {
  if (!ctx.embedded) return 0;
  return getSharedManager().count(ctx.name);
}

/** Get total job count (async, works with TCP) */
export async function countAsync(ctx: CountsContext): Promise<number> {
  if (ctx.embedded) return count(ctx);

  const response = await ctx.tcp!.send({ cmd: 'Count', queue: ctx.name });
  if (!response.ok) return 0;
  return (response.count ?? 0) as number;
}

/** Get counts grouped by priority */
export function getCountsPerPriority(ctx: CountsContext): Record<number, number> {
  if (!ctx.embedded) return {};
  return getSharedManager().getCountsPerPriority(ctx.name);
}

/** Get counts grouped by priority (async) */
export async function getCountsPerPriorityAsync(
  ctx: CountsContext
): Promise<Record<number, number>> {
  if (ctx.embedded) return getCountsPerPriority(ctx);

  const response = await ctx.tcp!.send({ cmd: 'GetCountsPerPriority', queue: ctx.name });
  if (!response.ok) return {};
  return (response.counts ?? {}) as Record<number, number>;
}
