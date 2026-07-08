/**
 * Queue query surface: job lookup, state, results, counts, logs, children.
 * Methods are merged onto Queue.prototype by queue.ts.
 */

import { CommandError, CommandTimeoutError } from './errors.js';
import { compact } from './frame.js';
import { Job } from './job.js';
import type { Queue } from './queue.js';
import type { JobCounts } from './types.js';

type Ctx = Queue<unknown>;
type Raw = Record<string, unknown>;

function unwrapValues<R>(response: Raw): Record<string, R> {
  const data = (response.data ?? {}) as Raw;
  return (data.values ?? response.values ?? data ?? {}) as Record<string, R>;
}

export const queryMethods = {
  async getJob<T = unknown>(this: Ctx, id: string): Promise<Job<T> | null> {
    try {
      const response = await this.call({ cmd: 'GetJob', id });
      const raw = response.job as Raw | null;
      return raw ? new Job<T>(raw, this.connection) : null;
    } catch (err) {
      if (err instanceof CommandError) return null; // server: 'Job not found'
      throw err;
    }
  },

  async getJobByCustomId<T = unknown>(this: Ctx, customId: string): Promise<Job<T> | null> {
    try {
      const response = await this.call({ cmd: 'GetJobByCustomId', customId });
      const raw = response.job as Raw | null;
      return raw ? new Job<T>(raw, this.connection) : null;
    } catch (err) {
      if (err instanceof CommandError) return null;
      throw err;
    }
  },

  /** BullMQ v5: resolve the job id registered under a deduplication id. */
  async getDeduplicationJobId(this: Ctx, deduplicationId: string): Promise<string | null> {
    const job = await this.getJobByCustomId(deduplicationId);
    return job ? job.id : null;
  },

  async getJobs<T = unknown>(
    this: Ctx,
    opts: { state?: string | string[]; start?: number; end?: number } = {}
  ): Promise<Job<T>[]> {
    const start = opts.start ?? 0;
    const end = opts.end !== undefined && opts.end >= 0 ? opts.end : 1000;
    const response = await this.call(
      compact({
        cmd: 'GetJobs',
        queue: this.name,
        state: opts.state,
        offset: start,
        limit: end - start,
      }) as { cmd: string }
    );
    const jobs = (response.jobs ?? []) as Raw[];
    return jobs.map((raw) => new Job<T>(raw, this.connection));
  },

  getWaiting<T = unknown>(this: Ctx, start?: number, end?: number): Promise<Job<T>[]> {
    // Only the 'waiting' bucket — prioritized jobs live in a separate bucket
    // (getPrioritized), matching BullMQ and the Python SDK / reference client.
    return this.getJobs({ state: 'waiting', start, end });
  },

  getDelayed<T = unknown>(this: Ctx, start?: number, end?: number): Promise<Job<T>[]> {
    return this.getJobs({ state: 'delayed', start, end });
  },

  getActive<T = unknown>(this: Ctx, start?: number, end?: number): Promise<Job<T>[]> {
    return this.getJobs({ state: 'active', start, end });
  },

  getCompleted<T = unknown>(this: Ctx, start?: number, end?: number): Promise<Job<T>[]> {
    return this.getJobs({ state: 'completed', start, end });
  },

  getFailed<T = unknown>(this: Ctx, start?: number, end?: number): Promise<Job<T>[]> {
    return this.getJobs({ state: 'failed', start, end });
  },

  getPrioritized<T = unknown>(this: Ctx, start?: number, end?: number): Promise<Job<T>[]> {
    return this.getJobs({ state: 'prioritized', start, end });
  },

  getWaitingChildren<T = unknown>(this: Ctx, start?: number, end?: number): Promise<Job<T>[]> {
    return this.getJobs({ state: 'waiting-children', start, end });
  },

  async getJobState(this: Ctx, id: string): Promise<string> {
    const response = await this.call({ cmd: 'GetState', id });
    return String(response.state);
  },

  async getResult<R = unknown>(this: Ctx, id: string): Promise<R> {
    const response = await this.call({ cmd: 'GetResult', id });
    return response.result as R;
  },

  async getChildrenValues<R = unknown>(this: Ctx, id: string): Promise<Record<string, R>> {
    return unwrapValues<R>(await this.call({ cmd: 'GetChildrenValues', id }));
  },

  async getFailedChildrenValues(this: Ctx, id: string): Promise<Record<string, string>> {
    return unwrapValues<string>(await this.call({ cmd: 'GetFailedChildrenValues', id }));
  },

  async getIgnoredChildrenFailures(this: Ctx, id: string): Promise<Record<string, string>> {
    return unwrapValues<string>(await this.call({ cmd: 'GetIgnoredChildrenFailures', id }));
  },

  /** Detach a child job from its parent's dependency list. */
  async removeChildDependency(this: Ctx, id: string): Promise<void> {
    await this.call({ cmd: 'RemoveChildDependency', id });
  },

  /** Remove all still-unprocessed children of a parent job. */
  async removeUnprocessedChildren(this: Ctx, id: string): Promise<void> {
    await this.call({ cmd: 'RemoveUnprocessedChildren', id });
  },

  /**
   * Block until the job completes; returns its result.
   * The server's WaitJob waiter resolves only on completion, replying
   * `{ok:true, completed:false}` (no result) otherwise — so returning undefined
   * would be indistinguishable from a genuine undefined result. On
   * non-completion we probe the state: a `failed` job throws CommandError (it
   * will not complete), everything else throws CommandTimeoutError.
   */
  async waitForJob<R = unknown>(this: Ctx, id: string, ttlMs = 30_000): Promise<R> {
    const response = await this.call({ cmd: 'WaitJob', id, timeout: ttlMs }, ttlMs + 5000);
    if (response.completed !== true) {
      let state: string | undefined;
      try {
        state = await this.getJobState(id);
      } catch {
        /* ignore probe failure; fall through to timeout */
      }
      if (state === 'failed') throw new CommandError(`job ${id} failed before completion`);
      throw new CommandTimeoutError(`waitUntilFinished timed out after ${ttlMs}ms`);
    }
    return response.result as R;
  },

  /** BullMQ v5 alias for waitForJob (queueEvents param unused over TCP). */
  waitJobUntilFinished<R = unknown>(
    this: Ctx,
    id: string,
    _queueEvents?: unknown,
    ttlMs?: number
  ): Promise<R> {
    return this.waitForJob<R>(id, ttlMs);
  },

  async getProgress(this: Ctx, id: string): Promise<{ progress: number; message: string | null }> {
    const response = await this.call({ cmd: 'GetProgress', id });
    return {
      progress: Number(response.progress ?? 0),
      message: (response.message as string | null) ?? null,
    };
  },

  // ------------------------------------------------------------------- counts

  async getJobCounts(this: Ctx): Promise<JobCounts> {
    const response = await this.call({ cmd: 'GetJobCounts', queue: this.name });
    return response.counts as JobCounts;
  },

  async getWaitingCount(this: Ctx): Promise<number> {
    // 'waiting' only — prioritized jobs are counted by getPrioritizedCount,
    // matching BullMQ and the Python SDK / reference client.
    return (await this.getJobCounts()).waiting;
  },

  async getActiveCount(this: Ctx): Promise<number> {
    return (await this.getJobCounts()).active;
  },

  async getCompletedCount(this: Ctx): Promise<number> {
    return (await this.getJobCounts()).completed;
  },

  async getFailedCount(this: Ctx): Promise<number> {
    return (await this.getJobCounts()).failed;
  },

  async getDelayedCount(this: Ctx): Promise<number> {
    return (await this.getJobCounts()).delayed;
  },

  async getPrioritizedCount(this: Ctx): Promise<number> {
    return (await this.getJobCounts()).prioritized;
  },

  async getWaitingChildrenCount(this: Ctx): Promise<number> {
    return (await this.getJobCounts())['waiting-children'];
  },

  async count(this: Ctx): Promise<number> {
    const response = await this.call({ cmd: 'Count', queue: this.name });
    return Number(response.count ?? 0);
  },

  async getCountsPerPriority(this: Ctx): Promise<Record<string, number>> {
    const response = await this.call({ cmd: 'GetCountsPerPriority', queue: this.name });
    return (response.counts ?? response.data ?? {}) as Record<string, number>;
  },

  // --------------------------------------------------------------------- logs

  async addJobLog(
    this: Ctx,
    id: string,
    message: string,
    level?: 'info' | 'warn' | 'error'
  ): Promise<void> {
    await this.call(compact({ cmd: 'AddLog', id, message, level }) as { cmd: string });
  },

  async getJobLogs(this: Ctx, id: string, start?: number, end?: number): Promise<string[]> {
    const response = await this.call(
      compact({ cmd: 'GetLogs', id, start, end }) as {
        cmd: string;
      }
    );
    const data = (response.data ?? {}) as Raw;
    const logs = (data.logs ?? response.logs ?? []) as unknown[];
    // Format as `[level] message` (reference client parity); never drop level.
    return logs.map((row) => {
      if (typeof row === 'string') return row;
      const r = row as Raw;
      return r.level ? `[${r.level}] ${r.message}` : String(r.message ?? row);
    });
  },

  async clearJobLogs(this: Ctx, id: string, keepLogs?: number): Promise<void> {
    await this.call(compact({ cmd: 'ClearLogs', id, keepLogs }) as { cmd: string });
  },
};

export type QueueQueryApi = typeof queryMethods;
