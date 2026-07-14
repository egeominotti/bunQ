/**
 * Queue query surface: job lookup, state, results, counts, logs, children.
 * Methods are merged onto Queue.prototype by queue.ts.
 */

import { CommandError, CommandTimeoutError } from './errors.js';
import { compact } from './frame.js';
import { Job } from './job.js';
import type { Queue } from './queue.js';
import type {
  CountResponse,
  DataResponse,
  JobCountsResponse,
  JobResponse,
  JobsResponse,
  ProgressResponse,
  ResultResponse,
  StateResponse,
  WaitJobResponse,
} from './responses.js';
import type { JobCounts } from './types.js';

type Ctx = Queue<unknown>;
type Raw = Record<string, unknown>;

function unwrapValues<R>(response: { data?: unknown; values?: unknown }): Record<string, R> {
  const data = (response.data ?? {}) as Raw;
  return (data.values ?? response.values ?? data ?? {}) as Record<string, R>;
}

export const queryMethods = {
  async getJob<T = unknown>(this: Ctx, id: string): Promise<Job<T> | null> {
    try {
      const response = await this.call<JobResponse>({ cmd: 'GetJob', id });
      return response.job ? new Job<T>(response.job, this.connection) : null;
    } catch (err) {
      // Only the server's 'Job not found' maps to null; connection loss,
      // timeouts and other server errors must surface.
      if (err instanceof CommandError && /not found/i.test(err.message)) return null;
      throw err;
    }
  },

  async getJobByCustomId<T = unknown>(this: Ctx, customId: string): Promise<Job<T> | null> {
    try {
      const response = await this.call<JobResponse>({ cmd: 'GetJobByCustomId', customId });
      return response.job ? new Job<T>(response.job, this.connection) : null;
    } catch (err) {
      if (err instanceof CommandError && /not found/i.test(err.message)) return null;
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
    const response = await this.call<JobsResponse>(
      compact({
        cmd: 'GetJobs',
        queue: this.name,
        state: opts.state,
        offset: start,
        limit: end - start,
      }) as { cmd: string }
    );
    return (response.jobs ?? []).map((raw) => new Job<T>(raw, this.connection));
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
    return (await this.call<StateResponse>({ cmd: 'GetState', id })).state;
  },

  async getResult<R = unknown>(this: Ctx, id: string): Promise<R> {
    return (await this.call<ResultResponse<R>>({ cmd: 'GetResult', id })).result;
  },

  async getChildrenValues<R = unknown>(this: Ctx, id: string): Promise<Record<string, R>> {
    return unwrapValues<R>(await this.call<DataResponse>({ cmd: 'GetChildrenValues', id }));
  },

  async getFailedChildrenValues(this: Ctx, id: string): Promise<Record<string, string>> {
    return unwrapValues<string>(
      await this.call<DataResponse>({ cmd: 'GetFailedChildrenValues', id })
    );
  },

  async getIgnoredChildrenFailures(this: Ctx, id: string): Promise<Record<string, string>> {
    return unwrapValues<string>(
      await this.call<DataResponse>({ cmd: 'GetIgnoredChildrenFailures', id })
    );
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
    // The server validates 0 <= timeout <= 600000: clamp instead of erroring.
    ttlMs = Math.min(Math.max(ttlMs, 0), 600_000);
    const response = await this.call<WaitJobResponse<R>>(
      { cmd: 'WaitJob', id, timeout: ttlMs },
      ttlMs + 5000
    );
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
    const response = await this.call<ProgressResponse>({ cmd: 'GetProgress', id });
    return { progress: response.progress ?? 0, message: response.message ?? null };
  },

  // ------------------------------------------------------------------- counts

  async getJobCounts(this: Ctx): Promise<JobCounts> {
    return (await this.call<JobCountsResponse>({ cmd: 'GetJobCounts', queue: this.name })).counts;
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
    return (await this.call<CountResponse>({ cmd: 'Count', queue: this.name })).count ?? 0;
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
