/**
 * Queue control surface: pause/drain/clean, promotion, retry and per-job
 * mutations. Methods are merged onto Queue.prototype by queue.ts.
 */

import { UnrecoverableError } from './errors.js';
import { compact } from './frame.js';
import type { Queue } from './queue.js';
import type { CountResponse, PausedResponse } from './responses.js';
import type { JobStateName } from './types.js';
import { MAX_STACK_LINES } from './worker-types.js';

type Ctx = Queue<unknown>;

export const controlMethods = {
  async pause(this: Ctx): Promise<void> {
    await this.call({ cmd: 'Pause', queue: this.name });
  },

  async resume(this: Ctx): Promise<void> {
    await this.call({ cmd: 'Resume', queue: this.name });
  },

  async isPaused(this: Ctx): Promise<boolean> {
    return (await this.call<PausedResponse>({ cmd: 'IsPaused', queue: this.name })).paused === true;
  },

  /** Remove all waiting/delayed jobs; returns how many were dropped. */
  async drain(this: Ctx): Promise<number> {
    const response = await this.call<{ count?: number }>({ cmd: 'Drain', queue: this.name });
    return response.count ?? 0;
  },

  async obliterate(this: Ctx): Promise<void> {
    await this.call({ cmd: 'Obliterate', queue: this.name });
  },

  /** Remove old jobs; returns how many were removed. */
  async clean(this: Ctx, graceMs: number, limit?: number, state?: JobStateName): Promise<number> {
    const response = await this.call<CountResponse>(
      compact({ cmd: 'Clean', queue: this.name, grace: graceMs, limit, state }) as { cmd: string }
    );
    return response.count ?? 0;
  },

  async remove(this: Ctx, id: string): Promise<void> {
    await this.call({ cmd: 'Cancel', id });
  },

  async discard(this: Ctx, id: string): Promise<void> {
    await this.call({ cmd: 'Discard', id });
  },

  async promoteJob(this: Ctx, id: string): Promise<void> {
    await this.call({ cmd: 'Promote', id });
  },

  /** Promote delayed jobs to waiting; returns how many were promoted. */
  async promoteJobs(this: Ctx, opts: { count?: number } = {}): Promise<number> {
    const response = await this.call<CountResponse>(
      compact({ cmd: 'PromoteJobs', queue: this.name, count: opts.count }) as { cmd: string }
    );
    return response.count ?? 0;
  },

  /** BullMQ contract: failed → waiting. */
  async retryJob(this: Ctx, id: string): Promise<void> {
    await this.call({ cmd: 'MoveToWait', id });
  },

  async retryJobs(
    this: Ctx,
    opts: { state?: 'failed' | 'completed'; count?: number } = {}
  ): Promise<void> {
    if (opts.state === 'completed') {
      await this.call({ cmd: 'RetryCompleted', queue: this.name });
      return;
    }
    // `count` caps how many DLQ entries are retried (server >= 2.8.29). Older
    // servers ignore the field and retry the whole DLQ — forward-compatible.
    await this.call(
      compact({ cmd: 'RetryDlq', queue: this.name, count: opts.count }) as { cmd: string }
    );
  },

  async retryCompleted(this: Ctx, id?: string): Promise<void> {
    await this.call(compact({ cmd: 'RetryCompleted', queue: this.name, id }) as { cmd: string });
  },

  // ----------------------------------------------------------- job mutations

  async updateJobProgress(
    this: Ctx,
    id: string,
    progress: number,
    message?: string
  ): Promise<void> {
    await this.call(compact({ cmd: 'Progress', id, progress, message }) as { cmd: string });
  },

  async updateJobData(this: Ctx, id: string, data: unknown): Promise<void> {
    await this.call({ cmd: 'Update', id, data });
  },

  async changeJobPriority(
    this: Ctx,
    id: string,
    opts: { priority: number; lifo?: boolean }
  ): Promise<void> {
    await this.call({ cmd: 'ChangePriority', id, priority: opts.priority, lifo: opts.lifo });
  },

  async changeJobDelay(this: Ctx, id: string, delayMs: number): Promise<void> {
    await this.call({ cmd: 'ChangeDelay', id, delay: delayMs });
  },

  async moveJobToWait(this: Ctx, id: string): Promise<void> {
    await this.call({ cmd: 'MoveToWait', id });
  },

  async moveJobToDelayed(this: Ctx, id: string, delayMs: number): Promise<void> {
    await this.call({ cmd: 'MoveToDelayed', id, delay: delayMs });
  },

  async extendJobLock(this: Ctx, id: string, token: string, durationMs: number): Promise<void> {
    await this.call({ cmd: 'ExtendLock', id, token, duration: durationMs });
  },

  async moveJobToCompleted(
    this: Ctx,
    id: string,
    returnValue: unknown,
    token?: string
  ): Promise<void> {
    await this.call(compact({ cmd: 'ACK', id, result: returnValue, token }) as { cmd: string });
  },

  /**
   * Explicit failure path: mirrors the worker's FAIL wire so the stacktrace
   * and the UnrecoverableError "do not retry" intent are persisted (#111
   * silent-loss class), not just the message.
   */
  async moveJobToFailed(
    this: Ctx,
    id: string,
    error: Error | string,
    token?: string
  ): Promise<void> {
    const err = typeof error === 'string' ? undefined : error;
    const message = err ? err.message || err.name : (error as string);
    // Keep the FIRST lines (message + throw site), like the worker path.
    const stack = err
      ? (err.stack ?? err.message).split('\n').slice(0, MAX_STACK_LINES)
      : undefined;
    await this.call(
      compact({
        cmd: 'FAIL',
        id,
        error: message,
        stack,
        unrecoverable: err instanceof UnrecoverableError ? true : undefined,
        token,
      }) as { cmd: string }
    );
  },
};

export type QueueControlApi = typeof controlMethods;
