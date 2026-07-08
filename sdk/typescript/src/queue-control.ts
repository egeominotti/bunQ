/**
 * Queue control surface: pause/drain/clean, promotion, retry and per-job
 * mutations. Methods are merged onto Queue.prototype by queue.ts.
 */

import { compact } from './frame.js';
import type { Queue } from './queue.js';
import type { JobStateName } from './types.js';

type Ctx = Queue<unknown>;

export const controlMethods = {
  async pause(this: Ctx): Promise<void> {
    await this.call({ cmd: 'Pause', queue: this.name });
  },

  async resume(this: Ctx): Promise<void> {
    await this.call({ cmd: 'Resume', queue: this.name });
  },

  async isPaused(this: Ctx): Promise<boolean> {
    const response = await this.call({ cmd: 'IsPaused', queue: this.name });
    return response.paused === true;
  },

  async drain(this: Ctx): Promise<void> {
    await this.call({ cmd: 'Drain', queue: this.name });
  },

  async obliterate(this: Ctx): Promise<void> {
    await this.call({ cmd: 'Obliterate', queue: this.name });
  },

  /** Remove old jobs; returns how many were removed. */
  async clean(this: Ctx, graceMs: number, limit?: number, state?: JobStateName): Promise<number> {
    const response = await this.call(
      compact({ cmd: 'Clean', queue: this.name, grace: graceMs, limit, state }) as { cmd: string }
    );
    return Number(response.count ?? 0);
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
    const response = await this.call(
      compact({ cmd: 'PromoteJobs', queue: this.name, count: opts.count }) as { cmd: string }
    );
    return Number(response.count ?? 0);
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
    // `count` is accepted for API parity but not sent: the server has no
    // partial RetryDlq — it retries the whole DLQ (BullMQ semantics).
    await this.call({ cmd: 'RetryDlq', queue: this.name });
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

  async moveJobToFailed(
    this: Ctx,
    id: string,
    error: Error | string,
    token?: string
  ): Promise<void> {
    const message = typeof error === 'string' ? error : error.message;
    await this.call(compact({ cmd: 'FAIL', id, error: message, token }) as { cmd: string });
  },
};

export type QueueControlApi = typeof controlMethods;
