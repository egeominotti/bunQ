/**
 * Queue admin surface: DLQ, stall/DLQ configs, rate limits, schedulers,
 * monitoring and webhooks. Merged onto Queue.prototype by queue.ts.
 */

import { compact } from './frame.js';
import type { Queue } from './queue.js';
import { type JobOptions, jobPayload, wireJobOptions } from './types.js';

type Ctx = Queue<unknown>;
type Raw = Record<string, unknown>;

export interface SchedulerOptions {
  pattern?: string;
  every?: number;
  tz?: string;
  immediately?: boolean;
  limit?: number;
  skipMissedOnRestart?: boolean;
  skipIfNoWorker?: boolean;
  preventOverlap?: boolean;
}

export const adminMethods = {
  // ---------------------------------------------------------------------- dlq

  async getDlq(this: Ctx, count?: number): Promise<Raw[]> {
    const response = await this.call(
      compact({ cmd: 'Dlq', queue: this.name, count }) as {
        cmd: string;
      }
    );
    return (response.jobs ?? response.data ?? []) as Raw[];
  },

  async retryDlq(this: Ctx, jobId?: string): Promise<number> {
    const response = await this.call(
      compact({ cmd: 'RetryDlq', queue: this.name, jobId }) as {
        cmd: string;
      }
    );
    return Number(response.count ?? 0);
  },

  async purgeDlq(this: Ctx): Promise<number> {
    const response = await this.call({ cmd: 'PurgeDlq', queue: this.name });
    return Number(response.count ?? 0);
  },

  async setDlqConfig(this: Ctx, config: Raw): Promise<void> {
    await this.call({ cmd: 'SetDlqConfig', queue: this.name, config });
  },

  async getDlqConfig(this: Ctx): Promise<Raw> {
    const response = await this.call({ cmd: 'GetDlqConfig', queue: this.name });
    return (response.config ?? response.data ?? {}) as Raw;
  },

  // -------------------------------------------------------------------- stall

  async setStallConfig(this: Ctx, config: Raw): Promise<void> {
    await this.call({ cmd: 'SetStallConfig', queue: this.name, config });
  },

  async getStallConfig(this: Ctx): Promise<Raw> {
    const response = await this.call({ cmd: 'GetStallConfig', queue: this.name });
    return (response.config ?? response.data ?? {}) as Raw;
  },

  // -------------------------------------------------- rate limit / concurrency

  /** `duration` accepted for signature parity; the TCP wire carries only `limit`. */
  async setGlobalRateLimit(this: Ctx, max: number, _duration?: number): Promise<void> {
    await this.call({ cmd: 'RateLimit', queue: this.name, limit: max });
  },

  async removeGlobalRateLimit(this: Ctx): Promise<void> {
    await this.call({ cmd: 'RateLimitClear', queue: this.name });
  },

  async setGlobalConcurrency(this: Ctx, concurrency: number): Promise<void> {
    await this.call({ cmd: 'SetConcurrency', queue: this.name, limit: concurrency });
  },

  async removeGlobalConcurrency(this: Ctx): Promise<void> {
    await this.call({ cmd: 'ClearConcurrency', queue: this.name });
  },

  // ---------------------------------------------------------------- scheduler

  /** Create/update a recurring job scheduler (cron pattern or fixed interval). */
  async upsertJobScheduler(
    this: Ctx,
    schedulerId: string,
    repeat: SchedulerOptions,
    template: { name?: string; data?: unknown; opts?: JobOptions } = {}
  ): Promise<void> {
    await this.call(
      compact({
        cmd: 'Cron',
        name: schedulerId,
        queue: this.name,
        data: jobPayload(template.name ?? schedulerId, template.data ?? {}),
        schedule: repeat.pattern,
        repeatEvery: repeat.every,
        timezone: repeat.tz,
        immediately: repeat.immediately,
        maxLimit: repeat.limit,
        skipMissedOnRestart: repeat.skipMissedOnRestart,
        skipIfNoWorker: repeat.skipIfNoWorker,
        preventOverlap: repeat.preventOverlap,
        jobOptions: template.opts ? wireJobOptions(template.opts) : undefined,
      }) as { cmd: string }
    );
  },

  async removeJobScheduler(this: Ctx, schedulerId: string): Promise<void> {
    await this.call({ cmd: 'CronDelete', name: schedulerId });
  },

  async getJobScheduler(this: Ctx, schedulerId: string): Promise<Raw | null> {
    try {
      const response = await this.call({ cmd: 'CronGet', name: schedulerId });
      return (response.cron ?? response.data ?? null) as Raw | null;
    } catch {
      return null; // not-found surfaces as a CommandError
    }
  },

  async getJobSchedulers(this: Ctx): Promise<Raw[]> {
    const response = await this.call({ cmd: 'CronList' });
    const crons = (response.crons ?? []) as Raw[];
    return crons.filter((cron) => cron.queue === this.name);
  },

  async getJobSchedulersCount(this: Ctx): Promise<number> {
    return (await this.getJobSchedulers()).length;
  },

  /** Helper: run `name` on a cron `schedule`. */
  addCron(this: Ctx, name: string, schedule: string, data?: unknown, opts?: JobOptions) {
    return this.upsertJobScheduler(name, { pattern: schedule }, { name, data, opts });
  },

  /** Helper: run `name` every `intervalMs` milliseconds. */
  every(this: Ctx, name: string, intervalMs: number, data?: unknown, opts?: JobOptions) {
    return this.upsertJobScheduler(name, { every: intervalMs }, { name, data, opts });
  },

  // --------------------------------------------------------------- monitoring

  async getWorkers(this: Ctx): Promise<Raw[]> {
    const response = await this.call({ cmd: 'ListWorkers' });
    const data = (response.data ?? {}) as Raw;
    return (data.workers ?? response.workers ?? []) as Raw[];
  },

  async getWorkersCount(this: Ctx): Promise<number> {
    return (await this.getWorkers()).length;
  },

  async getStats(this: Ctx): Promise<Raw> {
    const response = await this.call({ cmd: 'Stats' });
    return (response.stats ?? {}) as Raw;
  },

  async getMetrics(this: Ctx): Promise<Raw> {
    const response = await this.call({ cmd: 'Metrics' });
    return (response.metrics ?? {}) as Raw;
  },

  /** Server returns queue NAMES (strings). */
  async listQueues(this: Ctx): Promise<string[]> {
    const response = await this.call({ cmd: 'ListQueues' });
    const queues = (response.queues ?? []) as unknown[];
    return queues.map((q) => (typeof q === 'string' ? q : String((q as Raw).name)));
  },

  // ----------------------------------------------------------------- webhooks

  async addWebhook(
    this: Ctx,
    opts: { url: string; events: string[]; queue?: string; secret?: string }
  ): Promise<Raw> {
    const response = await this.call(
      compact({
        cmd: 'AddWebhook',
        url: opts.url,
        events: opts.events,
        queue: opts.queue ?? this.name,
        secret: opts.secret,
      }) as { cmd: string }
    );
    const data = (response.data ?? {}) as Raw;
    return (data.webhook ?? data) as Raw;
  },

  async removeWebhook(this: Ctx, webhookId: string): Promise<void> {
    await this.call({ cmd: 'RemoveWebhook', webhookId });
  },

  async listWebhooks(this: Ctx): Promise<Raw[]> {
    const response = await this.call({ cmd: 'ListWebhooks' });
    const data = (response.data ?? {}) as Raw;
    return (data.webhooks ?? response.webhooks ?? []) as Raw[];
  },

  async setWebhookEnabled(this: Ctx, webhookId: string, enabled: boolean): Promise<void> {
    // wire field is `id` (SetWebhookEnabledCommand), unlike RemoveWebhook's `webhookId`
    await this.call({ cmd: 'SetWebhookEnabled', id: webhookId, enabled });
  },
};

export type QueueAdminApi = typeof adminMethods;
