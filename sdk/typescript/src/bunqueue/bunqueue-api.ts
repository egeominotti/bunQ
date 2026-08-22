/**
 * Bunqueue Simple Mode — delegation API surface (cron, cancellation, circuit
 * breaker, TTL, DLQ, rate limit, events, control). Merged onto the Bunqueue
 * prototype by bunqueue.ts; split out to keep files small.
 */

import type { Job } from '../job.js';
import type { JobOptions } from '../types.js';
import type { Bunqueue } from './bunqueue.js';
import type { DlqFilter, DlqStats } from './dlq-rate-limit.js';
import type { CircuitState, TriggerRule } from './types.js';

type Raw = Record<string, unknown>;
// oxlint-disable-next-line typescript/no-explicit-any -- mixin context needs the erased generic instance
type Ctx = Bunqueue<any, any>;

export const bunqueueApi = {
  // --------------------------------------------------------------------- cron

  async cron(
    this: Ctx,
    id: string,
    pattern: string,
    data?: unknown,
    opts?: { timezone?: string; limit?: number; jobOpts?: JobOptions }
  ): Promise<Raw | null> {
    await this.queue.upsertJobScheduler(
      id,
      { pattern, tz: opts?.timezone, limit: opts?.limit },
      { name: id, data, opts: opts?.jobOpts }
    );
    return this.queue.getJobScheduler(id);
  },

  async every(
    this: Ctx,
    id: string,
    intervalMs: number,
    data?: unknown,
    opts?: { limit?: number; jobOpts?: JobOptions }
  ): Promise<Raw | null> {
    await this.queue.upsertJobScheduler(
      id,
      { every: intervalMs, limit: opts?.limit },
      { name: id, data, opts: opts?.jobOpts }
    );
    return this.queue.getJobScheduler(id);
  },

  removeCron(this: Ctx, id: string): Promise<void> {
    return this.queue.removeJobScheduler(id);
  },
  listCrons(this: Ctx): Promise<Raw[]> {
    return this.queue.getJobSchedulers();
  },

  // ------------------------------------------------------------- cancellation

  cancel(this: Ctx, jobId: string, gracePeriodMs = 0): void {
    this.cancellation.cancel(jobId, gracePeriodMs);
  },
  isCancelled(this: Ctx, jobId: string): boolean {
    return this.cancellation.isCancelled(jobId);
  },
  getSignal(this: Ctx, jobId: string): AbortSignal | null {
    return this.cancellation.getSignal(jobId);
  },

  // ---------------------------------------------------------- circuit breaker

  getCircuitState(this: Ctx): CircuitState {
    return this.cb?.currentState ?? 'closed';
  },
  resetCircuit(this: Ctx): void {
    this.cb?.reset();
  },

  // ----------------------------------------------------------------- triggers

  trigger(this: Ctx, rule: TriggerRule): Ctx {
    this.triggerMgr.add(rule);
    return this;
  },

  // ---------------------------------------------------------------------- ttl

  setDefaultTtl(this: Ctx, ttlMs: number): void {
    this.ttlChecker?.setDefaultTtl(ttlMs);
  },
  setNameTtl(this: Ctx, name: string, ttlMs: number): void {
    this.ttlChecker?.setNameTtl(name, ttlMs);
  },

  // ---------------------------------------------------------------------- dlq

  setDlqConfig(this: Ctx, config: Raw): Promise<void> {
    return this.dlqrl.setDlqConfig(config);
  },
  getDlqConfig(this: Ctx): Promise<Raw> {
    return this.dlqrl.getDlqConfig();
  },
  getDlq(this: Ctx, filter?: DlqFilter): Promise<Raw[]> {
    return this.dlqrl.getDlq(filter);
  },
  getDlqStats(this: Ctx): Promise<DlqStats> {
    return this.dlqrl.getDlqStats();
  },
  retryDlq(this: Ctx, id?: string): Promise<number> {
    return this.dlqrl.retryDlq(id);
  },
  purgeDlq(this: Ctx): Promise<number> {
    return this.dlqrl.purgeDlq();
  },

  // ------------------------------------------------------------ rate limiting

  setGlobalRateLimit(this: Ctx, max: number, duration?: number): Promise<void> {
    return this.dlqrl.setGlobalRateLimit(max, duration);
  },
  removeGlobalRateLimit(this: Ctx): Promise<void> {
    return this.dlqrl.removeGlobalRateLimit();
  },

  // ------------------------------------------------------------------- events

  on(this: Ctx, event: string, listener: (...args: never[]) => void): Ctx {
    this.worker.on(event, listener as (...args: unknown[]) => void);
    return this;
  },
  once(this: Ctx, event: string, listener: (...args: never[]) => void): Ctx {
    this.worker.once(event, listener as (...args: unknown[]) => void);
    return this;
  },
  off(this: Ctx, event: string, listener: (...args: never[]) => void): Ctx {
    this.worker.off(event, listener as (...args: unknown[]) => void);
    return this;
  },

  // ------------------------------------------------------------------ control

  pause(this: Ctx): void {
    void this.queue.pause().catch(() => {});
    this.worker.pause();
  },
  resume(this: Ctx): void {
    void this.queue.resume().catch(() => {});
    this.worker.resume();
  },

  async close(this: Ctx, force = false): Promise<void> {
    this.ager?.destroy();
    this.cb?.destroy();
    this.batchAcc?.destroy();
    this.cancellation.destroyAll();
    await this.worker.close(force);
    this.queue.close();
  },

  isRunning(this: Ctx): boolean {
    return this.worker.isRunning();
  },
  isPaused(this: Ctx): boolean {
    return this.worker.isPaused();
  },
  isClosed(this: Ctx): boolean {
    return this.worker.isClosed();
  },
};

/** Declaration-merged API installed on the Bunqueue prototype. */
export interface BunqueueApi<T = unknown, R = unknown> {
  cron(
    id: string,
    pattern: string,
    data?: T,
    opts?: { timezone?: string; limit?: number; jobOpts?: JobOptions }
  ): Promise<Raw | null>;
  every(
    id: string,
    intervalMs: number,
    data?: T,
    opts?: { limit?: number; jobOpts?: JobOptions }
  ): Promise<Raw | null>;
  removeCron(id: string): Promise<void>;
  listCrons(): Promise<Raw[]>;
  cancel(jobId: string, gracePeriodMs?: number): void;
  isCancelled(jobId: string): boolean;
  getSignal(jobId: string): AbortSignal | null;
  getCircuitState(): CircuitState;
  resetCircuit(): void;
  trigger(rule: TriggerRule<T>): Bunqueue<T, R>;
  setDefaultTtl(ttlMs: number): void;
  setNameTtl(name: string, ttlMs: number): void;
  setDlqConfig(config: Raw): Promise<void>;
  getDlqConfig(): Promise<Raw>;
  getDlq(filter?: DlqFilter): Promise<Raw[]>;
  getDlqStats(): Promise<DlqStats>;
  retryDlq(id?: string): Promise<number>;
  purgeDlq(): Promise<number>;
  setGlobalRateLimit(max: number, duration?: number): Promise<void>;
  removeGlobalRateLimit(): Promise<void>;
  on(event: string, listener: (...args: never[]) => void): Bunqueue<T, R>;
  once(event: string, listener: (...args: never[]) => void): Bunqueue<T, R>;
  off(event: string, listener: (...args: never[]) => void): Bunqueue<T, R>;
  pause(): void;
  resume(): void;
  close(force?: boolean): Promise<void>;
  isRunning(): boolean;
  isPaused(): boolean;
  isClosed(): boolean;
  getJob(id: string): Promise<Job<T> | null>;
}
