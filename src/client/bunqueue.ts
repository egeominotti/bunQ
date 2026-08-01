// biome-ignore-all lint/suspicious/noExplicitAny: implementation signatures forward typed overloads.
import type { SchedulerInfo } from './queue/scheduler';
import type { DlqConfig, DlqEntry, DlqFilter, DlqStats, Job, JobOptions } from './types';
import { BunqueueRuntime } from './bunqueue/runtime';
import type { CircuitState, TriggerRule } from './bunqueue/types';

export type {
  BatchConfig,
  BatchProcessor,
  BunqueueDebounceConfig,
  BunqueueDeduplicationConfig,
  BunqueueDlqConfig,
  BunqueueMiddleware,
  BunqueueOptions,
  CircuitBreakerConfig,
  JobTtlConfig,
  PriorityAgingConfig,
  RetryConfig,
  RetryStrategy,
  TriggerRule,
} from './bunqueue/types';

/** Simplified all-in-one Queue and Worker façade. */
export class Bunqueue<T = unknown, R = unknown> extends BunqueueRuntime<T, R> {
  add(name: string, data: T, options?: JobOptions): Promise<Job<T>> {
    return this.queue.add(name, data, this.merger.merge(name, options, data));
  }

  addBulk(jobs: Array<{ name: string; data: T; opts?: JobOptions }>): Promise<Job<T>[]> {
    return this.queue.addBulk(
      jobs.map((job) => ({
        ...job,
        opts: this.merger.merge(job.name, job.opts, job.data),
      }))
    );
  }

  getJob(id: string): Promise<Job<T> | null> {
    return this.queue.getJob(id);
  }

  getJobCounts() {
    return this.queue.getJobCounts();
  }

  getJobCountsAsync() {
    return this.queue.getJobCountsAsync();
  }

  count() {
    return this.queue.count();
  }

  countAsync() {
    return this.queue.countAsync();
  }

  cron(
    id: string,
    pattern: string,
    data?: T,
    options?: { timezone?: string; limit?: number; jobOpts?: JobOptions }
  ): Promise<SchedulerInfo | null> {
    return this.queue.upsertJobScheduler(
      id,
      { pattern, timezone: options?.timezone, limit: options?.limit },
      { name: id, data, opts: options?.jobOpts }
    );
  }

  every(
    id: string,
    intervalMs: number,
    data?: T,
    options?: { limit?: number; jobOpts?: JobOptions }
  ): Promise<SchedulerInfo | null> {
    return this.queue.upsertJobScheduler(
      id,
      { every: intervalMs, limit: options?.limit },
      { name: id, data, opts: options?.jobOpts }
    );
  }

  removeCron(id: string) {
    return this.queue.removeJobScheduler(id);
  }

  listCrons() {
    return this.queue.getJobSchedulers();
  }

  cancel(jobId: string, gracePeriodMs = 0): void {
    this.cancellation.cancel(jobId, gracePeriodMs);
  }

  isCancelled(jobId: string): boolean {
    return this.cancellation.isCancelled(jobId);
  }

  getSignal(jobId: string): AbortSignal | null {
    return this.cancellation.getSignal(jobId);
  }

  getCircuitState(): CircuitState {
    return this.cb?.currentState ?? 'closed';
  }

  resetCircuit(): void {
    this.cb?.reset();
  }

  trigger(rule: TriggerRule<T>): this {
    this.triggerMgr.add(rule);
    return this;
  }

  setDefaultTtl(ttlMs: number): void {
    this.ttlChecker?.setDefaultTtl(ttlMs);
  }

  setNameTtl(name: string, ttlMs: number): void {
    this.ttlChecker?.setNameTtl(name, ttlMs);
  }

  setDlqConfig(config: Partial<DlqConfig>): void {
    this.dlqrl.setDlqConfig(config);
  }

  getDlqConfig(): DlqConfig {
    return this.dlqrl.getDlqConfig();
  }

  getDlq(filter?: DlqFilter): DlqEntry<T>[] {
    return this.dlqrl.getDlq(filter);
  }

  getDlqStats(): DlqStats {
    return this.dlqrl.getDlqStats();
  }

  retryDlq(id?: string) {
    return this.dlqrl.retryDlq(id);
  }

  purgeDlq() {
    return this.dlqrl.purgeDlq();
  }

  setGlobalRateLimit(max: number, duration?: number): void {
    this.dlqrl.setGlobalRateLimit(max, duration);
  }

  removeGlobalRateLimit(): void {
    this.dlqrl.removeGlobalRateLimit();
  }

  on(event: 'ready' | 'drained' | 'closed', listener: () => void): this;
  on(event: 'active', listener: (job: Job<T>) => void): this;
  on(event: 'completed', listener: (job: Job<T>, result: R) => void): this;
  on(event: 'failed', listener: (job: Job<T>, error: Error) => void): this;
  on(event: 'progress', listener: (job: Job<T> | null, progress: number) => void): this;
  on(event: 'stalled', listener: (jobId: string, reason: string) => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
  on(event: any, listener: (...args: any[]) => void): this {
    this.worker.on(event, listener);
    return this;
  }

  once(event: 'ready' | 'drained' | 'closed', listener: () => void): this;
  once(event: 'completed', listener: (job: Job<T>, result: R) => void): this;
  once(event: 'failed', listener: (job: Job<T>, error: Error) => void): this;
  once(event: any, listener: (...args: any[]) => void): this {
    this.worker.once(event, listener);
    return this;
  }

  off(event: any, listener: (...args: any[]) => void): this {
    this.worker.off(event, listener);
    return this;
  }

  pause(): void {
    this.queue.pause();
    this.worker.pause();
  }

  resume(): void {
    this.queue.resume();
    this.worker.resume();
  }

  async close(force = false): Promise<void> {
    this.ager?.destroy();
    this.cb?.destroy();
    this.batchAcc?.destroy();
    this.cancellation.destroyAll();
    await this.worker.close(force);
    this.queue.close();
  }

  isRunning(): boolean {
    return this.worker.isRunning();
  }

  isPaused(): boolean {
    return this.worker.isPaused();
  }

  isClosed(): boolean {
    return this.worker.isClosed();
  }
}
