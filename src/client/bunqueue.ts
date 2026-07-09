/**
 * Bunqueue - Simplified all-in-one Queue + Worker
 * Routes, middleware, cron, batch, retry, circuit breaker, TTL, aging, cancellation.
 */

import { Queue } from './queue/queue';
import { Worker } from './worker/worker';
import type {
  Job,
  JobOptions,
  QueueOptions,
  WorkerOptions,
  Processor,
  FlowJobData,
  DlqConfig,
  DlqEntry,
  DlqStats,
  DlqFilter,
} from './types';
import { DlqRateLimitManager } from './bunqueue/dlqRateLimit';
import type { SchedulerInfo } from './queue/scheduler';
import type {
  BunqueueOptions,
  BunqueueMiddleware,
  TriggerRule,
  CircuitState,
} from './bunqueue/types';
import { executeWithRetry } from './bunqueue/retry';
import { WorkerCircuitBreaker } from './bunqueue/circuitBreaker';
import { BatchAccumulator } from './bunqueue/batch';
import { TriggerManager } from './bunqueue/triggers';
import { PriorityAger } from './bunqueue/aging';
import { CancellationManager } from './bunqueue/cancellation';
import { TtlChecker } from './bunqueue/ttl';
import { DedupDebounceMerger } from './bunqueue/dedupDebounce';

// Re-export all types
export type {
  BunqueueMiddleware,
  BunqueueOptions,
  RetryStrategy,
  RetryConfig,
  CircuitBreakerConfig,
  TriggerRule,
  PriorityAgingConfig,
  BatchProcessor,
  BatchConfig,
  JobTtlConfig,
  BunqueueDeduplicationConfig,
  BunqueueDebounceConfig,
  BunqueueDlqConfig,
} from './bunqueue/types';

export class Bunqueue<T = unknown, R = unknown> {
  readonly name: string;
  readonly queue: Queue<T>;
  readonly worker: Worker<T, R>;
  private readonly middlewares: BunqueueMiddleware<T, R>[] = [];
  private readonly baseProcessor: Processor<T, R>;
  private readonly cb: WorkerCircuitBreaker | null;
  private readonly retryConfig: BunqueueOptions<T, R>['retry'] | null;
  private readonly triggerMgr: TriggerManager<T, R>;
  private readonly ager: PriorityAger<T> | null;
  private readonly cancellation = new CancellationManager();
  private readonly ttlChecker: TtlChecker | null;
  private readonly batchAcc: BatchAccumulator<T, R> | null;
  private readonly merger: DedupDebounceMerger;
  private readonly dlqrl: DlqRateLimitManager<T>;

  constructor(name: string, opts: BunqueueOptions<T, R>) {
    const modes = [opts.processor, opts.routes, opts.batch].filter(Boolean).length;
    if (modes === 0) throw new Error('Bunqueue requires "processor", "routes", or "batch"');
    if (modes > 1) throw new Error('Bunqueue: use only one of "processor", "routes", or "batch"');

    this.name = name;
    this.retryConfig = opts.retry ?? null;
    this.ttlChecker = opts.ttl ? new TtlChecker(opts.ttl) : null;
    this.merger = new DedupDebounceMerger(opts.deduplication ?? null, opts.debounce ?? null);

    // Build base processor
    if (opts.batch) {
      this.batchAcc = new BatchAccumulator<T, R>(opts.batch);
      this.baseProcessor = this.batchAcc.buildProcessor();
    } else {
      this.batchAcc = null;
      this.baseProcessor = opts.routes ? this.buildRouteProcessor(opts.routes) : opts.processor!; // eslint-disable-line @typescript-eslint/no-non-null-assertion
    }

    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this;
    const wrappedProcessor: Processor<T, R> = (job: Job<T & FlowJobData>) => self.processJob(job);

    this.queue = new Queue<T>(name, this.buildQueueOpts(opts));
    this.worker = new Worker<T, R>(name, wrappedProcessor, this.buildWorkerOpts(opts));

    // DLQ & Rate Limit manager
    this.dlqrl = new DlqRateLimitManager<T>(this.queue);
    if (opts.dlq) this.dlqrl.setDlqConfig(opts.dlq);

    // Initialize subsystems
    this.cb = opts.circuitBreaker
      ? new WorkerCircuitBreaker(opts.circuitBreaker, this.worker as unknown as Worker)
      : null;
    this.triggerMgr = new TriggerManager<T, R>(this.queue, this.worker);
    this.ager = opts.priorityAging ? new PriorityAger<T>(opts.priorityAging, this.queue) : null;
    this.ager?.start();
  }

  private buildRouteProcessor(routes: Record<string, Processor<T, R>>): Processor<T, R> {
    const routeMap: Partial<Record<string, Processor<T, R>>> = routes;
    return (job: Job<T & FlowJobData>): Promise<R> | R => {
      const handler = routeMap[job.name];
      if (!handler) throw new Error(`No route for job "${job.name}" in queue "${this.name}"`);
      return handler(job);
    };
  }

  private buildQueueOpts(opts: BunqueueOptions<T, R>): QueueOptions {
    return {
      connection: opts.connection,
      embedded: opts.embedded,
      dataPath: opts.dataPath,
      defaultJobOptions: opts.defaultJobOptions,
      autoBatch: opts.autoBatch,
      prefixKey: opts.prefixKey,
    };
  }

  private buildWorkerOpts(opts: BunqueueOptions<T, R>): WorkerOptions {
    return {
      connection: opts.connection,
      embedded: opts.embedded,
      dataPath: opts.dataPath,
      concurrency: opts.concurrency,
      autorun: opts.autorun,
      heartbeatInterval: opts.heartbeatInterval,
      batchSize: opts.batchSize,
      pollTimeout: opts.pollTimeout,
      limiter: opts.rateLimit ?? opts.limiter,
      removeOnComplete: opts.removeOnComplete,
      removeOnFail: opts.removeOnFail,
      prefixKey: opts.prefixKey,
    };
  }

  // ============ Core Processing Pipeline ============

  private processJob(job: Job<T & FlowJobData>): Promise<R> {
    // Circuit breaker check
    if (this.cb?.isOpen()) {
      return Promise.reject(new Error('Circuit breaker is open'));
    }
    // TTL check
    if (this.ttlChecker?.isExpired(job.name, job.timestamp)) {
      return Promise.reject(new Error(`Job expired (age: ${Date.now() - job.timestamp}ms)`));
    }
    // Register cancellation
    const ac = this.cancellation.register(job.id);
    const runChain = () => this.runMiddlewareChain(job, ac);
    const execute = this.retryConfig ? executeWithRetry(runChain, this.retryConfig) : runChain();

    return execute.then(
      (result) => {
        this.cb?.onSuccess();
        this.cancellation.unregister(job.id);
        return result;
      },
      (err: unknown) => {
        this.cb?.onFailure();
        this.cancellation.unregister(job.id);
        throw err;
      }
    );
  }

  private runMiddlewareChain(job: Job<T & FlowJobData>, ac: AbortController): Promise<R> {
    const asJob = job as unknown as Job<T>;
    if (this.middlewares.length === 0) {
      const result = this.baseProcessor(job);
      return result instanceof Promise ? result : Promise.resolve(result);
    }
    let index = 0;
    const mws = this.middlewares;
    const base = this.baseProcessor;
    const next = (): Promise<R> => {
      if (ac.signal.aborted) return Promise.reject(new Error('Job cancelled'));
      if (index < mws.length) return mws[index++](asJob, next);
      const result = base(job);
      return result instanceof Promise ? result : Promise.resolve(result);
    };
    return next();
  }

  // ============ Middleware ============

  use(middleware: BunqueueMiddleware<T, R>): this {
    this.middlewares.push(middleware);
    return this;
  }

  // ============ Queue Operations ============

  add(name: string, data: T, opts?: JobOptions): Promise<Job<T>> {
    return this.queue.add(name, data, this.merger.merge(name, opts, data));
  }

  addBulk(jobs: Array<{ name: string; data: T; opts?: JobOptions }>): Promise<Job<T>[]> {
    return this.queue.addBulk(
      jobs.map((j) => ({ ...j, opts: this.merger.merge(j.name, j.opts, j.data) }))
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

  // ============ Cron ============

  cron(
    id: string,
    pattern: string,
    data?: T,
    opts?: { timezone?: string; limit?: number; jobOpts?: JobOptions }
  ): Promise<SchedulerInfo | null> {
    return this.queue.upsertJobScheduler(
      id,
      { pattern, timezone: opts?.timezone, limit: opts?.limit },
      { name: id, data, opts: opts?.jobOpts }
    );
  }

  every(
    id: string,
    intervalMs: number,
    data?: T,
    opts?: { limit?: number; jobOpts?: JobOptions }
  ): Promise<SchedulerInfo | null> {
    return this.queue.upsertJobScheduler(
      id,
      { every: intervalMs, limit: opts?.limit },
      { name: id, data, opts: opts?.jobOpts }
    );
  }

  removeCron(id: string) {
    return this.queue.removeJobScheduler(id);
  }
  listCrons() {
    return this.queue.getJobSchedulers();
  }

  // ============ Cancellation (feature 3) ============

  cancel(jobId: string, gracePeriodMs = 0): void {
    this.cancellation.cancel(jobId, gracePeriodMs);
  }
  isCancelled(jobId: string): boolean {
    return this.cancellation.isCancelled(jobId);
  }
  getSignal(jobId: string): AbortSignal | null {
    return this.cancellation.getSignal(jobId);
  }

  // ============ Circuit Breaker (feature 5) ============

  getCircuitState(): CircuitState {
    return this.cb?.currentState ?? 'closed';
  }
  resetCircuit(): void {
    this.cb?.reset();
  }

  // ============ Triggers (feature 6) ============

  trigger(rule: TriggerRule<T>): this {
    this.triggerMgr.add(rule);
    return this;
  }

  // ============ TTL (feature 7) ============

  setDefaultTtl(ttlMs: number): void {
    this.ttlChecker?.setDefaultTtl(ttlMs);
  }
  setNameTtl(name: string, ttlMs: number): void {
    this.ttlChecker?.setNameTtl(name, ttlMs);
  }

  // ============ DLQ (feature 12) ============

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

  // ============ Rate Limiting ============

  setGlobalRateLimit(max: number, duration?: number): void {
    this.dlqrl.setGlobalRateLimit(max, duration);
  }
  removeGlobalRateLimit(): void {
    this.dlqrl.removeGlobalRateLimit();
  }

  // ============ Events ============
  /* eslint-disable @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-argument */

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
  /* eslint-enable @typescript-eslint/no-explicit-any, @typescript-eslint/no-unsafe-argument */

  // ============ Control ============

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
