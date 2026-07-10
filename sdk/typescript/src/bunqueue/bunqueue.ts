/**
 * Bunqueue — simplified all-in-one Queue + Worker (Simple Mode).
 * 1:1 port of src/client/bunqueue.ts from the official client, TCP mode only
 * (the embedded mode requires the in-process Bun runtime). The delegation
 * API (cron, DLQ, events, control, …) lives in bunqueue-api.ts and is merged
 * onto the prototype below.
 */

import type { Job } from '../job.js';
import { Queue } from '../queue.js';
import type { JobOptions } from '../types.js';
import { Worker } from '../worker.js';
import type { Processor } from '../worker-types.js';
import { PriorityAger } from './aging.js';
import { BatchAccumulator } from './batch.js';
import { type BunqueueApi, bunqueueApi } from './bunqueue-api.js';
import { CancellationManager } from './cancellation.js';
import { WorkerCircuitBreaker } from './circuit-breaker.js';
import { DedupDebounceMerger } from './dedup-debounce.js';
import { DlqRateLimitManager } from './dlq-rate-limit.js';
import { RateGate } from './rate-gate.js';
import { executeWithRetry } from './retry.js';
import { TriggerManager } from './triggers.js';
import { TtlChecker } from './ttl.js';
import type { BunqueueMiddleware, BunqueueOptions } from './types.js';

type Raw = Record<string, unknown>;

// biome-ignore lint/suspicious/noUnsafeDeclarationMerging: prototype-mixin composition — Object.assign below installs exactly the methods BunqueueApi declares
export class Bunqueue<T = unknown, R = unknown> {
  readonly name: string;
  readonly queue: Queue<T>;
  readonly worker: Worker<T, R>;
  /** @internal */ readonly cancellation = new CancellationManager();
  /** @internal */ readonly cb: WorkerCircuitBreaker | null;
  /** @internal */ readonly triggerMgr: TriggerManager<T, R>;
  /** @internal */ readonly ager: PriorityAger<T> | null;
  /** @internal */ readonly ttlChecker: TtlChecker | null;
  /** @internal */ readonly batchAcc: BatchAccumulator<T, R> | null;
  /** @internal */ readonly dlqrl: DlqRateLimitManager<T>;
  private readonly middlewares: BunqueueMiddleware<T, R>[] = [];
  private readonly baseProcessor: Processor<T, R>;
  private readonly retryConfig: BunqueueOptions<T, R>['retry'] | null;
  private readonly merger: DedupDebounceMerger;
  private readonly rateGate: RateGate | null;
  private readonly defaultJobOptions: JobOptions | undefined;

  constructor(name: string, opts: BunqueueOptions<T, R>) {
    if ((opts as Raw).embedded || (opts as Raw).dataPath) {
      throw new Error(
        'Bunqueue (bunqueue-client) is TCP-only: embedded mode requires the Bun runtime — use the official "bunqueue" package on Bun instead'
      );
    }
    const modes = [opts.processor, opts.routes, opts.batch].filter(Boolean).length;
    if (modes === 0) throw new Error('Bunqueue requires "processor", "routes", or "batch"');
    if (modes > 1) throw new Error('Bunqueue: use only one of "processor", "routes", or "batch"');

    this.name = (opts.prefixKey ?? '') + name;
    this.retryConfig = opts.retry ?? null;
    this.ttlChecker = opts.ttl ? new TtlChecker(opts.ttl) : null;
    this.merger = new DedupDebounceMerger(opts.deduplication ?? null, opts.debounce ?? null);
    const limiter = opts.rateLimit ?? opts.limiter;
    this.rateGate = limiter ? new RateGate(limiter) : null;
    this.defaultJobOptions = this.buildDefaultJobOptions(opts);

    // Build base processor
    if (opts.batch) {
      this.batchAcc = new BatchAccumulator<T, R>(opts.batch);
      this.baseProcessor = this.batchAcc.buildProcessor();
    } else {
      this.batchAcc = null;
      this.baseProcessor = opts.routes
        ? this.buildRouteProcessor(opts.routes)
        : (opts.processor as Processor<T, R>);
    }

    const wrappedProcessor: Processor<T, R> = (job: Job<T>) => this.processJob(job);

    const conn = opts.connection ?? {};
    this.queue = new Queue<T>(this.name, conn);
    this.worker = new Worker<T, R>(this.name, wrappedProcessor, {
      ...conn,
      concurrency: opts.concurrency,
      autorun: opts.autorun,
      heartbeatIntervalS:
        opts.heartbeatInterval !== undefined ? opts.heartbeatInterval / 1000 : undefined,
      batchSize: opts.batchSize,
      pollTimeoutMs: opts.pollTimeout,
    });

    // DLQ & rate limit manager
    this.dlqrl = new DlqRateLimitManager<T>(this.queue);
    // Fire-and-forget config push: without the catch, an unreachable server at
    // construction time becomes an unhandled rejection that kills the process.
    // Route the failure to the worker's 'error' event (the channel every other
    // background command failure uses); with no listener attached, swallow it
    // like pause()/resume() do — an unlistened 'error' emit would itself throw.
    if (opts.dlq) {
      void this.dlqrl.setDlqConfig(opts.dlq).catch((err: unknown) => {
        if (this.worker.listenerCount('error') > 0) {
          this.worker.emit('error', err instanceof Error ? err : new Error(String(err)));
        }
      });
    }

    // Subsystems
    this.cb = opts.circuitBreaker
      ? new WorkerCircuitBreaker(opts.circuitBreaker, this.worker)
      : null;
    this.triggerMgr = new TriggerManager<T, R>(this.queue, this.worker);
    this.ager = opts.priorityAging ? new PriorityAger<T>(opts.priorityAging, this.queue) : null;
    this.ager?.start();
  }

  private buildRouteProcessor(routes: Record<string, Processor<T, R>>): Processor<T, R> {
    const routeMap: Partial<Record<string, Processor<T, R>>> = routes;
    return (job: Job<T>): Promise<R> | R => {
      const handler = routeMap[job.name ?? ''];
      if (!handler) throw new Error(`No route for job "${job.name}" in queue "${this.name}"`);
      return handler(job);
    };
  }

  private buildDefaultJobOptions(opts: BunqueueOptions<T, R>): JobOptions | undefined {
    const base: JobOptions = { ...opts.defaultJobOptions };
    if (opts.removeOnComplete !== undefined) base.removeOnComplete ??= opts.removeOnComplete;
    if (opts.removeOnFail !== undefined) base.removeOnFail ??= opts.removeOnFail;
    return Object.keys(base).length > 0 ? base : undefined;
  }

  // ------------------------------------------------- core processing pipeline

  private async processJob(job: Job<T>): Promise<R> {
    if (this.rateGate) {
      this.rateGate.prune(); // evict fully-expired groups (high-cardinality groupKey)
      await this.rateGate.acquire(this.rateGate.groupFor(job.data));
    }
    // Circuit breaker check
    if (this.cb?.isOpen()) {
      throw new Error('Circuit breaker is open');
    }
    // TTL check
    if (this.ttlChecker?.isExpired(job.name ?? '', job.timestamp)) {
      throw new Error(`Job expired (age: ${Date.now() - job.timestamp}ms)`);
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

  private runMiddlewareChain(job: Job<T>, ac: AbortController): Promise<R> {
    if (this.middlewares.length === 0) {
      const result = this.baseProcessor(job);
      return result instanceof Promise ? result : Promise.resolve(result);
    }
    let index = 0;
    const mws = this.middlewares;
    const base = this.baseProcessor;
    const next = (): Promise<R> => {
      if (ac.signal.aborted) return Promise.reject(new Error('Job cancelled'));
      if (index < mws.length) return mws[index++](job, next);
      const result = base(job);
      return result instanceof Promise ? result : Promise.resolve(result);
    };
    return next();
  }

  // --------------------------------------------------------------- middleware

  use(middleware: BunqueueMiddleware<T, R>): this {
    this.middlewares.push(middleware);
    return this;
  }

  // --------------------------------------------------------- queue operations

  add(name: string, data: T, opts?: JobOptions): Promise<Job<T>> {
    const merged = { ...this.defaultJobOptions, ...opts };
    return this.queue.add(name, data, this.merger.merge(name, merged, data));
  }

  addBulk(jobs: Array<{ name: string; data: T; opts?: JobOptions }>): Promise<Job<T>[]> {
    return this.queue.addBulk(
      jobs.map((j) => ({
        ...j,
        opts: this.merger.merge(j.name, { ...this.defaultJobOptions, ...j.opts }, j.data),
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
    return this.queue.getJobCounts();
  }
  count() {
    return this.queue.count();
  }
  countAsync() {
    return this.queue.count();
  }
}

export interface Bunqueue<T = unknown, R = unknown> extends BunqueueApi<T, R> {}

Object.assign(Bunqueue.prototype, bunqueueApi);
