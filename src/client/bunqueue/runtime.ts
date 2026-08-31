import type { FlowJobData, Job, Processor, QueueOptions, WorkerOptions } from '../types';
import { Queue } from '../queue/queue';
import { Worker } from '../worker/worker';
import { PriorityAger } from './aging';
import { BatchAccumulator } from './batch';
import { CancellationManager } from './cancellation';
import { WorkerCircuitBreaker } from './circuitBreaker';
import { DedupDebounceMerger } from './dedupDebounce';
import { DlqRateLimitManager } from './dlqRateLimit';
import { executeWithRetry } from './retry';
import { TriggerManager } from './triggers';
import { TtlChecker } from './ttl';
import type { BunqueueMiddleware, BunqueueOptions } from './types';
import { resolveProcessorResult } from '../worker/processorResult';

/** Construction and processing pipeline shared by the compact Bunqueue façade. */
export abstract class BunqueueRuntime<T, R> {
  readonly name: string;
  readonly queue: Queue<T>;
  readonly worker: Worker<T, R>;
  private readonly middlewares: BunqueueMiddleware<T, R>[] = [];
  private readonly baseProcessor: Processor<T, R>;
  protected readonly cb: WorkerCircuitBreaker | null;
  private readonly retryConfig: BunqueueOptions<T, R>['retry'] | null;
  protected readonly triggerMgr: TriggerManager<T, R>;
  protected readonly ager: PriorityAger<T> | null;
  protected readonly cancellation = new CancellationManager();
  protected readonly ttlChecker: TtlChecker | null;
  protected readonly batchAcc: BatchAccumulator<T, R> | null;
  protected readonly merger: DedupDebounceMerger;
  protected readonly dlqrl: DlqRateLimitManager<T>;

  constructor(name: string, options: BunqueueOptions<T, R>) {
    const modes = [options.processor, options.routes, options.batch].filter(Boolean).length;
    if (modes === 0) throw new Error('Bunqueue requires "processor", "routes", or "batch"');
    if (modes > 1) throw new Error('Bunqueue: use only one of "processor", "routes", or "batch"');

    this.name = name;
    this.retryConfig = options.retry ?? null;
    this.ttlChecker = options.ttl ? new TtlChecker(options.ttl) : null;
    this.merger = new DedupDebounceMerger(options.deduplication ?? null, options.debounce ?? null);

    if (options.batch) {
      this.batchAcc = new BatchAccumulator<T, R>(options.batch);
      this.baseProcessor = this.batchAcc.buildProcessor();
    } else {
      this.batchAcc = null;
      this.baseProcessor = options.routes
        ? this.buildRouteProcessor(options.routes)
        : (options.processor as Processor<T, R>);
    }

    const wrappedProcessor: Processor<T, R> = (job: Job<T & FlowJobData>) => this.processJob(job);
    this.queue = new Queue<T>(name, this.buildQueueOptions(options));
    this.worker = new Worker<T, R>(name, wrappedProcessor, this.buildWorkerOptions(options));
    this.dlqrl = new DlqRateLimitManager<T>(this.queue);
    if (options.dlq) this.dlqrl.setDlqConfig(options.dlq);

    this.cb = options.circuitBreaker
      ? new WorkerCircuitBreaker(options.circuitBreaker, this.worker as unknown as Worker)
      : null;
    this.triggerMgr = new TriggerManager<T, R>(this.queue, this.worker);
    this.ager = options.priorityAging
      ? new PriorityAger<T>(options.priorityAging, this.queue)
      : null;
    this.ager?.start();
  }

  private buildRouteProcessor(routes: Record<string, Processor<T, R>>): Processor<T, R> {
    const routeMap: Partial<Record<string, Processor<T, R>>> = routes;
    return (job: Job<T & FlowJobData>, context) => {
      const handler = routeMap[job.name];
      if (!handler) throw new Error(`No route for job "${job.name}" in queue "${this.name}"`);
      return handler(job, context);
    };
  }

  private buildQueueOptions(options: BunqueueOptions<T, R>): QueueOptions {
    return {
      connection: options.connection,
      embedded: options.embedded,
      dataPath: options.dataPath,
      defaultJobOptions: options.defaultJobOptions,
      autoBatch: options.autoBatch,
      prefixKey: options.prefixKey,
    };
  }

  private buildWorkerOptions(options: BunqueueOptions<T, R>): WorkerOptions {
    return {
      connection: options.connection,
      embedded: options.embedded,
      dataPath: options.dataPath,
      concurrency: options.concurrency,
      autorun: options.autorun,
      heartbeatInterval: options.heartbeatInterval,
      batchSize: options.batchSize,
      pollTimeout: options.pollTimeout,
      limiter: options.rateLimit ?? options.limiter,
      removeOnComplete: options.removeOnComplete,
      removeOnFail: options.removeOnFail,
      prefixKey: options.prefixKey,
    };
  }

  private processJob(job: Job<T & FlowJobData>): Promise<R> {
    if (this.cb?.isOpen()) return Promise.reject(new Error('Circuit breaker is open'));
    if (this.ttlChecker?.isExpired(job.name, job.timestamp)) {
      return Promise.reject(new Error(`Job expired (age: ${Date.now() - job.timestamp}ms)`));
    }

    const abortController = this.cancellation.register(job.id);
    const runChain = () => this.runMiddlewareChain(job, abortController);
    let execution: Promise<R>;
    try {
      execution = this.retryConfig
        ? executeWithRetry(runChain, this.retryConfig, abortController.signal)
        : runChain();
    } catch (error) {
      execution = Promise.reject(error);
    }
    return execution.then(
      (result) => {
        try {
          this.cb?.onSuccess();
          return result;
        } finally {
          this.cancellation.unregister(job.id, abortController);
        }
      },
      (error: unknown) => {
        try {
          this.cb?.onFailure();
          throw error;
        } finally {
          this.cancellation.unregister(job.id, abortController);
        }
      }
    );
  }

  private runMiddlewareChain(
    job: Job<T & FlowJobData>,
    abortController: AbortController
  ): Promise<R> {
    if (abortController.signal.aborted) return Promise.reject(new Error('Job cancelled'));
    const publicJob = job as unknown as Job<T>;
    if (this.middlewares.length === 0) {
      return resolveProcessorResult(
        this.baseProcessor(job, { signal: abortController.signal }),
        abortController.signal
      );
    }
    let index = 0;
    const next = (): Promise<R> => {
      if (abortController.signal.aborted) return Promise.reject(new Error('Job cancelled'));
      if (index < this.middlewares.length) return this.middlewares[index++](publicJob, next);
      return resolveProcessorResult(
        this.baseProcessor(job, { signal: abortController.signal }),
        abortController.signal
      );
    };
    return next();
  }

  use(middleware: BunqueueMiddleware<T, R>): this {
    this.middlewares.push(middleware);
    return this;
  }
}
