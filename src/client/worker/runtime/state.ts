import { EventEmitter } from 'events';
import type { Job as InternalJob } from '../../../domain/types/job';
import type { Job, Processor, WorkerOptions } from '../../types';
import { getSharedManager } from '../../manager';
import type { TcpConnectionPool } from '../../tcpPool';
import { AckBatcher } from '../ackBatcher';
import { FORCE_EMBEDDED, WORKER_CONSTANTS } from '../constants';
import { GroupConcurrencyLimiter } from '../groupConcurrency';
import type { HeartbeatDeps } from '../workerHeartbeat';
import { WorkerRateLimiter } from '../workerRateLimiter';
import type { ExtendedWorkerOptions, TcpConnection } from '../types';
import { createTcpPool, resolveWorkerOptions } from './options';

export abstract class WorkerState<T = unknown, R = unknown> extends EventEmitter {
  readonly name: string;
  protected readonly queueKey: string;
  readonly opts: ExtendedWorkerOptions;
  protected readonly processor: Processor<T, R>;
  protected readonly embedded: boolean;
  protected readonly tcp: TcpConnection | null;
  protected readonly tcpPool: TcpConnectionPool | null;
  protected readonly ackBatcher: AckBatcher;
  protected readonly rateLimiter: WorkerRateLimiter;
  protected readonly groupLimiter: GroupConcurrencyLimiter | null;

  protected running = false;
  protected paused = false;
  protected _closing = false;
  protected _forceClose = false;
  protected _closingPromise: Promise<void> | null = null;
  protected closed = false;
  protected activeJobs = 0;
  protected pollTimer: ReturnType<typeof setTimeout> | null = null;
  protected consecutiveErrors = 0;
  protected readonly activeJobIds: Set<string> = new Set();
  protected readonly pulledJobIds: Set<string> = new Set();
  protected readonly jobTokens: Map<string, string> = new Map();
  protected readonly cancelledJobs: Set<string> = new Set();
  protected heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  protected workerHeartbeatTimer: ReturnType<typeof setInterval> | null = null;
  protected processedCount = 0;
  protected failedCount = 0;
  protected readonly startedAt: number;
  protected registered = false;
  protected readonly workerId: string;
  protected pendingJobs: Array<{ job: InternalJob; token: string | null }> = [];
  protected pendingJobsHead = 0;
  protected processingScheduled = false;
  protected pendingPull = 0;
  protected lastDrainedEmit = 0;
  protected stalledUnsubscribe: (() => void) | null = null;

  on(event: 'ready' | 'drained' | 'closed', listener: () => void): this;
  on(event: 'active', listener: (job: Job<T>) => void): this;
  on(event: 'completed', listener: (job: Job<T>, result: R) => void): this;
  on(event: 'failed', listener: (job: Job<T>, error: Error) => void): this;
  on(event: 'progress', listener: (job: Job<T> | null, progress: number) => void): this;
  on(event: 'stalled', listener: (jobId: string, reason: string) => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
  on(event: 'cancelled', listener: (data: { jobId: string; reason: string }) => void): this;
  on(event: 'log', listener: (job: Job<T>, message: string) => void): this;
  // biome-ignore lint/suspicious/noExplicitAny: EventEmitter's fallback listener is intentionally untyped
  on(event: string, listener: (...args: any[]) => void): this {
    return super.on(event, listener);
  }

  once(event: 'ready' | 'drained' | 'closed', listener: () => void): this;
  once(event: 'active', listener: (job: Job<T>) => void): this;
  once(event: 'completed', listener: (job: Job<T>, result: R) => void): this;
  once(event: 'failed', listener: (job: Job<T>, error: Error) => void): this;
  once(event: 'progress', listener: (job: Job<T> | null, progress: number) => void): this;
  once(event: 'stalled', listener: (jobId: string, reason: string) => void): this;
  once(event: 'error', listener: (error: Error) => void): this;
  once(event: 'cancelled', listener: (data: { jobId: string; reason: string }) => void): this;
  once(event: 'log', listener: (job: Job<T>, message: string) => void): this;
  // biome-ignore lint/suspicious/noExplicitAny: EventEmitter's fallback listener is intentionally untyped
  once(event: string, listener: (...args: any[]) => void): this {
    return super.once(event, listener);
  }

  off(event: 'ready' | 'drained' | 'closed', listener: () => void): this;
  off(event: 'active', listener: (job: Job<T>) => void): this;
  off(event: 'completed', listener: (job: Job<T>, result: R) => void): this;
  off(event: 'failed', listener: (job: Job<T>, error: Error) => void): this;
  off(event: 'progress', listener: (job: Job<T> | null, progress: number) => void): this;
  off(event: 'stalled', listener: (jobId: string, reason: string) => void): this;
  off(event: 'error', listener: (error: Error) => void): this;
  off(event: 'cancelled', listener: (data: { jobId: string; reason: string }) => void): this;
  off(event: 'log', listener: (job: Job<T>, message: string) => void): this;
  // biome-ignore lint/suspicious/noExplicitAny: EventEmitter's fallback listener is intentionally untyped
  off(event: string, listener: (...args: any[]) => void): this {
    return super.off(event, listener);
  }

  constructor(name: string, processor: Processor<T, R>, options: WorkerOptions = {}) {
    super();
    this.name = name;
    this.queueKey = (options.prefixKey ?? '') + name;
    this.processor = processor;
    this.embedded = options.embedded ?? FORCE_EMBEDDED;
    this.startedAt = Date.now();
    this.workerId = `worker-${this.queueKey}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
    this.opts = resolveWorkerOptions(options, this.embedded);
    this.rateLimiter = new WorkerRateLimiter(
      options.limiter?.groupKey ? null : (options.limiter ?? null)
    );
    this.groupLimiter = GroupConcurrencyLimiter.fromOptions(options.limiter);
    this.ackBatcher = new AckBatcher({
      batchSize: options.batchSize ?? 10,
      interval: WORKER_CONSTANTS.DEFAULT_ACK_INTERVAL,
      embedded: this.embedded,
    });

    if (this.embedded) {
      getSharedManager(options.dataPath);
      this.tcp = null;
      this.tcpPool = null;
    } else {
      this.tcpPool = createTcpPool(options, this.opts.concurrency);
      this.tcp = this.tcpPool;
      this.ackBatcher.setTcp(this.tcp);
      this.tcpPool.onReconnect(() => {
        if (this.closed || this._closing || !this.registered) return;
        this.registered = false;
        this.registerWithServer();
      });
    }

    if (this.opts.autorun) this.run();
  }

  abstract run(): void;
  protected abstract registerWithServer(): void;
  protected abstract getHeartbeatDeps(): HeartbeatDeps;
  protected abstract startWorkerHeartbeat(): void;
  protected abstract poll(): void;
}
