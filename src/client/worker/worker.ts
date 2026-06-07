/**
 * Worker
 * BullMQ-style worker for job processing
 */

import { EventEmitter } from 'events';
import { hostname } from 'os';
import { getSharedManager } from '../manager';
import { TcpConnectionPool } from '../tcpPool';
import { EventType } from '../../domain/types/queue';
import type { WorkerOptions, Processor, ConnectionOptions, Job } from '../types';
import { jobId, type Job as InternalJob } from '../../domain/types/job';
import type { TcpConnection, ExtendedWorkerOptions } from './types';
import { FORCE_EMBEDDED, WORKER_CONSTANTS } from './types';
import { AckBatcher } from './ackBatcher';
import { parseJobFromResponse } from './jobParser';
import { processJob } from './processor';
import { WorkerRateLimiter } from './workerRateLimiter';
import { GroupConcurrencyLimiter } from './groupConcurrency';
import { startHeartbeat, type HeartbeatDeps } from './workerHeartbeat';
import { pullEmbedded, pullTcp, type PullConfig } from './workerPull';
import { resolveToken } from '../resolveToken';

/** Resolve WorkerOptions into ExtendedWorkerOptions with defaults */
function resolveWorkerOptions(opts: WorkerOptions, embedded: boolean): ExtendedWorkerOptions {
  return {
    concurrency: opts.concurrency ?? 1,
    autorun: opts.autorun ?? true,
    heartbeatInterval: opts.heartbeatInterval ?? 10000,
    batchSize: Math.min(opts.batchSize ?? 10, 1000),
    pollTimeout: Math.min(opts.pollTimeout ?? 0, WORKER_CONSTANTS.MAX_POLL_TIMEOUT),
    embedded,
    useLocks: opts.useLocks ?? true,
    skipLockRenewal: opts.skipLockRenewal ?? false,
    skipStalledCheck: opts.skipStalledCheck ?? false,
    drainDelay: opts.drainDelay ?? 50,
    lockDuration: opts.lockDuration ?? 30000,
    maxStalledCount: opts.maxStalledCount ?? 1,
    removeOnComplete: opts.removeOnComplete,
    removeOnFail: opts.removeOnFail,
  };
}

/** Create TCP connection pool from options */
function createTcpPool(opts: WorkerOptions, concurrency: number): TcpConnectionPool {
  const connOpts: ConnectionOptions = opts.connection ?? {};
  const poolSize = connOpts.poolSize ?? Math.min(concurrency, 8);
  const token = resolveToken(connOpts.token);
  return new TcpConnectionPool({
    host: connOpts.host ?? 'localhost',
    port: connOpts.port ?? 6789,
    token,
    poolSize,
    pingInterval: connOpts.pingInterval,
    commandTimeout: connOpts.commandTimeout,
    maxCommandTimeouts: connOpts.maxCommandTimeouts,
    pipelining: connOpts.pipelining,
    maxInFlight: connOpts.maxInFlight,
  });
}

/**
 * Worker class for processing jobs
 */
export class Worker<T = unknown, R = unknown> extends EventEmitter {
  readonly name: string;
  /**
   * Server-side queue key. Equals `name` unless `prefixKey` is set, in which
   * case it is `prefixKey + name`. All broker pulls/registrations route
   * through this key so a Worker only sees jobs from its own namespace.
   */
  private readonly queueKey: string;
  readonly opts: ExtendedWorkerOptions;
  private readonly processor: Processor<T, R>;
  private readonly embedded: boolean;
  private readonly tcp: TcpConnection | null;
  private readonly tcpPool: TcpConnectionPool | null;
  private readonly ackBatcher: AckBatcher;
  private readonly rateLimiter: WorkerRateLimiter;
  private readonly groupLimiter: GroupConcurrencyLimiter | null;

  private running = false;
  private paused = false;
  private _closing = false;
  private _closingPromise: Promise<void> | null = null;
  private closed = false;
  private activeJobs = 0;
  private pollTimer: ReturnType<typeof setTimeout> | null = null;
  private consecutiveErrors = 0;

  // Heartbeat tracking with lock tokens (BullMQ-style ownership)
  // Track ALL pulled jobs (both active and buffered) for heartbeat
  private readonly activeJobIds: Set<string> = new Set();
  private readonly pulledJobIds: Set<string> = new Set(); // All pulled jobs (for heartbeat)
  private readonly jobTokens: Map<string, string> = new Map(); // jobId -> lockToken
  private readonly cancelledJobs: Set<string> = new Set(); // Jobs marked for cancellation
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;

  // Worker registration tracking
  private workerHeartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private processedCount = 0;
  private failedCount = 0;
  private readonly startedAt: number;
  private registered = false;

  // Unique worker ID for lock ownership
  private readonly workerId: string;

  // Job buffer for batch pulls (with tokens)
  private pendingJobs: Array<{ job: InternalJob; token: string | null }> = [];
  private pendingJobsHead = 0;
  private processingScheduled = false; // Prevent multiple setImmediate calls

  // Drained event tracking
  private lastDrainedEmit = 0;

  // Stalled event subscription (BullMQ v5 compatible)
  private stalledUnsubscribe: (() => void) | null = null;

  // ============ Typed Event Overloads ============

  on(event: 'ready' | 'drained' | 'closed', listener: () => void): this;
  on(event: 'active', listener: (job: Job<T>) => void): this;
  on(event: 'completed', listener: (job: Job<T>, result: R) => void): this;
  on(event: 'failed', listener: (job: Job<T>, error: Error) => void): this;
  on(event: 'progress', listener: (job: Job<T> | null, progress: number) => void): this;
  on(event: 'stalled', listener: (jobId: string, reason: string) => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
  on(event: 'cancelled', listener: (data: { jobId: string; reason: string }) => void): this;
  on(event: 'log', listener: (job: Job<T>, message: string) => void): this;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
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
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
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
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  off(event: string, listener: (...args: any[]) => void): this {
    return super.off(event, listener);
  }

  constructor(name: string, processor: Processor<T, R>, opts: WorkerOptions = {}) {
    super();
    this.name = name;
    this.queueKey = (opts.prefixKey ?? '') + name;
    this.processor = processor;
    this.embedded = opts.embedded ?? FORCE_EMBEDDED;
    this.startedAt = Date.now();
    this.workerId = `worker-${this.queueKey}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
    this.opts = resolveWorkerOptions(opts, this.embedded);
    this.rateLimiter = new WorkerRateLimiter(
      opts.limiter?.groupKey ? null : (opts.limiter ?? null)
    );
    this.groupLimiter = GroupConcurrencyLimiter.fromOptions(opts.limiter);
    this.ackBatcher = new AckBatcher({
      batchSize: opts.batchSize ?? 10,
      interval: WORKER_CONSTANTS.DEFAULT_ACK_INTERVAL,
      embedded: this.embedded,
    });

    if (this.embedded) {
      getSharedManager(opts.dataPath);
      this.tcp = null;
      this.tcpPool = null;
    } else {
      this.tcpPool = createTcpPool(opts, this.opts.concurrency);
      this.tcp = this.tcpPool;
      this.ackBatcher.setTcp(this.tcp);
    }

    if (this.opts.autorun) this.run();
  }

  /** Start processing */
  run(): void {
    if (this.running || this.closed) return;
    this.running = true;
    this.paused = false;
    this._closing = false;
    this._closingPromise = null;
    // Defer the 'ready' emit so listeners attached synchronously after
    // construction (e.g. `new Worker(...).on('ready', ...)`) still receive it.
    // run() is called from the constructor when autorun is true (the default),
    // at which point chained or post-construction listeners haven't been
    // registered yet. See issue #76.
    queueMicrotask(() => {
      if (!this.closed) this.emit('ready');
    });

    // Subscribe to stalled events in embedded mode (BullMQ v5)
    if (this.embedded && !this.stalledUnsubscribe && !this.opts.skipStalledCheck) {
      this.subscribeToStalledEvents();
    }

    // Register worker with server (TCP only)
    if (!this.embedded && this.tcp && !this.registered) {
      this.registerWithServer();
    }

    if (this.opts.heartbeatInterval > 0 && !this.opts.skipLockRenewal) {
      if (this.embedded) {
        this.heartbeatTimer = setInterval(() => {
          const manager = getSharedManager();
          for (const id of this.pulledJobIds) {
            manager.jobHeartbeat(jobId(id));
          }
        }, this.opts.heartbeatInterval);
      } else {
        const deps = this.getHeartbeatDeps();
        this.heartbeatTimer = startHeartbeat(deps, this.opts.heartbeatInterval);

        // Worker-level heartbeat (separate from job heartbeat)
        this.startWorkerHeartbeat();
      }
    }
    this.poll();
  }

  /** Subscribe to stalled events from QueueManager (BullMQ v5 compatible) */
  private subscribeToStalledEvents(): void {
    if (!this.embedded) return;

    const manager = getSharedManager();
    this.stalledUnsubscribe = manager.subscribe((event) => {
      // Events carry the prefixed server-side queue key, so we must compare
      // against `queueKey` (not the logical `name`) to scope correctly.
      if (event.queue !== this.queueKey) return;
      if (event.eventType === EventType.Stalled) {
        this.emit('stalled', event.jobId, 'active');
      }
    });
  }

  /** Pause processing */
  pause(): void {
    if (!this.running) return;
    this.running = false;
    this.paused = true;
    if (this.pollTimer) {
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }
  }

  /** Resume processing */
  resume(): void {
    if (this.closed) return;
    this.paused = false;
    this.run();
  }

  isRunning(): boolean {
    return this.running;
  }

  isPaused(): boolean {
    return this.paused && !this.closed;
  }

  isClosed(): boolean {
    return this.closed;
  }

  /** Get current concurrency */
  get concurrency(): number {
    return this.opts.concurrency;
  }

  /** Set concurrency at runtime (BullMQ v5 compatible) */
  set concurrency(val: number) {
    const clamped = Math.max(1, val);
    const prev = this.opts.concurrency;
    (this.opts as { concurrency: number }).concurrency = clamped;
    if (clamped > prev && this.running && !this._closing) {
      this.poll();
    }
  }

  /** Promise that resolves when close() finishes (BullMQ v5 compatible) */
  get closing(): Promise<void> | null {
    return this._closingPromise;
  }

  async waitUntilReady(): Promise<void> {
    if (this.embedded) return;
    if (this.tcpPool) {
      await this.tcpPool.send({ cmd: 'Ping' });
    }
  }

  cancelJob(jobId: string, reason?: string): boolean {
    if (this.activeJobIds.has(jobId)) {
      this.cancelledJobs.add(jobId);
      this.emit('cancelled', { jobId, reason: reason ?? 'Job cancelled by worker' });
      return true;
    }
    return false;
  }

  cancelAllJobs(reason?: string): void {
    for (const jobId of this.activeJobIds) {
      this.cancelledJobs.add(jobId);
      this.emit('cancelled', { jobId, reason: reason ?? 'All jobs cancelled' });
    }
  }

  isJobCancelled(jobId: string): boolean {
    return this.cancelledJobs.has(jobId);
  }

  // ============ Rate Limiter (delegated) ============

  getRateLimiterInfo() {
    return this.rateLimiter.getRateLimiterInfo();
  }

  rateLimit(expireTimeMs: number): void {
    this.rateLimiter.rateLimit(expireTimeMs);
  }

  isRateLimited(): boolean {
    return this.rateLimiter.isRateLimited();
  }

  // ============ BullMQ v5 Compatibility ============

  async startStalledCheckTimer(): Promise<void> {
    // No-op for API compatibility - stall detection is automatic
  }

  async delay(milliseconds = 0, abortController?: AbortController): Promise<void> {
    if (milliseconds <= 0) return;

    return new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(resolve, milliseconds);

      if (abortController) {
        abortController.signal.addEventListener('abort', () => {
          clearTimeout(timeout);
          reject(new Error('Delay aborted'));
        });
      }
    });
  }

  // ============ Manual Job Control ============

  async getNextJob(token?: string, _opts?: { block?: boolean }): Promise<InternalJob | undefined> {
    if (this.closed) return undefined;

    if (this.embedded) {
      const manager = getSharedManager();
      if (this.opts.useLocks) {
        const { job, token: lockToken } = await manager.pullWithLock(
          this.queueKey,
          this.workerId,
          0
        );
        if (job && lockToken) {
          const jobIdStr = String(job.id);
          this.pulledJobIds.add(jobIdStr);
          this.jobTokens.set(jobIdStr, lockToken);
        }
        return job ?? undefined;
      }
      const job = await manager.pull(this.queueKey, 0);
      if (job) {
        this.pulledJobIds.add(String(job.id));
      }
      return job ?? undefined;
    }

    // TCP mode
    if (!this.tcp) return undefined;

    const cmd: Record<string, unknown> = {
      cmd: 'PULL',
      queue: this.queueKey,
      timeout: 0,
    };
    if (this.opts.useLocks) {
      cmd.owner = this.workerId;
      if (token) cmd.token = token;
    }

    const response = await this.tcp.send(cmd);
    if (!response.ok || !response.job) return undefined;

    const job = parseJobFromResponse(response.job as Record<string, unknown>, this.queueKey);
    const jobIdStr = String(job.id);
    this.pulledJobIds.add(jobIdStr);

    if (this.opts.useLocks && response.token) {
      this.jobTokens.set(jobIdStr, response.token as string);
    }

    return job;
  }

  async processJobManually(
    job: InternalJob,
    token?: string,
    fetchNextCallback?: () => Promise<InternalJob | undefined>
  ): Promise<InternalJob | undefined> {
    if (this.closed) return undefined;

    const jobIdStr = String(job.id);
    this.activeJobs++;
    this.activeJobIds.add(jobIdStr);
    this.pulledJobIds.add(jobIdStr);

    if (this.opts.useLocks && token) {
      this.jobTokens.set(jobIdStr, token);
    }

    try {
      await processJob(job, {
        name: this.queueKey,
        processor: this.processor,
        embedded: this.embedded,
        tcp: this.tcp,
        ackBatcher: this.ackBatcher,
        emitter: this,
        token: this.opts.useLocks ? token : undefined,
      });

      if (fetchNextCallback) {
        return await fetchNextCallback();
      }
    } finally {
      this.activeJobs--;
      this.activeJobIds.delete(jobIdStr);
      this.pulledJobIds.delete(jobIdStr);
      this.cancelledJobs.delete(jobIdStr);
      if (this.opts.useLocks) {
        this.jobTokens.delete(jobIdStr);
      }
      this.rateLimiter.recordJobForLimiter();
    }
  }

  async extendJobLocks(jobIds: string[], tokens: string[], duration: number): Promise<number> {
    if (this.closed || jobIds.length === 0) return 0;
    if (jobIds.length !== tokens.length) {
      throw new Error('jobIds and tokens arrays must have the same length');
    }

    if (this.embedded) {
      const manager = getSharedManager();
      let extended = 0;
      for (let i = 0; i < jobIds.length; i++) {
        const success = await manager.extendLock(jobIds[i], tokens[i], duration);
        if (success) extended++;
      }
      return extended;
    }

    if (!this.tcp) return 0;

    const response = await this.tcp.send({
      cmd: 'ExtendLocks',
      ids: jobIds,
      tokens,
      duration,
    });

    const extended = response.extended as number | undefined;
    return extended ?? 0;
  }

  // ============ Lifecycle ============

  async close(force = false): Promise<void> {
    if (this.closed) return;
    if (this._closingPromise) return this._closingPromise;
    this._closingPromise = this._doClose(force);
    return this._closingPromise;
  }

  private async _doClose(force: boolean): Promise<void> {
    this._closing = true;
    this.running = false;
    this.paused = false;
    if (this.pollTimer) {
      clearTimeout(this.pollTimer);
      this.pollTimer = null;
    }
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    if (this.workerHeartbeatTimer) {
      clearInterval(this.workerHeartbeatTimer);
      this.workerHeartbeatTimer = null;
    }

    if (!force) {
      const bufferSize = () => this.pendingJobs.length - this.pendingJobsHead;
      while (this.activeJobs > 0 || bufferSize() > 0) {
        await Bun.sleep(50);
      }
    }

    await this.ackBatcher.flush();
    await this.ackBatcher.waitForInFlight();
    this.ackBatcher.stop();

    // Unregister from server before closing connection
    if (!this.embedded && this.tcp && this.registered) {
      try {
        await this.tcp.send({ cmd: 'UnregisterWorker', workerId: this.workerId });
      } catch {
        // Best-effort unregister — server will cleanup via stale timeout
      }
      this.registered = false;
    }

    await Bun.sleep(100);

    if (this.stalledUnsubscribe) {
      this.stalledUnsubscribe();
      this.stalledUnsubscribe = null;
    }

    this.activeJobIds.clear();
    this.pulledJobIds.clear();
    this.jobTokens.clear();
    this.cancelledJobs.clear();
    this.pendingJobs = [];
    this.pendingJobsHead = 0;
    if (this.groupLimiter) this.groupLimiter.clear();

    if (this.tcpPool) this.tcpPool.close();
    this.closed = true;
    this._closing = false;
    this.emit('closed');
  }

  // ============ Processing Pipeline ============

  private poll(): void {
    if (!this.running || this._closing) return;

    if (this.activeJobs >= this.opts.concurrency) {
      this.pollTimer = setTimeout(() => {
        this.poll();
      }, 10);
      return;
    }

    // Check rate limiter
    if (!this.rateLimiter.canProcessWithinLimit()) {
      const waitTime = this.rateLimiter.getTimeUntilNextSlot();
      this.pollTimer = setTimeout(
        () => {
          this.poll();
        },
        Math.max(waitTime, 10)
      );
      return;
    }

    void this.tryProcess();
  }

  private async tryProcess(): Promise<void> {
    if (!this.running || this._closing) return;

    try {
      let item = this.getNextEligibleJob();

      if (!item) {
        const items = await this.doPullBatch();
        // Re-check state after async operation (can be modified during await)
        // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
        if (!this.running || this._closing) return;
        if (items.length > 0) {
          this.registerPulledJobs(items);
          // Add all items to buffer, then find eligible one
          if (this.pendingJobsHead >= this.pendingJobs.length) {
            this.pendingJobs = items;
            this.pendingJobsHead = 0;
          } else {
            this.pendingJobs = this.pendingJobs.slice(this.pendingJobsHead).concat(items);
            this.pendingJobsHead = 0;
          }
          item = this.getNextEligibleJob();
        }
      }

      if (item) {
        this.consecutiveErrors = 0;
        this.startJob(item.job, item.token);
      } else {
        // Check if we have buffered jobs but all are group-limited
        const hasBuffered = this.pendingJobsHead < this.pendingJobs.length;
        if (hasBuffered && this.groupLimiter) {
          // Retry shortly - a running job may finish and free a group slot
          this.pollTimer = setTimeout(() => {
            this.poll();
          }, 10);
          return;
        }
        const now = Date.now();
        if (now - this.lastDrainedEmit > 1000) {
          this.lastDrainedEmit = now;
          this.emit('drained');
        }
        const waitTime = this.opts.pollTimeout > 0 ? 10 : this.opts.drainDelay;
        this.pollTimer = setTimeout(() => {
          this.poll();
        }, waitTime);
      }
    } catch (err) {
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
      if (!this.running) return;
      this.handlePullError(err);
    }
  }

  private registerPulledJobs(items: Array<{ job: InternalJob; token: string | null }>): void {
    for (const pulledItem of items) {
      const jobIdStr = String(pulledItem.job.id);
      this.pulledJobIds.add(jobIdStr);
      if (this.opts.useLocks && pulledItem.token) {
        this.jobTokens.set(jobIdStr, pulledItem.token);
      }
    }
  }

  private getBufferedJob(): { job: InternalJob; token: string | null } | null {
    if (this.pendingJobsHead >= this.pendingJobs.length) return null;

    const item = this.pendingJobs[this.pendingJobsHead++];
    if (this.pendingJobsHead > 500 && this.pendingJobsHead >= this.pendingJobs.length / 2) {
      this.pendingJobs = this.pendingJobs.slice(this.pendingJobsHead);
      this.pendingJobsHead = 0;
    }
    return item;
  }

  /**
   * Get the next buffered job eligible for processing.
   * When group concurrency is enabled, skips jobs whose group is at capacity
   * and leaves them in the buffer for later processing.
   */
  private getNextEligibleJob(): { job: InternalJob; token: string | null } | null {
    if (this.pendingJobsHead >= this.pendingJobs.length) return null;

    // No group limiter - just return the next buffered job
    if (!this.groupLimiter) {
      return this.getBufferedJob();
    }

    // Scan buffer for a job whose group has capacity
    const start = this.pendingJobsHead;
    const end = this.pendingJobs.length;
    for (let i = start; i < end; i++) {
      const item = this.pendingJobs[i];
      if (this.groupLimiter.canProcess(item.job)) {
        // Remove this item from the buffer by swapping with the head
        this.pendingJobs[i] = this.pendingJobs[this.pendingJobsHead];
        this.pendingJobsHead++;
        if (this.pendingJobsHead > 500 && this.pendingJobsHead >= this.pendingJobs.length / 2) {
          this.pendingJobs = this.pendingJobs.slice(this.pendingJobsHead);
          this.pendingJobsHead = 0;
        }
        return item;
      }
    }
    return null;
  }

  private async doPullBatch(): Promise<Array<{ job: InternalJob; token: string | null }>> {
    const slots = this.opts.concurrency - this.activeJobs;
    const batchSize = Math.min(this.opts.batchSize, slots, 1000);
    if (batchSize <= 0) return [];

    const config = this.getPullConfig();
    return this.embedded
      ? pullEmbedded(config, batchSize)
      : pullTcp(config, this.tcp as NonNullable<typeof this.tcp>, batchSize, this._closing);
  }

  private startJob(job: InternalJob, token: string | null): void {
    const jobIdStr = String(job.id);

    // Dedup: skip if this job is already being processed (Issue #33)
    // This happens when stall detection retries a job while the worker
    // is still processing it, and the worker's other concurrency slot
    // pulls the retried job from the queue.
    if (this.activeJobIds.has(jobIdStr)) {
      return;
    }

    this.activeJobs++;
    this.activeJobIds.add(jobIdStr);

    // Apply worker-level removeOnComplete/removeOnFail defaults to the job
    this.applyRemoveDefaults(job);

    // Track group concurrency
    if (this.groupLimiter) {
      this.groupLimiter.increment(job);
    }

    if (this.opts.useLocks && token && !this.jobTokens.has(jobIdStr)) {
      this.jobTokens.set(jobIdStr, token);
    }
    this.pulledJobIds.add(jobIdStr);

    const tokenForProcess = this.opts.useLocks ? token : undefined;

    void processJob(job, {
      name: this.queueKey,
      processor: this.processor,
      embedded: this.embedded,
      tcp: this.tcp,
      ackBatcher: this.ackBatcher,
      emitter: this,
      token: tokenForProcess,
      onOutcome: (ok) => {
        if (ok) this.processedCount++;
        else this.failedCount++;
      },
    }).finally(() => {
      this.activeJobs--;
      this.activeJobIds.delete(jobIdStr);
      this.pulledJobIds.delete(jobIdStr);
      this.cancelledJobs.delete(jobIdStr);
      if (this.opts.useLocks) {
        this.jobTokens.delete(jobIdStr);
      }
      // Release group concurrency slot
      if (this.groupLimiter) {
        this.groupLimiter.decrement(job);
      }
      this.rateLimiter.recordJobForLimiter();
      if (this.running && !this._closing) this.poll();
    });

    if (this.activeJobs < this.opts.concurrency && !this._closing && !this.processingScheduled) {
      this.processingScheduled = true;
      setImmediate(() => {
        this.processingScheduled = false;
        void this.tryProcess();
      });
    }
  }

  private handlePullError(err: unknown): void {
    this.consecutiveErrors++;
    const error = err instanceof Error ? err : new Error(String(err));
    this.emit(
      'error',
      Object.assign(error, {
        queue: this.name,
        consecutiveErrors: this.consecutiveErrors,
        context: 'pull',
      })
    );

    const backoffMs = Math.min(
      WORKER_CONSTANTS.BASE_BACKOFF_MS * Math.pow(2, this.consecutiveErrors - 1),
      WORKER_CONSTANTS.MAX_BACKOFF_MS
    );
    this.pollTimer = setTimeout(() => {
      this.poll();
    }, backoffMs);
  }

  // ============ Private Helpers ============

  /** Register this worker with the server (TCP only, fire-and-forget) */
  private registerWithServer(): void {
    if (!this.tcp || this.registered) return;
    void this.tcp
      .send({
        cmd: 'RegisterWorker',
        name: this.queueKey,
        queues: [this.queueKey],
        concurrency: this.opts.concurrency,
        workerId: this.workerId,
        hostname: hostname(),
        pid: process.pid,
        startedAt: this.startedAt,
      })
      .then(() => {
        this.registered = true;
      })
      .catch((err: unknown) => {
        const error = err instanceof Error ? err : new Error(String(err));
        this.emit('error', Object.assign(error, { context: 'worker-register' }));
      });
  }

  /** Start periodic worker-level heartbeat (separate from job heartbeat) */
  private startWorkerHeartbeat(): void {
    if (this.workerHeartbeatTimer || !this.tcp) return;
    this.workerHeartbeatTimer = setInterval(() => {
      if (!this.tcp || !this.registered) return;
      void this.tcp
        .send({
          cmd: 'Heartbeat',
          id: this.workerId,
          activeJobs: this.activeJobs,
          processed: this.processedCount,
          failed: this.failedCount,
        })
        .catch((err: unknown) => {
          const error = err instanceof Error ? err : new Error(String(err));
          this.emit('error', Object.assign(error, { context: 'worker-heartbeat' }));
        });
    }, this.opts.heartbeatInterval);
  }

  private getHeartbeatDeps(): HeartbeatDeps {
    return {
      pulledJobIds: this.pulledJobIds,
      jobTokens: this.jobTokens,
      tcp: this.tcp,
      useLocks: this.opts.useLocks,
      emitter: this,
    };
  }

  private getPullConfig(): PullConfig {
    return {
      name: this.queueKey,
      workerId: this.workerId,
      useLocks: this.opts.useLocks,
      pollTimeout: this.opts.pollTimeout,
    };
  }

  /** Apply worker-level removeOnComplete/removeOnFail defaults to a job */
  private applyRemoveDefaults(job: InternalJob): void {
    if (this.opts.removeOnComplete !== undefined && !job.removeOnComplete) {
      const val = this.opts.removeOnComplete;
      (job as { removeOnComplete: boolean }).removeOnComplete = val === true;
    }
    if (this.opts.removeOnFail !== undefined && !job.removeOnFail) {
      const val = this.opts.removeOnFail;
      (job as { removeOnFail: boolean }).removeOnFail = val === true;
    }
  }
}
