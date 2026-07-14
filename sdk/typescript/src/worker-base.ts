/**
 * WorkerBase: lifecycle state, cooperative cancellation and safe command
 * dispatch shared by the Worker pull loop (worker.ts).
 */

import { randomBytes } from 'node:crypto';
import { EventEmitter } from 'node:events';
import { hostname } from 'node:os';
import { Connection } from './connection.js';
import {
  MAX_POLL_TIMEOUT_MS,
  sleep,
  type WorkerEventMap,
  type WorkerOptions,
} from './worker-types.js';

export class WorkerBase<T = unknown, R = unknown> extends EventEmitter {
  readonly queue: string;
  readonly concurrency: number;
  readonly batchSize: number;
  readonly pollTimeoutMs: number;
  readonly lockTtlMs: number;
  readonly heartbeatIntervalS: number;
  readonly workerId: string;
  readonly name: string;
  readonly connection: Connection;

  protected readonly active = new Map<string, string>(); // job id -> lock token
  protected readonly cancelledJobs = new Set<string>();
  protected stopped = false;
  protected paused = false;
  protected closedFlag = false;
  protected running = false;
  protected wasBusy = false;
  protected processed = 0;
  protected failedCount = 0;
  protected readyPromise: Promise<void>;
  protected readyResolve: (() => void) | null = null;
  protected readyFired = false;
  protected loopPromise: Promise<void> | null = null;
  protected heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  protected registeredGeneration = -1;

  constructor(queue: string, opts: WorkerOptions = {}) {
    super();
    if ((opts.concurrency ?? 4) < 1) throw new Error('concurrency must be >= 1');
    this.queue = queue;
    this.concurrency = opts.concurrency ?? 4;
    // The server rejects PULLB count > 1000 (handlers/core.ts) — an unclamped
    // batchSize would wedge the pull loop in a permanent error cycle. The
    // finite-guard also catches NaN, which would otherwise pass both bounds.
    const rawBatch = opts.batchSize ?? 10;
    this.batchSize = Number.isFinite(rawBatch) ? Math.min(Math.max(1, rawBatch), 1000) : 10;
    this.pollTimeoutMs = Math.min(opts.pollTimeoutMs ?? 5000, MAX_POLL_TIMEOUT_MS);
    this.lockTtlMs = opts.lockTtlMs ?? 30_000;
    this.heartbeatIntervalS = opts.heartbeatIntervalS ?? 10;
    this.workerId = `ts-${hostname()}-${process.pid}-${randomBytes(4).toString('hex')}`;
    this.name = opts.name ?? this.workerId;
    this.connection = new Connection({
      host: opts.host,
      port: opts.port,
      token: opts.token,
      tls: opts.tls,
      logger: opts.logger,
      onTelemetry: opts.onTelemetry,
    });
    this.readyPromise = new Promise((resolve) => {
      this.readyResolve = resolve;
    });
  }

  pause(): void {
    this.paused = true;
  }

  resume(): void {
    this.paused = false;
  }

  isRunning(): boolean {
    return this.running && !this.closedFlag;
  }

  isPaused(): boolean {
    return this.paused;
  }

  isClosed(): boolean {
    return this.closedFlag;
  }

  async waitUntilReady(): Promise<void> {
    await this.readyPromise;
  }

  /**
   * 'ready' is replayed to listeners attached after it fired: with autorun the
   * loop starts inside the constructor, so a plain once-only event could be
   * missed by `new Worker(...).on('ready', ...)` patterns.
   *
   * The overloads give the known worker events typed parameters (see
   * WorkerEventMap); unknown event names keep the generic signature.
   */
  override on<E extends keyof WorkerEventMap<T, R>>(
    event: E,
    listener: WorkerEventMap<T, R>[E]
  ): this;
  override on(event: string | symbol, listener: (...args: unknown[]) => void): this;
  override on(event: string | symbol, listener: (...args: never[]) => void): this {
    if (event === 'ready' && this.readyFired) (listener as () => void)();
    return super.on(event, listener as (...args: unknown[]) => void);
  }

  override once<E extends keyof WorkerEventMap<T, R>>(
    event: E,
    listener: WorkerEventMap<T, R>[E]
  ): this;
  override once(event: string | symbol, listener: (...args: unknown[]) => void): this;
  override once(event: string | symbol, listener: (...args: never[]) => void): this {
    if (event === 'ready' && this.readyFired) {
      (listener as () => void)();
      return this;
    }
    return super.once(event, listener as (...args: unknown[]) => void);
  }

  override off<E extends keyof WorkerEventMap<T, R>>(
    event: E,
    listener: WorkerEventMap<T, R>[E]
  ): this;
  override off(event: string | symbol, listener: (...args: unknown[]) => void): this;
  override off(event: string | symbol, listener: (...args: never[]) => void): this {
    return super.off(event, listener as (...args: unknown[]) => void);
  }

  /**
   * Cooperative cancel of a locally active job (mirrors the official client):
   * marks the job and emits 'cancelled'; the processor is expected to check
   * isJobCancelled() and abort. Returns false if the job is not active here.
   */
  cancelJob(jobId: string, reason?: string): boolean {
    if (this.active.has(jobId)) {
      this.cancelledJobs.add(jobId);
      this.emit('cancelled', { jobId, reason: reason ?? 'Job cancelled by worker' });
      return true;
    }
    return false;
  }

  cancelAllJobs(reason?: string): void {
    for (const jobId of this.active.keys()) {
      this.cancelledJobs.add(jobId);
      this.emit('cancelled', { jobId, reason: reason ?? 'All jobs cancelled' });
    }
  }

  isJobCancelled(jobId: string): boolean {
    return this.cancelledJobs.has(jobId);
  }

  /** Graceful shutdown: stop pulling, wait for in-flight jobs.
   * With `force` the wait for in-flight jobs is skipped (parity with the
   * official client's `close(force)`). */
  async close(force = false): Promise<void> {
    if (this.closedFlag) return;
    this.stopped = true;
    if (this.loopPromise) await this.loopPromise;
    await this.beforeClose(); // flush any batched ACKs before draining
    while (!force && this.active.size > 0) await sleep(20);
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    // Only unregister when the loop actually registered (autorun: false and
    // never run() means the server does not know this worker).
    if (this.registeredGeneration >= 0) {
      await this.safeCall({ cmd: 'UnregisterWorker', workerId: this.workerId });
    }
    this.connection.close();
    this.closedFlag = true;
    this.running = false;
    this.emit('closed');
  }

  /** Dispatch a command, routing failures to 'error'. Returns whether the
   * command reached the server — callers gate success-only side effects
   * ('completed'/'failed' emits, counters) on it. */
  protected async safeCall(command: Record<string, unknown> & { cmd: string }): Promise<boolean> {
    try {
      await this.connection.call(command);
      return true;
    } catch (err) {
      this.emit('error', err instanceof Error ? err : new Error(String(err)));
      return false;
    }
  }

  /** Hook run during close() before draining in-flight jobs (see Worker). */
  protected async beforeClose(): Promise<void> {}
}
