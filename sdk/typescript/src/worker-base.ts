/**
 * WorkerBase: lifecycle state, cooperative cancellation and safe command
 * dispatch shared by the Worker pull loop (worker.ts).
 */

import { randomBytes } from 'node:crypto';
import { EventEmitter } from 'node:events';
import { hostname } from 'node:os';
import { Connection } from './connection.js';
import { MAX_POLL_TIMEOUT_MS, sleep, type WorkerOptions } from './worker-types.js';

export class WorkerBase extends EventEmitter {
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
  protected loopPromise: Promise<void> | null = null;
  protected heartbeatTimer: ReturnType<typeof setInterval> | null = null;

  constructor(queue: string, opts: WorkerOptions = {}) {
    super();
    if ((opts.concurrency ?? 4) < 1) throw new Error('concurrency must be >= 1');
    this.queue = queue;
    this.concurrency = opts.concurrency ?? 4;
    this.batchSize = Math.max(1, opts.batchSize ?? 10);
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

  /** Graceful shutdown: stop pulling, wait for in-flight jobs. */
  async close(): Promise<void> {
    if (this.closedFlag) return;
    this.stopped = true;
    if (this.loopPromise) await this.loopPromise;
    while (this.active.size > 0) await sleep(20);
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
    await this.safeCall({ cmd: 'UnregisterWorker', workerId: this.workerId });
    this.connection.close();
    this.closedFlag = true;
    this.running = false;
    this.emit('closed');
  }

  protected async safeCall(command: Record<string, unknown> & { cmd: string }): Promise<void> {
    try {
      await this.connection.call(command);
    } catch (err) {
      this.emit('error', err);
    }
  }
}
