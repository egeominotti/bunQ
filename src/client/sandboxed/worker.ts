/**
 * Sandboxed Worker
 * Runs job processors in isolated Bun Worker processes
 */

import { EventEmitter } from 'events';
import { getSharedManager } from '../manager';
import { getSharedPool, releaseSharedPool, type TcpConnectionPool } from '../tcpPool';
import { createPublicJob } from '../jobConversion';
import type { Job } from '../types';
import type { Job as DomainJob } from '../../domain/types/job';
import { jobId } from '../../domain/types/job';
import { buildFailCommand, failEmbeddedArgs } from '../queue/failWire';
import {
  createProgressHandler,
  createLogHandler,
  createGetStateHandler,
  createGetChildrenValuesHandler,
  createGetFailedChildrenValuesHandler,
  createGetIgnoredChildrenFailuresHandler,
  createRemoveChildDependencyHandler,
  createRemoveUnprocessedChildrenHandler,
  createRemoveHandler,
  createRetryHandler,
  createUpdateDataHandler,
  createPromoteHandler,
  createChangeDelayHandler,
  createChangePriorityHandler,
  createExtendLockHandler,
  createClearLogsHandler,
  createMoveToWaitHandler,
  createMoveToDelayedHandler,
  createMoveToWaitingChildrenHandler,
  createWaitUntilFinishedHandler,
  createDiscardHandler,
  createGetDependenciesHandler,
  createGetDependenciesCountHandler,
  createRemoveDeduplicationKeyHandler,
} from '../worker/processorHandlers';
import type {
  SandboxedWorkerOptions,
  RequiredSandboxedWorkerOptions,
  WorkerProcess,
  IPCRequest,
  IPCResponse,
} from './types';
import { createWrapperScript, cleanupWrapperScript } from './wrapper';
import { type QueueOps, createEmbeddedOps, createTcpOps } from './queueOps';

const LOG_PREFIX = '[SandboxedWorker]';

/** Structured log helper */
function log(
  level: 'info' | 'warn' | 'error',
  message: string,
  data?: Record<string, unknown>
): void {
  const entry = data ? { message, ...data } : message;
  switch (level) {
    case 'info':
      console.log(LOG_PREFIX, entry);
      break;
    case 'warn':
      console.warn(LOG_PREFIX, entry);
      break;
    case 'error':
      console.error(LOG_PREFIX, entry);
      break;
  }
}

/**
 * Sandboxed Worker - runs processors in isolated Bun Worker processes
 * @template T - Job data type for typed events
 */
export class SandboxedWorker<T = unknown> extends EventEmitter {
  // ============ Typed Event Overloads ============

  on(event: 'ready' | 'closed', listener: () => void): this;
  on(event: 'active', listener: (job: Job<T>) => void): this;
  on(event: 'completed', listener: (job: Job<T>, result: unknown) => void): this;
  on(event: 'failed', listener: (job: Job<T>, error: Error) => void): this;
  on(event: 'progress', listener: (job: Job<T>, progress: number) => void): this;
  on(event: 'log', listener: (job: Job<T>, message: string) => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  on(event: string, listener: (...args: any[]) => void): this {
    return super.on(event, listener);
  }

  once(event: 'ready' | 'closed', listener: () => void): this;
  once(event: 'active', listener: (job: Job<T>) => void): this;
  once(event: 'completed', listener: (job: Job<T>, result: unknown) => void): this;
  once(event: 'failed', listener: (job: Job<T>, error: Error) => void): this;
  once(event: 'progress', listener: (job: Job<T>, progress: number) => void): this;
  once(event: 'log', listener: (job: Job<T>, message: string) => void): this;
  once(event: 'error', listener: (error: Error) => void): this;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  once(event: string, listener: (...args: any[]) => void): this {
    return super.once(event, listener);
  }

  private readonly queueName: string;
  private readonly options: RequiredSandboxedWorkerOptions;
  private readonly workers: WorkerProcess[] = [];
  private running = false;
  private pullPromise: Promise<void> | null = null;
  private wrapperPath: string | null = null;
  private readonly ops: QueueOps;
  private readonly tcp: TcpConnectionPool | null;
  private readonly workerId: string;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private readonly heartbeatInterval: number;
  private readonly idleTimeout: number;
  private readonly idleRecycleMs: number;
  private readonly autoStart: boolean;
  private readonly autoStartPollMs: number;
  private lastActivityTime: number = 0;
  private autoStartTimer: ReturnType<typeof setInterval> | null = null;

  constructor(queueName: string, options: SandboxedWorkerOptions) {
    super();
    this.queueName = queueName;
    this.workerId = `sandboxed-worker-${queueName}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;

    if (options.connection) {
      this.tcp = getSharedPool(options.connection);
      this.ops = createTcpOps(this.tcp);
      this.heartbeatInterval = options.heartbeatInterval ?? 10000;
    } else {
      this.tcp = null;
      this.ops = createEmbeddedOps(options.manager ?? getSharedManager());
      this.heartbeatInterval = options.heartbeatInterval ?? 5000;
    }

    this.idleTimeout = options.idleTimeout ?? 0;
    this.idleRecycleMs = options.idleRecycleMs ?? 30000;
    this.autoStart = options.autoStart ?? false;
    this.autoStartPollMs = options.autoStartPollMs ?? 5000;

    this.options = {
      processor: options.processor,
      concurrency: options.concurrency ?? 1,
      maxMemory: options.maxMemory ?? 256,
      timeout: options.timeout ?? 30000,
      autoRestart: options.autoRestart ?? true,
      maxRestarts: options.maxRestarts ?? 10,
      pollInterval: options.pollInterval ?? 10,
    };
  }

  /** Start the sandboxed worker pool */
  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;
    this.lastActivityTime = Date.now();

    if (this.tcp) await this.tcp.connect();

    this.wrapperPath = await createWrapperScript(this.queueName, this.options.processor);

    // Spawn the first worker synchronously so Bun's bundler resolves and caches
    // the wrapper entry point before concurrent siblings race for it. Remaining
    // workers can then spawn in parallel without a ModuleNotFound flake.
    await this.spawnWorker(0);
    if (this.options.concurrency > 1) {
      const spawnPromises: Promise<void>[] = [];
      for (let i = 1; i < this.options.concurrency; i++) {
        spawnPromises.push(this.spawnWorker(i));
      }
      await Promise.all(spawnPromises);
    }

    this.startHeartbeat();
    this.emit('ready');
    this.pullPromise = this.pullLoop();
  }

  /** Stop all workers gracefully */
  async stop(force = false): Promise<void> {
    this.running = false;

    if (this.autoStartTimer) {
      clearInterval(this.autoStartTimer);
      this.autoStartTimer = null;
    }

    // Wait for pull loop to exit (it checks this.running)
    if (this.pullPromise) await this.pullPromise;

    // Unless force, wait for all busy workers to finish their current jobs
    if (!force) {
      while (this.workers.some((w) => w.busy && !w.terminated)) {
        await Bun.sleep(50);
      }
    }

    // Now safe to stop heartbeat and terminate workers
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }

    for (const wp of this.workers) {
      if (wp.timeoutId) clearTimeout(wp.timeoutId);
      if (!wp.terminated) wp.worker.terminate();
    }
    this.workers.length = 0;

    await cleanupWrapperScript(this.wrapperPath);
    if (this.tcp) releaseSharedPool(this.tcp);
    this.emit('closed');
  }

  /**
   * Stop workers but keep polling for new jobs.
   * When a job is found, automatically restart the worker pool.
   */
  private async stopAndWatch(): Promise<void> {
    await this.stop();

    this.autoStartTimer = setInterval(() => {
      void this.checkAndRestart();
    }, this.autoStartPollMs);
  }

  /** Check for waiting jobs and restart if found */
  private async checkAndRestart(): Promise<void> {
    try {
      const count = await this.ops.countWaiting(this.queueName);
      if (count > 0) {
        if (this.autoStartTimer) {
          clearInterval(this.autoStartTimer);
          this.autoStartTimer = null;
        }
        await this.start();
      }
    } catch {
      // Ignore poll errors during watch
    }
  }

  /** Check if the worker is currently running */
  isRunning(): boolean {
    return this.running;
  }

  /** Get worker pool stats */
  getStats(): { total: number; busy: number; idle: number; recycled: number; restarts: number } {
    const busy = this.workers.filter((w) => w.busy && !w.terminated).length;
    const recycled = this.workers.filter((w) => w.terminated).length;
    const alive = this.workers.length - recycled;
    const restarts = this.workers.reduce((sum, w) => sum + w.restarts, 0);
    return { total: this.workers.length, busy, idle: alive - busy, recycled, restarts };
  }

  /** Reset worker state to idle */
  private resetWorkerState(wp: WorkerProcess): void {
    if (wp.timeoutId) {
      clearTimeout(wp.timeoutId);
      wp.timeoutId = null;
    }
    wp.busy = false;
    wp.currentJob = null;
    wp.currentToken = null;
    wp.lastIdleAt = Date.now();
  }

  private spawnWorker(index: number): Promise<void> {
    return new Promise((resolve, reject) => {
      if (!this.wrapperPath) {
        resolve();
        return;
      }

      const worker = new Worker(this.wrapperPath, { smol: this.options.maxMemory <= 64 });
      const wp: WorkerProcess = {
        worker,
        busy: false,
        currentJob: null,
        currentToken: null,
        restarts: this.workers[index]?.restarts ?? 0,
        timeoutId: null,
        lastIdleAt: Date.now(),
        terminated: false,
      };

      let resolved = false;
      const readyTimeout = setTimeout(() => {
        if (!resolved) {
          resolved = true;
          resolve();
        }
      }, 5000);

      worker.onmessage = (event: MessageEvent<IPCResponse>) => {
        if (event.data.type === 'ready' && !resolved) {
          resolved = true;
          clearTimeout(readyTimeout);
          resolve();
          return;
        }
        this.handleMessage(wp, event.data);
      };

      worker.onerror = (error) => {
        log('error', 'Worker error', { workerIndex: index, error: error.message });
        if (!resolved) {
          resolved = true;
          clearTimeout(readyTimeout);
          reject(new Error(error.message));
        }
        this.handleCrash(wp, index);
      };

      if (this.workers[index]) this.workers[index] = wp;
      else this.workers.push(wp);
    });
  }

  private async pullLoop(): Promise<void> {
    while (this.running) {
      // Find an alive idle worker, or respawn a recycled one
      let idle = this.workers.find((w) => !w.busy && !w.terminated);
      if (!idle) {
        const recycled = this.workers.find((w) => w.terminated);
        if (recycled) {
          const index = this.workers.indexOf(recycled);
          await this.spawnWorker(index);
          idle = this.workers[index];
        } else {
          await Bun.sleep(this.options.pollInterval);
          continue;
        }
      }

      const { job, token } = await this.ops.pull(this.queueName, this.workerId, 1000);
      if (job) {
        // If the idle worker was recycled between find and dispatch, respawn
        if (idle.terminated) {
          const index = this.workers.indexOf(idle);
          await this.spawnWorker(index);
          idle = this.workers[index];
        }
        this.dispatch(idle, job, token);
      } else {
        this.recycleIdleWorkers();
        if (this.idleTimeout > 0 && Date.now() - this.lastActivityTime >= this.idleTimeout) {
          if (this.autoStart) {
            this.stopAndWatch().catch((err: unknown) => {
              log('error', 'Idle stop-and-watch failed', {
                queue: this.queueName,
                error: err instanceof Error ? err.message : String(err),
              });
            });
          } else {
            this.stop().catch((err: unknown) => {
              log('error', 'Idle timeout stop failed', {
                queue: this.queueName,
                error: err instanceof Error ? err.message : String(err),
              });
            });
          }
          return;
        }
      }
    }
  }

  /** Terminate individual idle workers that exceeded idleRecycleMs, keeping at least 1 alive */
  private recycleIdleWorkers(): void {
    if (this.idleRecycleMs <= 0) return;
    const now = Date.now();
    let aliveIdleCount = 0;

    for (const wp of this.workers) {
      if (!wp.busy && !wp.terminated) aliveIdleCount++;
    }

    for (const wp of this.workers) {
      if (wp.busy || wp.terminated) continue;
      if (aliveIdleCount <= 1) break;
      if (wp.lastIdleAt > 0 && now - wp.lastIdleAt >= this.idleRecycleMs) {
        wp.worker.terminate();
        wp.terminated = true;
        aliveIdleCount--;
      }
    }
  }

  private dispatch(wp: WorkerProcess, job: DomainJob, token: string | null): void {
    this.lastActivityTime = Date.now();
    wp.busy = true;
    wp.currentJob = job;
    wp.currentToken = token;

    // timeout: 0 disables the timeout (for long-running jobs)
    if (this.options.timeout > 0) {
      wp.timeoutId = setTimeout(() => {
        this.handleTimeout(wp, job);
      }, this.options.timeout);
    }

    this.emit('active', this.createEventJob(job));

    const jobData = job.data as Record<string, unknown> | null;
    const parentId = jobData?.__flowParentId as string | undefined;
    const request: IPCRequest = {
      type: 'job',
      job: {
        id: String(job.id),
        data: job.data,
        queue: job.queue,
        attempts: job.attempts,
        ...(parentId ? { parentId } : {}),
      },
    };
    try {
      wp.worker.postMessage(request);
    } catch (err) {
      const errMsg = err instanceof Error ? err.message : String(err);
      log('error', 'Failed to dispatch job', { jobId: String(job.id), error: errMsg });
      this.safeEmitError(
        Object.assign(new Error(`Dispatch failed: ${errMsg}`), {
          jobId: String(job.id),
          context: 'dispatch' as const,
        })
      );
      this.resetWorkerState(wp);
      this.ops
        .fail(job.id, 'Dispatch failed: worker terminated', token ?? undefined)
        .catch((e: unknown) => {
          log('error', 'Failed to mark dispatched job as failed', {
            jobId: String(job.id),
            error: e instanceof Error ? e.message : String(e),
          });
        });
    }
  }

  private handleMessage(wp: WorkerProcess, msg: IPCResponse): void {
    if (msg.type === 'ready') return;
    if (!wp.currentJob || msg.jobId !== String(wp.currentJob.id)) return;

    switch (msg.type) {
      case 'result':
        this.complete(wp, msg.result);
        break;
      case 'error':
        this.fail(wp, msg.error ?? 'Unknown error');
        break;
      case 'fail':
        this.fail(wp, msg.error ?? 'Job explicitly failed');
        break;
      case 'progress':
        if (msg.progress !== undefined) {
          this.ops.updateProgress(wp.currentJob.id, msg.progress).catch((e: unknown) => {
            log('error', 'Failed to update job progress', {
              jobId: String(wp.currentJob?.id),
              error: e instanceof Error ? e.message : String(e),
            });
          });
          this.emit('progress', this.createEventJob(wp.currentJob), msg.progress);
        }
        break;
      case 'log':
        if (msg.message) {
          this.ops.addLog(wp.currentJob.id, msg.message);
          this.emit('log', this.createEventJob(wp.currentJob), msg.message);
        }
        break;
    }
  }

  private complete(wp: WorkerProcess, result: unknown): void {
    this.lastActivityTime = Date.now();
    const job = wp.currentJob;
    if (job) {
      const token = wp.currentToken ?? undefined;
      this.ops.ack(job.id, result, token).catch((e: unknown) => {
        log('error', 'Failed to ack job', {
          jobId: String(job.id),
          error: e instanceof Error ? e.message : String(e),
        });
      });
      const eventJob = this.createEventJob(job);
      (eventJob as { returnvalue?: unknown }).returnvalue = result;
      this.emit('completed', eventJob, result);
    }
    this.resetWorkerState(wp);
  }

  private fail(wp: WorkerProcess, error: string): void {
    const job = wp.currentJob;
    if (job) {
      const token = wp.currentToken ?? undefined;
      this.ops.fail(job.id, error, token).catch((e: unknown) => {
        log('error', 'Failed to mark job as failed', {
          jobId: String(job.id),
          error: e instanceof Error ? e.message : String(e),
        });
      });
      const eventJob = this.createEventJob(job);
      (eventJob as { failedReason?: string }).failedReason = error;
      this.emit('failed', eventJob, new Error(error));
    }
    this.resetWorkerState(wp);
  }

  private handleTimeout(wp: WorkerProcess, job: DomainJob): void {
    wp.worker.terminate();
    const errorMsg = `Job timed out after ${this.options.timeout}ms`;
    const token = wp.currentToken ?? undefined;
    this.ops.fail(job.id, errorMsg, token).catch((e: unknown) => {
      log('error', 'Failed to mark timed-out job as failed', {
        jobId: String(job.id),
        error: e instanceof Error ? e.message : String(e),
      });
    });

    const eventJob = this.createEventJob(job);
    (eventJob as { failedReason?: string }).failedReason = errorMsg;
    this.emit('failed', eventJob, new Error(errorMsg));

    this.resetWorkerState(wp);

    const index = this.workers.indexOf(wp);
    if (index !== -1) this.handleCrash(wp, index);
  }

  private handleCrash(wp: WorkerProcess, index: number): void {
    if (wp.currentJob) {
      const token = wp.currentToken ?? undefined;
      this.ops.fail(wp.currentJob.id, 'Worker crashed', token).catch((e: unknown) => {
        log('error', 'Failed to mark crashed job as failed', {
          jobId: String(wp.currentJob?.id),
          error: e instanceof Error ? e.message : String(e),
        });
      });
    }

    this.safeEmitError(
      Object.assign(new Error('Worker crashed'), {
        workerIndex: index,
        context: 'crash' as const,
      })
    );

    this.resetWorkerState(wp);
    wp.restarts++;

    if (this.options.autoRestart && wp.restarts < this.options.maxRestarts && this.running) {
      this.spawnWorker(index).catch((err: unknown) => {
        log('error', 'Failed to restart worker', {
          workerIndex: index,
          error: err instanceof Error ? err.message : String(err),
        });
      });
    } else if (wp.restarts >= this.options.maxRestarts) {
      log('error', 'Worker exceeded max restarts', {
        workerIndex: index,
        maxRestarts: this.options.maxRestarts,
      });
    }
  }

  /** Start heartbeat timer for TCP lock renewal */
  private startHeartbeat(): void {
    if (this.heartbeatInterval <= 0) return;
    this.heartbeatTimer = setInterval(() => void this.sendHeartbeat(), this.heartbeatInterval);
  }

  /** Send heartbeat for all active jobs */
  private async sendHeartbeat(): Promise<void> {
    const active = this.workers.filter((w) => w.busy && w.currentJob && !w.terminated);
    if (active.length === 0) return;
    try {
      const ids = active.map((w) => String(w.currentJob?.id));
      const tokens = active.map((w) => w.currentToken ?? '');
      await this.ops.sendHeartbeat(ids, tokens);
    } catch (err) {
      this.safeEmitError(
        Object.assign(err instanceof Error ? err : new Error(String(err)), {
          context: 'heartbeat' as const,
        })
      );
    }
  }

  /** Emit 'error' only if there are listeners (avoids uncaught exception) */
  private safeEmitError(error: Error): void {
    if (this.listenerCount('error') > 0) {
      this.emit('error', error);
    }
  }

  /** Create a public Job object from a DomainJob for event payloads */
  private createEventJob(domainJob: DomainJob): Job {
    const data = domainJob.data as { name?: string } | null;
    const embedded = !this.tcp;
    const tcp = this.tcp;
    // Event-context moveToCompleted/moveToFailed: direct ack/fail without manual-move tracking
    const moveToCompleted = async (
      id: string,
      returnValue: unknown,
      _token?: string
    ): Promise<unknown> => {
      if (embedded) {
        await getSharedManager().ack(jobId(id), returnValue);
      } else if (tcp) {
        await tcp.send({ cmd: 'ACK', id, result: returnValue });
      }
      return null;
    };
    const moveToFailed = async (id: string, error: Error, token?: string): Promise<void> => {
      // Forward stacktrace + UnrecoverableError (was dropped; #74/#111-class).
      if (embedded) {
        await getSharedManager().fail(jobId(id), ...failEmbeddedArgs(error, token));
      } else if (tcp) {
        await tcp.send(buildFailCommand(id, error, token));
      }
    };
    return createPublicJob({
      job: domainJob,
      name: data?.name ?? 'default',
      updateProgress: createProgressHandler(embedded, tcp, this, { current: null }),
      log: createLogHandler(embedded, tcp, this, { current: null }),
      getState: createGetStateHandler(embedded, tcp),
      getChildrenValues: createGetChildrenValuesHandler(embedded, tcp),
      getFailedChildrenValues: createGetFailedChildrenValuesHandler(embedded, tcp),
      getIgnoredChildrenFailures: createGetIgnoredChildrenFailuresHandler(embedded, tcp),
      removeChildDependency: createRemoveChildDependencyHandler(embedded, tcp),
      removeUnprocessedChildren: createRemoveUnprocessedChildrenHandler(embedded, tcp),
      remove: createRemoveHandler(embedded, tcp),
      retry: createRetryHandler(embedded, tcp, domainJob),
      updateData: createUpdateDataHandler(embedded, tcp),
      promote: createPromoteHandler(embedded, tcp),
      changeDelay: createChangeDelayHandler(embedded, tcp),
      changePriority: createChangePriorityHandler(embedded, tcp),
      extendLock: createExtendLockHandler(embedded, tcp),
      clearLogs: createClearLogsHandler(embedded, tcp),
      moveToWait: createMoveToWaitHandler(embedded, tcp),
      moveToDelayed: createMoveToDelayedHandler(embedded, tcp),
      moveToWaitingChildren: createMoveToWaitingChildrenHandler(embedded, tcp),
      waitUntilFinished: createWaitUntilFinishedHandler(embedded, tcp),
      discard: createDiscardHandler(embedded, tcp),
      getDependencies: createGetDependenciesHandler(embedded, tcp, domainJob),
      getDependenciesCount: createGetDependenciesCountHandler(embedded, tcp, domainJob),
      removeDeduplicationKey: createRemoveDeduplicationKeyHandler(embedded, tcp),
      moveToCompleted,
      moveToFailed,
    });
  }
}
