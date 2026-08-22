import { hostname } from 'os';
import { jobId } from '../../../domain/types/job';
import { EventType } from '../../../domain/types/queue';
import { getSharedManager } from '../../manager';
import { TcpEventSubscription } from '../../queue-events/tcpSubscription';
import { startHeartbeat } from '../workerHeartbeat';
import { WorkerState } from './state';

export abstract class WorkerControl<T = unknown, R = unknown> extends WorkerState<T, R> {
  run(): void {
    if (this.running || this.closed || this._closing || this._closingPromise !== null) return;
    this.running = true;
    this.paused = false;
    queueMicrotask(() => {
      if (!this.closed) this.emit('ready');
    });

    if (!this.stalledUnsubscribe && !this.opts.skipStalledCheck) {
      this.subscribeToStalledEvents();
    }

    if (this.embedded && !this.registered) {
      getSharedManager().registerWorker(this.queueKey, [this.queueKey], this.opts.concurrency, {
        workerId: this.workerId,
        hostname: hostname(),
        pid: process.pid,
        startedAt: this.startedAt,
      });
      this.registered = true;
    } else if (this.tcp && !this.registered) {
      this.registerWithServer();
    }

    if (this.opts.heartbeatInterval > 0 && !this.opts.skipLockRenewal && !this.heartbeatTimer) {
      if (this.embedded) {
        this.heartbeatTimer = setInterval(() => {
          const manager = getSharedManager();
          for (const id of this.pulledJobIds) {
            const token = this.opts.useLocks ? this.jobTokens.get(id) : undefined;
            manager.jobHeartbeat(jobId(id), token);
          }
        }, this.opts.heartbeatInterval);
      } else {
        this.heartbeatTimer = startHeartbeat(this.getHeartbeatDeps(), this.opts.heartbeatInterval);
      }
    }
    if (this.opts.heartbeatInterval > 0) this.startWorkerHeartbeat();
    if (this.stalledSubscription) {
      void this.stalledSubscription.waitUntilReady().then(
        () => this.poll(),
        () => this.poll()
      );
    } else {
      this.poll();
    }
  }

  protected subscribeToStalledEvents(): void {
    const handleEvent = (event: { queue: string; eventType: EventType; jobId: string }) => {
      if (event.queue !== this.queueKey) return;
      if (event.eventType === EventType.Stalled) {
        this.emit('stalled', event.jobId, 'active');
      }
    };
    if (this.embedded) {
      this.stalledUnsubscribe = getSharedManager().subscribe(handleEvent);
      return;
    }

    const subscription = new TcpEventSubscription({
      connection: this.opts.connection,
      queue: this.queueKey,
      onEvent: handleEvent,
      onError: (error) => {
        if (this.listenerCount('error') > 0) this.emit('error', error);
      },
    });
    this.stalledSubscription = subscription;
    this.stalledUnsubscribe = () => {
      subscription.close();
      if (this.stalledSubscription === subscription) this.stalledSubscription = null;
    };
  }

  pause(): void {
    if (!this.running) return;
    this.running = false;
    this.paused = true;
    this.clearPollTimer();
  }

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

  get concurrency(): number {
    return this.opts.concurrency;
  }

  set concurrency(value: number) {
    const clamped = Math.max(1, value);
    const previous = this.opts.concurrency;
    (this.opts as { concurrency: number }).concurrency = clamped;
    if (clamped > previous && this.running && !this._closing) this.poll();
  }

  get closing(): Promise<void> | null {
    return this._closingPromise;
  }

  async waitUntilReady(): Promise<void> {
    if (this.embedded) return;
    if (this.tcpPool) await this.tcpPool.send({ cmd: 'Ping' });
    if (this.stalledSubscription) await this.stalledSubscription.waitUntilReady();
  }

  cancelJob(jobId: string, reason?: string): boolean {
    if (this.hasActiveDelivery(jobId)) {
      this.cancelledJobs.add(jobId);
      this.emit('cancelled', { jobId, reason: reason ?? 'Job cancelled by worker' });
      return true;
    }
    return false;
  }

  cancelAllJobs(reason?: string): void {
    for (const jobId of this.activeDeliveryIds()) {
      this.cancelledJobs.add(jobId);
      this.emit('cancelled', { jobId, reason: reason ?? 'All jobs cancelled' });
    }
  }

  isJobCancelled(jobId: string): boolean {
    return this.cancelledJobs.has(jobId);
  }

  getRateLimiterInfo() {
    return this.rateLimiter.getRateLimiterInfo();
  }

  rateLimit(expireTimeMs: number): void {
    this.rateLimiter.rateLimit(expireTimeMs);
  }

  isRateLimited(): boolean {
    return this.rateLimiter.isRateLimited();
  }

  async startStalledCheckTimer(): Promise<void> {
    // No-op for API compatibility; stall detection is automatic.
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
}
