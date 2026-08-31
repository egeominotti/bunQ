import { hostname } from 'os';
import { getSharedManager } from '../../manager';
import { processJob } from '../processor';
import type { HeartbeatDeps } from '../workerHeartbeat';
import type { PullConfig } from '../workerPull';
import { WorkerPolling } from './polling';
import type { WorkerDelivery } from './state';

export class WorkerExecution<T = unknown, R = unknown> extends WorkerPolling<T, R> {
  protected startJob(delivery: WorkerDelivery): boolean {
    const { job, token } = delivery;
    if (this.isActiveDelivery(delivery)) return true;
    if (this.groupLimiter && !this.groupLimiter.canProcess(job)) return false;
    if (!this.rateLimiter.tryAcquire()) return false;

    this.activeJobs++;
    this.beginDelivery(delivery);
    if (this.groupLimiter) this.groupLimiter.increment(job);

    const tokenForProcess = this.opts.useLocks ? token : undefined;
    void processJob(job, {
      name: this.queueKey,
      processor: this.processor,
      embedded: this.embedded,
      tcp: this.tcp,
      ackBatcher: this.ackBatcher,
      emitter: this,
      token: tokenForProcess,
      removeOnComplete: this.opts.removeOnComplete === true ? true : undefined,
      removeOnFail: this.opts.removeOnFail === true ? true : undefined,
      shouldAbandonOutcome: () => this._forceClose || !this.isCurrentDelivery(delivery),
      onOutcome: (ok) => {
        if (ok) this.processedCount++;
        else this.failedCount++;
      },
    }).finally(() => {
      this.activeJobs--;
      this.finishDelivery(delivery);
      if (this.groupLimiter) this.groupLimiter.decrement(job);
      if (this.running && !this._closing) this.scheduleProcessing();
    });

    if (this.activeJobs < this.opts.concurrency && !this._closing) this.scheduleProcessing();
    return true;
  }

  protected registerWithServer(): void {
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
      .catch((errorValue: unknown) => {
        const error = errorValue instanceof Error ? errorValue : new Error(String(errorValue));
        this.emit('error', Object.assign(error, { context: 'worker-register' }));
      });
  }

  protected startWorkerHeartbeat(): void {
    if (this.workerHeartbeatTimer) return;
    this.workerHeartbeatTimer = setInterval(() => {
      if (!this.registered) return;
      if (this.embedded) {
        getSharedManager().workerManager.heartbeat(this.workerId, {
          activeJobs: this.activeJobs,
          processed: this.processedCount,
          failed: this.failedCount,
        });
        return;
      }
      if (!this.tcp) return;
      void this.tcp
        .send({
          cmd: 'Heartbeat',
          id: this.workerId,
          activeJobs: this.activeJobs,
          processed: this.processedCount,
          failed: this.failedCount,
        })
        .catch((errorValue: unknown) => {
          const error = errorValue instanceof Error ? errorValue : new Error(String(errorValue));
          this.emit('error', Object.assign(error, { context: 'worker-heartbeat' }));
        });
    }, this.opts.heartbeatInterval);
  }

  protected getHeartbeatDeps(): HeartbeatDeps {
    return {
      pulledJobIds: this.pulledJobIds,
      jobTokens: this.jobTokens,
      tcp: this.tcp,
      useLocks: this.opts.useLocks,
      emitter: this,
    };
  }

  protected getPullConfig(): PullConfig {
    return {
      name: this.queueKey,
      workerId: this.workerId,
      useLocks: this.opts.useLocks,
      pollTimeout: this.opts.pollTimeout,
      lockDuration: this.opts.lockDuration,
      group: this.opts.group,
    };
  }
}
