import { hostname } from 'os';
import type { Job as InternalJob } from '../../../domain/types/job';
import { getSharedManager } from '../../manager';
import { processJob } from '../processor';
import type { HeartbeatDeps } from '../workerHeartbeat';
import type { PullConfig } from '../workerPull';
import { WorkerPolling } from './polling';

export class WorkerExecution<T = unknown, R = unknown> extends WorkerPolling<T, R> {
  protected startJob(job: InternalJob, token: string | null): void {
    const jobIdStr = String(job.id);
    if (this.activeJobIds.has(jobIdStr)) return;

    this.activeJobs++;
    this.activeJobIds.add(jobIdStr);
    this.applyRemoveDefaults(job);
    if (this.groupLimiter) this.groupLimiter.increment(job);
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
      if (this.opts.useLocks) this.jobTokens.delete(jobIdStr);
      if (this.groupLimiter) this.groupLimiter.decrement(job);
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
    };
  }

  protected applyRemoveDefaults(job: InternalJob): void {
    if (this.opts.removeOnComplete !== undefined && !job.removeOnComplete) {
      const value = this.opts.removeOnComplete;
      (job as { removeOnComplete: boolean }).removeOnComplete = value === true;
    }
    if (this.opts.removeOnFail !== undefined && !job.removeOnFail) {
      const value = this.opts.removeOnFail;
      (job as { removeOnFail: boolean }).removeOnFail = value === true;
    }
  }
}
