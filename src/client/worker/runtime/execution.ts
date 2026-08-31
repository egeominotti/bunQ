import { hostname } from 'os';
import { getSharedManager } from '../../manager';
import { BatchExecution } from '../batchExecution';
import { processJob } from '../processor';
import type { Processor } from '../../types';
import type { HeartbeatDeps } from '../workerHeartbeat';
import type { PullConfig } from '../workerPull';
import { WorkerPolling } from './polling';
import type { WorkerDelivery } from './state';

export class WorkerExecution<T = unknown, R = unknown> extends WorkerPolling<T, R> {
  private batchWaitDeadline: number | null = null;
  private batchFillPending = false;

  protected startJob(delivery: WorkerDelivery): boolean {
    const { job } = delivery;
    if (this.isActiveDelivery(delivery)) return true;
    if (this.groupLimiter && !this.groupLimiter.canProcess(job)) return false;

    if (this.opts.batch) return this.startBatch(delivery);
    if (!this.rateLimiter.tryAcquire()) return false;
    this.startDeliveries([delivery], this.processor);
    return true;
  }

  private startBatch(first: WorkerDelivery): boolean {
    const options = this.opts.batch;
    if (!options) return false;
    const deliveries = [first];
    const deferred: WorkerDelivery[] = [];
    const groupId = first.job.groupId;
    if (options.groupAffinity && this.batchAffinity === undefined) this.batchAffinity = groupId;
    while (deliveries.length < options.size) {
      const candidate = this.getBufferedJob();
      if (!candidate) break;
      if (options.groupAffinity && candidate.job.groupId !== groupId) deferred.push(candidate);
      else deliveries.push(candidate);
    }
    for (let index = deferred.length - 1; index >= 0; index--) this.requeueItem(deferred[index]);

    const now = Date.now();
    const ignoresMinimum = !options.groupAffinity && deliveries.some(({ job }) => job.groupId);
    if (deliveries.length < options.minSize && !ignoresMinimum) {
      if (options.timeout > 0) this.batchWaitDeadline ??= now + options.timeout;
      if (this.batchWaitDeadline === null || now < this.batchWaitDeadline) {
        for (let index = deliveries.length - 1; index >= 0; index--) {
          this.requeueItem(deliveries[index]);
        }
        const retryDelay =
          this.batchWaitDeadline === null ? 10 : Math.min(10, this.batchWaitDeadline - now);
        this.fillPendingBatch(retryDelay);
        return true;
      }
    }
    if (!this.rateLimiter.tryAcquire(deliveries.length)) {
      for (let index = deliveries.length - 1; index >= 1; index--) {
        this.requeueItem(deliveries[index]);
      }
      return false;
    }
    this.batchWaitDeadline = null;
    this.batchAffinity = undefined;
    const batch = new BatchExecution(this.processor, deliveries.length);
    this.startDeliveries(deliveries, batch);
    return true;
  }

  private fillPendingBatch(retryDelay: number): void {
    if (this.batchFillPending) return;
    this.batchFillPending = true;
    void this.doPullBatch()
      .then((items) => {
        if (items.length === 0) return;
        const deliveries = this.registerPulledJobs(items);
        if (this.pendingJobsHead >= this.pendingJobs.length) {
          this.pendingJobs = deliveries;
          this.pendingJobsHead = 0;
        } else {
          this.pendingJobs = this.pendingJobs.slice(this.pendingJobsHead).concat(deliveries);
          this.pendingJobsHead = 0;
        }
      })
      .catch((error) => this.handlePullError(error))
      .finally(() => {
        this.batchFillPending = false;
        setTimeout(() => this.scheduleProcessing(), retryDelay);
      });
  }

  private startDeliveries(
    deliveries: WorkerDelivery[],
    processor: Processor<T, R> | BatchExecution<T, R>
  ): void {
    const abortControllers: Array<{ id: string; controller: AbortController }> = [];
    const timers: Array<ReturnType<typeof setTimeout>> = [];

    this.activeJobs++;
    for (const delivery of deliveries) {
      this.beginDelivery(delivery);
      if (this.groupLimiter) this.groupLimiter.increment(delivery.job);
    }

    void Promise.all(
      deliveries.map((delivery, index) => {
        const jobId = String(delivery.job.id);
        const controller = this.createAbortController(jobId);
        let timedOut = false;
        abortControllers.push({ id: jobId, controller });
        if (delivery.job.timeout !== null) {
          timers.push(
            setTimeout(() => {
              timedOut = true;
              controller.abort(new Error(`Job ${jobId} timed out after ${delivery.job.timeout}ms`));
            }, delivery.job.timeout)
          );
        }
        return processJob(delivery.job, {
          name: this.queueKey,
          processor: processor instanceof BatchExecution ? processor.handler(index) : processor,
          embedded: this.embedded,
          tcp: this.tcp,
          ackBatcher: this.ackBatcher,
          emitter: this,
          token: this.opts.useLocks ? delivery.token : undefined,
          removeOnComplete: this.opts.removeOnComplete === true ? true : undefined,
          removeOnFail: this.opts.removeOnFail === true ? true : undefined,
          shouldAbandonOutcome: () =>
            timedOut || this._forceClose || !this.isCurrentDelivery(delivery),
          abortController: controller,
          onOutcome: (ok) => {
            if (ok) this.processedCount++;
            else this.failedCount++;
          },
        });
      })
    ).finally(() => {
      for (const timer of timers) clearTimeout(timer);
      for (const { id, controller } of abortControllers) {
        this.releaseAbortController(id, controller);
      }
      this.activeJobs--;
      for (const delivery of deliveries) {
        this.finishDelivery(delivery);
        if (this.groupLimiter) this.groupLimiter.decrement(delivery.job);
      }
      if (this.running && !this._closing) this.scheduleProcessing();
    });

    if (this.activeJobs < this.opts.concurrency && !this._closing) this.scheduleProcessing();
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
    const group =
      this.opts.batch?.groupAffinity && this.batchAffinity !== undefined
        ? { ...this.opts.group, affinity: this.batchAffinity }
        : this.opts.group;
    return {
      name: this.queueKey,
      workerId: this.workerId,
      useLocks: this.opts.useLocks,
      pollTimeout: this.opts.pollTimeout,
      lockDuration: this.opts.lockDuration,
      group,
    };
  }
}
