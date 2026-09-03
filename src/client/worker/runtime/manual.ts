import type { Job as InternalJob } from '../../../domain/types/job';
import { resolvePublicJobPayload } from '../../jobHelpers';
import { getSharedManager } from '../../manager';
import { parseJobFromResponse } from '../jobParser';
import { processJob } from '../processor';
import type { ManualJob } from '../types';
import { WorkerControl } from './control';

export abstract class WorkerManual<T = unknown, R = unknown> extends WorkerControl<T, R> {
  private registerManualJob(job: InternalJob, token: string | null): ManualJob<T> {
    const payload = resolvePublicJobPayload(job);
    const manualJob: ManualJob<T> = {
      ...job,
      name: payload.name,
      data: payload.data as T,
      ...(token ? { token } : {}),
    };
    this.trackDelivery({ job: manualJob, token });
    return manualJob;
  }

  async getNextJob(token?: string, _opts?: { block?: boolean }): Promise<ManualJob<T> | undefined> {
    if (this.closed) return undefined;

    if (this.embedded) {
      const manager = getSharedManager();
      if (this.opts.useLocks) {
        const { job, token: lockToken } = await manager.pullWithLock(
          this.queueKey,
          this.workerId,
          0,
          this.opts.lockDuration
        );
        return job ? this.registerManualJob(job, lockToken) : undefined;
      }
      const job = await manager.pull(this.queueKey, 0);
      return job ? this.registerManualJob(job, null) : undefined;
    }

    if (!this.tcp) return undefined;
    const command: Record<string, unknown> = {
      cmd: 'PULL',
      queue: this.queueKey,
      timeout: 0,
    };
    if (this.opts.useLocks) {
      command.owner = this.workerId;
      if (this.opts.lockDuration !== undefined) command.lockTtl = this.opts.lockDuration;
      if (token) command.token = token;
    }

    const response = await this.tcp.send(command);
    if (!response.ok || !response.job) return undefined;
    const job = parseJobFromResponse(response.job as Record<string, unknown>, this.queueKey);
    const lockToken = this.opts.useLocks ? ((response.token as string | undefined) ?? null) : null;
    return this.registerManualJob(job, lockToken);
  }

  async processJobManually(
    job: ManualJob<T>,
    token?: string,
    fetchNextCallback?: () => Promise<ManualJob<T> | undefined>
  ): Promise<ManualJob<T> | undefined> {
    if (this.closed) return undefined;

    const jobIdStr = String(job.id);
    let delivery = this.currentDelivery(jobIdStr);
    const expectedToken = this.opts.useLocks ? (token ?? job.token ?? null) : null;
    if (delivery) {
      if (token !== undefined && token !== delivery.token) {
        throw new Error(`Invalid or expired lock token for job ${jobIdStr}`);
      }
      if (delivery.job !== job && expectedToken !== delivery.token) return undefined;
    } else {
      if (this.opts.useLocks && expectedToken === null) {
        throw new Error(`Lock token required for manually processed job ${jobIdStr}`);
      }
      delivery = this.trackDelivery({ job, token: expectedToken });
    }
    const processingJob = delivery.job;
    while (!this.closed && !this._closing) {
      if (!this.isCurrentDelivery(delivery)) return undefined;
      if (this.isActiveDelivery(delivery)) return undefined;
      const concurrencyBlocked = this.activeJobs >= this.opts.concurrency;
      const groupBlocked =
        this.groupLimiter !== null && !this.groupLimiter.canProcess(processingJob);
      if (!concurrencyBlocked && !groupBlocked && this.rateLimiter.tryAcquire()) break;
      const waitTime =
        concurrencyBlocked || groupBlocked ? 10 : this.rateLimiter.getTimeUntilNextSlot();
      await Bun.sleep(Math.min(Math.max(waitTime, 10), 100));
    }
    if (this.closed || this._closing || !this.isCurrentDelivery(delivery)) return undefined;

    if (this.groupLimiter) this.groupLimiter.increment(processingJob);
    this.activeJobs++;
    this.beginDelivery(delivery);
    const jobId = String(processingJob.id);
    const abortController = this.createAbortController(jobId);
    let timedOut = false;
    const timeout =
      processingJob.timeout === null
        ? null
        : setTimeout(() => {
            timedOut = true;
            abortController.abort(
              new Error(`Job ${jobId} timed out after ${processingJob.timeout}ms`)
            );
          }, processingJob.timeout);

    try {
      await processJob(processingJob, {
        name: this.queueKey,
        processor: this.processor,
        embedded: this.embedded,
        tcp: this.tcp,
        ackBatcher: this.ackBatcher,
        emitter: this,
        token: this.opts.useLocks ? delivery.token : undefined,
        onAckQueued: () => this.queueAckCandidate(delivery),
        onAckUnavailable: () => this.retireAckCandidate(delivery),
        shouldAbandonOutcome: () =>
          timedOut || this._forceClose || !this.isCurrentDelivery(delivery),
        abortController,
      });
      if (fetchNextCallback) return await fetchNextCallback();
    } finally {
      this.retireAckCandidate(delivery);
      if (timeout !== null) clearTimeout(timeout);
      this.releaseAbortController(jobId, abortController);
      this.activeJobs--;
      this.finishDelivery(delivery);
      if (this.groupLimiter) this.groupLimiter.decrement(processingJob);
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
      for (let index = 0; index < jobIds.length; index++) {
        const success = await manager.extendLock(jobIds[index], tokens[index], duration);
        if (success) extended++;
      }
      return extended;
    }

    if (!this.tcp) return 0;
    const response = await this.tcp.send({
      cmd: 'ExtendLocks',
      ids: jobIds,
      tokens,
      durations: jobIds.map(() => duration),
    });
    const extended = response.count as number | undefined;
    return extended ?? 0;
  }
}
