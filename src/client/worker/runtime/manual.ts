import type { Job as InternalJob } from '../../../domain/types/job';
import { getSharedManager } from '../../manager';
import { parseJobFromResponse } from '../jobParser';
import { processJob } from '../processor';
import { WorkerControl } from './control';

export abstract class WorkerManual<T = unknown, R = unknown> extends WorkerControl<T, R> {
  async getNextJob(token?: string, _opts?: { block?: boolean }): Promise<InternalJob | undefined> {
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
        if (job && lockToken) {
          const jobIdStr = String(job.id);
          this.pulledJobIds.add(jobIdStr);
          this.jobTokens.set(jobIdStr, lockToken);
        }
        return job ?? undefined;
      }
      const job = await manager.pull(this.queueKey, 0);
      if (job) this.pulledJobIds.add(String(job.id));
      return job ?? undefined;
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
    if (this.opts.useLocks && token) this.jobTokens.set(jobIdStr, token);

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
      if (fetchNextCallback) return await fetchNextCallback();
    } finally {
      this.activeJobs--;
      this.activeJobIds.delete(jobIdStr);
      this.pulledJobIds.delete(jobIdStr);
      this.cancelledJobs.delete(jobIdStr);
      if (this.opts.useLocks) this.jobTokens.delete(jobIdStr);
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
