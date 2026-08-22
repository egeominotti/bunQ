import type { JobId } from '../../domain/types/job';
import { FailureReason } from '../../domain/types/dlq';
import { ackJob, ackJobBatch, ackJobBatchWithResults, failJob } from '../operations/ack';
import type { AckBatchOutcome, AckOutcome, CompletionOptions, FailJobOptions } from '../types/ack';
import * as lockMgr from '../lockManager';
import { QueueManagerDelivery } from './delivery';

export class QueueManagerAck extends QueueManagerDelivery {
  async ack(
    jobId: JobId,
    result?: unknown,
    token?: string,
    options: CompletionOptions = {}
  ): Promise<AckOutcome> {
    try {
      const lockCtx = this.contextFactory.getLockContext();
      if (this.isRetiredOutcome(jobId, token)) return this.ignoredOutcome();
      this.assertLeaseToken(jobId, token, lockCtx);
      const location = this.jobIndex.get(jobId);
      if (location?.type === 'queue') {
        if (
          !this.isQueuedTimeoutGeneration(jobId) &&
          this.isStallRetried(jobId) &&
          (await this.completeStallRetriedJob(jobId, result, options.removeOnComplete))
        ) {
          lockMgr.releaseLock(jobId, lockCtx, token);
          return;
        }
      }
      const applied = await ackJob(jobId, result, this.contextFactory.getAckContext(), {
        ...options,
        leaseToken: token,
      });
      if (!applied) {
        if (this.isRetiredOutcome(jobId, token)) return this.ignoredOutcome();
        if (
          !this.isQueuedTimeoutGeneration(jobId) &&
          this.isStallRetried(jobId) &&
          (await this.completeStallRetriedJob(jobId, result, options.removeOnComplete))
        ) {
          lockMgr.releaseLock(jobId, lockCtx, token);
          return;
        }
        throw this.missingJobError(jobId);
      }
      lockMgr.releaseLock(jobId, lockCtx, token);
    } finally {
      this.syncJobTimeout(jobId);
    }
  }

  async ackBatch(jobIds: JobId[], tokens?: string[]): Promise<AckBatchOutcome> {
    try {
      const lockCtx = this.contextFactory.getLockContext();
      const retiredOutcomes = jobIds.map((id, index) => this.isRetiredOutcome(id, tokens?.[index]));
      for (let index = 0; index < jobIds.length; index++) {
        if (!retiredOutcomes[index]) {
          this.assertLeaseToken(jobIds[index], tokens?.[index], lockCtx);
        }
      }
      const candidateIndices: number[] = [];
      for (let index = 0; index < jobIds.length; index++) {
        const id = jobIds[index];
        if (retiredOutcomes[index]) continue;
        if (this.jobIndex.get(id)?.type === 'queue') {
          if (!this.isQueuedTimeoutGeneration(id) && this.isStallRetried(id)) {
            await this.completeStallRetriedJob(id, undefined);
            lockMgr.releaseLock(id, lockCtx, tokens?.[index]);
            continue;
          }
        }
        candidateIndices.push(index);
      }
      const applied = await ackJobBatch(
        candidateIndices.map((index) => jobIds[index]),
        this.contextFactory.getAckContext(),
        candidateIndices.map((index) => tokens?.[index])
      );
      for (let offset = 0; offset < candidateIndices.length; offset++) {
        const index = candidateIndices[offset];
        const id = jobIds[index];
        if (applied[offset]) {
          lockMgr.releaseLock(id, lockCtx, tokens?.[index]);
        } else if (this.isRetiredOutcome(id, tokens?.[index])) {
          retiredOutcomes[index] = true;
        } else {
          throw this.missingJobError(id);
        }
      }
      return this.batchOutcome(jobIds, retiredOutcomes);
    } finally {
      for (const id of jobIds) this.syncJobTimeout(id);
    }
  }

  async ackBatchWithResults(
    items: Array<{ id: JobId; result: unknown; token?: string } & CompletionOptions>
  ): Promise<AckBatchOutcome> {
    try {
      const lockCtx = this.contextFactory.getLockContext();
      const retiredOutcomes = items.map((item) => this.isRetiredOutcome(item.id, item.token));
      for (let index = 0; index < items.length; index++) {
        const item = items[index];
        if (!retiredOutcomes[index]) this.assertLeaseToken(item.id, item.token, lockCtx);
      }
      const candidateIndices: number[] = [];
      for (let index = 0; index < items.length; index++) {
        const item = items[index];
        if (retiredOutcomes[index]) continue;
        if (this.jobIndex.get(item.id)?.type === 'queue') {
          if (!this.isQueuedTimeoutGeneration(item.id) && this.isStallRetried(item.id)) {
            await this.completeStallRetriedJob(item.id, item.result, item.removeOnComplete);
            lockMgr.releaseLock(item.id, lockCtx, item.token);
            continue;
          }
        }
        candidateIndices.push(index);
      }
      const applied = await ackJobBatchWithResults(
        candidateIndices.map((index) => items[index]),
        this.contextFactory.getAckContext()
      );
      for (let offset = 0; offset < candidateIndices.length; offset++) {
        const index = candidateIndices[offset];
        const item = items[index];
        if (applied[offset]) {
          lockMgr.releaseLock(item.id, lockCtx, item.token);
        } else if (this.isRetiredOutcome(item.id, item.token)) {
          retiredOutcomes[index] = true;
        } else {
          throw this.missingJobError(item.id);
        }
      }
      return this.batchOutcome(
        items.map((item) => item.id),
        retiredOutcomes
      );
    } finally {
      for (const item of items) this.syncJobTimeout(item.id);
    }
  }

  // oxlint-disable-next-line max-params -- public compatibility signature carries removal policy
  async fail(
    jobId: JobId,
    error?: string,
    token?: string,
    unrecoverable = false,
    stack?: string[],
    removeOnFail?: boolean
  ): Promise<AckOutcome> {
    return this.failWithOptions(jobId, error, token, { unrecoverable, stack, removeOnFail });
  }

  async failWithReason(
    jobId: JobId,
    error: string | undefined,
    failureReason: FailureReason
  ): Promise<AckOutcome> {
    const options: FailJobOptions = { failureReason };
    if (failureReason === FailureReason.Timeout) {
      options.onClaim = (job) => this.retireTimedOutGeneration(job);
    }
    return this.failWithOptions(jobId, error, undefined, options, false);
  }

  private isRetiredTimeoutOutcome(jobId: JobId, token: string | undefined): boolean {
    if (token) return this.retiredTimeoutLeaseTokens.get(token)?.jobId === jobId;
    const generation = this.timedOutJobs.get(jobId);
    if (!generation || generation.token) return false;
    const location = this.jobIndex.get(jobId);
    if (location?.type === 'processing') return false;
    if (location?.type === 'queue') {
      const queued = this.shards[location.shardIdx].getQueue(location.queueName).find(jobId);
      if (queued && queued.startedAt !== generation.startedAt) return false;
    }
    if (location?.type === 'completed') {
      const completed = this.completedJobsData.get(jobId);
      if (completed && completed.startedAt !== generation.startedAt) return false;
    }
    return true;
  }

  private isRetiredOutcome(jobId: JobId, token: string | undefined): boolean {
    return this.isRetiredCronOutcome(jobId, token) || this.isRetiredTimeoutOutcome(jobId, token);
  }

  private isQueuedTimeoutGeneration(jobId: JobId): boolean {
    const generation = this.timedOutJobs.get(jobId);
    const location = this.jobIndex.get(jobId);
    if (!generation || location?.type !== 'queue') return false;
    const queued = this.shards[location.shardIdx].getQueue(location.queueName).find(jobId);
    return queued?.startedAt === generation.startedAt;
  }

  private ignoredOutcome(): Exclude<AckOutcome, undefined> {
    return { applied: false, reason: 'already-finalized' };
  }

  private batchOutcome(jobIds: JobId[], ignored: boolean[]): AckBatchOutcome {
    const ignoredIndices = ignored.flatMap((value, index) => (value ? [index] : []));
    if (ignoredIndices.length === 0) return;
    return { ignoredIndices, ignoredIds: ignoredIndices.map((index) => jobIds[index]) };
  }

  private missingJobError(jobId: JobId): Error {
    return new Error(`Job not found or not in processing state: ${jobId}`);
  }

  private retireTimedOutGeneration(job: { id: JobId; startedAt: number | null }): void {
    if (job.startedAt === null) return;
    const lock = lockMgr.getLockInfo(job.id, this.contextFactory.getLockContext());
    const generation = { jobId: job.id, startedAt: job.startedAt, token: lock?.token };
    this.timedOutJobs.set(job.id, generation);
    if (lock) this.retiredTimeoutLeaseTokens.set(lock.token, generation);
    lockMgr.releaseLock(job.id, this.contextFactory.getLockContext(), lock?.token);
    for (const [clientId, jobs] of this.clientJobs) {
      jobs.delete(job.id);
      if (jobs.size === 0) this.clientJobs.delete(clientId);
    }
  }

  private async failWithOptions(
    jobId: JobId,
    error: string | undefined,
    token: string | undefined,
    options: FailJobOptions,
    enforceLease = true
  ): Promise<AckOutcome> {
    try {
      const lockCtx = this.contextFactory.getLockContext();
      if (enforceLease && this.isRetiredOutcome(jobId, token)) return this.ignoredOutcome();
      const hadLock = lockMgr.getLockInfo(jobId, lockCtx) !== null;
      if (enforceLease) this.assertLeaseToken(jobId, token, lockCtx);
      if (
        enforceLease &&
        token &&
        !hadLock &&
        !this.isQueuedTimeoutGeneration(jobId) &&
        this.isStallRetried(jobId)
      ) {
        return this.ignoredOutcome();
      }
      if (hadLock && this.jobIndex.get(jobId)?.type !== 'processing') {
        lockMgr.releaseLock(jobId, lockCtx, token);
        return this.ignoredOutcome();
      }
      try {
        await failJob(jobId, error, this.contextFactory.getAckContext(), options);
      } catch (caught) {
        if (enforceLease && this.isRetiredOutcome(jobId, token)) {
          return this.ignoredOutcome();
        }
        if (
          hadLock &&
          caught instanceof Error &&
          caught.message.includes('not found') &&
          this.jobIndex.get(jobId)?.type === 'queue'
        ) {
          lockMgr.releaseLock(jobId, lockCtx, token);
          return this.ignoredOutcome();
        }
        throw caught;
      }
      if (options.failureReason !== FailureReason.Timeout) {
        lockMgr.releaseLock(jobId, lockCtx, token);
      }
    } finally {
      this.syncJobTimeout(jobId);
    }
  }
}
