import type { JobId } from '../../domain/types/job';
import type { FailureReason } from '../../domain/types/dlq';
import { ackJob, ackJobBatch, ackJobBatchWithResults, failJob } from '../operations/ack';
import type { FailJobOptions } from '../types/ack';
import * as lockMgr from '../lockManager';
import { QueueManagerDelivery } from './delivery';

export class QueueManagerAck extends QueueManagerDelivery {
  async ack(jobId: JobId, result?: unknown, token?: string): Promise<void> {
    const lockCtx = this.contextFactory.getLockContext();
    if (
      token &&
      !lockMgr.verifyLock(jobId, token, lockCtx) &&
      !this.isExpiredButOwned(jobId, token, lockCtx)
    ) {
      this.throwIfOwnershipConflict(jobId, lockCtx);
      const location = this.jobIndex.get(jobId);
      if (location?.type !== 'processing') {
        if (location?.type === 'queue') {
          if (this.timedOutJobs.has(jobId)) {
            lockMgr.releaseLock(jobId, lockCtx, token);
            return;
          }
          await this.completeStallRetriedJob(jobId, result);
          lockMgr.releaseLock(jobId, lockCtx, token);
        }
        return;
      }
    }
    try {
      await ackJob(jobId, result, this.contextFactory.getAckContext());
    } catch (error) {
      if (error instanceof Error && error.message.includes('not found')) {
        if (this.timedOutJobs.has(jobId)) {
          if (token) lockMgr.releaseLock(jobId, lockCtx, token);
          return;
        }
        const shouldRecover = token ?? this.isStallRetried(jobId);
        if (shouldRecover && (await this.completeStallRetriedJob(jobId, result))) {
          if (token) lockMgr.releaseLock(jobId, lockCtx, token);
          return;
        }
      }
      throw error;
    }
    lockMgr.releaseLock(jobId, lockCtx, token);
  }

  async ackBatch(jobIds: JobId[], tokens?: string[]): Promise<void> {
    const lockCtx = this.contextFactory.getLockContext();
    const validJobIds: JobId[] = [];
    const validTokens: string[] | undefined = tokens ? [] : undefined;
    if (tokens?.length === jobIds.length) {
      for (let index = 0; index < jobIds.length; index++) {
        const token = tokens[index];
        if (
          token &&
          !lockMgr.verifyLock(jobIds[index], token, lockCtx) &&
          !this.isExpiredButOwned(jobIds[index], token, lockCtx)
        ) {
          this.throwIfOwnershipConflict(jobIds[index], lockCtx);
          if (this.jobIndex.get(jobIds[index])?.type === 'queue') {
            if (!this.timedOutJobs.has(jobIds[index])) {
              await this.completeStallRetriedJob(jobIds[index], undefined);
            }
            lockMgr.releaseLock(jobIds[index], lockCtx, token);
          }
          continue;
        }
        validJobIds.push(jobIds[index]);
        if (validTokens) validTokens.push(token);
      }
    } else {
      validJobIds.push(...jobIds);
    }
    if (validJobIds.length > 0) {
      await ackJobBatch(validJobIds, this.contextFactory.getAckContext());
    }
    if (validTokens) {
      for (let index = 0; index < validJobIds.length; index++) {
        lockMgr.releaseLock(validJobIds[index], lockCtx, validTokens[index]);
      }
    } else if (tokens) {
      for (let index = 0; index < jobIds.length; index++) {
        lockMgr.releaseLock(jobIds[index], lockCtx, tokens[index]);
      }
    }
  }

  async ackBatchWithResults(
    items: Array<{ id: JobId; result: unknown; token?: string }>
  ): Promise<void> {
    const lockCtx = this.contextFactory.getLockContext();
    const validItems: typeof items = [];
    for (const item of items) {
      if (
        item.token &&
        !lockMgr.verifyLock(item.id, item.token, lockCtx) &&
        !this.isExpiredButOwned(item.id, item.token, lockCtx)
      ) {
        this.throwIfOwnershipConflict(item.id, lockCtx);
        if (this.jobIndex.get(item.id)?.type === 'queue') {
          if (!this.timedOutJobs.has(item.id)) {
            await this.completeStallRetriedJob(item.id, item.result);
          }
          lockMgr.releaseLock(item.id, lockCtx, item.token);
        }
        continue;
      }
      validItems.push(item);
    }
    if (validItems.length > 0) {
      await ackJobBatchWithResults(validItems, this.contextFactory.getAckContext());
    }
    for (const item of validItems) lockMgr.releaseLock(item.id, lockCtx, item.token);
  }

  async fail(
    jobId: JobId,
    error?: string,
    token?: string,
    unrecoverable = false,
    stack?: string[]
  ): Promise<void> {
    await this.failWithOptions(jobId, error, token, { unrecoverable, stack });
  }

  async failWithReason(
    jobId: JobId,
    error: string | undefined,
    failureReason: FailureReason
  ): Promise<void> {
    await this.failWithOptions(jobId, error, undefined, { failureReason });
  }

  private async failWithOptions(
    jobId: JobId,
    error: string | undefined,
    token: string | undefined,
    options: FailJobOptions
  ): Promise<void> {
    const lockCtx = this.contextFactory.getLockContext();
    if (token && !lockMgr.verifyLock(jobId, token, lockCtx)) {
      this.throwIfOwnershipConflict(jobId, lockCtx);
      if (this.jobIndex.get(jobId)?.type !== 'processing') return;
    }
    try {
      await failJob(jobId, error, this.contextFactory.getAckContext(), options);
    } catch (caught) {
      if (
        token &&
        caught instanceof Error &&
        caught.message.includes('not found') &&
        this.jobIndex.get(jobId)?.type === 'queue'
      ) {
        return;
      }
      throw caught;
    }
    lockMgr.releaseLock(jobId, lockCtx, token);
  }
}
