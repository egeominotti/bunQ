import type { Job, JobId } from '../../domain/types/job';
import type { LockGuard } from '../../shared/lock';
import { processingShardIndex, shardIndex } from '../../shared/hash';
import * as queueControl from '../operations/queueControl';
import * as queryOps from '../operations/queryOperations';
import {
  assertFlowParentOwnership,
  backpatchDeclaredFlowChild,
  canAcceptRemovedFlowChild,
  flowChildFailureError,
  isDeclaredFlowChild,
} from '../flowParentBackpatch';
import { commitParentLink, parentLinkState, prepareParentLink } from '../operations/parentLink';
import { QueueManagerLocks } from './locks';

export class QueueManagerQueries extends QueueManagerLocks {
  async getJob(jobId: JobId): Promise<Job | null> {
    return queryOps.getJob(jobId, this.contextFactory.getQueryContext());
  }

  async getJobState(jobId: JobId): Promise<string> {
    return queryOps.getJobState(jobId, this.contextFactory.getQueryContext());
  }

  getResult(jobId: JobId): unknown {
    return queryOps.getJobResult(jobId, this.contextFactory.getQueryContext());
  }

  /** Distinguish a completed generation from a valid undefined result. */
  async getCompletionAsync(jobId: JobId): Promise<{ found: boolean; result: unknown }> {
    const found = this.completedJobs.has(jobId);
    return { found, result: found ? this.getResult(jobId) : undefined };
  }

  /** Async result port used by transports; memory and SQLite remain synchronous internally. */
  async getResultAsync(jobId: JobId): Promise<unknown> {
    return (await this.getCompletionAsync(jobId)).result;
  }

  async getChildrenValues(parentJobId: JobId): Promise<Record<string, unknown>> {
    const job = await this.getJob(parentJobId);
    if (!job?.childrenIds || job.childrenIds.length === 0) return {};

    const ctx = this.contextFactory.getQueryContext();
    const results: Record<string, unknown> = {};
    for (const childId of job.childrenIds) {
      const result = queryOps.getJobResult(childId, ctx);
      if (result !== undefined) {
        const childJob = await this.getJob(childId);
        results[childJob ? `${childJob.queue}:${childId}` : childId] = result;
      }
    }
    return results;
  }

  async updateJobParent(childJobId: JobId, parentJobId: JobId): Promise<void> {
    if (childJobId === parentJobId) throw new Error('A flow job cannot be its own parent');
    let childJob = await this.getJob(childJobId);
    const parentJob = await this.getJob(parentJobId);
    if (!childJob) {
      if (canAcceptRemovedFlowChild(parentJob, childJobId, this.depCompletions)) return;
      throw new Error(`Child job not found: ${String(childJobId)}`);
    }
    if (!parentJob) throw new Error(`Parent job not found: ${String(parentJobId)}`);
    assertFlowParentOwnership(childJob, parentJobId);

    if (isDeclaredFlowChild(parentJob, childJobId)) {
      childJob = await backpatchDeclaredFlowChild(childJob, parentJob, {
        storage: this.storage,
        customIdLock: this.customIdLock,
        shards: this.shards,
        shardLocks: this.shardLocks,
        processingShards: this.processingShards,
        processingLocks: this.processingLocks,
        jobIndex: this.jobIndex,
        completedJobsData: this.completedJobsData,
        depCompletions: this.depCompletions,
      });
    } else {
      const parentLocation = this.jobIndex.get(parentJobId);
      if (parentLocation?.type !== 'queue') {
        throw new Error(`Parent job ${String(parentJobId)} is not linkable`);
      }
      const childLocation = this.jobIndex.get(childJobId);
      const shardIndexes = [
        ...new Set([shardIndex(childJob.queue), shardIndex(parentJob.queue)]),
      ].sort((a, b) => a - b);
      const processingIndexes =
        childLocation?.type === 'processing' ? [processingShardIndex(childJobId)] : [];
      const guards: LockGuard[] = [await this.customIdLock.acquireWrite()];
      try {
        for (const index of shardIndexes) guards.push(await this.shardLocks[index].acquireWrite());
        for (const index of processingIndexes) {
          guards.push(await this.processingLocks[index].acquireWrite());
        }
        if (this.jobIndex.get(parentJobId)?.type !== 'queue') {
          throw new Error(`Parent job ${String(parentJobId)} changed state while linking`);
        }

        const childFinished =
          this.completedJobs.has(childJobId) || this.depCompletions.has(childJobId);
        const pushContext = this.contextFactory.getPushContext();
        const plan = prepareParentLink(childJob, pushContext, childFinished, parentJobId);
        this.storage?.updateFlowLink(plan.linkedChild, plan.linkedParent, parentLinkState(plan));
        commitParentLink(plan, pushContext);
      } finally {
        for (let index = guards.length - 1; index >= 0; index--) guards[index].release();
      }
    }

    const childLocation = this.jobIndex.get(childJobId);
    const childFailureError =
      childLocation?.type === 'dlq'
        ? (flowChildFailureError(childJob, this.shards) ?? 'Child job failed')
        : 'Child job failed';
    if (childLocation?.type === 'dlq' && childJob.failParentOnFailure) {
      await this.moveParentToFailed(parentJobId, childJob, childFailureError);
    }
    if (
      childLocation?.type === 'dlq' &&
      (childJob.removeDependencyOnFailure ||
        childJob.ignoreDependencyOnFailure ||
        childJob.continueParentOnFailure)
    ) {
      await this.onChildDependencyOption(childJob, childFailureError);
    }
  }

  getJobByCustomId(customId: string): Job | null {
    return queryOps.getJobByCustomId(customId, this.contextFactory.getQueryContext());
  }

  getProgress(jobId: JobId) {
    return queryOps.getJobProgress(jobId, this.contextFactory.getQueryContext());
  }

  count(queue: string): number {
    return queueControl.getQueueCount(queue, this.contextFactory.getQueueControlContext());
  }

  protected async moveParentToFailed(
    _parentId: JobId,
    _childJob: Job,
    _error: string | undefined
  ): Promise<void> {
    throw new Error('QueueManager flow module is not initialized');
  }

  protected async onChildDependencyOption(
    _childJob: Job,
    _error: string | undefined
  ): Promise<void> {
    throw new Error('QueueManager flow options module is not initialized');
  }
}
