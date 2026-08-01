import type { Job, JobId, JobInput, JobLock } from '../../domain/types/job';
import { DEFAULT_LOCK_TTL } from '../../domain/types/job';
import type { AtomicFlowBatchInput, AtomicFlowBatchResult } from '../../domain/types/flow';
import { EventType } from '../../domain/types/queue';
import { setDlqRetryState } from '../../domain/types/dlq';
import { withWriteLock } from '../../shared/lock';
import { pushJob, pushJobBatch } from '../operations/push';
import { pushFlowBatch } from '../operations/flowPush';
import { pullJob, pullJobBatch } from '../operations/pull';
import { commitRemovedCompletion } from '../dependencyCompletions';
import * as lockMgr from '../lockManager';
import { managerRuntime } from './context';
import { QueueManagerState } from './state';

export class QueueManagerDelivery extends QueueManagerState {
  async push(queue: string, input: JobInput): Promise<Job> {
    managerRuntime(this).registerQueueName(queue);
    return pushJob(queue, input, this.contextFactory.getPushContext());
  }

  async pushBatch(queue: string, inputs: JobInput[]): Promise<JobId[]> {
    managerRuntime(this).registerQueueName(queue);
    return pushJobBatch(queue, inputs, this.contextFactory.getPushContext());
  }

  async pushFlow(batch: AtomicFlowBatchInput): Promise<AtomicFlowBatchResult> {
    return pushFlowBatch(batch, this.contextFactory.getPushContext());
  }

  async pull(queue: string, timeoutMs = 0, signal?: AbortSignal): Promise<Job | null> {
    return pullJob(queue, timeoutMs, this.contextFactory.getPullContext(), signal);
  }

  async pullWithLock(
    queue: string,
    owner: string,
    timeoutMs = 0,
    lockTtl = DEFAULT_LOCK_TTL,
    signal?: AbortSignal
  ): Promise<{ job: Job | null; token: string | null }> {
    const job = await this.pull(queue, timeoutMs, signal);
    if (!job) return { job: null, token: null };
    const token = lockMgr.createLock(job.id, owner, this.contextFactory.getLockContext(), lockTtl);
    return { job, token };
  }

  async pullBatch(
    queue: string,
    count: number,
    timeoutMs = 0,
    signal?: AbortSignal
  ): Promise<Job[]> {
    return pullJobBatch(queue, count, timeoutMs, this.contextFactory.getPullContext(), signal);
  }

  // biome-ignore lint/complexity/useMaxParams: public API includes cancellation and lock policy
  async pullBatchWithLock(
    queue: string,
    count: number,
    owner: string,
    timeoutMs = 0,
    lockTtl = DEFAULT_LOCK_TTL,
    signal?: AbortSignal
  ): Promise<{ jobs: Job[]; tokens: string[] }> {
    const jobs = await this.pullBatch(queue, count, timeoutMs, signal);
    const tokens = jobs.map(
      (job) =>
        lockMgr.createLock(job.id, owner, this.contextFactory.getLockContext(), lockTtl) ?? ''
    );
    return { jobs, tokens };
  }

  protected throwIfOwnershipConflict(
    jobId: JobId,
    lockCtx: { jobLocks: Map<JobId, JobLock> }
  ): void {
    const location = this.jobIndex.get(jobId);
    if (location?.type === 'processing' && lockCtx.jobLocks.has(jobId)) {
      throw new Error(`Invalid or expired lock token for job ${jobId}`);
    }
  }

  protected isExpiredButOwned(
    jobId: JobId,
    token: string,
    lockCtx: { jobLocks: Map<JobId, JobLock> }
  ): boolean {
    const location = this.jobIndex.get(jobId);
    if (location?.type !== 'processing') return false;
    const lock = lockCtx.jobLocks.get(jobId);
    if (lock?.token !== token) return false;
    const job = this.processingShards[location.shardIdx].get(jobId);
    return !(job && job.startedAt !== null && job.startedAt > lock.createdAt);
  }

  protected isStallRetried(jobId: JobId): boolean {
    const location = this.jobIndex.get(jobId);
    if (location?.type !== 'queue') return false;
    const job = this.shards[location.shardIdx].getQueue(location.queueName).find(jobId);
    return job !== null && job.attempts > 0;
  }

  protected async completeStallRetriedJob(jobId: JobId, result: unknown): Promise<boolean> {
    const location = this.jobIndex.get(jobId);
    if (location?.type !== 'queue') return false;
    const shard = this.shards[location.shardIdx];
    let job: Job | null = null;
    await withWriteLock(this.shardLocks[location.shardIdx], () => {
      job = shard.getQueue(location.queueName).remove(jobId);
      if (job) {
        shard.decrementQueued(jobId);
        shard.releaseJobResources(location.queueName, job.uniqueKey, job.groupId, job.id);
      }
    });
    if (!job) return false;

    const completedJob = job as Job;
    setDlqRetryState(completedJob, null);
    const ctx = this.contextFactory.getAckContext();
    if (!completedJob.removeOnComplete) {
      ctx.completedJobs.add(jobId);
      ctx.completedJobsData.set(jobId, completedJob);
      if (result !== undefined) {
        ctx.jobResults.set(jobId, result);
        ctx.storage?.storeResult(jobId, result);
      }
      ctx.jobIndex.set(jobId, { type: 'completed', queueName: completedJob.queue });
      ctx.storage?.markCompleted(jobId, Date.now(), completedJob.timeline);
    } else {
      commitRemovedCompletion(completedJob, ctx);
      ctx.jobIndex.delete(jobId);
    }
    if (result !== undefined) ctx.dependencyResults.retain(jobId, result);
    ctx.dependencyResults.releaseConsumer(jobId);
    ctx.totalCompleted.value++;
    ctx.broadcast({
      eventType: EventType.Completed,
      queue: location.queueName,
      jobId,
      timestamp: Date.now(),
      data: result,
    });
    ctx.onJobCompleted(jobId);
    return true;
  }
}
