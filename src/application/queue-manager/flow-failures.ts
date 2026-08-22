import type { Shard } from '../../domain/queue/shard';
import type { DlqEntry } from '../../domain/types/dlq';
import { FailureReason } from '../../domain/types/dlq';
import type { Job, JobId } from '../../domain/types/job';
import type { JobLocation } from '../../domain/types/queue';
import { EventType } from '../../domain/types/queue';
import { shardIndex } from '../../shared/hash';
import { withWriteLock } from '../../shared/lock';
import type { SetLike } from '../../shared/lru';
import { QueueManagerDependencyRuntime } from './dependency-runtime';

export class QueueManagerFlowFailures extends QueueManagerDependencyRuntime {
  protected onDlqEvicted(entry: DlqEntry): void {
    const location = this.jobIndex.get(entry.job.id);
    const ownsTerminal =
      location === undefined || (location.type === 'dlq' && location.queueName === entry.job.queue);
    if (ownsTerminal) {
      this.jobIndex.delete(entry.job.id);
      this.jobResults.delete(entry.job.id);
      this.jobLogs.delete(entry.job.id);
      this.dependencyResults.releaseConsumer(entry.job.id);
      if (entry.job.customId && this.customIdMap.get(entry.job.customId) === entry.job.id) {
        this.customIdMap.delete(entry.job.customId);
      }
    }
    this.storage?.purgeDlqEntries(
      entry.job.queue,
      [entry.job.id],
      ownsTerminal ? [entry.job.id] : [],
      false
    );
  }

  getJobIndex(): Map<JobId, JobLocation> {
    return this.jobIndex;
  }

  getCompletedJobs(): SetLike<JobId> {
    return this.completedJobs;
  }

  getDepCompletions(): SetLike<JobId> {
    return this.depCompletions;
  }

  getShards(): Shard[] {
    return this.shards;
  }

  protected onJobCompleted(completedId: JobId): void {
    this.failedChildrenValues.delete(completedId);
    this.ignoredChildrenFailures.delete(completedId);
    this.storage?.deleteFlowFailure(completedId);
    this.pendingDepChecks.add(completedId);
    this.scheduleDependencyFlush();
    void this.checkFlowCompleted(completedId);
  }

  protected onJobFailed(failedId: JobId): void {
    this.failedChildrenValues.delete(failedId);
    this.ignoredChildrenFailures.delete(failedId);
    this.storage?.deleteFlowFailure(failedId);
  }

  protected async checkFlowCompleted(completedId: JobId): Promise<void> {
    const job = await this.getJob(completedId);
    if (!job?.parentId) return;
    const parent = await this.getJob(job.parentId);
    if (!parent?.childrenIds || parent.childrenIds.length === 0) return;
    if (parent.childrenIds.every((childId) => this.completedJobs.has(childId))) {
      this.dashboardEmit?.('flow:completed', {
        parentJobId: String(parent.id),
        queue: parent.queue,
        childrenCount: parent.childrenIds.length,
      });
    }
  }

  protected async failParentOnChildFailure(
    childJob: Job,
    error: string | undefined
  ): Promise<void> {
    if (childJob.parentId) await this.moveParentToFailed(childJob.parentId, childJob, error);
  }

  protected override async moveParentToFailed(
    parentId: JobId,
    childJob: Job,
    error: string | undefined
  ): Promise<void> {
    const parentJob = await this.getJob(parentId);
    if (!parentJob || this.jobIndex.get(parentId)?.type !== 'queue') return;

    const index = shardIndex(parentJob.queue);
    let releasedDependencies: JobId[] = [];
    await withWriteLock(this.shardLocks[index], () => {
      if (this.jobIndex.get(parentId)?.type !== 'queue') return;
      const shard = this.shards[index];
      if (shard.waitingDeps.has(parentId)) {
        releasedDependencies = [...parentJob.dependsOn];
        shard.waitingDeps.delete(parentId);
        shard.unregisterDependencies(parentId, parentJob.dependsOn);
      }
      shard.waitingChildren.delete(parentId);
      const queue = shard.getQueue(parentJob.queue);
      if (queue.find(parentId)) {
        queue.remove(parentId);
        shard.decrementQueued(parentId);
      }
      const failError = `Child job ${childJob.id} failed: ${error ?? 'unknown error'}`;
      const entry = shard.addToDlq(parentJob, FailureReason.Unknown, failError);
      this.jobIndex.set(parentId, { type: 'dlq', queueName: parentJob.queue });
      this.storage?.saveDlqEntry(entry);
      this.storage?.deleteJob(parentId);
      this.storage?.deleteFlowFailure(parentId, childJob.id);
    });
    this.releaseCompletionPins(releasedDependencies);
    this.failedChildrenValues.delete(parentId);
    this.ignoredChildrenFailures.delete(parentId);
    this.dependencyResults.releaseConsumer(parentId);
    const failError = `Child job ${childJob.id} failed: ${error ?? 'unknown error'}`;
    this.eventsManager.broadcast({
      eventType: EventType.Failed,
      queue: parentJob.queue,
      jobId: parentId,
      timestamp: Date.now(),
      error: failError,
      data: parentJob.data,
    });
    this.dashboardEmit?.('flow:failed', {
      parentJobId: String(parentId),
      failedChildId: String(childJob.id),
      queue: parentJob.queue,
      error: error ?? 'Child job failed',
    });
  }
}
