import type { JobId } from '../../domain/types/job';
import { EventType } from '../../domain/types/queue';
import {
  reconcileDependencyCompletionPins,
  releaseDependencyCompletionPins,
} from '../dependencyCompletions';
import * as dlqOps from '../dlqManager';
import * as queueControl from '../operations/queueControl';
import { QueueManagerQueries } from './queries';

export class QueueManagerControl extends QueueManagerQueries {
  pause(queue: string): void {
    queueControl.pauseQueue(queue, this.contextFactory.getQueueControlContext());
    this.persistQueueState(queue);
    this.dashboardEmit?.('queue:paused', { queue });
    this.eventsManager.broadcast({
      eventType: EventType.Paused,
      queue,
      jobId: '' as JobId,
      timestamp: Date.now(),
    });
  }

  resume(queue: string): void {
    queueControl.resumeQueue(queue, this.contextFactory.getQueueControlContext());
    this.persistQueueState(queue);
    this.dashboardEmit?.('queue:resumed', { queue });
    this.eventsManager.broadcast({
      eventType: EventType.Resumed,
      queue,
      jobId: '' as JobId,
      timestamp: Date.now(),
    });
  }

  isPaused(queue: string): boolean {
    return queueControl.isQueuePaused(queue, this.contextFactory.getQueueControlContext());
  }

  drain(queue: string): number {
    const count = queueControl.drainQueue(queue, this.contextFactory.getQueueControlContext());
    if (count > 0) this.dashboardEmit?.('queue:drained', { queue, count });
    return count;
  }

  obliterate(queue: string): void {
    const shardJobs = queueControl.obliterateQueue(
      queue,
      this.contextFactory.getQueueControlContext()
    );
    dlqOps.purgeDlqJobs(queue, this.contextFactory.getDlqContext());

    const toDrop = new Set<JobId>(shardJobs);
    for (const [jobId, location] of this.jobIndex) {
      if (location.type === 'processing') {
        const job = this.processingShards[location.shardIdx]?.get(jobId);
        if (job?.queue === queue) toDrop.add(jobId);
      } else if (location.queueName === queue) {
        toDrop.add(jobId);
      }
    }

    for (const jobId of toDrop) {
      const location = this.jobIndex.get(jobId);
      if (location?.type === 'processing') {
        this.processingShards[location.shardIdx]?.delete(jobId);
      }
      this.timeoutScheduler.cancel(jobId);
      this.jobIndex.delete(jobId);
      this.completedJobs.delete(jobId);
      this.completedJobsData.delete(jobId);
      this.jobResults.delete(jobId);
      this.jobLogs.delete(jobId);
      this.jobLocks.delete(jobId);
      this.dependencyResults.releaseConsumer(jobId);
      this.failedChildrenValues.delete(jobId);
      this.ignoredChildrenFailures.delete(jobId);
      this.pendingDepChecks.delete(jobId);
      this.stalledCandidates.delete(jobId);
      this.repeatChain.delete(jobId);
      this.storage?.deleteJob(jobId);
    }

    const chainKeysToDelete: JobId[] = [];
    for (const [oldId, newId] of this.repeatChain) {
      if (toDrop.has(newId)) chainKeysToDelete.push(oldId);
    }
    for (const oldId of chainKeysToDelete) this.repeatChain.delete(oldId);

    const customIdsToDelete: string[] = [];
    for (const [customId, jobId] of this.customIdMap.entries()) {
      if (toDrop.has(jobId)) customIdsToDelete.push(customId);
    }
    for (const customId of customIdsToDelete) this.customIdMap.delete(customId);

    const removedCompletions = this.storage?.deleteDependencyCompletionsForQueue(queue) ?? [];
    for (const jobId of removedCompletions) this.depCompletions.delete(jobId);
    this.reconcileCompletionPins();
    this.purgeQueueMetadata(queue);
    this.unregisterQueueName(queue);
    this.dashboardEmit?.('queue:obliterated', { queue });
    this.dashboardEmit?.('queue:removed', { queue });
  }

  protected purgeQueueMetadata(queue: string): void {
    this.perQueueMetrics.delete(queue);
    this.telemetryJournal.clearQueue(queue);
    this.storage?.deleteQueueState(queue);
    this.storage?.deleteGroupState(queue);
  }

  listQueues(): string[] {
    return Array.from(this.queueNamesCache);
  }

  protected registerQueueName(queue: string): void {
    const isNew = !this.queueNamesCache.has(queue);
    this.queueNamesCache.add(queue);
    if (isNew) this.dashboardEmit?.('queue:created', { queue });
  }

  protected unregisterQueueName(queue: string): void {
    this.queueNamesCache.delete(queue);
  }

  protected releaseCompletionPins(dependencyIds: Iterable<JobId>): void {
    releaseDependencyCompletionPins(dependencyIds, {
      storage: this.storage,
      shards: this.shards,
      depCompletions: this.depCompletions,
      maxDependencyCompletions: this.config.maxCompletedJobs,
    });
  }

  protected reconcileCompletionPins(): void {
    reconcileDependencyCompletionPins({
      storage: this.storage,
      shards: this.shards,
      depCompletions: this.depCompletions,
      maxDependencyCompletions: this.config.maxCompletedJobs,
    });
  }

  clean(queue: string, graceMs: number, state?: string, limit?: number): JobId[] {
    return queueControl.cleanQueue(
      queue,
      graceMs,
      this.contextFactory.getQueueControlContext(),
      state,
      limit
    );
  }

  protected persistQueueState(_queue: string): void {
    throw new Error('QueueManager limits module is not initialized');
  }
}
