import type { JobId } from '../../domain/types/job';
import { EventType } from '../../domain/types/queue';
import { shardIndex } from '../../shared/hash';
import {
  reconcileDependencyCompletionPins,
  releaseDependencyCompletionPins,
} from '../dependencyCompletions';
import * as queueControl from '../operations/queueControl';
import { QueueManagerQueries } from './queries';

export class QueueManagerControl extends QueueManagerQueries {
  pause(queue: string): void {
    queueControl.pauseQueue(queue, this.contextFactory.getQueueControlContext());
    this.registerQueueName(queue);
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
    const durableDeletion = this.storage?.deleteJobsForQueue(
      queue,
      this.dependencyReferencesAfterObliterate(queue),
      this.config.maxCompletedJobs
    );
    const bufferedJobs = durableDeletion?.bufferedJobIds ?? [];
    const shardJobs = queueControl.obliterateQueue(
      queue,
      this.contextFactory.getQueueControlContext()
    );

    const toDrop = new Set<JobId>();
    for (const jobId of [...bufferedJobs, ...shardJobs]) {
      const location = this.jobIndex.get(jobId);
      if (!location || location.queueName === queue) toDrop.add(jobId);
    }
    for (const [jobId, ownerQueue] of this.jobResultQueues) {
      if (ownerQueue !== queue) continue;
      this.jobResults.delete(jobId);
      this.jobResultQueues.delete(jobId);
      const location = this.jobIndex.get(jobId);
      if (location && location.queueName !== queue) continue;
      toDrop.add(jobId);
    }
    for (const [jobId, ownerQueue] of this.jobLogQueues) {
      if (ownerQueue !== queue) continue;
      this.jobLogs.delete(jobId);
      this.jobLogQueues.delete(jobId);
      const location = this.jobIndex.get(jobId);
      if (location && location.queueName !== queue) continue;
      toDrop.add(jobId);
    }
    for (const [jobId, location] of this.jobIndex) {
      if (location.type === 'processing') {
        if (location.queueName === queue) toDrop.add(jobId);
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
      this.jobResultQueues.delete(jobId);
      this.jobLogs.delete(jobId);
      this.jobLogQueues.delete(jobId);
      this.jobLocks.delete(jobId);
      this.dependencyResults.releaseConsumer(jobId);
      this.failedChildrenValues.delete(jobId);
      this.ignoredChildrenFailures.delete(jobId);
      this.pendingDepChecks.delete(jobId);
      this.stalledCandidates.delete(jobId);
      this.repeatChain.delete(jobId);
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

    if (durableDeletion) this.depCompletions.hydrate(durableDeletion.dependencyCompletions);
    else {
      this.depCompletions.deleteForQueue(queue);
      this.reconcileCompletionPins();
    }
    this.purgeQueueMetadata(queue);
    const hasPendingAdmission = (this.pendingQueueAdmissions.get(queue) ?? 0) > 0;
    if (hasPendingAdmission) this.pendingQueueObliterations.add(queue);
    else {
      this.pendingQueueObliterations.delete(queue);
      this.unregisterQueueName(queue);
    }
    this.dashboardEmit?.('queue:obliterated', { queue });
    if (!hasPendingAdmission) this.dashboardEmit?.('queue:removed', { queue });
  }

  protected onQueueAdmissionsDrained(queue: string): void {
    if (!this.pendingQueueObliterations.delete(queue)) return;
    if (this.hasLiveQueueProjection(queue)) {
      this.registerQueueName(queue);
      return;
    }
    this.unregisterQueueName(queue);
    try {
      this.dashboardEmit?.('queue:removed', { queue });
    } catch (error) {
      console.error('[QueueControl] Dashboard event failed after admission reconciliation', {
        error,
      });
    }
  }

  private hasLiveQueueProjection(queue: string): boolean {
    const shard = this.shards[shardIndex(queue)];
    if (
      (shard.queues.get(queue)?.size ?? 0) > 0 ||
      shard.getDlqCount(queue) > 0 ||
      shard.getConfiguredQueueNames().includes(queue)
    ) {
      return true;
    }
    for (const job of shard.waitingDeps.values()) if (job.queue === queue) return true;
    for (const job of shard.waitingChildren.values()) if (job.queue === queue) return true;
    for (const location of this.jobIndex.values()) {
      if (location.queueName === queue) return true;
    }
    return false;
  }

  protected purgeQueueMetadata(queue: string): void {
    this.perQueueMetrics.delete(queue);
    this.telemetryJournal.clearQueueMemory(queue);
  }

  private dependencyReferencesAfterObliterate(queue: string): Set<JobId> {
    const referenced = new Set<JobId>();
    for (const shard of this.shards) {
      for (const [dependencyId, waiterIds] of shard.dependencyIndex) {
        for (const waiterId of waiterIds) {
          const waiter = shard.waitingDeps.get(waiterId);
          if (!waiter || waiter.queue !== queue) {
            referenced.add(dependencyId);
            break;
          }
        }
      }
    }
    return referenced;
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
    this.telemetryJournal.clearQueueEventsMemory(queue);
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
