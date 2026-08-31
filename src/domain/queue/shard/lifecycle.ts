import type { JobId } from '../../types/job';
import { ShardMetrics } from './metrics';

/** Drain and obliterate transitions across all shard collaborators. */
export class ShardLifecycle extends ShardMetrics {
  drain(queue: string): { count: number; jobIds: JobId[] } {
    const priorityQueue = this.queues.get(queue);
    if (!priorityQueue) return { count: 0, jobIds: [] };

    const count = priorityQueue.size;
    const jobIds: JobId[] = [];
    for (const job of priorityQueue.values()) {
      jobIds.push(job.id);
      this.temporalManager.removeDelayed(job.id);
    }
    priorityQueue.clear();
    this.groupScheduler.clearQueue(queue);
    this.temporalManager.clearIndexForQueue(queue);
    this.counters.adjustQueued(-count);
    this.counters.syncDelayedCount();
    return { count, jobIds };
  }

  obliterate(queue: string): JobId[] {
    const removed = new Set<JobId>(this.dependencyTracker.removeQueue(queue));
    const priorityQueue = this.queues.get(queue);
    if (priorityQueue) {
      for (const job of priorityQueue.values()) {
        removed.add(job.id);
        this.temporalManager.removeDelayed(job.id);
      }
      this.counters.adjustQueued(-priorityQueue.size);
    }

    for (const entry of this.dlqManager.getEntries(queue)) removed.add(entry.job.id);
    const dlqCount = this.dlqManager.deleteQueue(queue);
    if (dlqCount > 0) this.counters.adjustDlq(-dlqCount);

    this.counters.syncDelayedCount();
    this.temporalManager.clearIndexForQueue(queue);
    this.queues.delete(queue);
    this.uniqueKeyManager.clearQueue(queue);
    this.limiterManager.deleteQueue(queue);
    this.groupScheduler.clearQueue(queue);
    this.groupLimiterManager.clearQueue(queue);
    this.activeGroups.delete(queue);
    this.activeGroupCounts.delete(queue);
    return Array.from(removed);
  }
}
