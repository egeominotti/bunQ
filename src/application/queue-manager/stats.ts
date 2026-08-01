import * as statsMgr from '../statsManager';
import { QueueManagerJobManagement } from './job-management';

export class QueueManagerStats extends QueueManagerJobManagement {
  getStats() {
    return statsMgr.getStats(this.contextFactory.getStatsContext(), this.cronScheduler);
  }

  getQueuesSummary(): Array<{
    name: string;
    paused: boolean;
    counts: {
      waiting: number;
      prioritized: number;
      active: number;
      completed: number;
      failed: number;
      delayed: number;
    };
  }> {
    const queues = this.listQueues();
    const result: Array<{
      name: string;
      paused: boolean;
      counts: {
        waiting: number;
        prioritized: number;
        active: number;
        completed: number;
        failed: number;
        delayed: number;
      };
    }> = [];
    const countsByQueue = statsMgr.getAllQueueJobCounts(
      queues,
      this.contextFactory.getStatsContext()
    );
    for (const name of queues) {
      const counts = countsByQueue.get(name);
      if (!counts) continue;
      result.push({
        name,
        paused: this.isPaused(name),
        counts: {
          waiting: counts.waiting,
          prioritized: counts.prioritized,
          active: counts.active,
          completed: counts.completed,
          failed: counts.failed,
          delayed: counts.delayed,
        },
      });
    }
    return result;
  }

  getAllQueueJobCounts() {
    return this.getQueueJobCountsBatch(this.queueNamesCache);
  }

  getQueueJobCountsBatch(queueNames: Iterable<string>) {
    return statsMgr.getAllQueueJobCounts(queueNames, this.contextFactory.getStatsContext());
  }

  getQueueJobCounts(queueName: string) {
    return statsMgr.getQueueJobCounts(queueName, this.contextFactory.getStatsContext());
  }

  getMemoryStats() {
    return statsMgr.getMemoryStats(this.contextFactory.getStatsContext());
  }

  getStorageStatus(): { diskFull: boolean; error: string | null; since: number | null } {
    return this.storage?.getDiskFullStatus() ?? { diskFull: false, error: null, since: null };
  }

  flushPersistence(): number {
    return this.storage?.flushWriteBuffer() ?? 0;
  }

  compactMemory(): void {
    statsMgr.compactMemory(this.contextFactory.getStatsContext());
    this.dashboardEmit?.('memory:compacted', {});
  }
}
