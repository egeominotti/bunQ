import type { DlqConfig, DlqEntry, DlqFilter } from '../../types/dlq';
import { FailureReason } from '../../types/dlq';
import type { Job, JobId } from '../../types/job';
import type { StallConfig } from '../../types/stall';
import { ShardDependencies } from './dependencies';

/** Dead-letter and stall configuration operations. */
export class ShardDlq extends ShardDependencies {
  get dlq(): Map<string, DlqEntry[]> {
    const map = new Map<string, DlqEntry[]>();
    for (const queue of this.dlqManager.getQueueNames()) {
      map.set(queue, this.dlqManager.getEntries(queue));
    }
    for (const queue of this.queues.keys()) {
      if (!map.has(queue)) map.set(queue, []);
    }
    return map;
  }

  get dlqConfig(): Map<string, DlqConfig> {
    const map = new Map<string, DlqConfig>();
    for (const queue of this.getQueueNames()) {
      map.set(queue, this.dlqManager.getConfig(queue));
    }
    return map;
  }

  get stallConfig(): Map<string, StallConfig> {
    const map = new Map<string, StallConfig>();
    for (const queue of this.getQueueNames()) {
      map.set(queue, this.dlqManager.getStallConfig(queue));
    }
    return map;
  }

  getDlqConfig(queue: string): DlqConfig {
    return this.dlqManager.getConfig(queue);
  }
  setDlqConfig(queue: string, config: Partial<DlqConfig>): void {
    this.dlqManager.setConfig(queue, config);
  }
  getStallConfig(queue: string): StallConfig {
    return this.dlqManager.getStallConfig(queue);
  }
  setStallConfig(queue: string, config: Partial<StallConfig>): void {
    this.dlqManager.setStallConfig(queue, config);
  }
  addToDlq(
    job: Job,
    reason: FailureReason = FailureReason.Unknown,
    error: string | null = null
  ): DlqEntry {
    return this.dlqManager.add(job, reason, error);
  }
  restoreDlqEntry(queue: string, entry: DlqEntry): void {
    this.dlqManager.restoreEntry(queue, entry);
  }
  getDlqEntries(queue: string): DlqEntry[] {
    return this.dlqManager.getEntries(queue);
  }
  getDlq(queue: string, count?: number): Job[] {
    return this.dlqManager.getJobs(queue, count);
  }
  getDlqFiltered(queue: string, filter: DlqFilter): DlqEntry[] {
    return this.dlqManager.getFiltered(queue, filter);
  }
  removeFromDlq(queue: string, jobId: JobId): DlqEntry | null {
    return this.dlqManager.remove(queue, jobId);
  }
  removeAllFromDlq(queue: string, jobId: JobId): DlqEntry[] {
    return this.dlqManager.removeAll(queue, jobId);
  }
  getAutoRetryEntries(queue: string, now: number = Date.now()): DlqEntry[] {
    return this.dlqManager.getAutoRetryEntries(queue, now);
  }
  getExpiredEntries(queue: string, now: number = Date.now()): DlqEntry[] {
    return this.dlqManager.getExpiredEntries(queue, now);
  }
  purgeExpired(queue: string, now: number = Date.now()): number {
    return this.dlqManager.purgeExpired(queue, now);
  }
  clearDlq(queue: string): number {
    return this.dlqManager.clear(queue);
  }
}
