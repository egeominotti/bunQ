import type { JobId, JobLock } from '../../domain/types/job';
import { jobId } from '../../domain/types/job';
import type { JobLogEntry } from '../../domain/types/worker';
import type { StallConfig } from '../../domain/types/stall';
import type { DlqConfig } from '../../domain/types/dlq';
import { shardIndex } from '../../shared/hash';
import { withWriteLock } from '../../shared/lock';
import { QueueManagerLimits } from './limits';

export class QueueManagerConfiguration extends QueueManagerLimits {
  getDeduplicationJobId(queue: string, key: string): string | null {
    return this.shards[shardIndex(queue)].getUniqueKeyEntry(queue, key)?.jobId ?? null;
  }

  async removeDeduplicationKey(queue: string, key: string): Promise<number> {
    const index = shardIndex(queue);
    return withWriteLock(this.shardLocks[index], () => {
      const shard = this.shards[index];
      const owner = shard.getUniqueKeyEntry(queue, key)?.jobId;
      if (!owner || !shard.releaseUniqueKeyIfOwned(queue, key, owner)) return 0;
      this.storage?.clearJobUniqueKey(owner);
      return 1;
    });
  }

  async removeJobDeduplicationKey(id: string): Promise<boolean> {
    const targetId = jobId(id);
    const job = await this.getJob(targetId);
    if (!job?.uniqueKey) return false;
    const uniqueKey = job.uniqueKey;
    const index = shardIndex(job.queue);
    return withWriteLock(this.shardLocks[index], () => {
      const removed = this.shards[index].releaseUniqueKeyIfOwned(job.queue, uniqueKey, targetId);
      if (removed) this.storage?.clearJobUniqueKey(targetId);
      return removed;
    });
  }

  getAllJobResults(): Map<JobId, unknown> {
    return new Map(this.jobResults.entries());
  }

  getAllJobLogs(): Map<JobId, JobLogEntry[]> {
    return new Map(this.jobLogs.entries());
  }

  getAllJobLocks(): Map<JobId, JobLock> {
    return this.jobLocks;
  }

  setStallConfig(queue: string, config: Record<string, unknown>): void {
    this.shards[shardIndex(queue)].setStallConfig(queue, config);
    this.persistQueueState(queue);
  }

  getStallConfig(queue: string): StallConfig {
    return this.shards[shardIndex(queue)].getStallConfig(queue);
  }

  setDlqConfig(queue: string, config: Record<string, unknown>): void {
    this.shards[shardIndex(queue)].setDlqConfig(queue, config);
    this.persistQueueState(queue);
  }

  getDlqConfig(queue: string): DlqConfig {
    return this.shards[shardIndex(queue)].getDlqConfig(queue);
  }

  getCloudTelemetry(queueNames: string[]) {
    const perQueue: Record<
      string,
      { uniqueKeys: number; activeGroups: number; waitingDeps: number; waitingChildren: number }
    > = {};

    for (const name of queueNames) {
      const shard = this.shards[shardIndex(name)];
      let waitingDeps = 0;
      for (const job of shard.waitingDeps.values()) {
        if (job.queue === name) waitingDeps++;
      }
      let waitingChildren = 0;
      for (const job of shard.waitingChildren.values()) {
        if (job.queue === name) waitingChildren++;
      }
      perQueue[name] = {
        uniqueKeys: shard.uniqueKeys.get(name)?.size ?? 0,
        activeGroups: shard.activeGroups.get(name)?.size ?? 0,
        waitingDeps,
        waitingChildren,
      };
    }

    return {
      perQueue,
      eventSubscribers: this.eventsManager.subscriberCount,
      pendingDepChecks: this.pendingDepChecks.size,
    };
  }
}
