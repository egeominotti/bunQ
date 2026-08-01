import type { JobId } from '../../domain/types/job';
import { EventType } from '../../domain/types/queue';
import { processingShardIndex, shardIndex } from '../../shared/hash';
import type { LockGuard } from '../../shared/lock';
import { QueueManagerFlowOptions } from './flow-options';

export class QueueManagerDependencies extends QueueManagerFlowOptions {
  async getFailedChildrenValues(parentJobId: JobId): Promise<Record<string, string>> {
    return this.failedChildrenValues.get(parentJobId) ?? {};
  }

  async getIgnoredChildrenFailures(parentJobId: JobId): Promise<Record<string, string>> {
    return this.ignoredChildrenFailures.get(parentJobId) ?? {};
  }

  async removeChildDependency(childJobId: JobId): Promise<boolean> {
    const childJob = await this.getJob(childJobId);
    if (!childJob) throw new Error(`Job not found: ${childJobId}`);
    if (!childJob.parentId) throw new Error(`Job ${childJobId} has no parent`);

    const parentId = childJob.parentId;
    const parentJob = await this.getJob(parentId);
    if (!parentJob || this.jobIndex.get(parentId)?.type !== 'queue') return false;
    const childLocation = this.jobIndex.get(childJobId);
    if (childLocation?.type !== 'queue' && childLocation?.type !== 'processing') return false;
    const parentIndex = shardIndex(parentJob.queue);
    const shardIndexes = [...new Set([parentIndex, shardIndex(childJob.queue)])].sort(
      (a, b) => a - b
    );
    const processingIndexes =
      childLocation.type === 'processing' ? [processingShardIndex(childJobId)] : [];
    const guards: LockGuard[] = [];
    let removed = false;
    let promoted = false;
    let releasedDependencies: JobId[] = [];
    try {
      for (const index of shardIndexes) guards.push(await this.shardLocks[index].acquireWrite());
      for (const index of processingIndexes) {
        guards.push(await this.processingLocks[index].acquireWrite());
      }
      if (
        this.jobIndex.get(parentId)?.type !== 'queue' ||
        this.jobIndex.get(childJobId)?.type !== childLocation.type
      ) {
        return false;
      }

      const shard = this.shards[parentIndex];
      if (!shard.waitingDeps.has(parentId) || !parentJob.dependsOn.includes(childJobId)) {
        return false;
      }

      const unresolved = parentJob.dependsOn.filter(
        (dependency) =>
          dependency !== childJobId &&
          !this.completedJobs.has(dependency) &&
          !this.depCompletions.has(dependency)
      );
      releasedDependencies = parentJob.dependsOn.filter(
        (dependency) => !unresolved.includes(dependency)
      );
      const childrenIds = parentJob.childrenIds.filter((childId) => childId !== childJobId);
      const childData = { ...(childJob.data as Record<string, unknown>) };
      delete childData.__parentId;
      delete childData.__parentQueue;
      const parentData = { ...(parentJob.data as Record<string, unknown>) };
      if (childrenIds.length > 0) parentData.__childrenIds = childrenIds.map(String);
      else delete parentData.__childrenIds;
      const runAt = unresolved.length === 0 ? Date.now() : parentJob.runAt;
      const parentState =
        unresolved.length > 0
          ? 'waiting-children'
          : parentJob.priority > 0
            ? 'prioritized'
            : 'waiting';
      const detachedChild = { ...childJob, parentId: null, data: childData };
      const detachedParent = {
        ...parentJob,
        childrenIds,
        dependsOn: unresolved,
        data: parentData,
        runAt,
      };
      this.storage?.removeFlowLink(detachedChild, detachedParent, parentState);

      (childJob as { parentId: JobId | null }).parentId = null;
      (childJob as { data: unknown }).data = childData;
      parentJob.childrenIds = childrenIds;
      (parentJob as { dependsOn: JobId[] }).dependsOn = unresolved;
      (parentJob as { data: unknown }).data = parentData;
      parentJob.runAt = runAt;
      shard.unregisterDependencies(parentId, releasedDependencies);
      for (const dependency of releasedDependencies) {
        this.dependencyResults.releaseDependency(parentId, dependency);
      }

      if (unresolved.length === 0) {
        shard.waitingDeps.delete(parentId);
        shard.getQueue(parentJob.queue).push(parentJob);
        shard.incrementQueued(parentId, false, parentJob.createdAt, parentJob.queue, runAt);
        this.jobIndex.set(parentId, {
          type: 'queue',
          shardIdx: parentIndex,
          queueName: parentJob.queue,
        });
        shard.notify(parentJob.queue);
        promoted = true;
      }
      removed = true;
    } finally {
      for (let index = guards.length - 1; index >= 0; index--) guards[index].release();
    }
    this.releaseCompletionPins(releasedDependencies);
    if (promoted) {
      this.eventsManager.broadcast({
        eventType: 'waiting' as EventType,
        queue: parentJob.queue,
        jobId: parentId,
        timestamp: Date.now(),
        prev: 'waiting-children',
      });
    }
    return removed;
  }

  async removeUnprocessedChildren(parentJobId: JobId): Promise<void> {
    const parent = await this.getJob(parentJobId);
    if (!parent?.childrenIds || parent.childrenIds.length === 0) return;
    for (const childId of parent.childrenIds) {
      if (this.jobIndex.get(childId)?.type !== 'queue') continue;
      try {
        await this.cancel(childId);
      } catch {
        // Best effort: active and terminal children are intentionally retained.
      }
    }
  }
}
