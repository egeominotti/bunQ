import type { Job, JobId } from '../../domain/types/job';
import { EventType } from '../../domain/types/queue';
import { shardIndex } from '../../shared/hash';
import { withWriteLock } from '../../shared/lock';
import { QueueManagerFlowFailures } from './flow-failures';

export class QueueManagerFlowOptions extends QueueManagerFlowFailures {
  protected override async onChildDependencyOption(
    childJob: Job,
    error: string | undefined
  ): Promise<void> {
    if (!childJob.parentId) return;
    if (childJob.continueParentOnFailure) {
      await this.continueParentOnChildFailure(childJob, error);
    } else {
      await this.removeChildFromParentDeps(childJob, error, childJob.ignoreDependencyOnFailure);
    }
  }

  protected async continueParentOnChildFailure(
    childJob: Job,
    error: string | undefined
  ): Promise<void> {
    const parentId = childJob.parentId;
    if (!parentId) return;
    const parentJob = await this.getJob(parentId);
    if (!parentJob || this.jobIndex.get(parentId)?.type !== 'queue') return;

    const childKey = `${childJob.queue}:${childJob.id}`;
    const existing = this.failedChildrenValues.get(parentId) ?? {};
    existing[childKey] = error ?? 'unknown error';
    this.failedChildrenValues.set(parentId, existing);

    const index = shardIndex(parentJob.queue);
    let releasedDependencies: JobId[] = [];
    await withWriteLock(this.shardLocks[index], () => {
      if (this.jobIndex.get(parentId)?.type !== 'queue') return;
      const shard = this.shards[index];
      releasedDependencies = [...parentJob.dependsOn];
      shard.unregisterDependencies(parentId, releasedDependencies);
      for (const dependency of releasedDependencies) {
        this.dependencyResults.releaseDependency(parentId, dependency);
      }
      (parentJob as { dependsOn: JobId[] }).dependsOn = [];
      this.storage?.updateFlowParentResolution(parentJob);
    });
    this.releaseCompletionPins(releasedDependencies);
    await this.promoteParentAfterChildFailure(parentId, parentJob, index);
  }

  protected async promoteParentAfterChildFailure(
    parentId: JobId,
    parentJob: Job,
    index: number
  ): Promise<void> {
    await new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, 0);
      timer.unref?.();
    });

    let promoted = false;
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
      if (!queue.find(parentId)) {
        const now = Date.now();
        parentJob.runAt = now;
        queue.push(parentJob);
        shard.incrementQueued(parentId, false, parentJob.createdAt, parentJob.queue, now);
        this.jobIndex.set(parentId, {
          type: 'queue',
          shardIdx: index,
          queueName: parentJob.queue,
        });
        this.storage?.updateFlowParentResolution(parentJob);
        shard.notify(parentJob.queue);
        promoted = true;
      }
    });
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
  }

  protected async removeChildFromParentDeps(
    childJob: Job,
    error: string | undefined,
    storeIgnored: boolean
  ): Promise<void> {
    const parentId = childJob.parentId;
    if (!parentId) return;
    const parentJob = await this.getJob(parentId);
    if (!parentJob || this.jobIndex.get(parentId)?.type !== 'queue') return;

    if (storeIgnored) {
      const childKey = `${childJob.queue}:${childJob.id}`;
      const existing = this.ignoredChildrenFailures.get(parentId) ?? {};
      existing[childKey] = error ?? 'unknown error';
      this.ignoredChildrenFailures.set(parentId, existing);
    }

    const index = shardIndex(parentJob.queue);
    let readyToPromote = false;
    let releasedDependency: JobId | null = null;
    await withWriteLock(this.shardLocks[index], () => {
      if (this.jobIndex.get(parentId)?.type !== 'queue') return;
      const shard = this.shards[index];
      if (!shard.waitingDeps.has(parentId)) return;
      const dependencyIndex = parentJob.dependsOn.indexOf(childJob.id);
      if (dependencyIndex !== -1) {
        parentJob.dependsOn.splice(dependencyIndex, 1);
        shard.unregisterDependencies(parentId, [childJob.id]);
        releasedDependency = childJob.id;
        this.dependencyResults.releaseDependency(parentId, childJob.id);
        this.storage?.updateFlowParentResolution(parentJob);
      }
      readyToPromote =
        parentJob.dependsOn.length === 0 ||
        parentJob.dependsOn.every((dependency) => this.completedJobs.has(dependency));
    });
    if (releasedDependency) this.releaseCompletionPins([releasedDependency]);
    if (readyToPromote) {
      await this.promoteParentAfterChildFailure(parentId, parentJob, index);
    }
    if (!storeIgnored) this.storage?.deleteFlowFailure(parentId, childJob.id);
  }
}
