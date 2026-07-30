import type { Job, JobId } from '../domain/types/job';
import { FailureReason } from '../domain/types/dlq';
import type { JobLocation } from '../domain/types/queue';
import type { Shard } from '../domain/queue/shard';
import type { SqliteStorage } from '../infrastructure/persistence/sqlite';
import { shardIndex } from '../shared/hash';
import type { SetLike } from '../shared/lru';
import type { DependencyResultTracker } from './dependencyResultTracker';

export interface FlowFailureRecoveryContext {
  readonly storage: SqliteStorage;
  readonly shards: Shard[];
  readonly jobIndex: Map<JobId, JobLocation>;
  readonly completedJobs: SetLike<JobId>;
  readonly dependencyResults: DependencyResultTracker;
  readonly failedChildrenValues: Map<JobId, Record<string, string>>;
  readonly ignoredChildrenFailures: Map<JobId, Record<string, string>>;
}

function findQueuedJob(ctx: FlowFailureRecoveryContext, id: JobId): Job | null {
  const location = ctx.jobIndex.get(id);
  if (location?.type !== 'queue') return null;
  const shard = ctx.shards[location.shardIdx];
  return (
    shard.getQueue(location.queueName).find(id) ??
    shard.waitingDeps.get(id) ??
    shard.waitingChildren.get(id) ??
    null
  );
}

function promote(parent: Job, ctx: FlowFailureRecoveryContext): void {
  const idx = shardIndex(parent.queue);
  const shard = ctx.shards[idx];
  if (shard.waitingDeps.delete(parent.id)) {
    shard.unregisterDependencies(parent.id, parent.dependsOn);
  }
  shard.waitingChildren.delete(parent.id);
  const queue = shard.getQueue(parent.queue);
  if (!queue.find(parent.id)) {
    parent.runAt = Date.now();
    queue.push(parent);
    shard.incrementQueued(parent.id, false, parent.createdAt, parent.queue, parent.runAt);
    shard.notify(parent.queue);
  }
  ctx.storage.updateFlowParentResolution(parent);
}

function failParent(
  parent: Job,
  childId: JobId,
  error: string,
  ctx: FlowFailureRecoveryContext
): void {
  const shard = ctx.shards[shardIndex(parent.queue)];
  if (shard.waitingDeps.delete(parent.id)) {
    shard.unregisterDependencies(parent.id, parent.dependsOn);
  }
  shard.waitingChildren.delete(parent.id);
  const queued = shard.getQueue(parent.queue).remove(parent.id);
  if (queued) shard.decrementQueued(parent.id);
  const message = `Child job ${String(childId)} failed: ${error}`;
  const entry = shard.addToDlq(parent, FailureReason.Unknown, message);
  ctx.storage.commitFailedJob(parent.id, entry, null);
  ctx.jobIndex.set(parent.id, { type: 'dlq', queueName: parent.queue });
  ctx.dependencyResults.releaseConsumer(parent.id);
}

/** Replay the durable flow-failure outbox before workers can observe recovered jobs. */
export function recoverFlowFailures(ctx: FlowFailureRecoveryContext): void {
  for (const record of ctx.storage.loadFlowFailures()) {
    const parent = findQueuedJob(ctx, record.parentId);
    if (!parent) {
      if (ctx.jobIndex.get(record.parentId)?.type !== 'queue') {
        ctx.storage.deleteFlowFailure(record.parentId, record.childId);
      }
      continue;
    }

    const childKey = `${record.childQueue}:${String(record.childId)}`;
    if (record.mode === 'fail') {
      failParent(parent, record.childId, record.error, ctx);
      ctx.storage.deleteFlowFailure(record.parentId, record.childId);
      continue;
    }

    if (record.mode === 'continue') {
      const values = ctx.failedChildrenValues.get(parent.id) ?? {};
      values[childKey] = record.error;
      ctx.failedChildrenValues.set(parent.id, values);
      const dependencies = [...parent.dependsOn];
      ctx.shards[shardIndex(parent.queue)].unregisterDependencies(parent.id, dependencies);
      for (const dependency of dependencies) {
        ctx.dependencyResults.releaseDependency(parent.id, dependency);
      }
      (parent as { dependsOn: JobId[] }).dependsOn = [];
      promote(parent, ctx);
      continue;
    }

    if (record.mode === 'ignore') {
      const values = ctx.ignoredChildrenFailures.get(parent.id) ?? {};
      values[childKey] = record.error;
      ctx.ignoredChildrenFailures.set(parent.id, values);
    }

    const dependencyIndex = parent.dependsOn.indexOf(record.childId);
    if (dependencyIndex !== -1) {
      parent.dependsOn.splice(dependencyIndex, 1);
      ctx.shards[shardIndex(parent.queue)].unregisterDependencies(parent.id, [record.childId]);
      ctx.dependencyResults.releaseDependency(parent.id, record.childId);
      ctx.storage.updateFlowParentResolution(parent);
    }
    const ready =
      parent.dependsOn.length === 0 ||
      parent.dependsOn.every((dependency) => ctx.completedJobs.has(dependency));
    if (ready) promote(parent, ctx);
    if (record.mode === 'remove') {
      ctx.storage.deleteFlowFailure(record.parentId, record.childId);
    }
  }
}
