import type { Job, JobId } from '../domain/types/job';
import type { JobLocation } from '../domain/types/queue';
import type { Shard } from '../domain/queue/shard';
import type { SqliteStorage } from '../infrastructure/persistence/sqlite';
import type { LockGuard, RWLock } from '../shared/lock';
import { processingShardIndex, shardIndex } from '../shared/hash';
import type { MapLike, SetLike } from '../shared/lru';

export interface FlowParentBackpatchContext {
  readonly storage: SqliteStorage | null;
  readonly customIdLock: RWLock;
  readonly shards: Shard[];
  readonly shardLocks: RWLock[];
  readonly processingShards: Map<JobId, Job>[];
  readonly processingLocks: RWLock[];
  readonly jobIndex: Map<JobId, JobLocation>;
  readonly completedJobsData: MapLike<JobId, Job>;
  readonly depCompletions: SetLike<JobId>;
}

function parentFromData(job: Job): string | null {
  if (!job.data || typeof job.data !== 'object' || Array.isArray(job.data)) return null;
  const value = (job.data as Record<string, unknown>).__parentId;
  return value === undefined || value === null ? null : String(value);
}

/** Reject a backpatch that would steal a child from an existing real parent. */
export function assertFlowParentOwnership(child: Job, parentId: JobId): void {
  const candidates = [child.parentId ? String(child.parentId) : null, parentFromData(child)];
  for (const owner of candidates) {
    if (owner && owner !== 'pending' && owner !== String(parentId)) {
      throw new Error(`Child job ${String(child.id)} already belongs to parent ${owner}`);
    }
  }
}

export function isDeclaredFlowChild(parent: Job, childId: JobId): boolean {
  return parent.childrenIds.includes(childId);
}

export function canAcceptRemovedFlowChild(
  parent: Job | null,
  childId: JobId,
  depCompletions: SetLike<JobId>
): boolean {
  return Boolean(parent && isDeclaredFlowChild(parent, childId) && depCompletions.has(childId));
}

function queuedJob(
  id: JobId,
  location: Extract<JobLocation, { type: 'queue' }>,
  ctx: FlowParentBackpatchContext
) {
  const shard = ctx.shards[location.shardIdx];
  return (
    shard.getQueue(location.queueName).find(id) ??
    shard.waitingDeps.get(id) ??
    shard.waitingChildren.get(id) ??
    null
  );
}

function dlqJob(id: JobId, queue: string, ctx: FlowParentBackpatchContext): Job | null {
  return ctx.shards[shardIndex(queue)].getDlq(queue).find((job) => job.id === id) ?? null;
}

export function flowChildFailureError(child: Job, shards: Shard[]): string | undefined {
  const entry = shards[shardIndex(child.queue)]
    .getDlqEntries(child.queue)
    .find((candidate) => candidate.job.id === child.id);
  return entry?.error ?? undefined;
}

/** Resolve the canonical in-memory object while all relevant locks are held. */
function lockedJob(id: JobId, fallback: Job, ctx: FlowParentBackpatchContext): Job {
  const location = ctx.jobIndex.get(id);
  if (!location) return ctx.completedJobsData.get(id) ?? fallback;
  if (location.type === 'queue') return queuedJob(id, location, ctx) ?? fallback;
  if (location.type === 'processing') {
    return ctx.processingShards[location.shardIdx].get(id) ?? fallback;
  }
  if (location.type === 'completed') return ctx.completedJobsData.get(id) ?? fallback;
  return dlqJob(id, location.queueName, ctx) ?? fallback;
}

async function acquireBackpatchLocks(
  child: Job,
  parent: Job,
  ctx: FlowParentBackpatchContext
): Promise<LockGuard[]> {
  const guards: LockGuard[] = [];
  try {
    guards.push(await ctx.customIdLock.acquireWrite());
    const shardIndexes = [...new Set([shardIndex(child.queue), shardIndex(parent.queue)])].sort(
      (a, b) => a - b
    );
    for (const index of shardIndexes) guards.push(await ctx.shardLocks[index].acquireWrite());
    const processingIndexes = [
      ...new Set([processingShardIndex(child.id), processingShardIndex(parent.id)]),
    ].sort((a, b) => a - b);
    for (const index of processingIndexes) {
      guards.push(await ctx.processingLocks[index].acquireWrite());
    }
    return guards;
  } catch (error) {
    for (let index = guards.length - 1; index >= 0; index--) guards[index].release();
    throw error;
  }
}

function linkedData(child: Job, parent: Job): Record<string, unknown> {
  const source =
    child.data && typeof child.data === 'object' && !Array.isArray(child.data)
      ? (child.data as Record<string, unknown>)
      : {};
  return {
    ...source,
    __parentId: String(parent.id),
    __parentQueue: parent.queue,
  };
}

function mutateChild(target: Job, parentId: JobId, data: Record<string, unknown>): void {
  (target as { parentId: JobId | null }).parentId = parentId;
  (target as { data: unknown }).data = data;
}

/**
 * Replace the legacy `pending` parent marker for an edge already declared by
 * the parent. This operation never changes parent scheduling or terminal state.
 */
export async function backpatchDeclaredFlowChild(
  child: Job,
  parent: Job,
  ctx: FlowParentBackpatchContext
): Promise<Job> {
  const guards = await acquireBackpatchLocks(child, parent, ctx);
  try {
    const lockedParent = lockedJob(parent.id, parent, ctx);
    if (
      !ctx.jobIndex.has(child.id) &&
      !ctx.completedJobsData.has(child.id) &&
      ctx.depCompletions.has(child.id)
    ) {
      return child;
    }
    const lockedChild = lockedJob(child.id, child, ctx);
    if (!isDeclaredFlowChild(lockedParent, child.id)) {
      throw new Error(`Parent job ${String(parent.id)} is not linkable`);
    }
    assertFlowParentOwnership(lockedChild, parent.id);

    const data = linkedData(lockedChild, lockedParent);
    const linkedChild = { ...lockedChild, parentId: lockedParent.id, data };
    ctx.storage?.backpatchFlowChild(linkedChild, lockedChild.parentId);

    const targets = [child, lockedChild, dlqJob(child.id, child.queue, ctx)];
    for (const target of targets) {
      if (target) mutateChild(target, lockedParent.id, data);
    }
    return linkedChild;
  } finally {
    for (let index = guards.length - 1; index >= 0; index--) guards[index].release();
  }
}
