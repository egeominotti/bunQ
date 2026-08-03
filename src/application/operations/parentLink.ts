import { MAX_TIMELINE_ENTRIES, type Job, type JobId, type JobInput } from '../../domain/types/job';
import type { Shard } from '../../domain/queue/shard';
import type { FlowLinkWriteMode } from '../../infrastructure/persistence/sqlite/flows';
import type { DurableAdmissionMetadata } from '../../infrastructure/persistence/sqlite';
import { shardIndex } from '../../shared/hash';
import {
  type DedupResult,
  replaceActiveDedupJob,
  replacePendingDedupJob,
} from './pushDeduplication';
import { admissionMetadata, type AcceptedCustomId, publishPreparedJob } from './pushAdmission';
import type { PushContext } from './pushContext';
import { prepareJobInsertion } from './pushInsert';
import { parentLinkQueue } from './parentLinkInput';

type ParentMembership = 'queue' | 'waiting-deps' | 'waiting-children';

export interface ParentLinkPlan {
  readonly child: Job;
  readonly parent: Job;
  readonly linkedChild: Job;
  readonly linkedParent: Job;
  readonly parentMembership: ParentMembership;
  readonly childFinished: boolean;
}

function recordData(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function locateParent(
  parentId: JobId,
  ctx: PushContext
): {
  job: Job;
  membership: ParentMembership;
} {
  const location = ctx.jobIndex.get(parentId);
  if (!location) throw new Error(`Parent job not found: ${String(parentId)}`);
  if (location.type !== 'queue') {
    throw new Error(`Parent job ${String(parentId)} is not linkable`);
  }
  const shard = ctx.shards[location.shardIdx];
  const queued = shard.getQueue(location.queueName).find(parentId);
  if (queued) return { job: queued, membership: 'queue' };
  const waiting = shard.waitingDeps.get(parentId);
  if (waiting) return { job: waiting, membership: 'waiting-deps' };
  const waitingChildren = shard.waitingChildren.get(parentId);
  if (waitingChildren) return { job: waitingChildren, membership: 'waiting-children' };
  throw new Error(`Parent job ${String(parentId)} changed state while linking`);
}

/** Validate every parent before a batch mutates its accepted prefix. */
export function validateParentLinkInputs(inputs: readonly JobInput[], ctx: PushContext): void {
  for (const input of inputs) {
    const expectedQueue = parentLinkQueue(input);
    if (!input.parentId || !expectedQueue) continue;
    if (input.customId && String(input.parentId) === input.customId) {
      throw new Error('A job cannot be its own parent');
    }
    const { job: parent } = locateParent(input.parentId, ctx);
    if (parent.queue !== expectedQueue) {
      throw new Error(
        `Parent job ${String(input.parentId)} belongs to queue ${parent.queue}, not ${expectedQueue}`
      );
    }
  }
}

export function prepareParentLink(
  child: Job,
  ctx: PushContext,
  childFinished = false,
  parentIdOverride?: JobId,
  now = Date.now()
): ParentLinkPlan {
  const parentId = parentIdOverride ?? child.parentId;
  if (!parentId) throw new Error(`Job ${String(child.id)} has no parent`);
  if (child.id === parentId) throw new Error('A job cannot be its own parent');
  const { job: parent, membership } = locateParent(parentId, ctx);
  const childrenIds = parent.childrenIds.includes(child.id)
    ? [...parent.childrenIds]
    : [...parent.childrenIds, child.id];
  const dependsOn = parent.dependsOn.includes(child.id)
    ? [...parent.dependsOn]
    : [...parent.dependsOn, child.id];
  const childData = {
    ...recordData(child.data),
    __parentId: String(parent.id),
    __parentQueue: parent.queue,
  };
  const parentData = { ...recordData(parent.data), __childrenIds: childrenIds.map(String) };
  const timeline = [...parent.timeline];
  if (
    !childFinished &&
    timeline.at(-1)?.state !== 'waiting-children' &&
    timeline.length < MAX_TIMELINE_ENTRIES
  ) {
    timeline.push({ state: 'waiting-children', timestamp: now });
  }
  return {
    child,
    parent,
    linkedChild: { ...child, parentId: parent.id, data: childData },
    linkedParent: { ...parent, childrenIds, dependsOn, data: parentData, timeline },
    parentMembership: membership,
    childFinished,
  };
}

/** Publish a persisted link while the parent and child queue shards are locked. */
export function commitParentLink(plan: ParentLinkPlan, ctx: PushContext): void {
  const { child, parent, linkedChild, linkedParent } = plan;
  (child as { parentId: JobId | null }).parentId = linkedChild.parentId;
  (child as { data: unknown }).data = linkedChild.data;
  parent.childrenIds = linkedParent.childrenIds;
  (parent as { dependsOn: JobId[] }).dependsOn = linkedParent.dependsOn;
  (parent as { data: unknown }).data = linkedParent.data;
  parent.timeline = linkedParent.timeline;

  if (!plan.childFinished) {
    const parentShard = ctx.shards[shardIndex(parent.queue)];
    if (plan.parentMembership === 'queue') {
      if (parentShard.getQueue(parent.queue).remove(parent.id)) {
        parentShard.decrementQueued(parent.id);
        parentShard.removeFromTemporalIndex(parent.id);
      }
      parentShard.waitingDeps.set(parent.id, parent);
    } else if (plan.parentMembership === 'waiting-children') {
      parentShard.waitingChildren.delete(parent.id);
      parentShard.waitingDeps.set(parent.id, parent);
    }
    parentShard.registerDependencies(parent.id, linkedParent.dependsOn);
  }

  ctx.dependencyResults.registerConsumer(parent.id, linkedParent.dependsOn);
  for (const dependencyId of linkedParent.dependsOn) {
    if (ctx.jobResults.has(dependencyId)) {
      ctx.dependencyResults.retain(dependencyId, ctx.jobResults.get(dependencyId));
    }
  }
}

export function parentLinkState(
  plan: ParentLinkPlan
): 'waiting-children' | 'waiting' | 'prioritized' | 'delayed' {
  if (!plan.childFinished) return 'waiting-children';
  if (plan.parent.runAt > Date.now()) return 'delayed';
  return plan.parent.priority > 0 ? 'prioritized' : 'waiting';
}

function persistLink(
  plan: ParentLinkPlan,
  ctx: PushContext,
  mode: FlowLinkWriteMode,
  admission?: DurableAdmissionMetadata
): void {
  ctx.storage?.commitFlowLink(
    plan.linkedChild,
    plan.linkedParent,
    parentLinkState(plan),
    mode,
    admission
  );
}

/** Persist and publish a new child and its existing parent as one locked operation. */
export function acceptParentedJob(options: {
  job: Job;
  input: JobInput;
  target: { queue: string; shard: Shard; shardIdx: number };
  dedup: Exclude<DedupResult, { skip: true }>;
  customId: AcceptedCustomId;
  ctx: PushContext;
}): JobId[] {
  const { job, input, target, dedup, customId, ctx } = options;
  const plan = prepareParentLink(job, ctx);
  let released: JobId[] = [];
  if (dedup.replacement) {
    const replacement = dedup.replacement;
    released = replacePendingDedupJob(replacement, job, input, target, {
      ctx,
      customId,
      persist: (admission) => {
        persistLink(plan, ctx, { type: 'replace', previousJobId: replacement.job.id }, admission);
      },
    });
  } else if (dedup.activeOwnerId) {
    const activeOwnerId = dedup.activeOwnerId;
    replaceActiveDedupJob(activeOwnerId, job, input, target, {
      ctx,
      customId,
      persist: (admission) => {
        persistLink(
          plan,
          ctx,
          { type: 'transfer-active', previousJobId: activeOwnerId },
          admission
        );
      },
    });
  } else {
    const prepared = prepareJobInsertion(job, ctx);
    persistLink(plan, ctx, { type: 'insert' }, admissionMetadata(customId, prepared));
    publishPreparedJob({ job, input, target, customId, prepared, ctx });
  }
  commitParentLink(plan, ctx);
  return released;
}
