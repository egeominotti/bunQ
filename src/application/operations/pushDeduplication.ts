import type { Shard } from '../../domain/queue/shard';
import type { Job, JobId, JobInput } from '../../domain/types/job';
import { EventType } from '../../domain/types/queue';
import type { DurableAdmissionMetadata } from '../../infrastructure/persistence/sqlite';
import { commitCustomIdAdmission } from './customId';
import { admissionMetadata, type AcceptedCustomId } from './pushAdmission';
import type { PushContext } from './push';
import { insertPreparedJobToShard, prepareJobInsertion } from './pushInsert';

type PendingLocation = 'queue' | 'waiting-deps' | 'waiting-children';

export interface DedupReplacement {
  job: Job;
  location: PendingLocation;
}

export interface DedupCommitOptions {
  readonly ctx: PushContext;
  readonly customId: AcceptedCustomId;
  readonly persist?: (admission?: DurableAdmissionMetadata) => void;
}

export type DedupResult =
  | { skip: true; existingId: JobId }
  | {
      skip: false;
      replacement?: DedupReplacement;
      activeOwnerId?: JobId;
    };

function findPendingOwner(
  shard: Shard,
  queue: string,
  ownerId: JobId
): DedupReplacement | undefined {
  const queued = shard.getQueue(queue).find(ownerId);
  if (queued) return { job: queued, location: 'queue' };
  const waitingDeps = shard.waitingDeps.get(ownerId);
  if (waitingDeps) return { job: waitingDeps, location: 'waiting-deps' };
  const waitingChildren = shard.waitingChildren.get(ownerId);
  if (waitingChildren) return { job: waitingChildren, location: 'waiting-children' };
  return undefined;
}

/** Inspect dedup state while deferring destructive replacement mutations. */
export function handleDeduplication(
  job: Job,
  input: JobInput,
  queue: string,
  shard: Shard,
  ctx: PushContext
): DedupResult {
  if (!job.uniqueKey) return { skip: false };

  const existingEntry = shard.getUniqueKeyEntry(queue, job.uniqueKey);
  if (!existingEntry) return { skip: false };

  if (input.dedup?.replace) {
    if (
      ctx.dependencyResults.hasConsumers(existingEntry.jobId) ||
      job.dependsOn.includes(existingEntry.jobId)
    ) {
      rollbackIncomingCustomId(job, ctx);
      throw new Error(
        `Cannot replace deduplicated job ${String(existingEntry.jobId)} because live jobs depend on it`
      );
    }

    const replacement = findPendingOwner(shard, queue, existingEntry.jobId);
    if (replacement) return { skip: false, replacement };

    if (ctx.jobIndex.get(existingEntry.jobId)?.type === 'processing') {
      return { skip: false, activeOwnerId: existingEntry.jobId };
    }

    return { skip: false };
  }

  const queued = shard.getQueue(queue).find(existingEntry.jobId);
  if (input.dedup?.extend && input.dedup.ttl) {
    shard.extendUniqueKeyTtl(queue, job.uniqueKey, input.dedup.ttl);
    if (queued) {
      ctx.dashboardEmit?.('job:deduplicated', {
        queue,
        jobId: String(existingEntry.jobId),
        strategy: 'extend',
      });
      return { skip: true, existingId: existingEntry.jobId };
    }
    throw new Error('Duplicate unique_key (extended TTL)');
  }

  if (queued || ctx.jobIndex.has(existingEntry.jobId)) {
    ctx.broadcast({
      eventType: EventType.Duplicated,
      queue,
      jobId: existingEntry.jobId,
      timestamp: Date.now(),
    });
    ctx.dashboardEmit?.('job:deduplicated', {
      queue,
      jobId: String(existingEntry.jobId),
      strategy: 'default',
    });
    return { skip: true, existingId: existingEntry.jobId };
  }

  return { skip: false };
}

/** Commit the RAM half of a replacement after persistence has succeeded. */
export function commitDedupReplacement(
  replacement: DedupReplacement,
  newJob: Job,
  queue: string,
  shard: Shard,
  ctx: PushContext
): JobId[] {
  const oldJob = replacement.job;
  let releasedDependencies: JobId[] = [];

  if (replacement.location === 'queue') {
    shard.getQueue(queue).remove(oldJob.id);
    shard.decrementQueued(oldJob.id);
    shard.removeFromTemporalIndex(oldJob.id);
  } else if (replacement.location === 'waiting-deps') {
    shard.waitingDeps.delete(oldJob.id);
    shard.unregisterDependencies(oldJob.id, oldJob.dependsOn);
    releasedDependencies = oldJob.dependsOn;
  } else {
    shard.waitingChildren.delete(oldJob.id);
  }

  ctx.jobIndex.delete(oldJob.id);
  ctx.dependencyResults.releaseConsumer(oldJob.id);
  if (oldJob.customId && ctx.customIdMap.get(oldJob.customId) === oldJob.id) {
    ctx.customIdMap.delete(oldJob.customId);
  }
  if (oldJob.uniqueKey) {
    shard.releaseUniqueKeyIfOwned(queue, oldJob.uniqueKey, oldJob.id);
  }
  if (newJob.uniqueKey) {
    shard.registerUniqueKeyWithTtl(
      queue,
      newJob.uniqueKey,
      newJob.id,
      newJob.deduplicationTtl ?? undefined
    );
  }
  return releasedDependencies;
}

/** Defensive cleanup for callers that may have pre-published custom-ID ownership. */
export function rollbackIncomingCustomId(job: Job, ctx: PushContext): void {
  if (job.customId && ctx.customIdMap.get(job.customId) === job.id) {
    ctx.customIdMap.delete(job.customId);
  }
}

/** Persist, detach, and insert a replacement without exposing a half-transition. */
export function replacePendingDedupJob(
  replacement: DedupReplacement,
  newJob: Job,
  input: JobInput,
  target: { queue: string; shard: Shard; shardIdx: number },
  options: DedupCommitOptions
): JobId[] {
  const { ctx, customId, persist } = options;
  const prepared = prepareJobInsertion(newJob, ctx);
  const admission = admissionMetadata(customId, prepared);
  if (persist) persist(admission);
  else ctx.storage?.replaceJob(replacement.job.id, newJob, input.durable, admission);

  commitCustomIdAdmission(input, customId, ctx);
  const released = commitDedupReplacement(replacement, newJob, target.queue, target.shard, ctx);
  insertPreparedJobToShard(newJob, target, ctx, prepared);
  return released;
}

/** Persist and publish a successor while leaving its active predecessor live. */
export function replaceActiveDedupJob(
  activeOwnerId: JobId,
  newJob: Job,
  input: JobInput,
  target: { queue: string; shard: Shard; shardIdx: number },
  options: DedupCommitOptions
): void {
  const { ctx, customId, persist } = options;
  const prepared = prepareJobInsertion(newJob, ctx);
  const admission = admissionMetadata(customId, prepared);
  if (persist) persist(admission);
  else ctx.storage?.transferActiveDedupJob(activeOwnerId, newJob, input.durable, admission);

  commitCustomIdAdmission(input, customId, ctx);
  if (newJob.uniqueKey) {
    target.shard.releaseUniqueKeyIfOwned(target.queue, newJob.uniqueKey, activeOwnerId);
    target.shard.registerUniqueKeyWithTtl(
      target.queue,
      newJob.uniqueKey,
      newJob.id,
      newJob.deduplicationTtl ?? undefined
    );
  }
  insertPreparedJobToShard(newJob, target, ctx, prepared);
}
