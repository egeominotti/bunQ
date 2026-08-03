import type { JobId, JobInput } from '../../domain/types/job';
import { jobId } from '../../domain/types/job';
import type { JobLocation } from '../../domain/types/queue';
import { shardIndex } from '../../shared/hash';
import type { LockGuard, RWLock } from '../../shared/lock';
import { isParentLinkInput } from './parentLinkInput';

interface PushLockContext {
  shardLocks: RWLock[];
  customIdLock: RWLock;
  jobIndex: Map<JobId, JobLocation>;
}

function requiredShardIndexes(
  queue: string,
  inputs: readonly JobInput[],
  jobIndex: Map<JobId, JobLocation>
): number[] {
  const indexes = new Set<number>([shardIndex(queue)]);
  for (const input of inputs) {
    if (input.customId) {
      const location = jobIndex.get(jobId(input.customId));
      if (location?.type === 'queue') {
        indexes.add(location.shardIdx);
      } else if (location?.type === 'dlq') {
        indexes.add(shardIndex(location.queueName));
      }
    }
    const parentLocation =
      input.parentId && isParentLinkInput(input) ? jobIndex.get(input.parentId) : undefined;
    if (parentLocation?.type === 'queue') indexes.add(parentLocation.shardIdx);
  }
  return [...indexes].sort((a, b) => a - b);
}

/**
 * Lock the target shard plus any shard owning a terminal custom-ID generation.
 * The required set is rechecked after acquisition because jobIndex may change
 * while an earlier lock is awaited.
 */
export async function withPushWriteLocks<T>(
  queue: string,
  inputs: readonly JobInput[],
  ctx: PushLockContext,
  fn: (lockedShardIndexes: ReadonlySet<number>) => T
): Promise<T> {
  while (true) {
    const customIdGuard = inputs.some((input) => input.customId || isParentLinkInput(input))
      ? await ctx.customIdLock.acquireWrite()
      : null;
    const guards: LockGuard[] = [];
    try {
      const indexes = requiredShardIndexes(queue, inputs, ctx.jobIndex);
      for (const index of indexes) {
        guards.push(await ctx.shardLocks[index].acquireWrite());
      }

      const locked = new Set(indexes);
      const current = requiredShardIndexes(queue, inputs, ctx.jobIndex);
      if (current.some((index) => !locked.has(index))) continue;
      return fn(locked);
    } finally {
      for (let i = guards.length - 1; i >= 0; i--) {
        guards[i].release();
      }
      customIdGuard?.release();
    }
  }
}
