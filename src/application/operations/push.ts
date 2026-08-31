/**
 * Push Operations
 * Single-job push logic and stable batch re-export
 */

import { type Job, type JobId, type JobInput, createJob } from '../../domain/types/job';
import { assertOptionalGroupId } from '../../domain/types/group';
import { EventType } from '../../domain/types/queue';
import { shardIndex } from '../../shared/hash';
import { latencyTracker } from '../latencyTracker';
import { throughputTracker } from '../throughputTracker';
import { handleCustomId } from './customId';
import { acceptParentedJob, validateParentLinkInputs } from './parentLink';
import { isParentLinkInput } from './parentLinkInput';
import { acceptStandardJob, throwPushErrors } from './pushAdmission';
import { withPushWriteLocks } from './pushLocks';
import { releaseDependencyCompletionPins } from '../dependencyCompletions';
import {
  handleDeduplication,
  replaceActiveDedupJob,
  replacePendingDedupJob,
} from './pushDeduplication';
import type { PushContext } from './pushContext';
import { validateRepeatJobInput } from '../repeatJobs';

export type { PushContext } from './pushContext';
export { pushJobBatch } from './pushBatch';

/**
 * Push a single job to queue
 * NOTE: customId check happens INSIDE lock to prevent race conditions
 */
export async function pushJob(queue: string, input: JobInput, ctx: PushContext): Promise<Job> {
  assertOptionalGroupId(input.groupId);
  validateRepeatJobInput(input);
  const startNs = Bun.nanoseconds();
  const idx = shardIndex(queue);
  const now = Date.now();
  const releasedDependencies: JobId[] = [];
  let result: { job: Job; persisted: boolean; storageHandled?: boolean } | undefined;

  await withPushWriteLocks(queue, [input], ctx, (lockedShardIndexes) => {
    const shard = ctx.shards[idx];
    validateParentLinkInputs([input], ctx);
    const linkParent = isParentLinkInput(input);

    // Check custom ID idempotency INSIDE lock to prevent race conditions
    const customIdResult = handleCustomId(input, ctx, lockedShardIndexes);
    if (customIdResult.skip) {
      // Idempotent re-add: return the live queued job if we have it, otherwise
      // (active / waiting-children) a placeholder carrying the existing id so the
      // caller sees the right id without inserting a duplicate row.
      result = {
        job:
          'existingJob' in customIdResult
            ? customIdResult.existingJob
            : createJob(customIdResult.existingId, queue, input, now),
        persisted: false,
      };
      return;
    }

    const job = createJob(customIdResult.id, queue, input, now);

    // Check deduplication
    const dedupResult = handleDeduplication(job, input, queue, shard, ctx);
    if (dedupResult.skip) {
      const existingJob = shard.getQueue(queue).find(dedupResult.existingId);
      // Return existing waiting job if found; if active (not in queue), use a
      // placeholder with the correct ID so the caller sees the right ID without
      // inserting a duplicate.
      result = {
        job: existingJob ?? { ...job, id: dedupResult.existingId },
        persisted: false,
      };
      return;
    }
    shard.assignGroupFifoOrder(job);

    const target = { queue, shard, shardIdx: idx };
    let storageHandled = false;
    if (linkParent) {
      releasedDependencies.push(
        ...acceptParentedJob({
          job,
          input,
          target,
          dedup: dedupResult,
          customId: customIdResult,
          ctx,
        })
      );
    } else if (dedupResult.replacement) {
      releasedDependencies.push(
        ...replacePendingDedupJob(dedupResult.replacement, job, input, target, {
          ctx,
          customId: customIdResult,
        })
      );
    } else if (dedupResult.activeOwnerId) {
      replaceActiveDedupJob(dedupResult.activeOwnerId, job, input, target, {
        ctx,
        customId: customIdResult,
      });
    } else {
      storageHandled = acceptStandardJob(job, input, target, customIdResult, ctx).storageHandled;
    }
    shard.notify(queue);
    result = {
      job,
      persisted: true,
      storageHandled: Boolean(
        storageHandled || linkParent || dedupResult.replacement || dedupResult.activeOwnerId
      ),
    };
  });

  let cleanupError: unknown;
  try {
    releaseDependencyCompletionPins(releasedDependencies, ctx);
  } catch (error) {
    cleanupError = error;
  }

  if (!result) {
    if (cleanupError) throw cleanupError;
    console.error('[Push] Push failed unexpectedly', { queue, input });
    throw new Error('Push failed');
  }

  let persistenceError: unknown;
  if (result.persisted) {
    try {
      if (!result.storageHandled) ctx.storage?.insertJob(result.job, input.durable);
    } catch (error) {
      persistenceError = error;
    }
    ctx.totalPushed.value++;
    throughputTracker.pushRate.increment();
    ctx.broadcast({
      eventType: 'pushed' as EventType,
      queue,
      jobId: result.job.id,
      timestamp: now,
    });
  }

  latencyTracker.push.observe((Bun.nanoseconds() - startNs) / 1e6);
  throwPushErrors([cleanupError, persistenceError], 'Push failed while finalizing an accepted job');
  return result.job;
}
