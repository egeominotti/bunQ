import {
  type Job,
  type JobId,
  type JobInput,
  createJob,
  normalizeJobPayload,
} from '../../domain/types/job';
import {
  assertGroupPriority,
  assertOptionalGroupId,
  assertPositiveSafeInteger,
} from '../../domain/types/group';
import { EventType } from '../../domain/types/queue';
import { shardIndex } from '../../shared/hash';
import { releaseDependencyCompletionPins } from '../dependencyCompletions';
import { latencyTracker } from '../latencyTracker';
import { validateRepeatJobInput } from '../repeatJobs';
import { throughputTracker } from '../throughputTracker';
import { handleCustomId } from './customId';
import { acceptParentedJob, validateParentLinkInputs } from './parentLink';
import { isParentLinkInput } from './parentLinkInput';
import { acceptStandardJob, throwPushErrors } from './pushAdmission';
import type { PushContext } from './pushContext';
import {
  handleDeduplication,
  replaceActiveDedupJob,
  replacePendingDedupJob,
} from './pushDeduplication';
import { withPushWriteLocks } from './pushLocks';

interface PendingInsert {
  job: Job;
  durable: boolean;
  storageHandled: boolean;
  superseded: boolean;
}

/** Push an ordered batch while preserving its accepted-prefix contract. */
export async function pushJobBatch(
  queue: string,
  inputs: JobInput[],
  ctx: PushContext
): Promise<JobId[]> {
  for (const input of inputs) {
    assertOptionalGroupId(input.groupId);
    if (input.groupId !== undefined) assertGroupPriority(input.priority);
    if (input.groupMaxSize !== undefined) {
      assertPositiveSafeInteger(input.groupMaxSize, 'group.maxSize');
    }
    validateRepeatJobInput(input);
    normalizeJobPayload(input);
  }
  const startNs = Bun.nanoseconds();
  const now = Date.now();
  const idx = shardIndex(queue);
  const resultIds: JobId[] = [];
  const releasedDependencies: JobId[] = [];
  const acceptedJobs: Job[] = [];
  const jobsToInsert: PendingInsert[] = [];
  let jobsToInsertById: Map<JobId, PendingInsert> | null = null;
  let pendingInsertCount = 0;
  let batchError: unknown;

  const supersedePendingInsert = (id: JobId): void => {
    const index =
      jobsToInsertById ?? new Map(jobsToInsert.map((pending) => [pending.job.id, pending]));
    jobsToInsertById = index;
    const pending = index.get(id);
    if (!pending) return;
    pending.superseded = true;
    index.delete(id);
    pendingInsertCount--;
  };

  try {
    await withPushWriteLocks(queue, inputs, ctx, (lockedShardIndexes) => {
      const shard = ctx.shards[idx];
      validateParentLinkInputs(inputs, ctx);

      for (const input of inputs) {
        const linkParent = isParentLinkInput(input);
        const customIdResult = handleCustomId(input, ctx, lockedShardIndexes);
        if (customIdResult.skip) {
          resultIds.push(
            'existingJob' in customIdResult
              ? customIdResult.existingJob.id
              : customIdResult.existingId
          );
          continue;
        }

        const job = createJob(customIdResult.id, queue, input, now);
        const dedupResult = handleDeduplication(job, input, queue, shard, ctx);
        if (dedupResult.skip) {
          resultIds.push(dedupResult.existingId);
          continue;
        }
        if (
          job.groupId &&
          input.groupMaxSize !== undefined &&
          !dedupResult.replacement &&
          shard.getGroupJobsCount(queue, job.groupId) >= input.groupMaxSize
        ) {
          throw new Error(
            `Group ${job.groupId} has reached its maximum size of ${input.groupMaxSize}`
          );
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
          if (dedupResult.replacement) {
            supersedePendingInsert(dedupResult.replacement.job.id);
          }
          storageHandled = true;
        } else if (dedupResult.replacement) {
          releasedDependencies.push(
            ...replacePendingDedupJob(dedupResult.replacement, job, input, target, {
              ctx,
              customId: customIdResult,
            })
          );
          supersedePendingInsert(dedupResult.replacement.job.id);
          storageHandled = true;
        } else if (dedupResult.activeOwnerId) {
          replaceActiveDedupJob(dedupResult.activeOwnerId, job, input, target, {
            ctx,
            customId: customIdResult,
          });
          storageHandled = true;
        } else {
          storageHandled = acceptStandardJob(
            job,
            input,
            target,
            customIdResult,
            ctx
          ).storageHandled;
        }
        const pending = {
          job,
          durable: Boolean(input.durable),
          storageHandled,
          superseded: false,
        };
        jobsToInsert.push(pending);
        jobsToInsertById?.set(job.id, pending);
        pendingInsertCount++;
        acceptedJobs.push(job);
        resultIds.push(job.id);
      }
    });
  } catch (error) {
    batchError = error;
  }

  let cleanupError: unknown;
  try {
    releaseDependencyCompletionPins(releasedDependencies, ctx);
  } catch (error) {
    cleanupError = error;
  }

  let persistenceError: unknown;
  if (acceptedJobs.length > 0) {
    const pendingPersistence = jobsToInsert.filter(
      ({ storageHandled, superseded }) => !storageHandled && !superseded
    );
    const durableJobs = pendingPersistence.filter(({ durable }) => durable).map(({ job }) => job);
    const bufferedJobs = pendingPersistence.filter(({ durable }) => !durable).map(({ job }) => job);
    try {
      if (bufferedJobs.length > 0) ctx.storage?.insertJobsBatch(bufferedJobs);
      if (durableJobs.length > 0) ctx.storage?.insertJobsBatch(durableJobs, true);
    } catch (error) {
      persistenceError = error;
    }
    ctx.totalPushed.value += BigInt(acceptedJobs.length);
    throughputTracker.pushRate.increment(acceptedJobs.length);

    const events = acceptedJobs.map((job) => ({
      eventType: 'pushed' as EventType,
      queue: job.queue,
      jobId: job.id,
      timestamp: now,
    }));
    if (ctx.broadcastBatch) ctx.broadcastBatch(events);
    else {
      for (const event of events) ctx.broadcast(event);
    }
  }
  if (pendingInsertCount > 0) ctx.shards[idx].notifyBatch(queue, pendingInsertCount);

  if (!batchError && !persistenceError && acceptedJobs.length > 0 && inputs.length > 1) {
    ctx.dashboardEmit?.('batch:pushed', {
      queue,
      total: inputs.length,
      inserted: acceptedJobs.length,
      duplicates: inputs.length - acceptedJobs.length,
    });
  }

  latencyTracker.push.observe((Bun.nanoseconds() - startNs) / 1e6);
  throwPushErrors(
    [batchError, cleanupError, persistenceError],
    'Batch push failed while finalizing its accepted prefix'
  );
  return resultIds;
}
