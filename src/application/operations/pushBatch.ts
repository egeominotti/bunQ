import {
  type Job,
  type JobId,
  type JobInput,
  createJob,
  normalizeJobPayload,
} from '../../domain/types/job';
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

/** Push an ordered batch while preserving its accepted-prefix contract. */
export async function pushJobBatch(
  queue: string,
  inputs: JobInput[],
  ctx: PushContext
): Promise<JobId[]> {
  for (const input of inputs) {
    validateRepeatJobInput(input);
    normalizeJobPayload(input);
  }
  const startNs = Bun.nanoseconds();
  const now = Date.now();
  const idx = shardIndex(queue);
  const resultIds: JobId[] = [];
  const releasedDependencies: JobId[] = [];
  const acceptedJobs: Job[] = [];
  const jobsToInsert: Array<{ job: Job; durable: boolean; storageHandled: boolean }> = [];
  let batchError: unknown;

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
          const supersededIndex = jobsToInsert.findIndex(
            ({ job: pending }) => pending.id === dedupResult.replacement?.job.id
          );
          if (supersededIndex !== -1) jobsToInsert.splice(supersededIndex, 1);
          storageHandled = true;
        } else if (dedupResult.replacement) {
          releasedDependencies.push(
            ...replacePendingDedupJob(dedupResult.replacement, job, input, target, {
              ctx,
              customId: customIdResult,
            })
          );
          const supersededIndex = jobsToInsert.findIndex(
            ({ job: pending }) => pending.id === dedupResult.replacement?.job.id
          );
          if (supersededIndex !== -1) jobsToInsert.splice(supersededIndex, 1);
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
        jobsToInsert.push({ job, durable: Boolean(input.durable), storageHandled });
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
    const pendingPersistence = jobsToInsert.filter(({ storageHandled }) => !storageHandled);
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
  if (jobsToInsert.length > 0) ctx.shards[idx].notifyBatch(queue, jobsToInsert.length);

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
