import type { Job, JobId } from '../../../domain/types/job';
import {
  isCorruptDependsOn,
  persistedJobState,
} from '../../../infrastructure/persistence/sqliteSerializer';
import { shardIndex } from '../../../shared/hash';
import { checkpointDependencyPromotion, dependencyReadyState } from '../../dependencyProcessor';
import type { BackgroundContext } from '../../types';
import { quarantineCorruptDependsOn, RECOVERY_BATCH_SIZE } from './shared';

export function recoverPendingJobs(
  ctx: BackgroundContext,
  completedInDatabase: Set<JobId>,
  now: number
): void {
  if (!ctx.storage) return;
  let offset = 0;
  const corruptPendingJobs: Job[] = [];

  while (true) {
    const jobs = ctx.storage.loadPendingJobs(RECOVERY_BATCH_SIZE, offset);
    if (jobs.length === 0) break;

    for (const job of jobs) {
      const shardIndexValue = shardIndex(job.queue);
      const shard = ctx.shards[shardIndexValue];

      if (isCorruptDependsOn(job)) {
        corruptPendingJobs.push(job);
        continue;
      }

      const hasDependencies = job.dependsOn && job.dependsOn.length > 0;
      const recoveredState = persistedJobState(job);
      const wasAlreadyPromoted =
        recoveredState === 'waiting' ||
        recoveredState === 'prioritized' ||
        recoveredState === 'delayed';
      const needsWaitingDeps =
        hasDependencies &&
        !wasAlreadyPromoted &&
        !job.dependsOn.every(
          (dependencyId) =>
            ctx.completedJobs.has(dependencyId) || completedInDatabase.has(dependencyId)
        );
      const manuallyWaitingChildren = recoveredState === 'waiting-children' && !hasDependencies;

      if (manuallyWaitingChildren) {
        shard.waitingChildren.set(job.id, job);
      } else if (needsWaitingDeps) {
        shard.waitingDeps.set(job.id, job);
        shard.registerDependencies(job.id, job.dependsOn);
      } else {
        shard.getQueue(job.queue).push(job);
        const state = dependencyReadyState(job, now);
        const isDelayed = state === 'delayed';
        shard.incrementQueued(job.id, isDelayed, job.createdAt, job.queue, job.runAt);
        if (hasDependencies) checkpointDependencyPromotion(job, state, now, ctx.storage);
      }

      ctx.jobIndex.set(job.id, {
        type: 'queue',
        shardIdx: shardIndexValue,
        queueName: job.queue,
      });
      ctx.dependencyResults.registerConsumer(job.id, job.dependsOn);
      for (const dependencyId of job.dependsOn) {
        if (ctx.jobResults.has(dependencyId)) {
          ctx.dependencyResults.retain(dependencyId, ctx.jobResults.get(dependencyId));
        }
      }

      if (job.customId) ctx.customIdMap.set(job.customId, job.id);
      if (job.uniqueKey) {
        shard.registerUniqueKeyWithTtl(
          job.queue,
          job.uniqueKey,
          job.id,
          job.deduplicationTtl ?? undefined
        );
      }
      ctx.registerQueueName(job.queue);
    }

    offset += jobs.length;
    if (jobs.length < RECOVERY_BATCH_SIZE) break;
  }

  for (const job of corruptPendingJobs) quarantineCorruptDependsOn(ctx, job);
}
