import type { Shard } from '../../domain/queue/shard';
import {
  assertWellFormedJobId,
  type Job,
  type JobId,
  type JobInput,
  generateJobId,
  jobId,
} from '../../domain/types/job';
import type { JobLocation } from '../../domain/types/queue';
import type { JobLogEntry } from '../../domain/types/worker';
import type { SqliteStorage } from '../../infrastructure/persistence/sqlite';
import { shardIndex } from '../../shared/hash';
import type { MapLike, SetLike } from '../../shared/lru';
import type { DependencyCompletionTracker } from '../dependencyCompletions';
import type { RetiredTimeoutGeneration } from '../types/background';

export interface CustomIdContext {
  storage: SqliteStorage | null;
  shards: Shard[];
  completedJobs: SetLike<JobId>;
  completedJobsData: MapLike<JobId, Job>;
  depCompletions?: DependencyCompletionTracker;
  maxDependencyCompletions: number;
  timedOutJobs?: MapLike<JobId, RetiredTimeoutGeneration>;
  retiredTimeoutLeaseTokens?: MapLike<string, RetiredTimeoutGeneration>;
  retiredCronLeaseTokens?: MapLike<JobId, string>;
  jobResults: MapLike<JobId, unknown>;
  jobResultQueues: Map<JobId, string>;
  jobLogs: MapLike<JobId, JobLogEntry[]>;
  jobLogQueues: Map<JobId, string>;
  customIdMap: MapLike<string, JobId>;
  jobIndex: Map<JobId, JobLocation>;
}

export type CustomIdResult =
  | { skip: true; existingJob: Job }
  | { skip: true; existingId: JobId }
  | { skip: false; id: JobId; retirement?: CustomIdRetirement };

export interface CustomIdRetirement {
  readonly completed: boolean;
  readonly dlqQueue?: string;
  readonly dependencyCompletion: boolean;
}

/**
 * Inspect custom-ID idempotency while the caller holds the target shard lock.
 * Terminal retirement is deliberately deferred until persistence commits, so
 * a rejected durable generation cannot destroy the previous one.
 */
export function handleCustomId(
  input: JobInput,
  ctx: CustomIdContext,
  lockedShardIndexes: ReadonlySet<number>
): CustomIdResult {
  if (!input.customId) {
    return { skip: false, id: generateJobId() };
  }

  assertWellFormedJobId(input.customId, 'custom ID');
  const id = jobId(input.customId);
  const existing = ctx.customIdMap.get(input.customId);

  if (existing && !ctx.completedJobs.has(id)) {
    const location = ctx.jobIndex.get(existing);
    if (location?.type === 'queue') {
      if (!lockedShardIndexes.has(location.shardIdx)) {
        throw new Error('Live custom ID shard must be locked before lookup');
      }
      const existingShard = ctx.shards[location.shardIdx];
      const existingJob = existingShard.getQueue(location.queueName).find(existing);
      if (existingJob) {
        return { skip: true, existingJob };
      }
      if (existingShard.waitingDeps.has(existing)) {
        return { skip: true, existingId: id };
      }
    } else if (location?.type === 'processing') {
      return { skip: true, existingId: id };
    }
  }

  const buffered = ctx.storage?.getBufferedJob(id);
  const completed =
    ctx.completedJobs.has(id) ||
    Boolean(ctx.storage?.getCompletedJob(id)) ||
    Boolean(buffered && buffered.completedAt !== null);

  const terminalLocation = ctx.jobIndex.get(id);
  let dlqQueue: string | undefined;
  if (terminalLocation?.type === 'dlq') {
    const terminalShardIdx = shardIndex(terminalLocation.queueName);
    if (!lockedShardIndexes.has(terminalShardIdx)) {
      throw new Error('Terminal custom ID shard must be locked before reuse');
    }
    dlqQueue = terminalLocation.queueName;
  }

  let dependencyCompletion = false;
  if (ctx.depCompletions?.has(id)) {
    const hasUnresolvedConsumers = ctx.shards.some(
      (shard) => (shard.getJobsWaitingFor(id)?.size ?? 0) > 0
    );
    if (hasUnresolvedConsumers) {
      throw new Error(`Custom ID ${String(id)} still has unresolved dependency consumers`);
    }
    dependencyCompletion = true;
  }

  const retirement =
    completed || dlqQueue || dependencyCompletion
      ? { completed, dlqQueue, dependencyCompletion }
      : undefined;
  return { skip: false, id, retirement };
}

/** Publish custom-ID ownership after the matching storage admission succeeds. */
export function commitCustomIdAdmission(
  input: JobInput,
  decision: Exclude<CustomIdResult, { skip: true }>,
  ctx: CustomIdContext
): void {
  const { id, retirement } = decision;
  if (retirement?.completed) {
    ctx.completedJobs.delete(id);
    ctx.completedJobsData.delete(id);
    ctx.jobIndex.delete(id);
  }
  if (retirement?.dlqQueue) {
    const terminalShard = ctx.shards[shardIndex(retirement.dlqQueue)];
    terminalShard.removeFromDlq(retirement.dlqQueue, id);
    ctx.jobIndex.delete(id);
  }
  if (retirement?.dependencyCompletion) ctx.depCompletions?.delete(id);

  // A deterministic ID is a new generation after admission commits. No
  // generation-scoped result, log, or owner metadata may cross that boundary.
  if (input.customId) {
    ctx.jobResults.delete(id);
    ctx.jobResultQueues.delete(id);
    ctx.jobLogs.delete(id);
    ctx.jobLogQueues.delete(id);
  }

  // A recycled deterministic ID starts with no timeout or retired-lease state.
  const timedOutGeneration = ctx.timedOutJobs?.get(id);
  ctx.timedOutJobs?.delete(id);
  if (timedOutGeneration?.token) ctx.retiredTimeoutLeaseTokens?.delete(timedOutGeneration.token);
  ctx.retiredCronLeaseTokens?.delete(id);
  if (input.customId) ctx.customIdMap.set(input.customId, id);
}
