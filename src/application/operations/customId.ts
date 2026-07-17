import type { Shard } from '../../domain/queue/shard';
import { type Job, type JobId, type JobInput, generateJobId, jobId } from '../../domain/types/job';
import type { JobLocation } from '../../domain/types/queue';
import type { SqliteStorage } from '../../infrastructure/persistence/sqlite';
import { shardIndex } from '../../shared/hash';
import type { MapLike, SetLike } from '../../shared/lru';

export interface CustomIdContext {
  storage: SqliteStorage | null;
  shards: Shard[];
  completedJobs: SetLike<JobId>;
  completedJobsData: MapLike<JobId, Job>;
  timedOutJobs?: SetLike<JobId>;
  jobResults: MapLike<JobId, unknown>;
  customIdMap: MapLike<string, JobId>;
  jobIndex: Map<JobId, JobLocation>;
}

export type CustomIdResult =
  | { skip: true; existingJob: Job }
  | { skip: true; existingId: JobId }
  | { skip: false; id: JobId };

/**
 * Enforce custom-ID idempotency while the caller holds the target shard lock.
 * Live generations are returned unchanged; terminal generations are retired
 * before the deterministic ID is admitted again.
 */
export function handleCustomId(
  input: JobInput,
  ctx: CustomIdContext,
  lockedShardIndexes: ReadonlySet<number>
): CustomIdResult {
  if (!input.customId) {
    return { skip: false, id: generateJobId() };
  }

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

  if (ctx.completedJobs.has(id)) {
    ctx.completedJobs.delete(id);
    ctx.completedJobsData.delete(id);
    ctx.jobResults.delete(id);
    ctx.jobIndex.delete(id);
    ctx.storage?.deleteJob(id);
  }

  const terminalLocation = ctx.jobIndex.get(id);
  if (terminalLocation?.type === 'dlq') {
    const terminalShardIdx = shardIndex(terminalLocation.queueName);
    if (!lockedShardIndexes.has(terminalShardIdx)) {
      throw new Error('Terminal custom ID shard must be locked before reuse');
    }
    const terminalShard = ctx.shards[terminalShardIdx];
    terminalShard.removeFromDlq(terminalLocation.queueName, id);
    ctx.jobIndex.delete(id);
    // deleteDlqEntry removes every persisted generation for this deterministic
    // id. The fresh jobs row is inserted only after this retirement completes.
    ctx.storage?.deleteDlqEntry(id);
  }

  // A durable jobs row may outlive its in-memory tracking after an interrupted
  // cleanup. The storage insert upserts that orphan without adding a DELETE to
  // every custom-ID push.
  ctx.timedOutJobs?.delete(id);
  ctx.customIdMap.set(input.customId, id);
  return { skip: false, id };
}
