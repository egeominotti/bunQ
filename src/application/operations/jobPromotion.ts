/** Delayed-job promotion operations shared by embedded and server modes. */

import type { Job, JobId } from '../../domain/types/job';
import { shardIndex } from '../../shared/hash';
import { withWriteLock } from '../../shared/lock';
import type { JobManagementContext } from './jobManagement';

function compareOldestFirst(a: Job, b: Job): number {
  if (a.createdAt !== b.createdAt) return a.createdAt - b.createdAt;
  if (a.id === b.id) return 0;
  return a.id < b.id ? -1 : 1;
}

/** Promote one delayed job and persist the transition before resolving. */
export async function promoteJob(jobId: JobId, ctx: JobManagementContext): Promise<boolean> {
  const location = ctx.jobIndex.get(jobId);
  if (location?.type !== 'queue') return false;

  const promotedAt = Date.now();
  const success = await withWriteLock(ctx.shardLocks[location.shardIdx], () => {
    const shard = ctx.shards[location.shardIdx];
    const queue = shard.getQueue(location.queueName);
    const job = queue.find(jobId);
    if (!job || job.runAt <= promotedAt || !queue.updateRunAt(jobId, promotedAt)) return false;

    shard.promoteDelayed(jobId);
    shard.notify(location.queueName);
    return true;
  });

  if (success) ctx.storage?.updateRunAt(jobId, promotedAt);
  return success;
}

/** Promote the oldest delayed jobs from the live shard, never an eventual SQL listing. */
export async function promoteJobs(
  queueName: string,
  count: number | undefined,
  ctx: JobManagementContext
): Promise<number> {
  const idx = shardIndex(queueName);
  const promotedAt = Date.now();
  const promotedIds = await withWriteLock(ctx.shardLocks[idx], () => {
    const shard = ctx.shards[idx];
    const queue = shard.getQueue(queueName);
    const delayed = queue
      .values()
      .filter((job) => job.runAt > promotedAt)
      .sort(compareOldestFirst);
    const limit = count === undefined ? delayed.length : Math.max(0, Math.floor(count));
    const ids: JobId[] = [];

    for (let i = 0; i < Math.min(limit, delayed.length); i++) {
      const id = delayed[i].id;
      if (!queue.updateRunAt(id, promotedAt)) continue;
      shard.promoteDelayed(id);
      ids.push(id);
    }
    if (ids.length > 0) shard.notifyBatch(queueName, ids.length);
    return ids;
  });

  for (const id of promotedIds) ctx.storage?.updateRunAt(id, promotedAt);
  return promotedIds.length;
}
