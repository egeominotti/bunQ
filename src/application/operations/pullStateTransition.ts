/**
 * Atomic queue-state transitions used by single and batch pull orchestration.
 */

import type { Shard } from '../../domain/queue/shard';
import {
  type Job,
  type JobId,
  isExpired,
  isReady,
  MAX_TIMELINE_ENTRIES,
} from '../../domain/types/job';
import type { EventType, JobEvent, JobLocation } from '../../domain/types/queue';
import type { SqliteStorage } from '../../infrastructure/persistence/sqlite';
import { processingShardIndex } from '../../shared/hash';
import type { RWLock } from '../../shared/lock';
import { withWriteLock } from '../../shared/lock';

/** Dependencies shared by the single and batch pull paths. */
export interface PullContext {
  storage: SqliteStorage | null;
  shards: Shard[];
  shardLocks: RWLock[];
  processingShards: Map<JobId, Job>[];
  processingLocks: RWLock[];
  jobIndex: Map<JobId, JobLocation>;
  totalPulled: { value: bigint };
  broadcast: (event: {
    eventType: EventType;
    queue: string;
    jobId: JobId;
    timestamp: number;
  }) => void;
  broadcastBatch?: (events: readonly JobEvent[]) => void;
  dashboardEmit?: (event: string, data: Record<string, unknown>) => void;
}

export interface DequeueScan {
  parked: Job[];
  nextRunAt: number | null;
}

export type DequeueResult = { status: 'job'; job: Job } | { status: 'stop' };

export function createDequeueScan(): DequeueScan {
  return { parked: [], nextRunAt: null };
}

/** Restore physical heap membership without changing logical queue state. */
export function restoreParkedJobs(shard: Shard, queue: string, scan: DequeueScan): void {
  const priorityQueue = shard.getQueue(queue);
  for (const job of scan.parked) priorityQueue.push(job);
  scan.parked.length = 0;
}

/**
 * Remove the best eligible job while retaining ineligible roots in `scan`.
 *
 * A batch reuses one scan, because readiness cannot change while the synchronous
 * shard critical section is held: time is fixed and active FIFO groups only
 * grow. Its parked jobs therefore need to be restored only once after the batch.
 */
export function tryDequeueNextJob(
  shard: Shard,
  queue: string,
  now: number,
  ctx: PullContext,
  scan: DequeueScan
): DequeueResult {
  const priorityQueue = shard.getQueue(queue);

  while (true) {
    const job = priorityQueue.peek();
    if (!job) return { status: 'stop' };

    if (isExpired(job, now)) {
      // Persistence is removed first so a failed durable delete leaves the
      // in-memory job available for a later attempt.
      ctx.storage?.deleteJob(job.id);
      priorityQueue.pop();
      shard.decrementQueued(job.id);
      ctx.jobIndex.delete(job.id);
      ctx.dashboardEmit?.('job:expired', {
        queue,
        jobId: String(job.id),
        ttl: job.ttl,
        age: now - job.createdAt,
      });
      continue;
    }

    if (!isReady(job, now)) {
      const delayed = priorityQueue.pop();
      if (delayed) scan.parked.push(delayed);
      scan.nextRunAt = scan.nextRunAt === null ? job.runAt : Math.min(scan.nextRunAt, job.runAt);
      continue;
    }

    if (job.groupId && shard.isGroupActive(queue, job.groupId)) {
      const blocked = priorityQueue.pop();
      if (blocked) scan.parked.push(blocked);
      continue;
    }

    // Capacity is consumed only after an eligible job has been found.
    if (!shard.tryAcquireConcurrency(queue)) {
      ctx.dashboardEmit?.('concurrency:rejected', { queue });
      return { status: 'stop' };
    }
    if (!shard.tryAcquireRateLimit(queue)) {
      shard.releaseConcurrency(queue);
      ctx.dashboardEmit?.('ratelimit:rejected', { queue });
      return { status: 'stop' };
    }

    const dequeued = priorityQueue.pop();
    if (!dequeued) {
      shard.releaseConcurrency(queue);
      return { status: 'stop' };
    }
    shard.decrementQueued(dequeued.id);

    if (dequeued.groupId) shard.activateGroup(queue, dequeued.groupId);

    dequeued.startedAt = now;
    dequeued.lastHeartbeat = now;
    if (dequeued.timeline.length < MAX_TIMELINE_ENTRIES) {
      dequeued.timeline.push({ state: 'active', timestamp: now });
    }

    // Queue -> processing is atomic for observers while the shard lock is held.
    const procIdx = processingShardIndex(dequeued.id);
    ctx.processingShards[procIdx].set(dequeued.id, dequeued);
    ctx.jobIndex.set(dequeued.id, { type: 'processing', shardIdx: procIdx });

    return { status: 'job', job: dequeued };
  }
}

/** Restore a dequeued job when its handoff to the worker cannot complete. */
export async function requeueJob(
  job: Job,
  queue: string,
  idx: number,
  ctx: PullContext
): Promise<void> {
  const procIdx = processingShardIndex(job.id);
  let requeued = false;

  await withWriteLock(ctx.shardLocks[idx], () => {
    if (!ctx.processingShards[procIdx].has(job.id)) return;
    const shard = ctx.shards[idx];
    if (job.groupId) shard.releaseGroup(queue, job.groupId);
    shard.releaseConcurrency(queue);
    job.startedAt = null;
    shard.getQueue(queue).push(job);
    shard.incrementQueued(job.id, false, job.createdAt, queue, job.runAt);
    ctx.jobIndex.set(job.id, { type: 'queue', shardIdx: idx, queueName: queue });
    shard.notify(queue);
    requeued = true;
  });

  if (!requeued) return;

  await withWriteLock(ctx.processingLocks[procIdx], () => {
    // A concurrent pull may already have moved the same object back to active.
    if (ctx.jobIndex.get(job.id)?.type !== 'processing') {
      ctx.processingShards[procIdx].delete(job.id);
    }
  });
}
