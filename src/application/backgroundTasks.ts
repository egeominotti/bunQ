/**
 * Background Tasks - Periodic maintenance operations
 * Orchestrates stall detection, cleanup, recovery, DLQ maintenance
 */

import { queueLog } from '../shared/logger';
import { shardIndex } from '../shared/hash';
import { FailureReason, createDlqEntry } from '../domain/types/dlq';
import { calculateBackoff } from '../domain/types/job';
import * as dlqOps from './dlqManager';
import { checkExpiredLocks } from './lockManager';
import { cleanup } from './cleanupTasks';
import { checkStalledJobs } from './stallDetection';
import {
  checkpointDependencyPromotion,
  dependencyReadyState,
  processPendingDependencies,
} from './dependencyProcessor';
import { handleTaskError, handleTaskSuccess, getTaskErrorStats } from './taskErrorTracking';
import { runMonitoringChecks } from './monitoringChecks';
import {
  isCorruptDependsOn,
  persistedJobState,
} from '../infrastructure/persistence/sqliteSerializer';
import type { BackgroundContext, LockContext } from './types';
import type { CronScheduler } from '../infrastructure/scheduler/cronScheduler';
import type { Job } from '../domain/types/job';
import { reconcileDependencyCompletionPins } from './dependencyCompletions';

export { getTaskErrorStats };

/** Background task handles for cleanup */
export interface BackgroundTaskHandles {
  cleanupInterval: ReturnType<typeof setInterval>;
  timeoutInterval: ReturnType<typeof setInterval>;
  depCheckInterval: ReturnType<typeof setInterval>;
  stallCheckInterval: ReturnType<typeof setInterval>;
  dlqMaintenanceInterval: ReturnType<typeof setInterval>;
  lockCheckInterval: ReturnType<typeof setInterval>;
  cronScheduler: CronScheduler;
}

/**
 * Start all background tasks
 * Returns handles that can be used to stop tasks later
 */
export function startBackgroundTasks(
  ctx: BackgroundContext,
  cronScheduler: CronScheduler
): BackgroundTaskHandles {
  const cleanupInterval = setInterval(() => {
    cleanup(ctx)
      .then(() => {
        handleTaskSuccess('cleanup');
        // Run monitoring checks after cleanup (same interval)
        runMonitoringChecks({
          queueNamesCache: ctx.queueNamesCache,
          shards: ctx.shards,
          processingShards: ctx.processingShards,
          workerManager: ctx.workerManager,
          storage: ctx.storage,
          dashboardEmit: ctx.dashboardEmit,
          state: ctx.monitoringState,
        });
      })
      .catch((err: unknown) => {
        handleTaskError('cleanup', err);
      });
  }, ctx.config.cleanupIntervalMs);

  const timeoutInterval = setInterval(() => {
    checkJobTimeouts(ctx);
  }, ctx.config.jobTimeoutCheckMs);

  // Safety fallback: event-driven path in queueManager handles the fast path
  const depCheckInterval = setInterval(() => {
    if (ctx.pendingDepChecks.size === 0) return;
    processPendingDependencies(ctx)
      .then(() => {
        handleTaskSuccess('dependency');
      })
      .catch((err: unknown) => {
        handleTaskError('dependency', err);
      });
  }, ctx.config.dependencyCheckMs);

  const stallCheckInterval = setInterval(() => {
    checkStalledJobs(ctx);
  }, ctx.config.stallCheckMs);

  const dlqMaintenanceInterval = setInterval(() => {
    performDlqMaintenance(ctx);
  }, ctx.config.dlqMaintenanceMs);

  // Lock expiration check runs at same interval as stall check
  const lockCheckInterval = setInterval(() => {
    checkExpiredLocks(getLockContext(ctx))
      .then(() => {
        handleTaskSuccess('lockExpiration');
      })
      .catch((err: unknown) => {
        handleTaskError('lockExpiration', err);
      });
  }, ctx.config.stallCheckMs);

  cronScheduler.start();

  return {
    cleanupInterval,
    timeoutInterval,
    depCheckInterval,
    stallCheckInterval,
    dlqMaintenanceInterval,
    lockCheckInterval,
    cronScheduler,
  };
}

/**
 * Stop all background tasks
 */
export function stopBackgroundTasks(handles: BackgroundTaskHandles): void {
  clearInterval(handles.cleanupInterval);
  clearInterval(handles.timeoutInterval);
  clearInterval(handles.depCheckInterval);
  clearInterval(handles.stallCheckInterval);
  clearInterval(handles.dlqMaintenanceInterval);
  clearInterval(handles.lockCheckInterval);
  handles.cronScheduler.stop();
}

/** Extract lock context from background context */
function getLockContext(ctx: BackgroundContext): LockContext {
  return {
    jobIndex: ctx.jobIndex,
    jobLocks: ctx.jobLocks,
    clientJobs: ctx.clientJobs,
    processingShards: ctx.processingShards,
    processingLocks: ctx.processingLocks,
    shards: ctx.shards,
    shardLocks: ctx.shardLocks,
    eventsManager: ctx.eventsManager,
    dashboardEmit: ctx.dashboardEmit,
    // #110: without storage, handleMaxStallsExceeded's saveDlqEntry/deleteJob
    // (the #97 fix) silently no-op via optional chaining on the ONLY
    // production path to checkExpiredLocks — the DLQ move never persists and
    // SQLite keeps an orphan `active` row. LockContext.storage is optional,
    // which is why this omission compiled unnoticed since 2.8.17.
    storage: ctx.storage,
  };
}

// ============ Job Timeouts ============

export function checkJobTimeouts(ctx: BackgroundContext): void {
  const now = Date.now();
  for (const procShard of ctx.processingShards) {
    for (const [jobId, job] of procShard) {
      if (job.timeout && job.startedAt && now - job.startedAt > job.timeout) {
        ctx.dashboardEmit?.('job:timeout', {
          jobId: String(jobId),
          queue: job.queue,
          timeout: job.timeout,
        });
        // Mark as timed-out BEFORE requeuing for retry, so a late ACK from the
        // (still-hung) worker that exceeded the deadline is discarded instead of
        // phantom-completing the job and skipping the retry. See ack-recovery in
        // queueManager.ack (timedOutJobs guard).
        ctx.timedOutJobs?.add(jobId);
        ctx.fail(jobId, 'Job timeout exceeded').catch((err: unknown) => {
          queueLog.error('Failed to mark timed out job as failed', {
            jobId: String(jobId),
            error: String(err),
          });
        });
      }
    }
  }
}

// ============ DLQ Maintenance ============

function performDlqMaintenance(ctx: BackgroundContext): void {
  const dlqCtx = {
    shards: ctx.shards,
    jobIndex: ctx.jobIndex,
    jobResults: ctx.jobResults,
    jobLogs: ctx.jobLogs,
    storage: ctx.storage,
  };

  for (const queueName of ctx.queueNamesCache) {
    try {
      const retried = dlqOps.processAutoRetry(queueName, dlqCtx);
      if (retried > 0) {
        ctx.dashboardEmit?.('dlq:auto-retried', { queue: queueName, count: retried });
      }
      const expired = dlqOps.purgeExpiredDlq(queueName, dlqCtx);
      if (expired > 0) ctx.dashboardEmit?.('dlq:expired', { queue: queueName, count: expired });
    } catch (err) {
      queueLog.error('DLQ maintenance failed', { queue: queueName, error: String(err) });
    }
  }
}

// ============ Recovery ============

/** Batch size for paginated recovery */
const RECOVERY_BATCH_SIZE = 10000;

/**
 * Route a job whose `depends_on` blob was corrupt on disk straight to the DLQ.
 *
 * Its real dependency metadata is unrecoverable, so it must never be enqueued
 * as "ready" (out-of-order execution) nor parked in waitingDeps behind a
 * dependency that can never complete (an unbounded leak).
 *
 * We persist the DLQ entry to disk and drop the jobs row here, but do NOT add
 * it to the in-memory DLQ — the later `loadDlq()` pass (which runs after both
 * recovery phases) restores every persisted entry into memory exactly once.
 * Adding it here too would double-count it. The job row is deleted so that pass
 * sees only the DLQ table.
 */
function quarantineCorruptDependsOn(ctx: BackgroundContext, job: Job): void {
  if (!ctx.storage) return;
  const entry = createDlqEntry(job, FailureReason.Unknown, 'corrupt-dependency-metadata');
  ctx.storage.saveDlqEntry(entry);
  ctx.storage.deleteJob(job.id);
  ctx.registerQueueName(job.queue);
  queueLog.error('Recovered job with corrupt depends_on metadata -> routed to DLQ', {
    jobId: String(job.id),
    queue: job.queue,
  });
}

// eslint-disable-next-line complexity
export function recover(ctx: BackgroundContext): void {
  if (!ctx.storage) return;

  // Keep the full pre-reconciliation window outside the bounded RAM tracker.
  // A restart may lower maxCompletedJobs; pruning before Phase 2 reconstructs
  // reverse edges could discard an old proof still owned by a waiting parent.
  const dependencyCompletions = ctx.storage.loadDependencyCompletions();

  // Load completed job IDs from SQLite for dependency checking
  const completedInDb = ctx.storage.loadCompletedJobIds();
  for (const record of dependencyCompletions) completedInDb.add(record.jobId);
  // Load DLQ job IDs so Phase 1 can skip stale active rows for DLQ'd jobs
  // (legacy DBs predate the DLQ-row cleanup fix in failJob).
  const dlqJobIds = ctx.storage.loadDlqJobIds();
  const queueStates = ctx.storage.loadQueueState();

  // Recovery bounds are needed before Phase 1 classifies interrupted active
  // jobs. Restore the stall policy first; paused/limiter state can wait until
  // the queues themselves have been rebuilt below.
  for (const qs of queueStates) {
    if (qs.stallConfig) {
      ctx.shards[shardIndex(qs.name)].setStallConfig(qs.name, qs.stallConfig);
    }
    if (qs.dlqConfig) {
      ctx.shards[shardIndex(qs.name)].setDlqConfig(qs.name, qs.dlqConfig);
    }
  }

  const now = Date.now();

  // === PHASE 1: Recover active jobs (were processing when server stopped) ===
  // These jobs are considered "stalled" and need to be retried or moved to DLQ
  while (true) {
    // Every successfully handled row leaves state='active' (retry -> waiting, or
    // delete -> DLQ/discard). Always read the first page: advancing OFFSET over
    // this shrinking result set would skip up to half of the active rows.
    const activeJobs = ctx.storage.loadActiveJobs(RECOVERY_BATCH_SIZE, 0);
    if (activeJobs.length === 0) break;

    for (const job of activeJobs) {
      const idx = shardIndex(job.queue);
      const shard = ctx.shards[idx];
      const stallConfig = shard.getStallConfig(job.queue);

      // Skip recovery for cron jobs with preventOverlap (uniqueKey='cron:*').
      // These jobs will be re-created by the cron scheduler at the next tick.
      // Re-queuing them would cause the "starts right away" bug (#73).
      if (job.uniqueKey?.startsWith('cron:')) {
        ctx.storage.deleteJob(job.id);
        ctx.registerQueueName(job.queue);
        continue;
      }

      // Skip jobs already present in DLQ (stale 'active' row from before the
      // failJob fix). Drop the orphan row so Phase 2 and subsequent queries
      // don't double-count it.
      if (dlqJobIds.has(job.id)) {
        ctx.storage.deleteJob(job.id);
        ctx.registerQueueName(job.queue);
        continue;
      }

      // A corrupt depends_on blob makes the job's real dependencies
      // unrecoverable: route it to the DLQ instead of retrying it (which could
      // execute it out-of-order once stalls are exhausted).
      if (isCorruptDependsOn(job)) {
        quarantineCorruptDependsOn(ctx, job);
        continue;
      }

      // Increment stall count (job was interrupted)
      job.stallCount = (job.stallCount || 0) + 1;
      job.attempts++;
      job.startedAt = null;
      job.lastHeartbeat = now;

      // A crash consumes one processing attempt and one stall allowance. Both
      // bounds are terminal: requeueing after either reaches its configured
      // maximum would make attempts/stalls unbounded across restart loops.
      const maxStalls = stallConfig.maxStalls ?? 3;
      const attemptsExhausted = job.attempts >= job.maxAttempts;
      const stallsExhausted = maxStalls > 0 && job.stallCount >= maxStalls;
      if (attemptsExhausted || stallsExhausted) {
        // Move to DLQ
        const reason = attemptsExhausted
          ? FailureReason.MaxAttemptsExceeded
          : FailureReason.Stalled;
        const error = attemptsExhausted
          ? `Job reached max attempts (${job.maxAttempts}) during startup recovery`
          : `Job stalled ${job.stallCount} times (recovered at startup)`;
        // Persist only here; the later loadDlq() phase is the single in-memory
        // restore path. Adding to the shard now and restoring the same row below
        // would duplicate the DLQ entry after every terminal crash recovery.
        const entry = createDlqEntry(job, reason, error, shard.getDlqConfig(job.queue));
        ctx.storage.saveDlqEntry(entry);
        ctx.storage.deleteJob(job.id);
      } else {
        // Persist the retry here, but let Phase 2 be the single authoritative
        // enqueue path. Enqueuing in both phases replaces the priority-queue entry
        // by ID but increments queued/delayed counters twice.
        job.runAt = now + calculateBackoff(job);
        ctx.storage.updateForRetry(job);
      }

      ctx.registerQueueName(job.queue);
    }

    if (activeJobs.length < RECOVERY_BATCH_SIZE) break;
  }

  // === PHASE 2: Load pending jobs ===
  let offset = 0;
  const corruptPendingJobs: Job[] = [];

  // Load pending jobs in batches to avoid memory spikes
  while (true) {
    const jobs = ctx.storage.loadPendingJobs(RECOVERY_BATCH_SIZE, offset);
    if (jobs.length === 0) break;

    for (const job of jobs) {
      const idx = shardIndex(job.queue);
      const shard = ctx.shards[idx];

      // A corrupt depends_on blob means the real dependencies are unrecoverable.
      // Route the job to the DLQ rather than enqueuing it as ready (out-of-order
      // execution) or parking it in waitingDeps forever (unbounded leak).
      if (isCorruptDependsOn(job)) {
        // Keep the SQLite pending result set stable until OFFSET pagination has
        // finished. Deleting this row now shifts a healthy row across the next
        // page boundary and leaves it absent from the in-memory queue.
        corruptPendingJobs.push(job);
        continue;
      }

      // Check if job has unmet dependencies
      // A ready persisted state is an authoritative checkpoint: dependency
      // proofs are deliberately bounded and may have expired after promotion.
      const hasDependencies = job.dependsOn && job.dependsOn.length > 0;
      const recoveredState = persistedJobState(job);
      const wasAlreadyPromoted =
        recoveredState === 'waiting' ||
        recoveredState === 'prioritized' ||
        recoveredState === 'delayed';
      const needsWaitingDeps =
        hasDependencies &&
        !wasAlreadyPromoted &&
        !job.dependsOn.every((depId) => ctx.completedJobs.has(depId) || completedInDb.has(depId));
      const manuallyWaitingChildren = recoveredState === 'waiting-children' && !hasDependencies;

      if (manuallyWaitingChildren) {
        shard.waitingChildren.set(job.id, job);
      } else if (needsWaitingDeps) {
        // Job is waiting for dependencies - don't add to main queue
        shard.waitingDeps.set(job.id, job);
        shard.registerDependencies(job.id, job.dependsOn);
        // Note: don't call incrementQueued for waitingDeps jobs (matches push.ts behavior)
      } else {
        // Job is ready to process
        shard.getQueue(job.queue).push(job);
        // Update running counters for O(1) stats and temporal index
        const state = dependencyReadyState(job, now);
        const isDelayed = state === 'delayed';
        shard.incrementQueued(job.id, isDelayed, job.createdAt, job.queue, job.runAt);
        if (hasDependencies) checkpointDependencyPromotion(job, state, now, ctx.storage);
      }

      ctx.jobIndex.set(job.id, { type: 'queue', shardIdx: idx, queueName: job.queue });
      ctx.dependencyResults.registerConsumer(job.id, job.dependsOn);
      for (const dependencyId of job.dependsOn) {
        if (ctx.jobResults.has(dependencyId)) {
          ctx.dependencyResults.retain(dependencyId, ctx.jobResults.get(dependencyId));
        }
      }

      // Restore customId mapping for deduplication (fixes idempotency on restart)
      if (job.customId) {
        ctx.customIdMap.set(job.customId, job.id);
      }

      // Restore uniqueKey mapping for TTL-based deduplication
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

    // If we got less than batch size, we're done
    if (jobs.length < RECOVERY_BATCH_SIZE) break;
  }

  // Quarantine only after the paginated scan so deletions cannot move page
  // boundaries. loadDlq() below restores these entries into memory exactly once.
  for (const job of corruptPendingJobs) {
    quarantineCorruptDependsOn(ctx, job);
  }

  // Load DLQ entries
  const dlqEntries = ctx.storage.loadDlq();
  for (const [queue, entries] of dlqEntries) {
    const idx = shardIndex(queue);
    const shard = ctx.shards[idx];
    for (const entry of entries) {
      shard.restoreDlqEntry(queue, entry);
      // Populate jobIndex so getJob/getJobState resolve DLQ'd jobs post-restart.
      ctx.jobIndex.set(entry.job.id, { type: 'dlq', queueName: queue });
    }
    ctx.registerQueueName(queue);
  }

  // === Restore queue control-state (#100) ===
  // paused / rate-limit / concurrency live only in LimiterManager's in-memory
  // Map; without this load every queue silently un-pauses and loses its limits
  // on restart. Applied directly to the owning shard (in-memory only — these
  // setters do not re-persist, so there is no write-back loop).
  for (const qs of queueStates) {
    const shard = ctx.shards[shardIndex(qs.name)];
    if (qs.paused) shard.pause(qs.name);
    if (qs.rateLimit !== null) {
      // TTL'd limits are temporary by definition: an already-expired one must
      // not resurrect as permanent. A still-live one is restored with its
      // remaining TTL so it keeps expiring broker-side.
      const remainingTtl =
        qs.rateLimitExpiresAt === null ? undefined : qs.rateLimitExpiresAt - Date.now();
      if (remainingTtl === undefined || remainingTtl > 0) {
        shard.setRateLimit(qs.name, qs.rateLimit, qs.rateLimitDuration ?? undefined, remainingTtl);
      }
    }
    if (qs.concurrencyLimit !== null) shard.setConcurrency(qs.name, qs.concurrencyLimit);
    ctx.registerQueueName(qs.name);
  }

  // === PHASE 3: Recover completed jobs ===
  // Required for clean('completed'), stats.completed, and in-memory lookups
  // on jobs that completed before a server restart (issue #84).
  // Capped at maxCompletedJobs (matches BoundedSet cap) so we don't exceed
  // the in-memory budget when SQLite contains more rows than the cap.
  // Note: we deliberately do NOT populate customIdMap here — Phase 2 owns
  // dedup for pending jobs, and push.ts handles customId collision against
  // completed jobs via the storage fallback. Touching customIdMap here would
  // LRU-evict pending-job mappings when many completed jobs have customIds.
  const completedCap = ctx.config.maxCompletedJobs;
  let completedLoaded = 0;
  let completedOffset = 0;

  while (completedLoaded < completedCap) {
    const remaining = completedCap - completedLoaded;
    const batchSize = Math.min(RECOVERY_BATCH_SIZE, remaining);
    const completedBatch = ctx.storage.loadCompletedJobs(batchSize, completedOffset);
    if (completedBatch.length === 0) break;

    for (const job of completedBatch) {
      ctx.jobIndex.set(job.id, { type: 'completed', queueName: job.queue });
      ctx.completedJobs.add(job.id);
      ctx.completedJobsData.set(job.id, job);
      ctx.registerQueueName(job.queue);
    }

    completedLoaded += completedBatch.length;
    completedOffset += completedBatch.length;
    if (completedBatch.length < batchSize) break;
  }

  reconcileDependencyCompletionPins(ctx);
}

// Re-export for backward compatibility
export { processPendingDependencies };
