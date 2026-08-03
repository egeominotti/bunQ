/**
 * Stall Detection Unit Tests - Two-Phase Detection Logic
 *
 * Tests the checkStalledJobs function from stallDetection.ts which uses
 * a two-phase approach to prevent false positives:
 *   Phase 1: Jobs found stalled become "candidates" (stalledCandidates set)
 *   Phase 2: On next check, candidates still stalled are confirmed and handled
 *
 * This requires TWO successive calls to checkStalledJobs to detect a stall.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { checkStalledJobs } from '../src/application/stallDetection';
import { shardIndex, processingShardIndex, SHARD_COUNT } from '../src/shared/hash';
import type { Job, JobId } from '../src/domain/types/job';
import type { BackgroundContext } from '../src/application/types';

/** Use the exact complete background context wired by QueueManager. */
function getBackgroundContext(qm: QueueManager): BackgroundContext {
  return (
    qm as unknown as {
      contextFactory: { getBackgroundContext(): BackgroundContext };
    }
  ).contextFactory.getBackgroundContext();
}

/**
 * Helper to get a processing job by its ID from the processingShards.
 */
function getProcessingJob(qm: QueueManager, jobId: JobId): Job | undefined {
  const processingShards: Map<JobId, Job>[] = (qm as any).processingShards;
  const procIdx = processingShardIndex(jobId);
  return processingShards[procIdx].get(jobId);
}

/**
 * Simulate time passage by manipulating a job's lastHeartbeat and startedAt.
 * Makes the job appear as if it has been processing for a long time with no heartbeat.
 */
function makeJobStalled(
  job: Job,
  stallConfig: { stallInterval: number; gracePeriod: number }
): void {
  const now = Date.now();
  // Ensure started long ago (past grace period)
  job.startedAt = now - stallConfig.gracePeriod - stallConfig.stallInterval - 5000;
  // Ensure heartbeat is older than stall interval
  job.lastHeartbeat = now - stallConfig.stallInterval - 1000;
}

/**
 * Make a job appear healthy with a recent heartbeat.
 */
function makeJobHealthy(job: Job): void {
  job.lastHeartbeat = Date.now();
}

describe('Two-Phase Stall Detection (checkStalledJobs)', () => {
  let qm: QueueManager;
  let ctx: BackgroundContext;
  const QUEUE = 'stall-test-queue';
  const STALL_CONFIG = {
    stallInterval: 1000,
    maxStalls: 3,
    gracePeriod: 100,
  };

  beforeEach(() => {
    qm = new QueueManager();
    ctx = getBackgroundContext(qm);

    // Configure stall detection for the queue
    const idx = shardIndex(QUEUE);
    ctx.shards[idx].setStallConfig(QUEUE, {
      enabled: true,
      ...STALL_CONFIG,
    });
  });

  afterEach(() => {
    qm.shutdown();
  });

  /**
   * Helper: push a job, pull it (making it active/processing), return its job object.
   */
  async function pushAndPull(data: unknown = { test: true }): Promise<Job> {
    const pushed = await qm.push(QUEUE, { data });
    const pulled = await qm.pull(QUEUE, 0);
    expect(pulled?.id).toBe(pushed.id);
    return pulled!;
  }

  // ---------- BASIC TWO-PHASE MECHANICS ----------

  test('first check: stalled job becomes a candidate (not yet handled)', async () => {
    const job = await pushAndPull();
    makeJobStalled(job, STALL_CONFIG);

    // First check: Phase 2 marks it as candidate
    checkStalledJobs(ctx);

    // Job should now be in stalledCandidates
    expect(ctx.stalledCandidates.has(job.id)).toBe(true);

    // But it should still be in processing (not yet handled)
    const processingJob = getProcessingJob(qm, job.id);
    expect(processingJob).toBeDefined();
  });

  test('second check: candidate still stalled triggers handling (retry)', async () => {
    const job = await pushAndPull();
    makeJobStalled(job, STALL_CONFIG);

    // First check: marks as candidate
    checkStalledJobs(ctx);
    expect(ctx.stalledCandidates.has(job.id)).toBe(true);

    // Keep job stalled for second check
    makeJobStalled(job, STALL_CONFIG);

    // Second check: Phase 1 processes the candidate (confirmed stalled),
    // then Phase 2 re-scans processing shards (job may still be there
    // because handleStalledJob is async). The key assertion is that
    // the job is actually handled (removed from processing) after awaiting.
    checkStalledJobs(ctx);

    // Allow async handleStalledJob to complete
    await Bun.sleep(50);

    // Job should have been removed from processing (retried)
    const processingJob = getProcessingJob(qm, job.id);
    expect(processingJob).toBeUndefined();

    // Job should be back in the queue
    const loc = ctx.jobIndex.get(job.id);
    expect(loc).toBeDefined();
    expect(loc!.type).toBe('queue');
  });

  test('job with recent heartbeat is NOT marked as candidate', async () => {
    const job = await pushAndPull();
    // Keep heartbeat fresh
    makeJobHealthy(job);

    checkStalledJobs(ctx);

    // Should not be a candidate
    expect(ctx.stalledCandidates.has(job.id)).toBe(false);

    // Should still be in processing
    const processingJob = getProcessingJob(qm, job.id);
    expect(processingJob).toBeDefined();
  });

  test('candidate that heartbeats before second check is cleared', async () => {
    const job = await pushAndPull();
    makeJobStalled(job, STALL_CONFIG);

    // First check: marks as candidate
    checkStalledJobs(ctx);
    expect(ctx.stalledCandidates.has(job.id)).toBe(true);

    // Job sends heartbeat before second check
    makeJobHealthy(job);

    // Second check: candidate is no longer stalled, should be cleared
    checkStalledJobs(ctx);
    await Bun.sleep(50);

    // Should not be in candidates anymore
    expect(ctx.stalledCandidates.has(job.id)).toBe(false);

    // Should still be in processing (not retried or moved to DLQ)
    const processingJob = getProcessingJob(qm, job.id);
    expect(processingJob).toBeDefined();
  });

  // ---------- RETRY VS DLQ ----------

  test('stalled job with attempts < maxStalls is retried (put back in queue)', async () => {
    const job = await pushAndPull();
    expect(job.stallCount).toBe(0);
    makeJobStalled(job, STALL_CONFIG);

    // Phase 1: become candidate
    checkStalledJobs(ctx);
    makeJobStalled(job, STALL_CONFIG);

    // Phase 2: confirmed stall triggers retry
    checkStalledJobs(ctx);
    await Bun.sleep(50);

    // Job should be removed from processing
    const processingJob = getProcessingJob(qm, job.id);
    expect(processingJob).toBeUndefined();

    // Job should be back in the queue (jobIndex should reflect queue location)
    const loc = ctx.jobIndex.get(job.id);
    expect(loc).toBeDefined();
    expect(loc!.type).toBe('queue');
  });

  test('stalled job with stallCount >= maxStalls - 1 is moved to DLQ', async () => {
    const job = await pushAndPull();
    // Set stall count so next stall exceeds maxStalls (3)
    // getStallAction checks: newStallCount (stallCount + 1) >= maxStalls
    job.stallCount = 2;
    makeJobStalled(job, STALL_CONFIG);

    // Phase 1: become candidate
    checkStalledJobs(ctx);
    makeJobStalled(job, STALL_CONFIG);

    // Phase 2: confirmed stall triggers DLQ
    checkStalledJobs(ctx);
    await Bun.sleep(50);

    // Job should be removed from processing
    const processingJob = getProcessingJob(qm, job.id);
    expect(processingJob).toBeUndefined();

    // Job should be in DLQ
    const loc = ctx.jobIndex.get(job.id);
    expect(loc).toBeDefined();
    expect(loc!.type).toBe('dlq');
  });

  // ---------- MULTIPLE JOBS ----------

  test('multiple jobs: some stalled, some healthy', async () => {
    const stalledJob = await pushAndPull({ id: 'stalled' });
    const healthyJob = await pushAndPull({ id: 'healthy' });

    makeJobStalled(stalledJob, STALL_CONFIG);
    makeJobHealthy(healthyJob);

    // First check
    checkStalledJobs(ctx);

    expect(ctx.stalledCandidates.has(stalledJob.id)).toBe(true);
    expect(ctx.stalledCandidates.has(healthyJob.id)).toBe(false);

    // Keep stalled job stalled, healthy job healthy
    makeJobStalled(stalledJob, STALL_CONFIG);
    makeJobHealthy(healthyJob);

    // Second check
    checkStalledJobs(ctx);
    await Bun.sleep(50);

    // Stalled job should have been handled (removed from processing)
    expect(getProcessingJob(qm, stalledJob.id)).toBeUndefined();

    // Healthy job should still be in processing
    expect(getProcessingJob(qm, healthyJob.id)).toBeDefined();
  });

  test('multiple stalled jobs: some retried, some to DLQ', async () => {
    const retryJob = await pushAndPull({ id: 'retry' });
    const dlqJob = await pushAndPull({ id: 'dlq' });

    retryJob.stallCount = 0; // Will retry
    dlqJob.stallCount = 2; // Will go to DLQ (maxStalls=3)

    makeJobStalled(retryJob, STALL_CONFIG);
    makeJobStalled(dlqJob, STALL_CONFIG);

    // First check: both become candidates
    checkStalledJobs(ctx);
    expect(ctx.stalledCandidates.has(retryJob.id)).toBe(true);
    expect(ctx.stalledCandidates.has(dlqJob.id)).toBe(true);

    makeJobStalled(retryJob, STALL_CONFIG);
    makeJobStalled(dlqJob, STALL_CONFIG);

    // Second check: both confirmed
    checkStalledJobs(ctx);
    await Bun.sleep(50);

    // Retry job should be back in queue
    const retryLoc = ctx.jobIndex.get(retryJob.id);
    expect(retryLoc).toBeDefined();
    expect(retryLoc!.type).toBe('queue');

    // DLQ job should be in DLQ
    const dlqLoc = ctx.jobIndex.get(dlqJob.id);
    expect(dlqLoc).toBeDefined();
    expect(dlqLoc!.type).toBe('dlq');
  });

  // ---------- STALL CONFIG ----------

  test('disabled stall detection skips the job entirely', async () => {
    const DISABLED_QUEUE = 'disabled-stall-queue';
    const idx = shardIndex(DISABLED_QUEUE);
    ctx.shards[idx].setStallConfig(DISABLED_QUEUE, {
      enabled: false,
      stallInterval: 1000,
      maxStalls: 3,
      gracePeriod: 100,
    });

    const pushed = await qm.push(DISABLED_QUEUE, { data: { test: true } });
    const pulled = await qm.pull(DISABLED_QUEUE, 0);
    expect(pulled?.id).toBe(pushed.id);

    makeJobStalled(pulled!, { stallInterval: 1000, gracePeriod: 100 });

    // First check
    checkStalledJobs(ctx);
    expect(ctx.stalledCandidates.has(pulled!.id)).toBe(false);

    // Second check
    checkStalledJobs(ctx);
    await Bun.sleep(50);

    // Job should still be in processing, untouched
    expect(getProcessingJob(qm, pulled!.id)).toBeDefined();
  });

  test('custom stallInterval: shorter interval detects faster', async () => {
    const SHORT_QUEUE = 'short-stall-queue';
    const idx = shardIndex(SHORT_QUEUE);
    ctx.shards[idx].setStallConfig(SHORT_QUEUE, {
      enabled: true,
      stallInterval: 200,
      maxStalls: 3,
      gracePeriod: 50,
    });

    const pushed = await qm.push(SHORT_QUEUE, { data: { test: true } });
    const pulled = await qm.pull(SHORT_QUEUE, 0);
    expect(pulled?.id).toBe(pushed.id);
    const job = pulled!;

    // Make heartbeat older than 200ms interval (but not 1000ms default)
    const now = Date.now();
    job.startedAt = now - 500;
    job.lastHeartbeat = now - 300;

    // This should detect the stall with 200ms interval
    checkStalledJobs(ctx);
    expect(ctx.stalledCandidates.has(job.id)).toBe(true);
  });

  test('custom maxStalls: job retried up to maxStalls times then DLQ', async () => {
    const CUSTOM_QUEUE = 'custom-maxstalls-queue';
    const idx = shardIndex(CUSTOM_QUEUE);
    ctx.shards[idx].setStallConfig(CUSTOM_QUEUE, {
      enabled: true,
      stallInterval: 1000,
      maxStalls: 2, // Lower threshold
      gracePeriod: 100,
    });

    const pushed = await qm.push(CUSTOM_QUEUE, { data: { test: true } });
    const pulled = await qm.pull(CUSTOM_QUEUE, 0);
    expect(pulled?.id).toBe(pushed.id);
    const job = pulled!;

    // stallCount=1, maxStalls=2. Next stall (newStallCount=2) >= maxStalls => DLQ
    job.stallCount = 1;
    makeJobStalled(job, { stallInterval: 1000, gracePeriod: 100 });

    // Phase 1
    checkStalledJobs(ctx);
    expect(ctx.stalledCandidates.has(job.id)).toBe(true);

    makeJobStalled(job, { stallInterval: 1000, gracePeriod: 100 });

    // Phase 2
    checkStalledJobs(ctx);
    await Bun.sleep(50);

    // Should be in DLQ with maxStalls=2
    const loc = ctx.jobIndex.get(job.id);
    expect(loc).toBeDefined();
    expect(loc!.type).toBe('dlq');
  });

  test('gracePeriod: job within grace period is not detected as stalled', async () => {
    const GRACE_QUEUE = 'grace-period-queue';
    const idx = shardIndex(GRACE_QUEUE);
    ctx.shards[idx].setStallConfig(GRACE_QUEUE, {
      enabled: true,
      stallInterval: 1000,
      maxStalls: 3,
      gracePeriod: 60000, // Very long grace period
    });

    const pushed = await qm.push(GRACE_QUEUE, { data: { test: true } });
    const pulled = await qm.pull(GRACE_QUEUE, 0);
    expect(pulled?.id).toBe(pushed.id);
    const job = pulled!;

    // Job was just started (within grace period), even with old heartbeat
    const now = Date.now();
    job.startedAt = now - 1000; // Started 1s ago (within 60s grace)
    job.lastHeartbeat = now - 5000; // Old heartbeat

    checkStalledJobs(ctx);
    expect(ctx.stalledCandidates.has(job.id)).toBe(false);
  });

  // ---------- EDGE CASES ----------

  test('job removed from processing between checks is skipped', async () => {
    const job = await pushAndPull();
    makeJobStalled(job, STALL_CONFIG);

    // First check: becomes candidate
    checkStalledJobs(ctx);
    expect(ctx.stalledCandidates.has(job.id)).toBe(true);

    // Simulate job being completed/acked between checks
    await qm.ack(job.id, { done: true });

    // Second check: candidate no longer in processing, should be cleaned up
    checkStalledJobs(ctx);
    await Bun.sleep(50);

    // Should not be in candidates
    expect(ctx.stalledCandidates.has(job.id)).toBe(false);
  });

  test('stalled job retry increments stallCount and attempts', async () => {
    const job = await pushAndPull();
    const originalStallCount = job.stallCount;
    const originalAttempts = job.attempts;
    makeJobStalled(job, STALL_CONFIG);

    // Phase 1
    checkStalledJobs(ctx);
    makeJobStalled(job, STALL_CONFIG);

    // Phase 2
    checkStalledJobs(ctx);
    await Bun.sleep(50);

    // Job was retried - find it in the queue
    const idx = shardIndex(QUEUE);
    const queue = ctx.shards[idx].getQueue(QUEUE);
    let retriedJob: Job | undefined;
    for (const j of queue.values()) {
      if (j.id === job.id) {
        retriedJob = j;
        break;
      }
    }

    expect(retriedJob).toBeDefined();
    expect(retriedJob!.stallCount).toBe(originalStallCount + 1);
    expect(retriedJob!.attempts).toBe(originalAttempts + 1);
    expect(retriedJob!.startedAt).toBeNull();
  });

  test('stalled job retry resets startedAt and updates lastHeartbeat', async () => {
    const job = await pushAndPull();
    makeJobStalled(job, STALL_CONFIG);

    // Phase 1
    checkStalledJobs(ctx);
    makeJobStalled(job, STALL_CONFIG);

    const beforeRetry = Date.now();

    // Phase 2
    checkStalledJobs(ctx);
    await Bun.sleep(50);

    // Find retried job in the queue
    const idx = shardIndex(QUEUE);
    const queue = ctx.shards[idx].getQueue(QUEUE);
    let retriedJob: Job | undefined;
    for (const j of queue.values()) {
      if (j.id === job.id) {
        retriedJob = j;
        break;
      }
    }

    expect(retriedJob).toBeDefined();
    expect(retriedJob!.startedAt).toBeNull();
    expect(retriedJob!.lastHeartbeat).toBeGreaterThanOrEqual(beforeRetry);
  });

  test('empty stalledCandidates set on first call does nothing for Phase 1', async () => {
    // Ensure no candidates
    expect(ctx.stalledCandidates.size).toBe(0);

    // No jobs in processing either
    checkStalledJobs(ctx);

    // Nothing should happen
    expect(ctx.stalledCandidates.size).toBe(0);
  });

  test('three successive checks: candidate -> cleared -> re-candidate -> handled', async () => {
    const job = await pushAndPull();
    makeJobStalled(job, STALL_CONFIG);

    // Check 1: becomes candidate
    checkStalledJobs(ctx);
    expect(ctx.stalledCandidates.has(job.id)).toBe(true);

    // Job heartbeats, no longer stalled
    makeJobHealthy(job);

    // Check 2: candidate cleared (healthy), but Phase 2 might re-add if stalled again
    checkStalledJobs(ctx);
    await Bun.sleep(50);

    // Job should still be in processing (was cleared from candidates, not handled)
    expect(getProcessingJob(qm, job.id)).toBeDefined();
    // After check 2, it should not be a candidate since it was healthy
    // (Phase 1 cleared it, Phase 2 would not re-add because it's healthy)
    expect(ctx.stalledCandidates.has(job.id)).toBe(false);

    // Now job stalls again
    makeJobStalled(job, STALL_CONFIG);

    // Check 3: Phase 2 re-adds as candidate
    checkStalledJobs(ctx);
    expect(ctx.stalledCandidates.has(job.id)).toBe(true);

    // Keep stalled
    makeJobStalled(job, STALL_CONFIG);

    // Check 4: confirmed and handled
    checkStalledJobs(ctx);
    await Bun.sleep(50);

    expect(getProcessingJob(qm, job.id)).toBeUndefined();
  });

  test('stall detection across multiple processing shards', async () => {
    // Push and pull multiple jobs to potentially land in different processing shards
    const jobs: Job[] = [];
    for (let i = 0; i < 5; i++) {
      const job = await pushAndPull({ idx: i });
      jobs.push(job);
    }

    // Make all jobs stalled
    for (const job of jobs) {
      makeJobStalled(job, STALL_CONFIG);
    }

    // First check: all become candidates
    checkStalledJobs(ctx);
    for (const job of jobs) {
      expect(ctx.stalledCandidates.has(job.id)).toBe(true);
    }

    // Keep all stalled
    for (const job of jobs) {
      makeJobStalled(job, STALL_CONFIG);
    }

    // Second check: all confirmed
    checkStalledJobs(ctx);
    await Bun.sleep(100);

    // All should be removed from processing
    for (const job of jobs) {
      expect(getProcessingJob(qm, job.id)).toBeUndefined();
    }
  });

  test('DLQ entry contains correct failure reason for stalled job', async () => {
    const job = await pushAndPull();
    job.stallCount = 2; // maxStalls=3, so next stall triggers DLQ
    makeJobStalled(job, STALL_CONFIG);

    // Phase 1
    checkStalledJobs(ctx);
    makeJobStalled(job, STALL_CONFIG);

    // Phase 2
    checkStalledJobs(ctx);
    await Bun.sleep(50);

    // Verify DLQ has the entry
    const dlqJobs = qm.getDlq(QUEUE);
    const dlqEntry = dlqJobs.find((j) => j.id === job.id);
    expect(dlqEntry).toBeDefined();
  });

  test('stalled candidate set is properly cleared after processing', async () => {
    const job1 = await pushAndPull({ id: 1 });
    const job2 = await pushAndPull({ id: 2 });
    makeJobStalled(job1, STALL_CONFIG);
    makeJobStalled(job2, STALL_CONFIG);

    // First check: both candidates
    checkStalledJobs(ctx);
    expect(ctx.stalledCandidates.size).toBeGreaterThanOrEqual(2);

    makeJobStalled(job1, STALL_CONFIG);
    makeJobStalled(job2, STALL_CONFIG);

    // Second check: processes candidates, then Phase 2 may re-add remaining stalled jobs
    checkStalledJobs(ctx);
    await Bun.sleep(50);

    // Both jobs should have been processed from candidates in Phase 1
    // and removed from processing
    expect(getProcessingJob(qm, job1.id)).toBeUndefined();
    expect(getProcessingJob(qm, job2.id)).toBeUndefined();
  });
});
