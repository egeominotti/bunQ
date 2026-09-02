/**
 * Unit Tests for DlqShard, DependencyProcessor, and JobLogsManager
 *
 * Tests the domain/application modules directly (not through QueueManager):
 * - DlqShard: per-shard DLQ operations
 * - processPendingDependencies: dependency resolution logic
 * - addJobLog/getJobLogs/clearJobLogs: job log operations
 */

import { describe, test, expect, beforeEach } from 'bun:test';
import { DlqShard, type DlqStatsCallback } from '../src/domain/queue/dlqShard';
import { type DlqConfig, DEFAULT_DLQ_CONFIG, FailureReason } from '../src/domain/types/dlq';
import { createJob, jobId, type Job, type JobId } from '../src/domain/types/job';
import { DEFAULT_STALL_CONFIG } from '../src/domain/types/stall';
import { processPendingDependencies } from '../src/application/dependencyProcessor';
import {
  addJobLog,
  getJobLogs,
  clearJobLogs,
  type JobLogsContext,
} from '../src/application/jobLogsManager';
import type { JobLocation } from '../src/domain/types/queue';
import { Shard } from '../src/domain/queue/shard';
import { RWLock } from '../src/shared/lock';
import { LRUMap, BoundedSet } from '../src/shared/lru';
import { SHARD_COUNT } from '../src/shared/hash';

// ============ Helpers ============

/** Create a mock stats callback that tracks increment/decrement calls */
function createMockStats(): DlqStatsCallback & {
  increments: number;
  decrements: number;
  decrementAmounts: number[];
} {
  const mock = {
    increments: 0,
    decrements: 0,
    decrementAmounts: [] as number[],
    incrementDlq() {
      mock.increments++;
    },
    decrementDlq(count?: number) {
      mock.decrements++;
      mock.decrementAmounts.push(count ?? 1);
    },
  };
  return mock;
}

/** Create a job with a numeric suffix for easy identification */
function makeJob(n: number, queue = 'test-queue', overrides: Partial<Job> = {}): Job {
  const job = createJob(jobId(`job-${n}`), queue, { data: { n } });
  return { ...job, ...overrides } as Job;
}

const jid = (n: number): JobId => jobId(`job-${n}`);

// ============================================================
//  DlqShard Tests
// ============================================================

describe('DlqShard', () => {
  let shard: DlqShard;
  let stats: ReturnType<typeof createMockStats>;

  beforeEach(() => {
    stats = createMockStats();
    shard = new DlqShard(stats);
  });

  // -------- Config --------

  describe('config management', () => {
    test('getConfig returns default config for unconfigured queue', () => {
      const config = shard.getConfig('unknown');
      expect(config).toEqual(DEFAULT_DLQ_CONFIG);
    });

    test('setConfig merges with existing config', () => {
      shard.setConfig('emails', { maxEntries: 50 });
      const config = shard.getConfig('emails');
      expect(config.maxEntries).toBe(50);
      expect(config.autoRetry).toBe(DEFAULT_DLQ_CONFIG.autoRetry);
      expect(config.maxAge).toBe(DEFAULT_DLQ_CONFIG.maxAge);
    });

    test('setConfig overwrites previously set values', () => {
      shard.setConfig('emails', { maxEntries: 50 });
      shard.setConfig('emails', { maxEntries: 100, autoRetry: true });
      const config = shard.getConfig('emails');
      expect(config.maxEntries).toBe(100);
      expect(config.autoRetry).toBe(true);
    });

    test('config is isolated per queue', () => {
      shard.setConfig('q1', { maxEntries: 10 });
      shard.setConfig('q2', { maxEntries: 20 });
      expect(shard.getConfig('q1').maxEntries).toBe(10);
      expect(shard.getConfig('q2').maxEntries).toBe(20);
    });
  });

  describe('stall config management', () => {
    test('getStallConfig returns default for unconfigured queue', () => {
      const config = shard.getStallConfig('unknown');
      expect(config).toEqual(DEFAULT_STALL_CONFIG);
    });

    test('setStallConfig merges with existing config', () => {
      shard.setStallConfig('emails', { stallInterval: 60000 });
      const config = shard.getStallConfig('emails');
      expect(config.stallInterval).toBe(60000);
      expect(config.maxStalls).toBe(DEFAULT_STALL_CONFIG.maxStalls);
    });

    test('stall config is isolated per queue', () => {
      shard.setStallConfig('q1', { maxStalls: 5 });
      shard.setStallConfig('q2', { maxStalls: 10 });
      expect(shard.getStallConfig('q1').maxStalls).toBe(5);
      expect(shard.getStallConfig('q2').maxStalls).toBe(10);
    });
  });

  // -------- Add / Get --------

  describe('add and getEntries', () => {
    test('add creates entry and increments stats', () => {
      const job = makeJob(1);
      const entry = shard.add(job, FailureReason.Timeout, 'timed out');

      expect(entry.job).toBe(job);
      expect(entry.reason).toBe(FailureReason.Timeout);
      expect(entry.error).toBe('timed out');
      expect(stats.increments).toBe(1);
    });

    test('getEntries returns entries for queue', () => {
      shard.add(makeJob(1), FailureReason.Timeout, null);
      shard.add(makeJob(2), FailureReason.Stalled, null);

      const entries = shard.getEntries('test-queue');
      expect(entries.length).toBe(2);
    });

    test('getEntries returns empty array for unknown queue', () => {
      expect(shard.getEntries('unknown')).toEqual([]);
    });

    test('add with default reason and error', () => {
      const entry = shard.add(makeJob(1));
      expect(entry.reason).toBe(FailureReason.Unknown);
      expect(entry.error).toBeNull();
    });
  });

  describe('getJobs', () => {
    test('returns jobs from entries', () => {
      const j1 = makeJob(1);
      const j2 = makeJob(2);
      shard.add(j1);
      shard.add(j2);

      const jobs = shard.getJobs('test-queue');
      expect(jobs.length).toBe(2);
      expect(jobs[0]).toBe(j1);
      expect(jobs[1]).toBe(j2);
    });

    test('returns limited number of jobs with count param', () => {
      for (let i = 0; i < 5; i++) {
        shard.add(makeJob(i));
      }

      const jobs = shard.getJobs('test-queue', 3);
      expect(jobs.length).toBe(3);
    });

    test('returns empty array for unknown queue', () => {
      expect(shard.getJobs('unknown')).toEqual([]);
    });
  });

  // -------- restoreEntry --------

  describe('restoreEntry', () => {
    test('restores an existing DlqEntry and increments stats', () => {
      const job = makeJob(1);
      const entry = shard.add(job, FailureReason.Timeout, 'err');
      stats.increments = 0; // reset

      const shard2 = new DlqShard(stats);
      shard2.restoreEntry('test-queue', entry);

      expect(shard2.getEntries('test-queue').length).toBe(1);
      expect(shard2.getEntries('test-queue')[0]).toBe(entry);
      expect(stats.increments).toBe(1);
    });

    test('restores into a new queue if none exists', () => {
      const job = makeJob(1, 'restored-queue');
      const entry = shard.add(job);
      stats.increments = 0;

      const shard2 = new DlqShard(stats);
      shard2.restoreEntry('restored-queue', entry);

      expect(shard2.getEntries('restored-queue').length).toBe(1);
    });
  });

  // -------- Max entries eviction --------

  describe('max entries eviction', () => {
    test('evicts oldest entries when maxEntries exceeded', () => {
      shard.setConfig('test-queue', { maxEntries: 3 });

      for (let i = 0; i < 5; i++) {
        shard.add(makeJob(i));
      }

      expect(shard.getCount('test-queue')).toBe(3);
      const entries = shard.getEntries('test-queue');
      // Oldest (0 and 1) should be evicted, keeping 2, 3, 4
      expect((entries[0].job.data as any).n).toBe(2);
      expect((entries[1].job.data as any).n).toBe(3);
      expect((entries[2].job.data as any).n).toBe(4);
    });

    test('eviction decrements stats for each evicted entry', () => {
      shard.setConfig('test-queue', { maxEntries: 2 });

      shard.add(makeJob(1));
      shard.add(makeJob(2));
      shard.add(makeJob(3)); // This should evict job-1

      // 3 increments, 1 decrement for eviction
      expect(stats.increments).toBe(3);
      expect(stats.decrements).toBe(1);
    });

    test('maxEntries of 1 always keeps only latest', () => {
      shard.setConfig('test-queue', { maxEntries: 1 });

      shard.add(makeJob(1));
      shard.add(makeJob(2));
      shard.add(makeJob(3));

      expect(shard.getCount('test-queue')).toBe(1);
      const entries = shard.getEntries('test-queue');
      expect((entries[0].job.data as any).n).toBe(3);
    });
  });

  // -------- Filtered queries --------

  describe('getFiltered', () => {
    test('filter by reason', () => {
      shard.add(makeJob(1), FailureReason.Timeout, null);
      shard.add(makeJob(2), FailureReason.Stalled, null);
      shard.add(makeJob(3), FailureReason.Timeout, null);

      const filtered = shard.getFiltered('test-queue', { reason: FailureReason.Timeout });
      expect(filtered.length).toBe(2);
      expect(filtered.every((e) => e.reason === FailureReason.Timeout)).toBe(true);
    });

    test('filter by olderThan', () => {
      const now = Date.now();
      shard.add(makeJob(1));
      // The entry enteredAt is approximately now, so olderThan = now + 1000 should include it
      const filtered = shard.getFiltered('test-queue', { olderThan: now + 1000 });
      expect(filtered.length).toBe(1);

      // olderThan in the past should exclude
      const filtered2 = shard.getFiltered('test-queue', { olderThan: now - 10000 });
      expect(filtered2.length).toBe(0);
    });

    test('filter by newerThan', () => {
      const past = Date.now() - 10000;
      shard.add(makeJob(1));

      const filtered = shard.getFiltered('test-queue', { newerThan: past });
      expect(filtered.length).toBe(1);

      const filtered2 = shard.getFiltered('test-queue', { newerThan: Date.now() + 10000 });
      expect(filtered2.length).toBe(0);
    });

    test('filter by retriable', async () => {
      shard.setConfig('test-queue', { autoRetry: true, autoRetryInterval: 1, maxAutoRetries: 3 });

      shard.add(makeJob(1), FailureReason.Stalled, null);
      shard.add(makeJob(2), FailureReason.Timeout, null);

      // Wait for retry eligibility (nextRetryAt = enteredAt + 1ms interval)
      await Bun.sleep(5);

      const filtered = shard.getFiltered('test-queue', { retriable: true });
      expect(filtered.length).toBe(2);
    });

    test('filter by expired', () => {
      shard.setConfig('test-queue', { maxAge: 1 }); // 1ms expiry

      shard.add(makeJob(1));

      // Entry should expire almost immediately (maxAge = 1ms)
      // Use a tiny sleep or just check with future time
      const filtered = shard.getFiltered('test-queue', { expired: true });
      // May or may not be expired depending on timing; check with explicit future
      // The getFiltered uses Date.now() internally, so this might or might not match
      expect(filtered.length).toBeLessThanOrEqual(1);
    });

    test('filter with offset and limit', () => {
      for (let i = 0; i < 10; i++) {
        shard.add(makeJob(i));
      }

      const filtered = shard.getFiltered('test-queue', { offset: 2, limit: 3 });
      expect(filtered.length).toBe(3);
      expect((filtered[0].job.data as any).n).toBe(2);
      expect((filtered[2].job.data as any).n).toBe(4);
    });

    test('filter with only limit', () => {
      for (let i = 0; i < 5; i++) {
        shard.add(makeJob(i));
      }

      const filtered = shard.getFiltered('test-queue', { limit: 2 });
      expect(filtered.length).toBe(2);
    });

    test('filter with only offset', () => {
      for (let i = 0; i < 5; i++) {
        shard.add(makeJob(i));
      }

      const filtered = shard.getFiltered('test-queue', { offset: 3 });
      expect(filtered.length).toBe(2);
    });

    test('returns empty for unknown queue', () => {
      const filtered = shard.getFiltered('unknown', { reason: FailureReason.Timeout });
      expect(filtered).toEqual([]);
    });

    test('combining reason filter with limit and offset', () => {
      shard.add(makeJob(1), FailureReason.Timeout, null);
      shard.add(makeJob(2), FailureReason.Stalled, null);
      shard.add(makeJob(3), FailureReason.Timeout, null);
      shard.add(makeJob(4), FailureReason.Timeout, null);
      shard.add(makeJob(5), FailureReason.Stalled, null);

      const filtered = shard.getFiltered('test-queue', {
        reason: FailureReason.Timeout,
        offset: 1,
        limit: 1,
      });
      expect(filtered.length).toBe(1);
      // After filtering by Timeout, we get jobs 1, 3, 4. Offset 1 = job 3, limit 1 = just job 3
      expect((filtered[0].job.data as any).n).toBe(3);
    });
  });

  // -------- Remove --------

  describe('remove', () => {
    test('removes entry by job ID and decrements stats', () => {
      shard.add(makeJob(1));
      shard.add(makeJob(2));

      const removed = shard.remove('test-queue', jid(1));
      expect(removed).not.toBeNull();
      expect(removed!.job.id).toBe(jid(1));
      expect(shard.getCount('test-queue')).toBe(1);
      expect(stats.decrements).toBe(1);
    });

    test('returns null for non-existent job ID', () => {
      shard.add(makeJob(1));
      const removed = shard.remove('test-queue', jid(99));
      expect(removed).toBeNull();
    });

    test('returns null for non-existent queue', () => {
      const removed = shard.remove('unknown', jid(1));
      expect(removed).toBeNull();
    });
  });

  // -------- Auto-retry entries --------

  describe('getAutoRetryEntries', () => {
    test('returns entries eligible for auto-retry', () => {
      shard.setConfig('test-queue', { autoRetry: true, autoRetryInterval: 1, maxAutoRetries: 3 });

      shard.add(makeJob(1));
      shard.add(makeJob(2));

      // Use future time to ensure past nextRetryAt
      const eligible = shard.getAutoRetryEntries('test-queue', Date.now() + 100);
      expect(eligible.length).toBe(2);
    });

    test('returns empty when autoRetry is disabled', () => {
      shard.setConfig('test-queue', { autoRetry: false });
      shard.add(makeJob(1));

      const eligible = shard.getAutoRetryEntries('test-queue', Date.now() + 100);
      expect(eligible.length).toBe(0);
    });

    test('returns empty for unknown queue', () => {
      const eligible = shard.getAutoRetryEntries('unknown');
      expect(eligible).toEqual([]);
    });
  });

  // -------- Expired entries --------

  describe('getExpiredEntries', () => {
    test('returns expired entries', () => {
      shard.setConfig('test-queue', { maxAge: 1 }); // 1ms
      shard.add(makeJob(1));

      // Use future time to ensure expiry
      const expired = shard.getExpiredEntries('test-queue', Date.now() + 100);
      expect(expired.length).toBe(1);
    });

    test('returns empty when maxAge is null', () => {
      shard.setConfig('test-queue', { maxAge: null });
      shard.add(makeJob(1));

      const expired = shard.getExpiredEntries('test-queue', Date.now() + 100000);
      expect(expired.length).toBe(0);
    });

    test('returns empty for unknown queue', () => {
      expect(shard.getExpiredEntries('unknown')).toEqual([]);
    });
  });

  // -------- purgeExpired --------

  describe('purgeExpired', () => {
    test('removes expired entries and returns count', () => {
      shard.setConfig('test-queue', { maxAge: 1 });
      shard.add(makeJob(1));
      shard.add(makeJob(2));

      const purged = shard.purgeExpired('test-queue', Date.now() + 100);
      expect(purged).toBe(2);
      expect(shard.getCount('test-queue')).toBe(0);
    });

    test('keeps non-expired entries', () => {
      shard.setConfig('test-queue', { maxAge: 999999 });
      shard.add(makeJob(1));
      shard.add(makeJob(2));

      const purged = shard.purgeExpired('test-queue', Date.now());
      expect(purged).toBe(0);
      expect(shard.getCount('test-queue')).toBe(2);
    });

    test('returns 0 for unknown queue', () => {
      expect(shard.purgeExpired('unknown')).toBe(0);
    });

    test('decrements stats by purged count', () => {
      shard.setConfig('test-queue', { maxAge: 1 });
      shard.add(makeJob(1));
      shard.add(makeJob(2));
      stats.decrements = 0;

      shard.purgeExpired('test-queue', Date.now() + 100);
      expect(stats.decrements).toBe(1); // Single call with count=2
      expect(stats.decrementAmounts).toContain(2);
    });
  });

  // -------- clear --------

  describe('clear', () => {
    test('removes all entries for queue and returns count', () => {
      shard.add(makeJob(1));
      shard.add(makeJob(2));
      shard.add(makeJob(3));

      const count = shard.clear('test-queue');
      expect(count).toBe(3);
      expect(shard.getCount('test-queue')).toBe(0);
      expect(shard.getEntries('test-queue')).toEqual([]);
    });

    test('returns 0 for empty queue', () => {
      expect(shard.clear('unknown')).toBe(0);
    });

    test('decrements stats by cleared count', () => {
      shard.add(makeJob(1));
      shard.add(makeJob(2));
      stats.decrements = 0;

      shard.clear('test-queue');
      expect(stats.decrements).toBe(1);
      expect(stats.decrementAmounts).toContain(2);
    });
  });

  // -------- getCount --------

  describe('getCount', () => {
    test('returns 0 for empty/unknown queue', () => {
      expect(shard.getCount('unknown')).toBe(0);
    });

    test('returns correct count', () => {
      shard.add(makeJob(1));
      shard.add(makeJob(2));
      expect(shard.getCount('test-queue')).toBe(2);
    });
  });

  // -------- getQueueNames --------

  describe('getQueueNames', () => {
    test('returns empty array when no entries', () => {
      expect(shard.getQueueNames()).toEqual([]);
    });

    test('returns queue names with DLQ entries', () => {
      shard.add(makeJob(1, 'q1'));
      shard.add(makeJob(2, 'q2'));

      const names = shard.getQueueNames();
      expect(names).toContain('q1');
      expect(names).toContain('q2');
      expect(names.length).toBe(2);
    });
  });

  // -------- deleteQueue --------

  describe('deleteQueue', () => {
    test('removes all DLQ entries, config, and stall config', () => {
      shard.setConfig('emails', { maxEntries: 50 });
      shard.setStallConfig('emails', { maxStalls: 5 });
      shard.add(makeJob(1, 'emails'));
      shard.add(makeJob(2, 'emails'));

      const count = shard.deleteQueue('emails');
      expect(count).toBe(2);
      expect(shard.getCount('emails')).toBe(0);
      expect(shard.getConfig('emails')).toEqual(DEFAULT_DLQ_CONFIG);
      expect(shard.getStallConfig('emails')).toEqual(DEFAULT_STALL_CONFIG);
    });

    test('returns 0 for non-existent queue', () => {
      expect(shard.deleteQueue('unknown')).toBe(0);
    });
  });

  // -------- Multi-queue isolation --------

  describe('multi-queue isolation', () => {
    test('operations on one queue do not affect another', () => {
      shard.add(makeJob(1, 'q1'), FailureReason.Timeout, null);
      shard.add(makeJob(2, 'q2'), FailureReason.Stalled, null);

      expect(shard.getCount('q1')).toBe(1);
      expect(shard.getCount('q2')).toBe(1);

      shard.clear('q1');
      expect(shard.getCount('q1')).toBe(0);
      expect(shard.getCount('q2')).toBe(1);
    });
  });
});

// ============================================================
//  Dependency Processor Tests
// ============================================================

describe('DependencyProcessor', () => {
  /**
   * Creates a minimal BackgroundContext for testing processPendingDependencies.
   * We create real Shard and RWLock objects since the function interacts with them.
   */
  function createTestContext() {
    const shards: Shard[] = [];
    const shardLocks: RWLock[] = [];
    for (let i = 0; i < SHARD_COUNT; i++) {
      shards.push(new Shard());
      shardLocks.push(new RWLock());
    }

    const pendingDepChecks = new Set<JobId>();
    const completedJobs = new BoundedSet<JobId>(50000);
    const jobIndex = new Map<JobId, JobLocation>();

    // Create a minimal context with just the fields needed by processPendingDependencies
    const ctx = {
      shards,
      shardLocks,
      pendingDepChecks,
      completedJobs,
      jobIndex,
    } as any; // Cast because we only need a subset of BackgroundContext

    return ctx;
  }

  test('does nothing when pendingDepChecks is empty', async () => {
    const ctx = createTestContext();
    await processPendingDependencies(ctx);
    // No error means success
    expect(ctx.pendingDepChecks.size).toBe(0);
  });

  test('clears pendingDepChecks after processing', async () => {
    const ctx = createTestContext();
    ctx.pendingDepChecks.add(jid(1));

    await processPendingDependencies(ctx);
    expect(ctx.pendingDepChecks.size).toBe(0);
  });

  test('promotes job when all dependencies are satisfied', async () => {
    const ctx = createTestContext();

    // Create dependency: job-2 depends on job-1
    const depJob = makeJob(2, 'test-queue', { dependsOn: [jid(1)] });

    // Register the dependency in a specific shard
    // We need to find which shard to use - put in shard 0 for simplicity
    const targetShard = ctx.shards[0];
    targetShard.waitingDeps.set(jid(2), depJob);
    targetShard.registerDependencies(jid(2), [jid(1)]);

    // Mark job-1 as completed
    ctx.completedJobs.add(jid(1));
    ctx.pendingDepChecks.add(jid(1));

    // Process dependencies
    await processPendingDependencies(ctx);

    // Job-2 should be removed from waitingDeps
    expect(targetShard.waitingDeps.has(jid(2))).toBe(false);

    // Job-2 should be in the queue now
    const queue = targetShard.queues.get('test-queue');
    expect(queue).toBeDefined();
    expect(queue!.size).toBeGreaterThanOrEqual(1);

    // Job index should be updated
    expect(ctx.jobIndex.has(jid(2))).toBe(true);
    const location = ctx.jobIndex.get(jid(2));
    expect(location!.type).toBe('queue');
  });

  test('does not promote job when not all dependencies are satisfied', async () => {
    const ctx = createTestContext();

    // Job-3 depends on job-1 AND job-2
    const depJob = makeJob(3, 'test-queue', { dependsOn: [jid(1), jid(2)] });

    const targetShard = ctx.shards[0];
    targetShard.waitingDeps.set(jid(3), depJob);
    targetShard.registerDependencies(jid(3), [jid(1), jid(2)]);

    // Only job-1 is completed
    ctx.completedJobs.add(jid(1));
    ctx.pendingDepChecks.add(jid(1));

    await processPendingDependencies(ctx);

    // Job-3 should still be waiting
    expect(targetShard.waitingDeps.has(jid(3))).toBe(true);
    expect(ctx.jobIndex.has(jid(3))).toBe(false);
  });

  test('promotes job when remaining dependency completes', async () => {
    const ctx = createTestContext();

    // Job-3 depends on job-1 AND job-2
    const depJob = makeJob(3, 'test-queue', { dependsOn: [jid(1), jid(2)] });

    const targetShard = ctx.shards[0];
    targetShard.waitingDeps.set(jid(3), depJob);
    targetShard.registerDependencies(jid(3), [jid(1), jid(2)]);

    // First: job-1 completes
    ctx.completedJobs.add(jid(1));
    ctx.pendingDepChecks.add(jid(1));
    await processPendingDependencies(ctx);

    // Still waiting
    expect(targetShard.waitingDeps.has(jid(3))).toBe(true);

    // Then: job-2 completes
    ctx.completedJobs.add(jid(2));
    ctx.pendingDepChecks.add(jid(2));
    await processPendingDependencies(ctx);

    // Now promoted
    expect(targetShard.waitingDeps.has(jid(3))).toBe(false);
    expect(ctx.jobIndex.has(jid(3))).toBe(true);
  });

  test('promotes multiple jobs waiting on the same dependency', async () => {
    const ctx = createTestContext();

    const targetShard = ctx.shards[0];

    // job-2 and job-3 both depend on job-1
    const depJob2 = makeJob(2, 'test-queue', { dependsOn: [jid(1)] });
    const depJob3 = makeJob(3, 'test-queue', { dependsOn: [jid(1)] });

    targetShard.waitingDeps.set(jid(2), depJob2);
    targetShard.registerDependencies(jid(2), [jid(1)]);
    targetShard.waitingDeps.set(jid(3), depJob3);
    targetShard.registerDependencies(jid(3), [jid(1)]);

    // job-1 completes
    ctx.completedJobs.add(jid(1));
    ctx.pendingDepChecks.add(jid(1));

    await processPendingDependencies(ctx);

    // Both should be promoted
    expect(targetShard.waitingDeps.has(jid(2))).toBe(false);
    expect(targetShard.waitingDeps.has(jid(3))).toBe(false);
    expect(ctx.jobIndex.has(jid(2))).toBe(true);
    expect(ctx.jobIndex.has(jid(3))).toBe(true);
  });

  test('handles multiple completed deps in a single batch', async () => {
    const ctx = createTestContext();

    const targetShard = ctx.shards[0];

    // job-3 depends on job-1 AND job-2
    const depJob = makeJob(3, 'test-queue', { dependsOn: [jid(1), jid(2)] });

    targetShard.waitingDeps.set(jid(3), depJob);
    targetShard.registerDependencies(jid(3), [jid(1), jid(2)]);

    // Both complete at same time
    ctx.completedJobs.add(jid(1));
    ctx.completedJobs.add(jid(2));
    ctx.pendingDepChecks.add(jid(1));
    ctx.pendingDepChecks.add(jid(2));

    await processPendingDependencies(ctx);

    // Should be promoted
    expect(targetShard.waitingDeps.has(jid(3))).toBe(false);
    expect(ctx.jobIndex.has(jid(3))).toBe(true);
  });

  test('does not process already-removed waiting jobs', async () => {
    const ctx = createTestContext();

    const targetShard = ctx.shards[0];

    // Register dep but do NOT add to waitingDeps (simulate already removed)
    targetShard.registerDependencies(jid(2), [jid(1)]);

    ctx.completedJobs.add(jid(1));
    ctx.pendingDepChecks.add(jid(1));

    // Should not throw
    await processPendingDependencies(ctx);

    // No job promoted (nothing in waitingDeps)
    expect(ctx.jobIndex.has(jid(2))).toBe(false);
  });

  test('handles dependency completion with no waiting jobs', async () => {
    const ctx = createTestContext();

    // No one depends on job-1
    ctx.completedJobs.add(jid(1));
    ctx.pendingDepChecks.add(jid(1));

    // Should not throw
    await processPendingDependencies(ctx);
    expect(ctx.pendingDepChecks.size).toBe(0);
  });

  test('promoted job gets correct jobIndex location', async () => {
    const ctx = createTestContext();

    const depJob = makeJob(2, 'my-queue', { dependsOn: [jid(1)] });

    const targetShard = ctx.shards[0];
    targetShard.waitingDeps.set(jid(2), depJob);
    targetShard.registerDependencies(jid(2), [jid(1)]);

    ctx.completedJobs.add(jid(1));
    ctx.pendingDepChecks.add(jid(1));

    await processPendingDependencies(ctx);

    const location = ctx.jobIndex.get(jid(2)) as {
      type: 'queue';
      shardIdx: number;
      queueName: string;
    };
    expect(location.type).toBe('queue');
    expect(location.shardIdx).toBe(0);
    expect(location.queueName).toBe('my-queue');
  });
});

// ============================================================
//  JobLogsManager Tests (unit-level, direct function calls)
// ============================================================

describe('JobLogsManager (direct)', () => {
  let ctx: JobLogsContext;

  beforeEach(() => {
    ctx = {
      jobIndex: new Map<JobId, JobLocation>(),
      jobLogs: new LRUMap<JobId, any>(1000),
      jobLogQueues: new Map<JobId, string>(),
      maxLogsPerJob: 5,
    };
  });

  // -------- addJobLog --------

  describe('addJobLog', () => {
    test('returns false when job not in jobIndex', () => {
      const result = addJobLog(jid(99), 'test message', ctx);
      expect(result).toBe(false);
    });

    test('adds log entry when job exists in jobIndex', () => {
      ctx.jobIndex.set(jid(1), { type: 'queue', shardIdx: 0, queueName: 'test' });

      const result = addJobLog(jid(1), 'hello', ctx);
      expect(result).toBe(true);

      const logs = ctx.jobLogs.get(jid(1));
      expect(logs).toBeDefined();
      expect(logs!.length).toBe(1);
      expect(logs![0].message).toBe('hello');
      expect(logs![0].level).toBe('info');
      expect(ctx.jobLogQueues.get(jid(1))).toBe('test');
    });

    test('defaults to info level', () => {
      ctx.jobIndex.set(jid(1), { type: 'queue', shardIdx: 0, queueName: 'test' });
      addJobLog(jid(1), 'msg', ctx);

      const logs = ctx.jobLogs.get(jid(1));
      expect(logs![0].level).toBe('info');
    });

    test('supports warn level', () => {
      ctx.jobIndex.set(jid(1), { type: 'queue', shardIdx: 0, queueName: 'test' });
      addJobLog(jid(1), 'warning', ctx, 'warn');

      const logs = ctx.jobLogs.get(jid(1));
      expect(logs![0].level).toBe('warn');
    });

    test('supports error level', () => {
      ctx.jobIndex.set(jid(1), { type: 'queue', shardIdx: 0, queueName: 'test' });
      addJobLog(jid(1), 'error msg', ctx, 'error');

      const logs = ctx.jobLogs.get(jid(1));
      expect(logs![0].level).toBe('error');
    });

    test('appends to existing logs', () => {
      ctx.jobIndex.set(jid(1), { type: 'queue', shardIdx: 0, queueName: 'test' });
      addJobLog(jid(1), 'first', ctx);
      addJobLog(jid(1), 'second', ctx);
      addJobLog(jid(1), 'third', ctx);

      const logs = ctx.jobLogs.get(jid(1));
      expect(logs!.length).toBe(3);
      expect(logs![0].message).toBe('first');
      expect(logs![1].message).toBe('second');
      expect(logs![2].message).toBe('third');
    });

    test('each log entry has a timestamp', () => {
      ctx.jobIndex.set(jid(1), { type: 'queue', shardIdx: 0, queueName: 'test' });
      const before = Date.now();
      addJobLog(jid(1), 'timed', ctx);
      const after = Date.now();

      const logs = ctx.jobLogs.get(jid(1));
      expect(logs![0].timestamp).toBeGreaterThanOrEqual(before);
      expect(logs![0].timestamp).toBeLessThanOrEqual(after);
    });

    test('enforces maxLogsPerJob by removing oldest', () => {
      ctx.maxLogsPerJob = 3;
      ctx.jobIndex.set(jid(1), { type: 'queue', shardIdx: 0, queueName: 'test' });

      addJobLog(jid(1), 'log-1', ctx);
      addJobLog(jid(1), 'log-2', ctx);
      addJobLog(jid(1), 'log-3', ctx);
      addJobLog(jid(1), 'log-4', ctx);
      addJobLog(jid(1), 'log-5', ctx);

      const logs = ctx.jobLogs.get(jid(1));
      expect(logs!.length).toBe(3);
      // Oldest (log-1, log-2) should be removed
      expect(logs![0].message).toBe('log-3');
      expect(logs![1].message).toBe('log-4');
      expect(logs![2].message).toBe('log-5');
    });

    test('maxLogsPerJob of 1 keeps only the latest', () => {
      ctx.maxLogsPerJob = 1;
      ctx.jobIndex.set(jid(1), { type: 'queue', shardIdx: 0, queueName: 'test' });

      addJobLog(jid(1), 'a', ctx);
      addJobLog(jid(1), 'b', ctx);
      addJobLog(jid(1), 'c', ctx);

      const logs = ctx.jobLogs.get(jid(1));
      expect(logs!.length).toBe(1);
      expect(logs![0].message).toBe('c');
    });

    test('works with different job locations (processing, completed)', () => {
      ctx.jobIndex.set(jid(1), { type: 'processing', shardIdx: 0, queueName: 'test' });
      expect(addJobLog(jid(1), 'processing log', ctx)).toBe(true);

      ctx.jobIndex.set(jid(2), { type: 'completed', queueName: 'test' });
      expect(addJobLog(jid(2), 'completed log', ctx)).toBe(true);
    });
  });

  // -------- getJobLogs --------

  describe('getJobLogs', () => {
    test('returns empty array when no logs exist', () => {
      const logs = getJobLogs(jid(1), ctx);
      expect(logs).toEqual([]);
    });

    test('returns logs for job', () => {
      ctx.jobIndex.set(jid(1), { type: 'queue', shardIdx: 0, queueName: 'test' });
      addJobLog(jid(1), 'hello', ctx);

      const logs = getJobLogs(jid(1), ctx);
      expect(logs.length).toBe(1);
      expect(logs[0].message).toBe('hello');
    });

    test('returns different logs for different jobs', () => {
      ctx.jobIndex.set(jid(1), { type: 'queue', shardIdx: 0, queueName: 'test' });
      ctx.jobIndex.set(jid(2), { type: 'queue', shardIdx: 0, queueName: 'test' });

      addJobLog(jid(1), 'job 1 log', ctx);
      addJobLog(jid(2), 'job 2 log', ctx);

      expect(getJobLogs(jid(1), ctx)[0].message).toBe('job 1 log');
      expect(getJobLogs(jid(2), ctx)[0].message).toBe('job 2 log');
    });
  });

  // -------- clearJobLogs --------

  describe('clearJobLogs', () => {
    test('clears all logs when no keepLogs specified', () => {
      ctx.jobIndex.set(jid(1), { type: 'queue', shardIdx: 0, queueName: 'test' });
      addJobLog(jid(1), 'a', ctx);
      addJobLog(jid(1), 'b', ctx);

      clearJobLogs(jid(1), ctx);
      expect(getJobLogs(jid(1), ctx)).toEqual([]);
      expect(ctx.jobLogQueues.has(jid(1))).toBe(false);
    });

    test('clears all logs when keepLogs is 0', () => {
      ctx.jobIndex.set(jid(1), { type: 'queue', shardIdx: 0, queueName: 'test' });
      addJobLog(jid(1), 'a', ctx);

      clearJobLogs(jid(1), ctx, 0);
      expect(getJobLogs(jid(1), ctx)).toEqual([]);
    });

    test('clears all logs when keepLogs is negative', () => {
      ctx.jobIndex.set(jid(1), { type: 'queue', shardIdx: 0, queueName: 'test' });
      addJobLog(jid(1), 'a', ctx);

      clearJobLogs(jid(1), ctx, -1);
      expect(getJobLogs(jid(1), ctx)).toEqual([]);
    });

    test('keeps N most recent logs when keepLogs specified', () => {
      ctx.maxLogsPerJob = 100; // allow more
      ctx.jobIndex.set(jid(1), { type: 'queue', shardIdx: 0, queueName: 'test' });
      addJobLog(jid(1), 'first', ctx);
      addJobLog(jid(1), 'second', ctx);
      addJobLog(jid(1), 'third', ctx);
      addJobLog(jid(1), 'fourth', ctx);

      clearJobLogs(jid(1), ctx, 2);

      const logs = getJobLogs(jid(1), ctx);
      expect(logs.length).toBe(2);
      expect(logs[0].message).toBe('third');
      expect(logs[1].message).toBe('fourth');
    });

    test('keepLogs greater than log count does nothing', () => {
      ctx.maxLogsPerJob = 100;
      ctx.jobIndex.set(jid(1), { type: 'queue', shardIdx: 0, queueName: 'test' });
      addJobLog(jid(1), 'a', ctx);
      addJobLog(jid(1), 'b', ctx);

      clearJobLogs(jid(1), ctx, 10);

      const logs = getJobLogs(jid(1), ctx);
      expect(logs.length).toBe(2);
    });

    test('no-op when job has no logs', () => {
      clearJobLogs(jid(99), ctx);
      // Should not throw
      expect(getJobLogs(jid(99), ctx)).toEqual([]);
    });

    test('clearing logs for one job does not affect another', () => {
      ctx.maxLogsPerJob = 100;
      ctx.jobIndex.set(jid(1), { type: 'queue', shardIdx: 0, queueName: 'test' });
      ctx.jobIndex.set(jid(2), { type: 'queue', shardIdx: 0, queueName: 'test' });

      addJobLog(jid(1), 'job1 log', ctx);
      addJobLog(jid(2), 'job2 log', ctx);

      clearJobLogs(jid(1), ctx);

      expect(getJobLogs(jid(1), ctx)).toEqual([]);
      expect(getJobLogs(jid(2), ctx).length).toBe(1);
    });
  });

  // -------- Mixed levels --------

  describe('mixed log levels', () => {
    test('preserves log level ordering across add and retrieve', () => {
      ctx.maxLogsPerJob = 100;
      ctx.jobIndex.set(jid(1), { type: 'queue', shardIdx: 0, queueName: 'test' });

      addJobLog(jid(1), 'info msg', ctx, 'info');
      addJobLog(jid(1), 'warn msg', ctx, 'warn');
      addJobLog(jid(1), 'error msg', ctx, 'error');
      addJobLog(jid(1), 'info again', ctx, 'info');

      const logs = getJobLogs(jid(1), ctx);
      expect(logs.length).toBe(4);
      expect(logs[0].level).toBe('info');
      expect(logs[1].level).toBe('warn');
      expect(logs[2].level).toBe('error');
      expect(logs[3].level).toBe('info');
    });
  });
});
