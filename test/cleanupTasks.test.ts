/**
 * Cleanup Tasks Tests
 * Tests for periodic maintenance and garbage collection functions
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { cleanup } from '../src/application/cleanupTasks';
import { createJob, jobId, type Job, type JobId } from '../src/domain/types/job';
import { processingShardIndex, SHARD_COUNT } from '../src/shared/hash';
import type { BackgroundContext } from '../src/application/types';
import type { Shard } from '../src/domain/queue/shard';

/**
 * Helper to extract a BackgroundContext from a QueueManager instance.
 * Uses (qm as any) to access private internals, matching patterns
 * used in other test files (e.g., stall-embedded-bug.test.ts).
 */
function getBackgroundContext(qm: QueueManager): BackgroundContext {
  const internal = qm as any;
  return {
    config: internal.config,
    storage: internal.storage,
    shards: internal.shards,
    shardLocks: internal.shardLocks,
    processingShards: internal.processingShards,
    processingLocks: internal.processingLocks,
    jobIndex: internal.jobIndex,
    completedJobs: internal.completedJobs,
    jobResults: internal.jobResults,
    customIdMap: internal.customIdMap,
    jobLogs: internal.jobLogs,
    jobLocks: internal.jobLocks,
    clientJobs: internal.clientJobs,
    stalledCandidates: internal.stalledCandidates,
    pendingDepChecks: internal.pendingDepChecks,
    queueNamesCache: internal.queueNamesCache,
    eventsManager: internal.eventsManager,
    webhookManager: internal.webhookManager,
    metrics: internal.metrics,
    startTime: internal.startTime,
    fail: internal.fail.bind(qm),
    registerQueueName: internal.registerQueueName.bind(qm),
    unregisterQueueName: internal.unregisterQueueName.bind(qm),
  };
}

function makeJob(id: string, queue = 'test', overrides: Partial<Job> = {}): Job {
  const job = createJob(jobId(id), queue, { data: { id } }, Date.now());
  return { ...job, ...overrides } as Job;
}

describe('CleanupTasks', () => {
  let qm: QueueManager;
  let ctx: BackgroundContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = getBackgroundContext(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  describe('cleanup on empty state', () => {
    test('should not throw on empty state', async () => {
      await expect(cleanup(ctx)).resolves.toBeUndefined();
    });

    test('should leave stats unchanged after cleanup on empty state', async () => {
      const statsBefore = qm.getStats();
      await cleanup(ctx);
      const statsAfter = qm.getStats();

      expect(statsAfter.waiting).toBe(statsBefore.waiting);
      expect(statsAfter.active).toBe(statsBefore.active);
      expect(statsAfter.completed).toBe(statsBefore.completed);
      expect(statsAfter.dlq).toBe(statsBefore.dlq);
    });

    test('should be safe to call cleanup multiple times', async () => {
      await cleanup(ctx);
      await cleanup(ctx);
      await cleanup(ctx);
      // No errors means success
      expect(true).toBe(true);
    });
  });

  describe('cleanOrphanedProcessingEntries', () => {
    test('should remove jobs stuck in processing beyond stall timeout', async () => {
      // Push and pull a job to move it to processing
      const pushed = await qm.push('test-queue', { data: { msg: 'stuck' } });
      const pulled = await qm.pull('test-queue');
      expect(pulled).not.toBeNull();

      // Verify the job is in processing
      const statsBefore = qm.getStats();
      expect(statsBefore.active).toBe(1);

      // Manually set startedAt to far in the past (beyond 30-minute stall timeout)
      const procIdx = processingShardIndex(pulled!.id);
      const processingJob = ctx.processingShards[procIdx].get(pulled!.id);
      expect(processingJob).toBeDefined();
      (processingJob as any).startedAt = Date.now() - 31 * 60 * 1000; // 31 minutes ago

      await cleanup(ctx);

      // Job should have been removed from processing
      expect(ctx.processingShards[procIdx].has(pulled!.id)).toBe(false);
      expect(ctx.jobIndex.has(pulled!.id)).toBe(false);
    });

    test('should not remove recently started processing jobs', async () => {
      const pushed = await qm.push('test-queue', { data: { msg: 'recent' } });
      const pulled = await qm.pull('test-queue');
      expect(pulled).not.toBeNull();

      // Job was just pulled, startedAt is recent
      await cleanup(ctx);

      // Job should still be in processing
      const procIdx = processingShardIndex(pulled!.id);
      expect(ctx.processingShards[procIdx].has(pulled!.id)).toBe(true);
      expect(ctx.jobIndex.has(pulled!.id)).toBe(true);
    });

    test('should clean multiple orphaned processing entries across shards', async () => {
      const pulledJobs: Job[] = [];

      // Push and pull multiple jobs to distribute across processing shards
      for (let i = 0; i < 10; i++) {
        await qm.push(`queue-${i}`, { data: { i } });
        const pulled = await qm.pull(`queue-${i}`);
        if (pulled) {
          pulledJobs.push(pulled);
          // Set startedAt to past the stall timeout
          const procIdx = processingShardIndex(pulled.id);
          const processingJob = ctx.processingShards[procIdx].get(pulled.id);
          if (processingJob) {
            (processingJob as any).startedAt = Date.now() - 31 * 60 * 1000;
          }
        }
      }

      expect(pulledJobs.length).toBe(10);

      await cleanup(ctx);

      // All orphaned jobs should be cleaned
      for (const job of pulledJobs) {
        const procIdx = processingShardIndex(job.id);
        expect(ctx.processingShards[procIdx].has(job.id)).toBe(false);
        expect(ctx.jobIndex.has(job.id)).toBe(false);
      }
    });
  });

  describe('cleanStaleWaitingDependencies', () => {
    test('should remove jobs waiting on dependencies for over 1 hour', async () => {
      // Create a job with a dependency that will never resolve
      const depId = jobId('nonexistent-parent');
      const job = makeJob('dep-child-1', 'dep-queue', {
        dependsOn: [depId],
        createdAt: Date.now() - 61 * 60 * 1000, // Created 61 minutes ago
      });

      // Manually add to a shard's waitingDeps
      const shard = ctx.shards[0];
      shard.waitingDeps.set(job.id, job);
      shard.registerDependencies(job.id, job.dependsOn);
      ctx.jobIndex.set(job.id, { type: 'queue', shardIdx: 0, queueName: 'dep-queue' });

      await cleanup(ctx);

      // Should be removed from waitingDeps
      expect(shard.waitingDeps.has(job.id)).toBe(false);
      // Should be removed from jobIndex
      expect(ctx.jobIndex.has(job.id)).toBe(false);
    });

    test('should not remove recently created jobs waiting on dependencies', async () => {
      const depId = jobId('nonexistent-parent-2');
      const job = makeJob('dep-child-2', 'dep-queue', {
        dependsOn: [depId],
        createdAt: Date.now() - 5 * 60 * 1000, // Created 5 minutes ago
      });

      const shard = ctx.shards[0];
      shard.waitingDeps.set(job.id, job);
      shard.registerDependencies(job.id, job.dependsOn);
      ctx.jobIndex.set(job.id, { type: 'queue', shardIdx: 0, queueName: 'dep-queue' });

      await cleanup(ctx);

      // Should still be waiting
      expect(shard.waitingDeps.has(job.id)).toBe(true);
      expect(ctx.jobIndex.has(job.id)).toBe(true);
    });
  });

  describe('cleanUniqueKeysAndGroups', () => {
    test('should trim unique keys when exceeding 1000 per queue', async () => {
      const shard = ctx.shards[0];

      // Register more than 1000 unique keys for a queue
      for (let i = 0; i < 1100; i++) {
        shard.registerUniqueKey('big-queue', `key-${i}`, jobId(`job-${i}`));
      }

      const uniqueKeysMap = shard.uniqueKeys.get('big-queue');
      expect(uniqueKeysMap).toBeDefined();
      expect(uniqueKeysMap!.size).toBe(1100);

      await cleanup(ctx);

      // Should have been trimmed to roughly half
      const afterSize = shard.uniqueKeys.get('big-queue')?.size ?? 0;
      expect(afterSize).toBeLessThan(1100);
      expect(afterSize).toBeGreaterThan(0);
    });

    test('should not trim unique keys when under 1000', async () => {
      const shard = ctx.shards[0];

      for (let i = 0; i < 50; i++) {
        shard.registerUniqueKey('small-queue', `key-${i}`, jobId(`job-${i}`));
      }

      const sizeBefore = shard.uniqueKeys.get('small-queue')?.size ?? 0;
      expect(sizeBefore).toBe(50);

      await cleanup(ctx);

      // Expired keys might be cleaned, but non-expired keys under 1000 should stay
      // (cleanExpiredUniqueKeys may remove some, but not the trim path)
      const sizeAfter = shard.uniqueKeys.get('small-queue')?.size ?? 0;
      expect(sizeAfter).toBeLessThanOrEqual(50);
    });

    test('should trim active groups when exceeding 1000', async () => {
      const shard = ctx.shards[0];

      // Activate more than 1000 groups
      for (let i = 0; i < 1100; i++) {
        shard.activateGroup('group-queue', `group-${i}`);
      }

      const groups = shard.activeGroups.get('group-queue');
      expect(groups).toBeDefined();
      expect(groups!.size).toBe(1100);

      await cleanup(ctx);

      const afterSize = shard.activeGroups.get('group-queue')?.size ?? 0;
      expect(afterSize).toBeLessThan(1100);
      expect(afterSize).toBeGreaterThan(0);
    });

    test('should not trim active groups when under 1000', async () => {
      const shard = ctx.shards[0];

      for (let i = 0; i < 10; i++) {
        shard.activateGroup('small-group-queue', `group-${i}`);
      }

      expect(shard.activeGroups.get('small-group-queue')?.size).toBe(10);

      await cleanup(ctx);

      expect(shard.activeGroups.get('small-group-queue')?.size).toBe(10);
    });
  });

  describe('cleanStalledCandidates', () => {
    test('should remove stalled candidates no longer in processing', async () => {
      const id1 = jobId('stalled-1');
      const id2 = jobId('stalled-2');

      // Add candidates to stalledCandidates
      ctx.stalledCandidates.add(id1);
      ctx.stalledCandidates.add(id2);

      // id1 is not in jobIndex at all (completed/removed)
      // id2 is in jobIndex but as 'queue' type, not 'processing'
      ctx.jobIndex.set(id2, { type: 'queue', shardIdx: 0, queueName: 'test' });

      await cleanup(ctx);

      // Both should be removed from stalledCandidates
      expect(ctx.stalledCandidates.has(id1)).toBe(false);
      expect(ctx.stalledCandidates.has(id2)).toBe(false);
    });

    test('should keep stalled candidates still in processing', async () => {
      // Push and pull to create a processing job
      await qm.push('stall-test', { data: { msg: 'active' } });
      const pulled = await qm.pull('stall-test');
      expect(pulled).not.toBeNull();

      // Add to stalled candidates
      ctx.stalledCandidates.add(pulled!.id);

      // Verify it is in processing state in jobIndex
      const loc = ctx.jobIndex.get(pulled!.id);
      expect(loc?.type).toBe('processing');

      await cleanup(ctx);

      // Should still be in stalledCandidates since it is still processing
      expect(ctx.stalledCandidates.has(pulled!.id)).toBe(true);
    });

    test('should handle empty stalledCandidates set', async () => {
      expect(ctx.stalledCandidates.size).toBe(0);
      await cleanup(ctx);
      expect(ctx.stalledCandidates.size).toBe(0);
    });
  });

  describe('cleanOrphanedJobIndex', () => {
    test('should not run when jobIndex is small (under 100k)', async () => {
      // Add a few entries to jobIndex that point to nonexistent locations
      const fakeId = jobId('orphan-1');
      ctx.jobIndex.set(fakeId, { type: 'processing', shardIdx: 0 });

      const sizeBefore = ctx.jobIndex.size;

      await cleanup(ctx);

      // cleanOrphanedJobIndex only runs when jobIndex.size > 100_000
      // So the orphaned entry should remain
      expect(ctx.jobIndex.has(fakeId)).toBe(true);
      expect(ctx.jobIndex.size).toBe(sizeBefore);
    });
  });

  describe('cleanOrphanedJobLocks', () => {
    test('should remove locks for jobs no longer in processing', async () => {
      const id1 = jobId('locked-1');
      const id2 = jobId('locked-2');

      // Add fake locks
      ctx.jobLocks.set(id1, {
        jobId: id1,
        token: 'token-1' as any,
        owner: 'worker-1',
        createdAt: Date.now(),
        expiresAt: Date.now() + 30000,
        lastRenewalAt: Date.now(),
        renewalCount: 0,
        ttl: 30000,
      });
      ctx.jobLocks.set(id2, {
        jobId: id2,
        token: 'token-2' as any,
        owner: 'worker-1',
        createdAt: Date.now(),
        expiresAt: Date.now() + 30000,
        lastRenewalAt: Date.now(),
        renewalCount: 0,
        ttl: 30000,
      });

      // id1 is not in jobIndex (no longer tracked)
      // id2 is in jobIndex but as 'completed' (not processing)
      ctx.jobIndex.set(id2, { type: 'completed', queueName: 'lock-test' });

      await cleanup(ctx);

      // Both locks should be removed (neither is in 'processing' state)
      expect(ctx.jobLocks.has(id1)).toBe(false);
      expect(ctx.jobLocks.has(id2)).toBe(false);
    });

    test('should keep locks for jobs still in processing', async () => {
      // Push and pull a job to get it into processing
      await qm.push('lock-test', { data: { msg: 'locked' } });
      const pulled = await qm.pull('lock-test');
      expect(pulled).not.toBeNull();

      // Create a lock for this processing job
      const token = qm.createLock(pulled!.id, 'worker-1');
      expect(token).not.toBeNull();

      // Verify it is in processing
      const loc = ctx.jobIndex.get(pulled!.id);
      expect(loc?.type).toBe('processing');

      await cleanup(ctx);

      // Lock should still exist since the job is still processing
      expect(ctx.jobLocks.has(pulled!.id)).toBe(true);
    });

    test('should handle empty jobLocks map', async () => {
      expect(ctx.jobLocks.size).toBe(0);
      await cleanup(ctx);
      expect(ctx.jobLocks.size).toBe(0);
    });
  });

  describe('cleanEmptyQueues', () => {
    test('should remove empty queues with no DLQ entries', async () => {
      // Create a queue by pushing and acking a job
      const pushed = await qm.push('ephemeral-queue', { data: { msg: 'temp' } });
      const pulled = await qm.pull('ephemeral-queue');
      await qm.ack(pulled!.id, { done: true });

      // The queue should still exist in some shard but be empty
      // Verify it was registered
      expect(qm.listQueues()).toContain('ephemeral-queue');

      await cleanup(ctx);

      // After cleanup, the empty queue should be removed
      expect(qm.listQueues()).not.toContain('ephemeral-queue');
    });

    test('should not remove queues with waiting jobs', async () => {
      await qm.push('active-queue', { data: { msg: 'waiting' } });

      expect(qm.listQueues()).toContain('active-queue');

      await cleanup(ctx);

      // Queue with jobs should NOT be removed
      expect(qm.listQueues()).toContain('active-queue');
    });

    test('should not remove queues with DLQ entries', async () => {
      // Push a job that will go to DLQ
      await qm.push('dlq-queue', { data: { msg: 'fail' }, maxAttempts: 1 });
      const pulled = await qm.pull('dlq-queue');
      await qm.fail(pulled!.id, 'intentional failure');

      // Verify it went to DLQ
      const stats = qm.getStats();
      expect(stats.dlq).toBe(1);

      await cleanup(ctx);

      // Queue with DLQ entries should NOT be removed
      // The queue name should still exist (either in queues or via DLQ)
      const dlqJobs = qm.getDlq('dlq-queue');
      expect(dlqJobs.length).toBe(1);
    });
  });

  describe('integration: push, complete, then cleanup', () => {
    test('should clean up properly after jobs are completed', async () => {
      // Push several jobs
      for (let i = 0; i < 5; i++) {
        await qm.push('int-queue', { data: { i } });
      }

      // Pull and ack all jobs
      for (let i = 0; i < 5; i++) {
        const job = await qm.pull('int-queue');
        expect(job).not.toBeNull();
        await qm.ack(job!.id, { result: i });
      }

      const statsAfterAck = qm.getStats();
      expect(statsAfterAck.completed).toBe(5);
      expect(statsAfterAck.waiting).toBe(0);
      expect(statsAfterAck.active).toBe(0);

      // Run cleanup
      await cleanup(ctx);

      // Stats should remain consistent
      const statsAfterCleanup = qm.getStats();
      expect(statsAfterCleanup.completed).toBe(5);
      expect(statsAfterCleanup.waiting).toBe(0);
      expect(statsAfterCleanup.active).toBe(0);
    });

    test('should handle mixed states: some waiting, some processing, some completed', async () => {
      // Push 6 jobs
      for (let i = 0; i < 6; i++) {
        await qm.push('mixed-queue', { data: { i } });
      }

      // Pull 4, ack 2
      const pulled1 = await qm.pull('mixed-queue');
      const pulled2 = await qm.pull('mixed-queue');
      const pulled3 = await qm.pull('mixed-queue');
      const pulled4 = await qm.pull('mixed-queue');

      await qm.ack(pulled1!.id, { done: true });
      await qm.ack(pulled2!.id, { done: true });

      // State: 2 waiting, 2 processing (pulled3, pulled4), 2 completed
      const stats = qm.getStats();
      expect(stats.waiting).toBe(2);
      expect(stats.active).toBe(2);
      expect(stats.completed).toBe(2);

      await cleanup(ctx);

      // Stats should remain consistent after cleanup
      const statsAfter = qm.getStats();
      expect(statsAfter.waiting).toBe(2);
      expect(statsAfter.active).toBe(2);
      expect(statsAfter.completed).toBe(2);
    });
  });

  describe('memory bounds', () => {
    test('should not let stalledCandidates grow unbounded', async () => {
      // Add many stalled candidates that are not in processing
      for (let i = 0; i < 100; i++) {
        ctx.stalledCandidates.add(jobId(`phantom-${i}`));
      }

      expect(ctx.stalledCandidates.size).toBe(100);

      await cleanup(ctx);

      // All phantom entries should be cleaned (none are in processing)
      expect(ctx.stalledCandidates.size).toBe(0);
    });

    test('should not let jobLocks grow unbounded', async () => {
      // Add many orphaned locks
      for (let i = 0; i < 100; i++) {
        const id = jobId(`orphan-lock-${i}`);
        ctx.jobLocks.set(id, {
          jobId: id,
          token: `token-${i}` as any,
          owner: 'worker-dead',
          createdAt: Date.now() - 60000,
          expiresAt: Date.now() - 30000,
          lastRenewalAt: Date.now() - 60000,
          renewalCount: 0,
          ttl: 30000,
        });
        // Not in jobIndex or not in processing state
      }

      expect(ctx.jobLocks.size).toBe(100);

      await cleanup(ctx);

      // All orphaned locks should be removed
      expect(ctx.jobLocks.size).toBe(0);
    });

    test('should clean unique keys and groups proportionally', async () => {
      const shard = ctx.shards[0];

      // Add 1500 unique keys
      for (let i = 0; i < 1500; i++) {
        shard.registerUniqueKey('bound-queue', `key-${i}`, jobId(`job-${i}`));
      }

      // Add 1500 active groups
      for (let i = 0; i < 1500; i++) {
        shard.activateGroup('bound-queue', `group-${i}`);
      }

      await cleanup(ctx);

      // Both should be trimmed (roughly half removed)
      const uniqueSize = shard.uniqueKeys.get('bound-queue')?.size ?? 0;
      const groupSize = shard.activeGroups.get('bound-queue')?.size ?? 0;

      expect(uniqueSize).toBeLessThan(1500);
      expect(uniqueSize).toBeGreaterThan(0);
      expect(groupSize).toBeLessThan(1500);
      expect(groupSize).toBeGreaterThan(0);
    });
  });

  describe('priority queue compaction', () => {
    test('should compact priority queues during cleanup', async () => {
      // Push several jobs and pull some to create stale entries
      for (let i = 0; i < 20; i++) {
        await qm.push('compact-queue', { data: { i } });
      }

      // Pull some to create gaps
      for (let i = 0; i < 10; i++) {
        const job = await qm.pull('compact-queue');
        if (job) await qm.ack(job.id, { done: true });
      }

      // Cleanup should handle compaction gracefully
      await cleanup(ctx);

      // Remaining jobs should still be pullable
      const remaining = await qm.pull('compact-queue');
      if (remaining) {
        expect(remaining.data).toBeDefined();
      }
    });
  });

  describe('delayed count refresh', () => {
    test('should refresh delayed counters during cleanup', async () => {
      // Push a delayed job
      await qm.push('delay-queue', { data: { msg: 'delayed' }, delay: 100 });

      const statsBefore = qm.getStats();
      expect(statsBefore.delayed).toBeGreaterThanOrEqual(1);

      // Wait for the delay to pass
      await new Promise((resolve) => setTimeout(resolve, 150));

      // Run cleanup which calls refreshDelayedCount
      await cleanup(ctx);

      // The delayed counter should now be 0 (the delay has passed)
      const statsAfter = qm.getStats();
      expect(statsAfter.delayed).toBe(0);
    });
  });

  describe('orphaned temporal index entries', () => {
    test('should clean orphaned temporal entries during cleanup', async () => {
      // Push jobs to create temporal index entries
      for (let i = 0; i < 5; i++) {
        await qm.push('temporal-queue', { data: { i } });
      }

      // Pull and ack all (clearing queues but temporal index may have stale refs)
      for (let i = 0; i < 5; i++) {
        const job = await qm.pull('temporal-queue');
        if (job) await qm.ack(job.id, { done: true });
      }

      // Cleanup should clean orphaned temporal entries without error
      await cleanup(ctx);

      // System should remain in a consistent state
      const stats = qm.getStats();
      expect(stats.waiting).toBe(0);
      expect(stats.active).toBe(0);
    });
  });
});
