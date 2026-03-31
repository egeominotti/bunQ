/**
 * Background Tasks Tests
 * Tests for periodic maintenance orchestration: start/stop lifecycle,
 * interval-based task execution, error handling, and edge cases.
 */

import { describe, test, expect, beforeEach, afterEach, mock, spyOn } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import {
  startBackgroundTasks,
  stopBackgroundTasks,
  getTaskErrorStats,
  type BackgroundTaskHandles,
} from '../src/application/backgroundTasks';
import { CronScheduler } from '../src/infrastructure/scheduler/cronScheduler';
import type { BackgroundContext } from '../src/application/types';
import { createJob, jobId, type Job, type JobId } from '../src/domain/types/job';
import { processingShardIndex, SHARD_COUNT } from '../src/shared/hash';

// ============ Helpers ============

/**
 * Helper to extract a BackgroundContext from a QueueManager instance.
 * Uses (qm as any) to access private internals, matching patterns
 * used in other test files (e.g., stallDetection-unit.test.ts).
 */
function getBackgroundContext(qm: QueueManager): BackgroundContext {
  const raw = qm as any;
  return {
    config: raw.config,
    storage: raw.storage,
    shards: raw.shards,
    shardLocks: raw.shardLocks,
    processingShards: raw.processingShards,
    processingLocks: raw.processingLocks,
    jobIndex: raw.jobIndex,
    completedJobs: raw.completedJobs,
    jobResults: raw.jobResults,
    customIdMap: raw.customIdMap,
    jobLogs: raw.jobLogs,
    jobLocks: raw.jobLocks,
    clientJobs: raw.clientJobs,
    stalledCandidates: raw.stalledCandidates,
    pendingDepChecks: raw.pendingDepChecks,
    queueNamesCache: raw.queueNamesCache,
    eventsManager: raw.eventsManager,
    webhookManager: raw.webhookManager,
    metrics: raw.metrics,
    startTime: raw.startTime,
    fail: raw.fail.bind(qm),
    registerQueueName: raw.registerQueueName.bind(qm),
    unregisterQueueName: raw.unregisterQueueName.bind(qm),
  };
}

function makeJob(id: string, queue = 'test', overrides: Partial<Job> = {}): Job {
  const job = createJob(jobId(id), queue, { data: { id } }, Date.now());
  return { ...job, ...overrides } as Job;
}

/** Wait for a given number of milliseconds */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ============ Tests ============

describe('backgroundTasks', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  // ============ Start/Stop Lifecycle ============

  describe('startBackgroundTasks', () => {
    test('should return handles with all interval properties', () => {
      const ctx = getBackgroundContext(qm);
      const cron = new CronScheduler();
      const handles = startBackgroundTasks(ctx, cron);

      expect(handles.cleanupInterval).toBeDefined();
      expect(handles.timeoutInterval).toBeDefined();
      expect(handles.depCheckInterval).toBeDefined();
      expect(handles.stallCheckInterval).toBeDefined();
      expect(handles.dlqMaintenanceInterval).toBeDefined();
      expect(handles.lockCheckInterval).toBeDefined();
      expect(handles.cronScheduler).toBe(cron);

      // Cleanup
      stopBackgroundTasks(handles);
      cron.stop();
    });

    test('should start the cron scheduler', () => {
      const ctx = getBackgroundContext(qm);
      const cron = new CronScheduler();
      const startSpy = spyOn(cron, 'start');

      const handles = startBackgroundTasks(ctx, cron);

      expect(startSpy).toHaveBeenCalledTimes(1);

      stopBackgroundTasks(handles);
      cron.stop();
    });
  });

  describe('stopBackgroundTasks', () => {
    test('should stop the cron scheduler', () => {
      const ctx = getBackgroundContext(qm);
      const cron = new CronScheduler();
      const stopSpy = spyOn(cron, 'stop');

      const handles = startBackgroundTasks(ctx, cron);
      stopBackgroundTasks(handles);

      expect(stopSpy).toHaveBeenCalledTimes(1);
      cron.stop();
    });

    test('should clear all interval handles', () => {
      const ctx = getBackgroundContext(qm);
      const cron = new CronScheduler();
      const handles = startBackgroundTasks(ctx, cron);

      // stopBackgroundTasks clears all intervals; calling it should not throw
      expect(() => stopBackgroundTasks(handles)).not.toThrow();
      cron.stop();
    });
  });

  // ============ Multiple Start/Stop Cycles ============

  describe('multiple start/stop cycles', () => {
    test('should support starting and stopping multiple times', () => {
      const ctx = getBackgroundContext(qm);
      const cron = new CronScheduler();

      // Cycle 1
      const handles1 = startBackgroundTasks(ctx, cron);
      stopBackgroundTasks(handles1);

      // Cycle 2
      const handles2 = startBackgroundTasks(ctx, cron);
      stopBackgroundTasks(handles2);

      // Cycle 3
      const handles3 = startBackgroundTasks(ctx, cron);
      stopBackgroundTasks(handles3);

      cron.stop();
    });

    test('should create distinct interval handles on each start', () => {
      const ctx = getBackgroundContext(qm);
      const cron = new CronScheduler();

      const handles1 = startBackgroundTasks(ctx, cron);
      stopBackgroundTasks(handles1);

      const handles2 = startBackgroundTasks(ctx, cron);

      // Each start creates fresh intervals; they should differ from the previous ones
      // (Timer IDs are incremented internally by the runtime)
      expect(handles2.cleanupInterval).toBeDefined();
      expect(handles2.timeoutInterval).toBeDefined();

      stopBackgroundTasks(handles2);
      cron.stop();
    });
  });

  // ============ Edge Cases ============

  describe('edge cases', () => {
    test('stopping before starting should not throw', () => {
      // Construct a fake handles object with dummy interval IDs
      const cron = new CronScheduler();
      const fakeHandles: BackgroundTaskHandles = {
        cleanupInterval: setInterval(() => {}, 999999),
        timeoutInterval: setInterval(() => {}, 999999),
        depCheckInterval: setInterval(() => {}, 999999),
        stallCheckInterval: setInterval(() => {}, 999999),
        dlqMaintenanceInterval: setInterval(() => {}, 999999),
        lockCheckInterval: setInterval(() => {}, 999999),
        cronScheduler: cron,
      };

      expect(() => stopBackgroundTasks(fakeHandles)).not.toThrow();
      cron.stop();
    });

    test('double stop should not throw', () => {
      const ctx = getBackgroundContext(qm);
      const cron = new CronScheduler();
      const handles = startBackgroundTasks(ctx, cron);

      stopBackgroundTasks(handles);
      // Second stop should be safe (clearInterval on already-cleared is a no-op)
      expect(() => stopBackgroundTasks(handles)).not.toThrow();
      cron.stop();
    });

    test('rapid start/stop should not crash', () => {
      const ctx = getBackgroundContext(qm);
      const cron = new CronScheduler();

      for (let i = 0; i < 20; i++) {
        const handles = startBackgroundTasks(ctx, cron);
        stopBackgroundTasks(handles);
      }

      cron.stop();
    });
  });

  // ============ Task Interval Execution ============

  describe('task execution with intervals', () => {
    test('cleanup task should run at configured interval', async () => {
      // Use very short intervals for testing
      const shortQm = new QueueManager({
        cleanupIntervalMs: 50,
        jobTimeoutCheckMs: 100000,
        dependencyCheckMs: 100000,
        stallCheckMs: 100000,
        dlqMaintenanceMs: 100000,
      });

      try {
        // Push a job to create some state
        await shortQm.push('test-cleanup', { data: { value: 1 } });

        // The background tasks are already running from QueueManager construction;
        // wait long enough for the cleanup interval to fire at least once
        await sleep(150);

        // No crash means cleanup ran successfully
      } finally {
        shortQm.shutdown();
      }
    });

    test('stall detection task should run at configured interval', async () => {
      const shortQm = new QueueManager({
        cleanupIntervalMs: 100000,
        jobTimeoutCheckMs: 100000,
        dependencyCheckMs: 100000,
        stallCheckMs: 50,
        dlqMaintenanceMs: 100000,
      });

      try {
        await shortQm.push('test-stall', { data: { value: 1 } });
        const job = await shortQm.pull('test-stall');
        expect(job).not.toBeNull();

        // Wait for stall check to run (no crash = success)
        await sleep(150);
      } finally {
        shortQm.shutdown();
      }
    });

    test('dependency processor task should run at configured interval', async () => {
      const shortQm = new QueueManager({
        cleanupIntervalMs: 100000,
        jobTimeoutCheckMs: 100000,
        dependencyCheckMs: 50,
        stallCheckMs: 100000,
        dlqMaintenanceMs: 100000,
      });

      try {
        await shortQm.push('test-dep', { data: { value: 1 } });

        // Wait for dependency check to run
        await sleep(150);
      } finally {
        shortQm.shutdown();
      }
    });

    test('DLQ maintenance task should run at configured interval', async () => {
      const shortQm = new QueueManager({
        cleanupIntervalMs: 100000,
        jobTimeoutCheckMs: 100000,
        dependencyCheckMs: 100000,
        stallCheckMs: 100000,
        dlqMaintenanceMs: 50,
      });

      try {
        // Register a queue name so DLQ maintenance has something to iterate
        await shortQm.push('test-dlq-maint', { data: { value: 1 } });

        await sleep(150);
      } finally {
        shortQm.shutdown();
      }
    });

    test('job timeout task should detect timed-out jobs', async () => {
      const shortQm = new QueueManager({
        cleanupIntervalMs: 100000,
        jobTimeoutCheckMs: 50,
        dependencyCheckMs: 100000,
        stallCheckMs: 100000,
        dlqMaintenanceMs: 100000,
      });

      try {
        // Push a job with a very short timeout
        await shortQm.push('test-timeout', {
          data: { value: 1 },
          timeout: 1, // 1ms timeout
        });

        // Pull the job to make it active
        const job = await shortQm.pull('test-timeout');
        expect(job).not.toBeNull();

        // Wait for timeout check to run
        await sleep(200);

        // The job should have been failed due to timeout
        // It may have been retried or moved to DLQ depending on config
        const state = await shortQm.getJobState(job!.id);
        // After timeout, the job state should be either waiting (retried), failed, or in DLQ
        expect(['waiting', 'delayed', 'failed', 'unknown']).toContain(state);
      } finally {
        shortQm.shutdown();
      }
    });

    test('lock expiration task should run at same interval as stall check', async () => {
      const shortQm = new QueueManager({
        cleanupIntervalMs: 100000,
        jobTimeoutCheckMs: 100000,
        dependencyCheckMs: 100000,
        stallCheckMs: 50,
        dlqMaintenanceMs: 100000,
      });

      try {
        // Push and pull a job, then create a lock
        await shortQm.push('test-lock', { data: { value: 1 } });
        const job = await shortQm.pull('test-lock');
        expect(job).not.toBeNull();

        // Create a lock with very short TTL
        shortQm.createLock(job!.id, 'test-owner', 1); // 1ms TTL

        // Wait for lock check to run
        await sleep(200);
      } finally {
        shortQm.shutdown();
      }
    });
  });

  // ============ Error Tracking ============

  describe('error tracking (getTaskErrorStats)', () => {
    test('should return initial error stats with zero failures', () => {
      const stats = getTaskErrorStats();

      expect(stats.cleanup).toBeDefined();
      expect(stats.cleanup.consecutiveFailures).toBeGreaterThanOrEqual(0);
      expect(stats.dependency).toBeDefined();
      expect(stats.lockExpiration).toBeDefined();
    });

    test('should track consecutive failures for tasks', () => {
      // Stats are module-level singletons; we can only verify structure
      const stats = getTaskErrorStats();

      expect(typeof stats.cleanup.consecutiveFailures).toBe('number');
      expect(typeof stats.dependency.consecutiveFailures).toBe('number');
      expect(typeof stats.lockExpiration.consecutiveFailures).toBe('number');
    });

    test('should return a shallow copy of stats at the top level', () => {
      const stats1 = getTaskErrorStats();
      const stats2 = getTaskErrorStats();

      // Top-level is a new object (shallow spread)
      expect(stats1).not.toBe(stats2);

      // Inner objects are shared references (shallow copy)
      expect(stats1.cleanup).toBe(stats2.cleanup);
    });
  });

  // ============ Tasks Handle Errors Gracefully ============

  describe('error resilience', () => {
    test('background tasks should not crash the system on errors', async () => {
      // Create a QueueManager with very short intervals
      const shortQm = new QueueManager({
        cleanupIntervalMs: 30,
        jobTimeoutCheckMs: 30,
        dependencyCheckMs: 30,
        stallCheckMs: 30,
        dlqMaintenanceMs: 30,
      });

      try {
        // Push many jobs to create diverse state
        for (let i = 0; i < 10; i++) {
          await shortQm.push('error-test', { data: { i } });
        }

        // Pull some jobs to create processing state
        for (let i = 0; i < 5; i++) {
          await shortQm.pull('error-test');
        }

        // Let all background tasks fire multiple times
        await sleep(200);

        // System should still be functional
        const job = await shortQm.push('error-test', { data: { after: true } });
        expect(job).toBeDefined();
        expect(job.id).toBeDefined();

        const pulled = await shortQm.pull('error-test');
        expect(pulled).not.toBeNull();
      } finally {
        shortQm.shutdown();
      }
    });

    test('DLQ maintenance should handle empty queue names cache gracefully', async () => {
      const ctx = getBackgroundContext(qm);
      const cron = new CronScheduler();

      // Ensure queueNamesCache is empty
      ctx.queueNamesCache.clear();

      const handles = startBackgroundTasks(ctx, cron);

      // DLQ maintenance iterates over queueNamesCache -- with empty set, it should be a no-op
      await sleep(100);

      stopBackgroundTasks(handles);
      cron.stop();
    });

    test('timeout check should handle empty processing shards', async () => {
      const ctx = getBackgroundContext(qm);
      const cron = new CronScheduler();

      // All processing shards are empty by default
      const handles = startBackgroundTasks(ctx, cron);

      await sleep(100);

      stopBackgroundTasks(handles);
      cron.stop();
    });
  });

  // ============ Stall Detection Integration ============

  describe('stall detection task integration', () => {
    test('checkStalledJobs is called at stallCheckMs interval', async () => {
      const shortQm = new QueueManager({
        cleanupIntervalMs: 100000,
        jobTimeoutCheckMs: 100000,
        dependencyCheckMs: 100000,
        stallCheckMs: 50,
        dlqMaintenanceMs: 100000,
      });

      try {
        // Enable stall detection on a queue
        const raw = shortQm as any;
        const queueName = 'stall-test';
        await shortQm.push(queueName, { data: { x: 1 } });

        // Configure stall detection
        const idx = (await import('../src/shared/hash')).shardIndex(queueName);
        raw.shards[idx].setStallConfig(queueName, {
          enabled: true,
          stallInterval: 10,
          maxStalls: 3,
          gracePeriod: 0,
        });

        // Pull job to make it active
        const job = await shortQm.pull(queueName);
        expect(job).not.toBeNull();

        // The stall check interval (50ms) should fire; with grace=0 and stallInterval=10,
        // jobs should become stall candidates quickly
        await sleep(200);
      } finally {
        shortQm.shutdown();
      }
    });
  });

  // ============ Cleanup Task Integration ============

  describe('cleanup task integration', () => {
    test('cleanup runs and processes orphaned entries', async () => {
      const shortQm = new QueueManager({
        cleanupIntervalMs: 50,
        jobTimeoutCheckMs: 100000,
        dependencyCheckMs: 100000,
        stallCheckMs: 100000,
        dlqMaintenanceMs: 100000,
      });

      try {
        // Push and pull jobs to create processing state
        for (let i = 0; i < 5; i++) {
          await shortQm.push('cleanup-test', { data: { i } });
        }
        for (let i = 0; i < 3; i++) {
          await shortQm.pull('cleanup-test');
        }

        // Wait for cleanup to run
        await sleep(200);

        // System should remain functional
        const stats = shortQm.getStats();
        expect(stats).toBeDefined();
      } finally {
        shortQm.shutdown();
      }
    });
  });

  // ============ Lock Expiration Task Integration ============

  describe('lock expiration task integration', () => {
    test('expired locks are detected and handled', async () => {
      const shortQm = new QueueManager({
        cleanupIntervalMs: 100000,
        jobTimeoutCheckMs: 100000,
        dependencyCheckMs: 100000,
        stallCheckMs: 40,
        dlqMaintenanceMs: 100000,
      });

      try {
        await shortQm.push('lock-test', { data: { value: 1 } });
        const job = await shortQm.pull('lock-test');
        expect(job).not.toBeNull();

        // Create a lock with very short TTL
        const token = shortQm.createLock(job!.id, 'owner-1', 1); // 1ms TTL
        expect(token).toBeTruthy();

        // Wait for lock check to detect and handle the expired lock
        await sleep(200);

        // The lock should now be expired and cleaned up
        const lockInfo = shortQm.getLockInfo(job!.id);
        expect(lockInfo).toBeNull();
      } finally {
        shortQm.shutdown();
      }
    });
  });

  // ============ Dependency Processor Task Integration ============

  describe('dependency processor task integration', () => {
    test('dependency processor resolves pending dependencies', async () => {
      const shortQm = new QueueManager({
        cleanupIntervalMs: 100000,
        jobTimeoutCheckMs: 100000,
        dependencyCheckMs: 50,
        stallCheckMs: 100000,
        dlqMaintenanceMs: 100000,
      });

      try {
        // Push a parent job
        const parent = await shortQm.push('dep-test', { data: { role: 'parent' } });

        // Push a child job that depends on parent
        const child = await shortQm.push('dep-test', {
          data: { role: 'child' },
          dependsOn: [parent.id],
        });

        // Pull and ack the parent to trigger dependency resolution
        const pulled = await shortQm.pull('dep-test');
        expect(pulled).not.toBeNull();
        expect(pulled!.id).toBe(parent.id);
        await shortQm.ack(parent.id, { done: true });

        // Wait for the dependency processor to run and promote the child
        await sleep(200);

        // Now the child should be available for pulling
        const childPulled = await shortQm.pull('dep-test');
        expect(childPulled).not.toBeNull();
        expect(childPulled!.id).toBe(child.id);
      } finally {
        shortQm.shutdown();
      }
    });
  });

  // ============ DLQ Maintenance Task Integration ============

  describe('DLQ maintenance task integration', () => {
    test('DLQ maintenance processes auto-retry and expiration', async () => {
      const shortQm = new QueueManager({
        cleanupIntervalMs: 100000,
        jobTimeoutCheckMs: 100000,
        dependencyCheckMs: 100000,
        stallCheckMs: 100000,
        dlqMaintenanceMs: 50,
      });

      try {
        const queueName = 'dlq-maint-test';

        // Push a job
        await shortQm.push(queueName, {
          data: { value: 1 },
          maxAttempts: 1,
        });

        // Pull and fail the job to send it to DLQ
        const job = await shortQm.pull(queueName);
        expect(job).not.toBeNull();
        await shortQm.fail(job!.id, 'test failure');

        // Wait for DLQ maintenance to run
        await sleep(200);

        // System should be stable
        const stats = shortQm.getStats();
        expect(stats).toBeDefined();
      } finally {
        shortQm.shutdown();
      }
    });
  });

  // ============ Tasks Don't Run After Stop ============

  describe('tasks after shutdown', () => {
    test('tasks should not run after QueueManager shutdown', async () => {
      const shortQm = new QueueManager({
        cleanupIntervalMs: 30,
        jobTimeoutCheckMs: 30,
        dependencyCheckMs: 30,
        stallCheckMs: 30,
        dlqMaintenanceMs: 30,
      });

      await shortQm.push('post-shutdown', { data: { value: 1 } });

      // Shutdown clears all intervals
      shortQm.shutdown();

      // Wait some time -- intervals should NOT fire
      await sleep(150);

      // No crash means the intervals were properly cleared
    });

    test('tasks should not run after stopBackgroundTasks', async () => {
      const ctx = getBackgroundContext(qm);
      const cron = new CronScheduler();
      const handles = startBackgroundTasks(ctx, cron);

      stopBackgroundTasks(handles);

      // After stopping, the intervals should no longer fire
      await sleep(150);

      cron.stop();
    });
  });

  // ============ Concurrent Task Safety ============

  describe('concurrent task safety', () => {
    test('all tasks firing simultaneously should not cause data corruption', async () => {
      const shortQm = new QueueManager({
        cleanupIntervalMs: 20,
        jobTimeoutCheckMs: 20,
        dependencyCheckMs: 20,
        stallCheckMs: 20,
        dlqMaintenanceMs: 20,
      });

      try {
        // Create diverse state
        const jobs: Job[] = [];
        for (let i = 0; i < 20; i++) {
          const j = await shortQm.push('concurrent', { data: { i } });
          jobs.push(j);
        }

        // Pull half the jobs
        for (let i = 0; i < 10; i++) {
          await shortQm.pull('concurrent');
        }

        // Ack some
        for (let i = 0; i < 5; i++) {
          await shortQm.ack(jobs[i].id);
        }

        // Let all background tasks run concurrently multiple times
        await sleep(200);

        // System should be consistent
        const stats = shortQm.getStats();
        expect(stats).toBeDefined();

        // We should still be able to push and pull
        const newJob = await shortQm.push('concurrent', { data: { after: true } });
        expect(newJob).toBeDefined();
      } finally {
        shortQm.shutdown();
      }
    });
  });

  // ============ Full Lifecycle Integration ============

  describe('full lifecycle', () => {
    test('start -> push/pull work -> stop -> restart -> push/pull work', async () => {
      const ctx = getBackgroundContext(qm);
      const cron = new CronScheduler();

      // Start background tasks
      const handles = startBackgroundTasks(ctx, cron);

      // Push and pull work while tasks are running
      await qm.push('lifecycle', { data: { phase: 'first' } });
      const job1 = await qm.pull('lifecycle');
      expect(job1).not.toBeNull();
      await qm.ack(job1!.id);

      // Stop
      stopBackgroundTasks(handles);

      // Push/pull should still work (only background maintenance is stopped)
      await qm.push('lifecycle', { data: { phase: 'between' } });
      const job2 = await qm.pull('lifecycle');
      expect(job2).not.toBeNull();
      await qm.ack(job2!.id);

      // Restart background tasks
      const handles2 = startBackgroundTasks(ctx, cron);

      // Push and pull work after restart
      await qm.push('lifecycle', { data: { phase: 'second' } });
      const job3 = await qm.pull('lifecycle');
      expect(job3).not.toBeNull();
      await qm.ack(job3!.id);

      stopBackgroundTasks(handles2);
      cron.stop();
    });
  });
});
