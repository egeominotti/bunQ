/**
 * StatsManager Tests
 * Tests for getStats(), getMemoryStats(), and compactMemory()
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';

describe('StatsManager', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  // ============ getStats() ============

  describe('getStats', () => {
    test('should return zeroed stats for empty queues', () => {
      const stats = qm.getStats();

      expect(stats.waiting).toBe(0);
      expect(stats.active).toBe(0);
      expect(stats.delayed).toBe(0);
      expect(stats.completed).toBe(0);
      expect(stats.dlq).toBe(0);
      expect(Number(stats.totalPushed)).toBe(0);
      expect(Number(stats.totalPulled)).toBe(0);
      expect(Number(stats.totalCompleted)).toBe(0);
      expect(Number(stats.totalFailed)).toBe(0);
    });

    test('should have non-negative uptime', () => {
      const stats = qm.getStats();
      expect(stats.uptime).toBeGreaterThanOrEqual(0);
    });

    test('should return cronJobs and cronPending as zero when no cron jobs exist', () => {
      const stats = qm.getStats();
      expect(stats.cronJobs).toBe(0);
      expect(stats.cronPending).toBe(0);
    });

    test('should increase waiting count after adding jobs', async () => {
      await qm.push('test-queue', { data: { id: 1 } });
      await qm.push('test-queue', { data: { id: 2 } });
      await qm.push('test-queue', { data: { id: 3 } });

      const stats = qm.getStats();
      expect(stats.waiting).toBe(3);
      expect(Number(stats.totalPushed)).toBe(3);
    });

    test('should track waiting count across multiple queues', async () => {
      await qm.push('queue-a', { data: { id: 1 } });
      await qm.push('queue-a', { data: { id: 2 } });
      await qm.push('queue-b', { data: { id: 3 } });

      const stats = qm.getStats();
      expect(stats.waiting).toBe(3);
      expect(Number(stats.totalPushed)).toBe(3);
    });

    test('should increase active count and decrease waiting count after pulling jobs', async () => {
      await qm.push('test-queue', { data: { id: 1 } });
      await qm.push('test-queue', { data: { id: 2 } });

      await qm.pull('test-queue');

      const stats = qm.getStats();
      expect(stats.waiting).toBe(1);
      expect(stats.active).toBe(1);
      expect(Number(stats.totalPulled)).toBe(1);
    });

    test('should increase completed count and decrease active count after acking jobs', async () => {
      await qm.push('test-queue', { data: { id: 1 } });
      const job = await qm.pull('test-queue');

      await qm.ack(job!.id, { done: true });

      const stats = qm.getStats();
      expect(stats.waiting).toBe(0);
      expect(stats.active).toBe(0);
      expect(stats.completed).toBe(1);
      expect(Number(stats.totalCompleted)).toBe(1);
    });

    test('should track delayed jobs separately from waiting', async () => {
      await qm.push('test-queue', { data: { id: 1 } });
      await qm.push('test-queue', { data: { id: 2 }, delay: 60000 });

      const stats = qm.getStats();
      expect(stats.waiting).toBe(1);
      expect(stats.delayed).toBe(1);
    });

    test('should not double-count a delayed job after it matures', async () => {
      await qm.push('matured-stats', { data: { id: 1 }, delay: 20 });
      await Bun.sleep(40);

      const stats = qm.getStats();
      expect(stats.waiting).toBe(1);
      expect(stats.delayed).toBe(0);
      expect(stats.waiting + stats.prioritized + stats.delayed).toBe(1);
    });

    test('should track DLQ count after max retries exceeded', async () => {
      await qm.push('test-queue', { data: { id: 1 }, maxAttempts: 1 });
      const job = await qm.pull('test-queue');

      await qm.fail(job!.id, 'fatal error');

      const stats = qm.getStats();
      expect(stats.dlq).toBe(1);
      expect(Number(stats.totalFailed)).toBe(1);
    });

    test('should not increment totalFailed when job fails but is retried', async () => {
      await qm.push('test-queue', { data: { id: 1 }, maxAttempts: 3 });
      const job = await qm.pull('test-queue');

      await qm.fail(job!.id, 'transient error');

      const stats = qm.getStats();
      // totalFailed only increments when job goes to DLQ or removeOnFail, not on retry
      expect(Number(stats.totalFailed)).toBe(0);
      // Job should be requeued (waiting or delayed due to backoff)
      expect(stats.waiting + stats.delayed).toBe(1);
      expect(stats.dlq).toBe(0);
    });

    test('should reflect full lifecycle: push -> pull -> ack', async () => {
      // Push 3 jobs
      await qm.push('test-queue', { data: { id: 1 } });
      await qm.push('test-queue', { data: { id: 2 } });
      await qm.push('test-queue', { data: { id: 3 } });

      let stats = qm.getStats();
      expect(stats.waiting).toBe(3);
      expect(Number(stats.totalPushed)).toBe(3);

      // Pull 2
      const job1 = await qm.pull('test-queue');
      const job2 = await qm.pull('test-queue');

      stats = qm.getStats();
      expect(stats.waiting).toBe(1);
      expect(stats.active).toBe(2);
      expect(Number(stats.totalPulled)).toBe(2);

      // Ack 1
      await qm.ack(job1!.id, { result: 'ok' });

      stats = qm.getStats();
      expect(stats.waiting).toBe(1);
      expect(stats.active).toBe(1);
      expect(stats.completed).toBe(1);
      expect(Number(stats.totalCompleted)).toBe(1);

      // Ack the other
      await qm.ack(job2!.id, { result: 'ok' });

      stats = qm.getStats();
      expect(stats.active).toBe(0);
      expect(stats.completed).toBe(2);
      expect(Number(stats.totalCompleted)).toBe(2);
    });

    test('should count cron jobs after adding a cron', () => {
      qm.addCron({
        name: 'test-cron',
        queue: 'cron-queue',
        data: { task: 'cleanup' },
        schedule: '*/5 * * * *',
      });

      const stats = qm.getStats();
      expect(stats.cronJobs).toBe(1);
    });
  });

  // ============ getMemoryStats() ============

  describe('getMemoryStats', () => {
    test('should return zeroed memory stats for fresh instance', () => {
      const mem = qm.getMemoryStats();

      expect(mem.jobIndex).toBe(0);
      expect(mem.completedJobs).toBe(0);
      expect(mem.jobResults).toBe(0);
      expect(mem.jobLogs).toBe(0);
      expect(mem.customIdMap).toBe(0);
      expect(mem.jobLocks).toBe(0);
      expect(mem.clientJobs).toBe(0);
      expect(mem.clientJobsTotal).toBe(0);
      expect(mem.pendingDepChecks).toBe(0);
      expect(mem.stalledCandidates).toBe(0);
      expect(mem.processingTotal).toBe(0);
      expect(mem.queuedTotal).toBe(0);
      expect(mem.waitingDepsTotal).toBe(0);
    });

    test('should reflect jobIndex size after pushing jobs', async () => {
      await qm.push('test-queue', { data: { id: 1 } });
      await qm.push('test-queue', { data: { id: 2 } });

      const mem = qm.getMemoryStats();
      expect(mem.jobIndex).toBe(2);
      expect(mem.queuedTotal).toBe(2);
    });

    test('should reflect processingTotal after pulling jobs', async () => {
      await qm.push('test-queue', { data: { id: 1 } });
      await qm.push('test-queue', { data: { id: 2 } });

      await qm.pull('test-queue');

      const mem = qm.getMemoryStats();
      expect(mem.processingTotal).toBe(1);
      expect(mem.queuedTotal).toBe(1);
      expect(mem.jobIndex).toBe(2);
    });

    test('should reflect completedJobs and jobResults after acking', async () => {
      await qm.push('test-queue', { data: { id: 1 } });
      const job = await qm.pull('test-queue');
      await qm.ack(job!.id, { result: 'success' });

      const mem = qm.getMemoryStats();
      expect(mem.completedJobs).toBe(1);
      expect(mem.jobResults).toBe(1);
      expect(mem.processingTotal).toBe(0);
    });

    test('should reflect customIdMap when using customId', async () => {
      await qm.push('test-queue', { data: { id: 1 }, customId: 'custom-key-1' });
      await qm.push('test-queue', { data: { id: 2 }, customId: 'custom-key-2' });

      const mem = qm.getMemoryStats();
      expect(mem.customIdMap).toBe(2);
    });

    test('should reflect jobLogs after adding logs', async () => {
      const job = await qm.push('test-queue', { data: { id: 1 } });

      qm.addLog(job.id, 'Step 1 started');
      qm.addLog(job.id, 'Step 1 completed');

      const mem = qm.getMemoryStats();
      expect(mem.jobLogs).toBe(1); // 1 entry (for 1 job, containing 2 log lines)
    });

    test('should reflect jobLocks when using lock-based pulling', async () => {
      await qm.push('test-queue', { data: { id: 1 } });

      const { job, token } = await qm.pullWithLock('test-queue', 'worker-1');

      expect(job).not.toBeNull();
      expect(token).not.toBeNull();

      const mem = qm.getMemoryStats();
      expect(mem.jobLocks).toBe(1);
    });

    test('should reflect clientJobs when registering client jobs', async () => {
      const job = await qm.push('test-queue', { data: { id: 1 } });

      qm.registerClientJob('client-1', job.id);

      const mem = qm.getMemoryStats();
      expect(mem.clientJobs).toBe(1);
      expect(mem.clientJobsTotal).toBe(1);
    });

    test('should track multiple clients and their jobs', async () => {
      const job1 = await qm.push('test-queue', { data: { id: 1 } });
      const job2 = await qm.push('test-queue', { data: { id: 2 } });
      const job3 = await qm.push('test-queue', { data: { id: 3 } });

      qm.registerClientJob('client-1', job1.id);
      qm.registerClientJob('client-1', job2.id);
      qm.registerClientJob('client-2', job3.id);

      const mem = qm.getMemoryStats();
      expect(mem.clientJobs).toBe(2); // 2 clients
      expect(mem.clientJobsTotal).toBe(3); // 3 total job registrations
    });

    test('should reflect all collections changing during a full lifecycle', async () => {
      // Push 3 jobs
      const pushed1 = await qm.push('test-queue', { data: { id: 1 }, customId: 'cid-1' });
      await qm.push('test-queue', { data: { id: 2 } });
      await qm.push('test-queue', { data: { id: 3 } });

      qm.addLog(pushed1.id, 'pushed');

      let mem = qm.getMemoryStats();
      expect(mem.jobIndex).toBe(3);
      expect(mem.queuedTotal).toBe(3);
      expect(mem.customIdMap).toBe(1);
      expect(mem.jobLogs).toBe(1);

      // Pull 2
      const pulled1 = await qm.pull('test-queue');
      await qm.pull('test-queue');

      mem = qm.getMemoryStats();
      expect(mem.processingTotal).toBe(2);
      expect(mem.queuedTotal).toBe(1);

      // Ack 1 (using the actually pulled job)
      await qm.ack(pulled1!.id, { done: true });

      mem = qm.getMemoryStats();
      expect(mem.completedJobs).toBe(1);
      expect(mem.jobResults).toBe(1);
      expect(mem.processingTotal).toBe(1);
    });
  });

  // ============ compactMemory() ============

  describe('compactMemory', () => {
    test('should not throw on empty queues', () => {
      expect(() => qm.compactMemory()).not.toThrow();
    });

    test('should not throw after adding and removing jobs', async () => {
      await qm.push('test-queue', { data: { id: 1 } });
      await qm.push('test-queue', { data: { id: 2 } });
      const job = await qm.pull('test-queue');
      await qm.ack(job!.id);

      expect(() => qm.compactMemory()).not.toThrow();
    });

    test('should clean up empty client tracking entries', async () => {
      const job1 = await qm.push('test-queue', { data: { id: 1 } });
      const job2 = await qm.push('test-queue', { data: { id: 2 } });

      // Register two jobs under one client
      qm.registerClientJob('client-1', job1.id);
      qm.registerClientJob('client-1', job2.id);

      let mem = qm.getMemoryStats();
      expect(mem.clientJobs).toBe(1);
      expect(mem.clientJobsTotal).toBe(2);

      // Unregister both - unregisterClientJob already cleans empty sets
      qm.unregisterClientJob('client-1', job1.id);
      qm.unregisterClientJob('client-1', job2.id);

      mem = qm.getMemoryStats();
      // Already cleaned by unregisterClientJob
      expect(mem.clientJobs).toBe(0);
      expect(mem.clientJobsTotal).toBe(0);

      // compactMemory should still work safely with no empty entries to clean
      qm.compactMemory();

      mem = qm.getMemoryStats();
      expect(mem.clientJobs).toBe(0);
      expect(mem.clientJobsTotal).toBe(0);
    });

    test('should clean orphaned job locks for non-processing jobs', async () => {
      await qm.push('test-queue', { data: { id: 1 } });
      const { job, token } = await qm.pullWithLock('test-queue', 'worker-1');
      expect(job).not.toBeNull();
      expect(token).not.toBeNull();

      // Ack the job (removes from processing, but lock might still be referenced)
      await qm.ack(job!.id, { done: true }, token!);

      // After ack, the lock should already be released
      // But if we manually test the compactMemory path for orphaned locks:
      // The ack path already cleans locks, so verify compactMemory doesn't break anything
      qm.compactMemory();

      const mem = qm.getMemoryStats();
      expect(mem.jobLocks).toBe(0);
    });

    test('should preserve active job locks during compaction', async () => {
      await qm.push('test-queue', { data: { id: 1 } });
      const { job, token } = await qm.pullWithLock('test-queue', 'worker-1');
      expect(job).not.toBeNull();
      expect(token).not.toBeNull();

      // Job is still processing - lock should be preserved
      qm.compactMemory();

      const mem = qm.getMemoryStats();
      expect(mem.jobLocks).toBe(1);
      expect(mem.processingTotal).toBe(1);
    });

    test('should be callable multiple times without side effects', async () => {
      await qm.push('test-queue', { data: { id: 1 } });
      await qm.push('test-queue', { data: { id: 2 } });

      qm.compactMemory();
      qm.compactMemory();
      qm.compactMemory();

      const stats = qm.getStats();
      expect(stats.waiting).toBe(2);

      const mem = qm.getMemoryStats();
      expect(mem.jobIndex).toBe(2);
    });

    test('should work after draining a queue', async () => {
      // Add many jobs then drain to create stale entries in priority queues
      for (let i = 0; i < 20; i++) {
        await qm.push('test-queue', { data: { id: i } });
      }

      const drained = qm.drain('test-queue');
      expect(drained).toBe(20);

      // Compact should clean up stale entries
      qm.compactMemory();

      const stats = qm.getStats();
      expect(stats.waiting).toBe(0);
    });

    test('should work after obliterating a queue', async () => {
      for (let i = 0; i < 10; i++) {
        await qm.push('test-queue', { data: { id: i } });
      }

      qm.obliterate('test-queue');
      qm.compactMemory();

      const stats = qm.getStats();
      expect(stats.waiting).toBe(0);

      const mem = qm.getMemoryStats();
      expect(mem.queuedTotal).toBe(0);
    });
  });
});
