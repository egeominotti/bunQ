/**
 * MCP Server Tests
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { getSharedManager, shutdownManager } from '../src/client/manager';

describe('MCP Server Tools', () => {
  beforeEach(() => {
    // Reset manager state
    shutdownManager();
  });

  afterEach(() => {
    shutdownManager();
  });

  describe('Job Operations', () => {
    test('add job to queue', async () => {
      const manager = getSharedManager();
      const job = await manager.push('test-queue', {
        data: { name: 'test-job', email: 'test@example.com' },
        priority: 5,
      });

      expect(job).toBeDefined();
      expect(job.id).toBeDefined();
      expect(job.queue).toBe('test-queue');
      expect(job.priority).toBe(5);
    });

    test('add multiple jobs in batch', async () => {
      const manager = getSharedManager();
      const jobIds = await manager.pushBatch('test-queue', [
        { data: { name: 'job1', value: 1 } },
        { data: { name: 'job2', value: 2 } },
        { data: { name: 'job3', value: 3 } },
      ]);

      expect(jobIds).toHaveLength(3);
      expect(jobIds[0]).toBeDefined();
      expect(jobIds[1]).toBeDefined();
      expect(jobIds[2]).toBeDefined();
    });

    test('get job by ID', async () => {
      const manager = getSharedManager();
      const created = await manager.push('test-queue', {
        data: { name: 'fetch-test', value: 42 },
      });

      const fetched = await manager.getJob(created.id);
      expect(fetched).toBeDefined();
      expect(fetched?.id).toBe(created.id);
      expect(fetched?.queue).toBe('test-queue');
    });

    test('get job returns null for non-existent ID', async () => {
      const manager = getSharedManager();
      const job = await manager.getJob(BigInt(999999));
      expect(job).toBeNull();
    });

    test('cancel job', async () => {
      const manager = getSharedManager();
      const job = await manager.push('test-queue', {
        data: { name: 'cancel-test' },
      });

      const cancelled = await manager.cancel(job.id);
      expect(cancelled).toBe(true);

      const fetched = await manager.getJob(job.id);
      expect(fetched).toBeNull();
    });

    test('update job progress requires active job', async () => {
      const manager = getSharedManager();
      const job = await manager.push('test-queue', {
        data: { name: 'progress-test' },
      });

      // Job is waiting, not active - progress update should return false
      const updated = await manager.updateProgress(job.id, 50, 'Halfway done');
      expect(updated).toBe(false);

      // Pull job to make it active
      const activeJob = await manager.pull('test-queue');
      expect(activeJob).toBeDefined();

      // Now we can update progress
      const updated2 = await manager.updateProgress(activeJob!.id, 50, 'Halfway done');
      expect(updated2).toBe(true);

      const progress = manager.getProgress(activeJob!.id);
      expect(progress?.progress).toBe(50);
      expect(progress?.message).toBe('Halfway done');
    });
  });

  describe('Queue Control', () => {
    test('pause and resume queue', () => {
      const manager = getSharedManager();

      manager.pause('control-queue');
      expect(manager.isPaused('control-queue')).toBe(true);

      manager.resume('control-queue');
      expect(manager.isPaused('control-queue')).toBe(false);
    });

    test('drain queue removes waiting jobs', async () => {
      const manager = getSharedManager();

      await manager.push('drain-queue', { data: { name: 'job1' } });
      await manager.push('drain-queue', { data: { name: 'job2' } });
      await manager.push('drain-queue', { data: { name: 'job3' } });

      const removed = manager.drain('drain-queue');
      expect(removed).toBe(3);
      expect(manager.count('drain-queue')).toBe(0);
    });

    test('list queues', async () => {
      const manager = getSharedManager();

      await manager.push('queue-a', { data: { name: 'test' } });
      await manager.push('queue-b', { data: { name: 'test' } });

      const queues = manager.listQueues();
      expect(queues).toContain('queue-a');
      expect(queues).toContain('queue-b');
    });

    test('count jobs in queue', async () => {
      const manager = getSharedManager();

      await manager.push('count-queue', { data: { name: 'job1' } });
      await manager.push('count-queue', { data: { name: 'job2' } });

      expect(manager.count('count-queue')).toBe(2);
    });
  });

  describe('Rate Limiting', () => {
    test('set and clear rate limit', () => {
      const manager = getSharedManager();

      manager.setRateLimit('rate-queue', 100);
      // Rate limit is internal, we just verify no errors
      manager.clearRateLimit('rate-queue');
    });

    test('set and clear concurrency', () => {
      const manager = getSharedManager();

      manager.setConcurrency('conc-queue', 5);
      manager.clearConcurrency('conc-queue');
    });
  });

  describe('DLQ Operations', () => {
    test('get empty DLQ', () => {
      const manager = getSharedManager();
      const dlqJobs = manager.getDlq('dlq-queue');
      expect(dlqJobs).toEqual([]);
    });

    test('retry DLQ returns 0 when empty', () => {
      const manager = getSharedManager();
      const retried = manager.retryDlq('dlq-queue');
      expect(retried).toBe(0);
    });

    test('purge DLQ returns 0 when empty', () => {
      const manager = getSharedManager();
      const purged = manager.purgeDlq('dlq-queue');
      expect(purged).toBe(0);
    });
  });

  describe('Cron Jobs', () => {
    test('add cron job with schedule', () => {
      const manager = getSharedManager();

      const cron = manager.addCron({
        name: 'hourly-job',
        queue: 'cron-queue',
        data: { task: 'cleanup' },
        schedule: '0 * * * *',
      });

      expect(cron.name).toBe('hourly-job');
      expect(cron.queue).toBe('cron-queue');
      expect(cron.schedule).toBe('0 * * * *');
    });

    test('add cron job with repeatEvery', () => {
      const manager = getSharedManager();

      const cron = manager.addCron({
        name: 'frequent-job',
        queue: 'cron-queue',
        data: { task: 'ping' },
        repeatEvery: 60000,
      });

      expect(cron.name).toBe('frequent-job');
      expect(cron.repeatEvery).toBe(60000);
    });

    test('list cron jobs', () => {
      const manager = getSharedManager();

      manager.addCron({
        name: 'list-test-1',
        queue: 'cron-queue',
        data: {},
        repeatEvery: 1000,
      });
      manager.addCron({
        name: 'list-test-2',
        queue: 'cron-queue',
        data: {},
        repeatEvery: 2000,
      });

      const crons = manager.listCrons();
      const names = crons.map((c) => c.name);
      expect(names).toContain('list-test-1');
      expect(names).toContain('list-test-2');
    });

    test('delete cron job', () => {
      const manager = getSharedManager();

      manager.addCron({
        name: 'delete-test',
        queue: 'cron-queue',
        data: {},
        repeatEvery: 1000,
      });

      const deleted = manager.removeCron('delete-test');
      expect(deleted).toBe(true);

      const cron = manager.getCron('delete-test');
      expect(cron).toBeUndefined();
    });

    test('delete non-existent cron job', () => {
      const manager = getSharedManager();
      const deleted = manager.removeCron('non-existent');
      expect(deleted).toBe(false);
    });
  });

  describe('Job Logs', () => {
    test('add and get job logs', async () => {
      const manager = getSharedManager();
      const job = await manager.push('log-queue', {
        data: { name: 'log-test' },
      });

      manager.addLog(job.id, 'Starting processing', 'info');
      manager.addLog(job.id, 'Warning: slow operation', 'warn');
      manager.addLog(job.id, 'Error occurred', 'error');

      const logs = manager.getLogs(job.id);
      expect(logs).toHaveLength(3);
      expect(logs[0].message).toBe('Starting processing');
      expect(logs[0].level).toBe('info');
      expect(logs[1].level).toBe('warn');
      expect(logs[2].level).toBe('error');
    });

    test('clear job logs', async () => {
      const manager = getSharedManager();
      const job = await manager.push('log-queue', {
        data: { name: 'clear-log-test' },
      });

      manager.addLog(job.id, 'Log entry', 'info');
      manager.clearLogs(job.id);

      const logs = manager.getLogs(job.id);
      expect(logs).toHaveLength(0);
    });
  });

  describe('Stats', () => {
    test('get stats', async () => {
      const manager = getSharedManager();

      await manager.push('stats-queue', { data: { name: 'job1' } });
      await manager.push('stats-queue', { data: { name: 'job2' } });

      const stats = manager.getStats();
      expect(stats).toBeDefined();
      expect(typeof stats.waiting).toBe('number');
      expect(typeof stats.active).toBe('number');
      expect(typeof stats.completed).toBe('number');
      expect(typeof stats.dlq).toBe('number');
    });
  });
});
