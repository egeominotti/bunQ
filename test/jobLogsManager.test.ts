/**
 * Job Logs Manager Tests
 * Per-job logging operations
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';

describe('Job Logs Manager', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  describe('addLog', () => {
    test('should add log to job', async () => {
      const job = await qm.push('test-queue', { data: {} });

      const added = qm.addLog(job.id, 'Processing started');
      expect(added).toBe(true);

      const logs = qm.getLogs(job.id);
      expect(logs.length).toBe(1);
      expect(logs[0].message).toBe('Processing started');
    });

    test('should add multiple logs', async () => {
      const job = await qm.push('test-queue', { data: {} });

      qm.addLog(job.id, 'Step 1');
      qm.addLog(job.id, 'Step 2');
      qm.addLog(job.id, 'Step 3');

      const logs = qm.getLogs(job.id);
      expect(logs.length).toBe(3);
    });

    test('should support log levels', async () => {
      const job = await qm.push('test-queue', { data: {} });

      qm.addLog(job.id, 'Info message', 'info');
      qm.addLog(job.id, 'Warning message', 'warn');
      qm.addLog(job.id, 'Error message', 'error');

      const logs = qm.getLogs(job.id);
      expect(logs[0].level).toBe('info');
      expect(logs[1].level).toBe('warn');
      expect(logs[2].level).toBe('error');
    });

    test('should return false for non-existent job', async () => {
      const { jobId } = await import('../src/domain/types/job');
      const added = qm.addLog(jobId('non-existent'), 'Test');
      expect(added).toBe(false);
    });

    test('should include timestamp', async () => {
      const before = Date.now();
      const job = await qm.push('test-queue', { data: {} });
      qm.addLog(job.id, 'Test');
      const after = Date.now();

      const logs = qm.getLogs(job.id);
      expect(logs[0].timestamp).toBeGreaterThanOrEqual(before);
      expect(logs[0].timestamp).toBeLessThanOrEqual(after);
    });
  });

  describe('getLogs', () => {
    test('should return empty array for job with no logs', async () => {
      const job = await qm.push('test-queue', { data: {} });
      const logs = qm.getLogs(job.id);
      expect(logs).toEqual([]);
    });

    test('should return empty array for non-existent job', async () => {
      const { jobId } = await import('../src/domain/types/job');
      const logs = qm.getLogs(jobId('non-existent'));
      expect(logs).toEqual([]);
    });

    test('should return logs in order', async () => {
      const job = await qm.push('test-queue', { data: {} });

      qm.addLog(job.id, 'First');
      qm.addLog(job.id, 'Second');
      qm.addLog(job.id, 'Third');

      const logs = qm.getLogs(job.id);
      expect(logs[0].message).toBe('First');
      expect(logs[1].message).toBe('Second');
      expect(logs[2].message).toBe('Third');
    });
  });

  describe('log bounds', () => {
    test('should respect max logs per job', async () => {
      const job = await qm.push('test-queue', { data: {} });

      // Add more logs than the limit (default is 100)
      for (let i = 0; i < 150; i++) {
        qm.addLog(job.id, `Log ${i}`);
      }

      const logs = qm.getLogs(job.id);
      expect(logs.length).toBeLessThanOrEqual(100);

      // Should have the latest logs (old ones removed first)
      expect(logs[0].message).toBe('Log 50');
    });
  });

  describe('logs for active jobs', () => {
    test('should add logs to active job', async () => {
      await qm.push('test-queue', { data: {} });
      const pulled = await qm.pull('test-queue');

      const added = qm.addLog(pulled!.id, 'Processing');
      expect(added).toBe(true);

      const logs = qm.getLogs(pulled!.id);
      expect(logs.length).toBe(1);
    });
  });
});
