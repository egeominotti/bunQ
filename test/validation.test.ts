/**
 * Validation Tests
 * Tests for input validation, edge cases, and error handling
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';

describe('Input Validation', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  describe('Queue Name Validation', () => {
    test('should accept valid queue names', async () => {
      const validNames = ['emails', 'user-notifications', 'task_queue', 'queue123', 'Q'];

      for (const name of validNames) {
        const job = await qm.push(name, { data: { test: true } });
        expect(job.queue).toBe(name);
      }
    });

    test('should handle queue names with special characters', async () => {
      const job = await qm.push('queue:with:colons', { data: { test: true } });
      expect(job.queue).toBe('queue:with:colons');
    });

    test('should handle queue names with unicode', async () => {
      const job = await qm.push('队列', { data: { test: true } });
      expect(job.queue).toBe('队列');
    });

    test('should handle empty queue name', async () => {
      const job = await qm.push('', { data: { test: true } });
      expect(job.queue).toBe('');
    });
  });

  describe('Priority Validation', () => {
    test('should accept positive priorities', async () => {
      const job = await qm.push('test', { data: {}, priority: 100 });
      expect(job.priority).toBe(100);
    });

    test('should accept zero priority', async () => {
      const job = await qm.push('test', { data: {}, priority: 0 });
      expect(job.priority).toBe(0);
    });

    test('should accept negative priorities', async () => {
      const job = await qm.push('test', { data: {}, priority: -10 });
      expect(job.priority).toBe(-10);
    });

    test('should handle very large priorities', async () => {
      const job = await qm.push('test', { data: {}, priority: Number.MAX_SAFE_INTEGER });
      expect(job.priority).toBe(Number.MAX_SAFE_INTEGER);
    });
  });

  describe('Delay Validation', () => {
    test('should accept zero delay', async () => {
      const before = Date.now();
      const job = await qm.push('test', { data: {}, delay: 0 });
      expect(job.runAt).toBeGreaterThanOrEqual(before);
      expect(job.runAt).toBeLessThanOrEqual(before + 100);
    });

    test('should accept positive delay', async () => {
      const before = Date.now();
      const job = await qm.push('test', { data: {}, delay: 5000 });
      expect(job.runAt).toBeGreaterThanOrEqual(before + 5000);
    });

    test('should handle very large delays', async () => {
      const before = Date.now();
      const delay = 24 * 60 * 60 * 1000; // 24 hours
      const job = await qm.push('test', { data: {}, delay });
      expect(job.runAt).toBeGreaterThanOrEqual(before + delay);
    });
  });

  describe('Data Validation', () => {
    test('should accept object data', async () => {
      const data = { key: 'value', nested: { a: 1 } };
      const job = await qm.push('test', { data });
      expect(job.data).toEqual(data);
    });

    test('should accept array data', async () => {
      const data = [1, 2, 3, 'four'];
      const job = await qm.push('test', { data });
      expect(job.data).toEqual(data);
    });

    test('should accept primitive data', async () => {
      const job1 = await qm.push('test', { data: 'string' });
      expect(job1.data).toBe('string');

      const job2 = await qm.push('test', { data: 123 });
      expect(job2.data).toBe(123);

      const job3 = await qm.push('test', { data: true });
      expect(job3.data).toBe(true);
    });

    test('should accept null data', async () => {
      const job = await qm.push('test', { data: null });
      expect(job.data).toBeNull();
    });

    test('should accept empty object data', async () => {
      const job = await qm.push('test', { data: {} });
      expect(job.data).toEqual({});
    });

    test('should handle deeply nested data', async () => {
      const data = {
        level1: {
          level2: {
            level3: {
              level4: {
                level5: { value: 'deep' },
              },
            },
          },
        },
      };
      const job = await qm.push('test', { data });
      expect(job.data).toEqual(data);
    });
  });

  describe('MaxAttempts Validation', () => {
    test('should accept maxAttempts = 1', async () => {
      const job = await qm.push('test', { data: {}, maxAttempts: 1 });
      expect(job.maxAttempts).toBe(1);
    });

    test('should accept large maxAttempts', async () => {
      const job = await qm.push('test', { data: {}, maxAttempts: 100 });
      expect(job.maxAttempts).toBe(100);
    });
  });

  describe('Tags Validation', () => {
    test('should accept empty tags array', async () => {
      const job = await qm.push('test', { data: {}, tags: [] });
      expect(job.tags).toEqual([]);
    });

    test('should accept single tag', async () => {
      const job = await qm.push('test', { data: {}, tags: ['important'] });
      expect(job.tags).toEqual(['important']);
    });

    test('should accept multiple tags', async () => {
      const tags = ['tag1', 'tag2', 'tag3'];
      const job = await qm.push('test', { data: {}, tags });
      expect(job.tags).toEqual(tags);
    });
  });

  describe('Unique Key Validation', () => {
    test('should accept unique key', async () => {
      const job = await qm.push('test', { data: {}, uniqueKey: 'unique-123' });
      expect(job.uniqueKey).toBe('unique-123');
    });

    test('should accept unique key with special characters', async () => {
      const job = await qm.push('test', { data: {}, uniqueKey: 'user:123:action:send' });
      expect(job.uniqueKey).toBe('user:123:action:send');
    });
  });

  describe('Custom ID Validation', () => {
    test('should accept custom ID', async () => {
      const job = await qm.push('test', { data: {}, customId: 'my-custom-id' });
      expect(job.customId).toBe('my-custom-id');
    });

    test('should find job by custom ID', async () => {
      await qm.push('test', { data: { msg: 'test' }, customId: 'findable-id' });

      const found = qm.getJobByCustomId('findable-id');
      expect(found).not.toBeNull();
      expect(found?.data).toEqual({ msg: 'test' });
    });

    test('should return null for non-existent custom ID', () => {
      const found = qm.getJobByCustomId('non-existent');
      expect(found).toBeNull();
    });
  });
});

describe('Edge Cases', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  describe('Empty Operations', () => {
    test('should handle pull from empty queue', async () => {
      const job = await qm.pull('empty-queue', 0);
      expect(job).toBeNull();
    });

    test('should throw on ack for non-existent job', async () => {
      await expect(qm.ack('non-existent' as any, {})).rejects.toThrow();
    });

    test('should throw on fail for non-existent job', async () => {
      await expect(qm.fail('non-existent' as any, 'error')).rejects.toThrow();
    });

    test('should handle cancel for non-existent job', async () => {
      const result = await qm.cancel('non-existent' as any);
      expect(result).toBe(false);
    });

    test('should handle getJob for non-existent job', async () => {
      const job = await qm.getJob('non-existent' as any);
      expect(job).toBeNull();
    });

    test('should return undefined for non-existent result', () => {
      const result = qm.getResult('non-existent' as any);
      expect(result).toBeUndefined();
    });
  });

  describe('State Transitions', () => {
    test('should throw on ack waiting job', async () => {
      const job = await qm.push('test', { data: {} });
      await expect(qm.ack(job.id, {})).rejects.toThrow();
    });

    test('should throw on fail waiting job', async () => {
      const job = await qm.push('test', { data: {} });
      await expect(qm.fail(job.id, 'error')).rejects.toThrow();
    });

    test('should cancel waiting job', async () => {
      const job = await qm.push('test', { data: {} });
      const result = await qm.cancel(job.id);
      expect(result).toBe(true);

      const fetched = await qm.getJob(job.id);
      expect(fetched).toBeNull();
    });

    test('should not cancel active job', async () => {
      const job = await qm.push('test', { data: {} });
      await qm.pull('test', 0);

      const result = await qm.cancel(job.id);
      expect(result).toBe(false);
    });
  });

  describe('Progress Updates', () => {
    test('should update progress on active job', async () => {
      const job = await qm.push('test', { data: {} });
      await qm.pull('test', 0);

      const result = await qm.updateProgress(job.id, 50, 'halfway');
      expect(result).toBe(true);

      const progress = qm.getProgress(job.id);
      expect(progress?.progress).toBe(50);
      expect(progress?.message).toBe('halfway');
    });

    test('should not update progress on waiting job', async () => {
      const job = await qm.push('test', { data: {} });

      const result = await qm.updateProgress(job.id, 50);
      expect(result).toBe(false);
    });

    test('should handle progress at boundaries', async () => {
      const job = await qm.push('test', { data: {} });
      await qm.pull('test', 0);

      await qm.updateProgress(job.id, 0);
      let progress = qm.getProgress(job.id);
      expect(progress?.progress).toBe(0);

      await qm.updateProgress(job.id, 100);
      progress = qm.getProgress(job.id);
      expect(progress?.progress).toBe(100);
    });
  });

  describe('Batch Operations', () => {
    test('should handle empty batch push', async () => {
      const ids = await qm.pushBatch('test', []);
      expect(ids).toEqual([]);
    });

    test('should handle single item batch push', async () => {
      const ids = await qm.pushBatch('test', [{ data: { single: true } }]);
      expect(ids.length).toBe(1);
    });

    test('should handle large batch push', async () => {
      const items = Array.from({ length: 100 }, (_, i) => ({ data: { index: i } }));
      const ids = await qm.pushBatch('test', items);
      expect(ids.length).toBe(100);
    });
  });

  describe('Logs Operations', () => {
    test('should add log to active job', async () => {
      const job = await qm.push('test', { data: {} });
      await qm.pull('test', 0);

      const result = qm.addLog(job.id, 'Test log message', 'info');
      expect(result).toBe(true);

      const logs = qm.getLogs(job.id);
      expect(logs.length).toBe(1);
      expect(logs[0].message).toBe('Test log message');
      expect(logs[0].level).toBe('info');
    });

    test('should add multiple logs', async () => {
      const job = await qm.push('test', { data: {} });
      await qm.pull('test', 0);

      qm.addLog(job.id, 'Log 1', 'info');
      qm.addLog(job.id, 'Log 2', 'warn');
      qm.addLog(job.id, 'Log 3', 'error');

      const logs = qm.getLogs(job.id);
      expect(logs.length).toBe(3);
    });

    test('should return empty logs for non-active job', () => {
      const logs = qm.getLogs('non-existent' as any);
      expect(logs).toEqual([]);
    });
  });
});
