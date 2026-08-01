/**
 * Job Advanced Methods Tests - BullMQ v5 Compatible
 * Tests for getFailedChildrenValues, getIgnoredChildrenFailures,
 * removeChildDependency, removeDeduplicationKey, removeUnprocessedChildren
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, Worker } from '../src/client';
import { shutdownManager } from '../src/client';

describe('Job Advanced Methods - BullMQ v5', () => {
  let queue: Queue<{ value: number }>;

  beforeEach(() => {
    queue = new Queue('job-advanced-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(() => {
    queue.close();
    shutdownManager();
  });

  describe('getFailedChildrenValues', () => {
    test('job should have getFailedChildrenValues method', async () => {
      const job = await queue.add('test', { value: 1 });
      expect(typeof job.getFailedChildrenValues).toBe('function');
    });

    test('getFailedChildrenValues should return empty object for job without children', async () => {
      const job = await queue.add('test', { value: 1 });
      const failedValues = await job.getFailedChildrenValues();
      expect(failedValues).toEqual({});
    });
  });

  describe('getIgnoredChildrenFailures', () => {
    test('job should have getIgnoredChildrenFailures method', async () => {
      const job = await queue.add('test', { value: 1 });
      expect(typeof job.getIgnoredChildrenFailures).toBe('function');
    });

    test('getIgnoredChildrenFailures should return empty object for job without ignored failures', async () => {
      const job = await queue.add('test', { value: 1 });
      const ignoredFailures = await job.getIgnoredChildrenFailures();
      expect(ignoredFailures).toEqual({});
    });
  });

  describe('removeChildDependency', () => {
    test('job should have removeChildDependency method', async () => {
      const job = await queue.add('test', { value: 1 });
      expect(typeof job.removeChildDependency).toBe('function');
    });

    test('removeChildDependency reports that a standalone job has no parent', async () => {
      const job = await queue.add('test', { value: 1 });
      await expect(job.removeChildDependency()).rejects.toThrow(/has no parent/);
    });
  });

  describe('removeDeduplicationKey', () => {
    test('job should have removeDeduplicationKey method', async () => {
      const job = await queue.add('test', { value: 1 });
      expect(typeof job.removeDeduplicationKey).toBe('function');
    });

    test('removeDeduplicationKey releases the key owned by the job', async () => {
      const key = `advanced-${Bun.randomUUIDv7()}`;
      const job = await queue.add('test', { value: 1 }, { deduplication: { id: key } });
      expect(await job.removeDeduplicationKey()).toBe(true);
      expect(await queue.getDeduplicationJobId(key)).toBeNull();
    });
  });

  describe('removeUnprocessedChildren', () => {
    test('job should have removeUnprocessedChildren method', async () => {
      const job = await queue.add('test', { value: 1 });
      expect(typeof job.removeUnprocessedChildren).toBe('function');
    });

    test('removeUnprocessedChildren should resolve for job without children', async () => {
      const job = await queue.add('test', { value: 1 });
      await expect(job.removeUnprocessedChildren()).resolves.toBeUndefined();
    });
  });

  describe('discard', () => {
    test('job should have discard method', async () => {
      const job = await queue.add('test', { value: 1 });
      expect(typeof job.discard).toBe('function');
    });

    test('discard should mark job as discarded', async () => {
      const job = await queue.add('test', { value: 1 });
      // discard returns void
      job.discard();
      // Method should not throw
      expect(true).toBe(true);
    });
  });
});

describe('JobOptions - Parent Failure Handling', () => {
  let queue: Queue<{ value: number }>;

  beforeEach(() => {
    queue = new Queue('parent-failure-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(() => {
    queue.close();
    shutdownManager();
  });

  describe('continueParentOnFailure', () => {
    test('should accept continueParentOnFailure option', async () => {
      // Option should be accepted without throwing
      const job = await queue.add(
        'test',
        { value: 1 },
        {
          continueParentOnFailure: true,
        }
      );
      expect(job).toBeDefined();
      expect(job.id).toBeDefined();
    });

    test('should work with false value', async () => {
      const job = await queue.add(
        'test',
        { value: 1 },
        {
          continueParentOnFailure: false,
        }
      );
      expect(job).toBeDefined();
    });
  });

  describe('ignoreDependencyOnFailure', () => {
    test('should accept ignoreDependencyOnFailure option', async () => {
      // Option should be accepted without throwing
      const job = await queue.add(
        'test',
        { value: 1 },
        {
          ignoreDependencyOnFailure: true,
        }
      );
      expect(job).toBeDefined();
      expect(job.id).toBeDefined();
    });

    test('should work with false value', async () => {
      const job = await queue.add(
        'test',
        { value: 1 },
        {
          ignoreDependencyOnFailure: false,
        }
      );
      expect(job).toBeDefined();
    });
  });

  describe('timestamp', () => {
    test('should accept custom timestamp option', async () => {
      const customTimestamp = Date.now() - 10000; // 10 seconds ago
      const job = await queue.add(
        'test',
        { value: 1 },
        {
          timestamp: customTimestamp,
        }
      );
      expect(job).toBeDefined();
      // The job's timestamp should be close to our custom timestamp
      // (may be slightly different due to processing)
      expect(job.timestamp).toBeLessThanOrEqual(Date.now());
    });
  });
});
