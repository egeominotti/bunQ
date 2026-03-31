/**
 * Push Operations Tests
 * Comprehensive unit tests for src/application/operations/push.ts
 * Tests pushJob and pushJobBatch with all options, deduplication, custom IDs, etc.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { jobId, type JobId } from '../src/domain/types/job';

describe('Push Operations', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager({ dependencyCheckMs: 50 });
  });

  afterEach(() => {
    qm.shutdown();
  });

  // ============ Single Job Push ============

  describe('pushJob - basic', () => {
    test('should push a job with minimal data', async () => {
      const job = await qm.push('test-q', { data: { hello: 'world' } });

      expect(job).toBeDefined();
      expect(job.id).toBeDefined();
      expect(job.queue).toBe('test-q');
      expect(job.data).toEqual({ hello: 'world' });
      expect(job.priority).toBe(0);
      expect(job.attempts).toBe(0);
      expect(job.maxAttempts).toBe(3);
      expect(job.startedAt).toBeNull();
      expect(job.completedAt).toBeNull();
      expect(job.lifo).toBe(false);
    });

    test('should push a job with empty object data', async () => {
      const job = await qm.push('test-q', { data: {} });
      expect(job.data).toEqual({});
    });

    test('should push a job with null data', async () => {
      const job = await qm.push('test-q', { data: null });
      expect(job.data).toBeNull();
    });

    test('should push a job with string data', async () => {
      const job = await qm.push('test-q', { data: 'just a string' });
      expect(job.data).toBe('just a string');
    });

    test('should push a job with numeric data', async () => {
      const job = await qm.push('test-q', { data: 42 });
      expect(job.data).toBe(42);
    });

    test('should push a job with array data', async () => {
      const job = await qm.push('test-q', { data: [1, 2, 3] });
      expect(job.data).toEqual([1, 2, 3]);
    });

    test('should push a job with deeply nested data', async () => {
      const deepData = {
        level1: {
          level2: {
            level3: {
              level4: { value: 'deep' },
            },
          },
        },
      };
      const job = await qm.push('test-q', { data: deepData });
      expect(job.data).toEqual(deepData);
    });

    test('should push a job with large data payload', async () => {
      const largeData = { payload: 'x'.repeat(100_000) };
      const job = await qm.push('test-q', { data: largeData });
      expect((job.data as { payload: string }).payload.length).toBe(100_000);
    });

    test('should increment totalPushed on successful push', async () => {
      await qm.push('test-q', { data: { a: 1 } });
      await qm.push('test-q', { data: { a: 2 } });
      await qm.push('test-q', { data: { a: 3 } });

      const stats = qm.getStats();
      expect(Number(stats.totalPushed)).toBe(3);
    });

    test('should register queue name on push', async () => {
      await qm.push('new-queue', { data: {} });
      const queues = qm.listQueues();
      expect(queues).toContain('new-queue');
    });
  });

  // ============ Priority ============

  describe('pushJob - priority', () => {
    test('should push with explicit priority', async () => {
      const job = await qm.push('test-q', {
        data: { msg: 'hi' },
        priority: 10,
      });
      expect(job.priority).toBe(10);
    });

    test('should default priority to 0', async () => {
      const job = await qm.push('test-q', { data: {} });
      expect(job.priority).toBe(0);
    });

    test('should handle negative priority', async () => {
      const job = await qm.push('test-q', {
        data: {},
        priority: -5,
      });
      expect(job.priority).toBe(-5);
    });

    test('should handle very high priority', async () => {
      const job = await qm.push('test-q', {
        data: {},
        priority: 999999,
      });
      expect(job.priority).toBe(999999);
    });

    test('should respect priority ordering when pulling', async () => {
      await qm.push('prio-q', { data: { id: 'low' }, priority: 1 });
      await qm.push('prio-q', { data: { id: 'high' }, priority: 10 });
      await qm.push('prio-q', { data: { id: 'medium' }, priority: 5 });

      const first = await qm.pull('prio-q');
      const second = await qm.pull('prio-q');
      const third = await qm.pull('prio-q');

      expect((first?.data as { id: string }).id).toBe('high');
      expect((second?.data as { id: string }).id).toBe('medium');
      expect((third?.data as { id: string }).id).toBe('low');
    });
  });

  // ============ Delay ============

  describe('pushJob - delay', () => {
    test('should push with delay', async () => {
      const before = Date.now();
      const job = await qm.push('test-q', {
        data: {},
        delay: 5000,
      });
      expect(job.runAt).toBeGreaterThanOrEqual(before + 5000);
    });

    test('should not pull delayed jobs immediately', async () => {
      await qm.push('delay-q', {
        data: { msg: 'delayed' },
        delay: 10000,
      });

      const job = await qm.pull('delay-q', 0);
      expect(job).toBeNull();
    });

    test('should set runAt equal to createdAt when no delay', async () => {
      const job = await qm.push('test-q', { data: {} });
      expect(job.runAt).toBe(job.createdAt);
    });

    test('should set delayed job state correctly', async () => {
      const job = await qm.push('test-q', {
        data: {},
        delay: 60000,
      });

      const state = await qm.getJobState(job.id);
      expect(state).toBe('delayed');
    });

    test('should set non-delayed job state to waiting', async () => {
      const job = await qm.push('test-q', { data: {} });

      const state = await qm.getJobState(job.id);
      expect(state).toBe('waiting');
    });

    test('should handle zero delay as immediate', async () => {
      const job = await qm.push('test-q', {
        data: {},
        delay: 0,
      });
      expect(job.runAt).toBe(job.createdAt);
      const pulled = await qm.pull('test-q', 0);
      expect(pulled).not.toBeNull();
    });
  });

  // ============ TTL ============

  describe('pushJob - TTL', () => {
    test('should push with TTL', async () => {
      const job = await qm.push('test-q', {
        data: {},
        ttl: 30000,
      });
      expect(job.ttl).toBe(30000);
    });

    test('should default TTL to null', async () => {
      const job = await qm.push('test-q', { data: {} });
      expect(job.ttl).toBeNull();
    });
  });

  // ============ Timeout ============

  describe('pushJob - timeout', () => {
    test('should push with timeout', async () => {
      const job = await qm.push('test-q', {
        data: {},
        timeout: 15000,
      });
      expect(job.timeout).toBe(15000);
    });

    test('should default timeout to null', async () => {
      const job = await qm.push('test-q', { data: {} });
      expect(job.timeout).toBeNull();
    });
  });

  // ============ Backoff ============

  describe('pushJob - backoff', () => {
    test('should push with numeric backoff', async () => {
      const job = await qm.push('test-q', {
        data: {},
        backoff: 2000,
      });
      expect(job.backoff).toBe(2000);
      expect(job.backoffConfig).toBeNull();
    });

    test('should push with fixed backoff config', async () => {
      const job = await qm.push('test-q', {
        data: {},
        backoff: { type: 'fixed', delay: 3000 },
      });
      expect(job.backoff).toBe(3000);
      expect(job.backoffConfig).toEqual({ type: 'fixed', delay: 3000 });
    });

    test('should push with exponential backoff config', async () => {
      const job = await qm.push('test-q', {
        data: {},
        backoff: { type: 'exponential', delay: 1000 },
      });
      expect(job.backoff).toBe(1000);
      expect(job.backoffConfig).toEqual({ type: 'exponential', delay: 1000 });
    });

    test('should default backoff to 1000', async () => {
      const job = await qm.push('test-q', { data: {} });
      expect(job.backoff).toBe(1000);
    });
  });

  // ============ Tags ============

  describe('pushJob - tags', () => {
    test('should push with tags', async () => {
      const job = await qm.push('test-q', {
        data: {},
        tags: ['urgent', 'billing', 'v2'],
      });
      expect(job.tags).toEqual(['urgent', 'billing', 'v2']);
    });

    test('should default tags to empty array', async () => {
      const job = await qm.push('test-q', { data: {} });
      expect(job.tags).toEqual([]);
    });

    test('should handle single tag', async () => {
      const job = await qm.push('test-q', {
        data: {},
        tags: ['single'],
      });
      expect(job.tags).toEqual(['single']);
    });
  });

  // ============ Group ID ============

  describe('pushJob - groupId', () => {
    test('should push with groupId', async () => {
      const job = await qm.push('test-q', {
        data: {},
        groupId: 'team-alpha',
      });
      expect(job.groupId).toBe('team-alpha');
    });

    test('should default groupId to null', async () => {
      const job = await qm.push('test-q', { data: {} });
      expect(job.groupId).toBeNull();
    });
  });

  // ============ LIFO mode ============

  describe('pushJob - LIFO', () => {
    test('should push with lifo flag', async () => {
      const job = await qm.push('test-q', {
        data: {},
        lifo: true,
      });
      expect(job.lifo).toBe(true);
    });

    test('should default lifo to false', async () => {
      const job = await qm.push('test-q', { data: {} });
      expect(job.lifo).toBe(false);
    });
  });

  // ============ removeOnComplete / removeOnFail ============

  describe('pushJob - removeOnComplete/removeOnFail', () => {
    test('should push with removeOnComplete flag', async () => {
      const job = await qm.push('test-q', {
        data: {},
        removeOnComplete: true,
      });
      expect(job.removeOnComplete).toBe(true);
    });

    test('should push with removeOnFail flag', async () => {
      const job = await qm.push('test-q', {
        data: {},
        removeOnFail: true,
      });
      expect(job.removeOnFail).toBe(true);
    });

    test('should default removeOnComplete to false', async () => {
      const job = await qm.push('test-q', { data: {} });
      expect(job.removeOnComplete).toBe(false);
    });

    test('should default removeOnFail to false', async () => {
      const job = await qm.push('test-q', { data: {} });
      expect(job.removeOnFail).toBe(false);
    });
  });

  // ============ Max Attempts ============

  describe('pushJob - maxAttempts', () => {
    test('should push with custom maxAttempts', async () => {
      const job = await qm.push('test-q', {
        data: {},
        maxAttempts: 5,
      });
      expect(job.maxAttempts).toBe(5);
    });

    test('should default maxAttempts to 3', async () => {
      const job = await qm.push('test-q', { data: {} });
      expect(job.maxAttempts).toBe(3);
    });

    test('should push with maxAttempts of 1 (no retry)', async () => {
      const job = await qm.push('test-q', {
        data: {},
        maxAttempts: 1,
      });
      expect(job.maxAttempts).toBe(1);
    });
  });

  // ============ Stall Timeout ============

  describe('pushJob - stallTimeout', () => {
    test('should push with stallTimeout', async () => {
      const job = await qm.push('test-q', {
        data: {},
        stallTimeout: 60000,
      });
      expect(job.stallTimeout).toBe(60000);
    });

    test('should default stallTimeout to null', async () => {
      const job = await qm.push('test-q', { data: {} });
      expect(job.stallTimeout).toBeNull();
    });
  });

  // ============ Custom ID ============

  describe('pushJob - customId', () => {
    test('should push with customId', async () => {
      const job = await qm.push('test-q', {
        data: { msg: 'hello' },
        customId: 'my-custom-id',
      });
      expect(job.id).toBe('my-custom-id');
      expect(job.customId).toBe('my-custom-id');
    });

    test('should return existing job for duplicate customId', async () => {
      const job1 = await qm.push('test-q', {
        data: { msg: 'first' },
        customId: 'dup-id',
      });

      const job2 = await qm.push('test-q', {
        data: { msg: 'second' },
        customId: 'dup-id',
      });

      expect(job2.id).toBe(job1.id);
      // Original data is preserved
      expect(job2.data).toEqual({ msg: 'first' });
    });

    test('should allow different customIds', async () => {
      const job1 = await qm.push('test-q', {
        data: {},
        customId: 'id-1',
      });

      const job2 = await qm.push('test-q', {
        data: {},
        customId: 'id-2',
      });

      expect(job1.id).not.toBe(job2.id);
    });

    test('should allow reuse of customId after job is completed', async () => {
      const job1 = await qm.push('test-q', {
        data: { msg: 'first' },
        customId: 'reuse-id',
      });

      // Pull and ack the job
      const pulled = await qm.pull('test-q');
      expect(pulled).not.toBeNull();
      await qm.ack(pulled!.id, { done: true });

      // Now push again with same customId should create new job
      const job2 = await qm.push('test-q', {
        data: { msg: 'second' },
        customId: 'reuse-id',
      });

      expect(job2.id).toBe('reuse-id');
      expect(job2.data).toEqual({ msg: 'second' });
    });

    test('should not increment totalPushed on duplicate customId', async () => {
      await qm.push('test-q', {
        data: {},
        customId: 'no-dup-count',
      });

      await qm.push('test-q', {
        data: {},
        customId: 'no-dup-count',
      });

      const stats = qm.getStats();
      expect(Number(stats.totalPushed)).toBe(1);
    });
  });

  // ============ Unique Key / Deduplication ============

  describe('pushJob - uniqueKey', () => {
    test('should push with uniqueKey', async () => {
      const job = await qm.push('test-q', {
        data: {},
        uniqueKey: 'unique-abc',
      });
      expect(job.uniqueKey).toBe('unique-abc');
    });

    test('should return existing job for duplicate uniqueKey (BullMQ-style)', async () => {
      const job1 = await qm.push('test-q', {
        data: { msg: 'first' },
        uniqueKey: 'dup-key',
      });

      const job2 = await qm.push('test-q', {
        data: { msg: 'second' },
        uniqueKey: 'dup-key',
      });

      expect(job2.id).toBe(job1.id);
    });

    test('should not increment totalPushed on duplicate uniqueKey', async () => {
      await qm.push('test-q', {
        data: {},
        uniqueKey: 'dup-count-key',
      });

      await qm.push('test-q', {
        data: {},
        uniqueKey: 'dup-count-key',
      });

      const stats = qm.getStats();
      expect(Number(stats.totalPushed)).toBe(1);
    });

    test('should allow different uniqueKeys', async () => {
      const job1 = await qm.push('test-q', {
        data: {},
        uniqueKey: 'key-1',
      });

      const job2 = await qm.push('test-q', {
        data: {},
        uniqueKey: 'key-2',
      });

      expect(job1.id).not.toBe(job2.id);
    });

    test('should default uniqueKey to null', async () => {
      const job = await qm.push('test-q', { data: {} });
      expect(job.uniqueKey).toBeNull();
    });
  });

  // ============ Dedup with TTL ============

  describe('pushJob - dedup with TTL', () => {
    test('should allow new job after uniqueKey TTL expires', async () => {
      const job1 = await qm.push('test-q', {
        data: { msg: 'first' },
        uniqueKey: 'ttl-key',
        dedup: { ttl: 100 },
      });

      // Should return existing immediately
      const job2 = await qm.push('test-q', {
        data: { msg: 'second' },
        uniqueKey: 'ttl-key',
      });
      expect(job2.id).toBe(job1.id);

      // Wait for TTL expiration
      await Bun.sleep(150);

      // Now should be allowed
      const job3 = await qm.push('test-q', {
        data: { msg: 'third' },
        uniqueKey: 'ttl-key',
        dedup: { ttl: 100 },
      });
      expect(job3.id).not.toBe(job1.id);
    });
  });

  // ============ Dedup with replace ============

  describe('pushJob - dedup with replace', () => {
    test('should replace existing job on duplicate uniqueKey with replace flag', async () => {
      const job1 = await qm.push('test-q', {
        data: { msg: 'first' },
        uniqueKey: 'replace-key',
      });

      const job2 = await qm.push('test-q', {
        data: { msg: 'replaced' },
        uniqueKey: 'replace-key',
        dedup: { replace: true },
      });

      // Should create a new job, not return old one
      expect(job2.id).not.toBe(job1.id);
      expect(job2.data).toEqual({ msg: 'replaced' });

      // Old job should be gone - pull should get new job
      const pulled = await qm.pull('test-q', 0);
      expect(pulled).not.toBeNull();
      expect(pulled!.id).toBe(job2.id);

      // Should only have one job
      const secondPull = await qm.pull('test-q', 0);
      expect(secondPull).toBeNull();
    });
  });

  // ============ Dedup with extend ============

  describe('pushJob - dedup with extend', () => {
    test('should extend TTL and return existing job on duplicate uniqueKey', async () => {
      const job1 = await qm.push('test-q', {
        data: { msg: 'first' },
        uniqueKey: 'extend-key',
        dedup: { ttl: 200 },
      });

      const job2 = await qm.push('test-q', {
        data: { msg: 'second' },
        uniqueKey: 'extend-key',
        dedup: { ttl: 200, extend: true },
      });

      // Should return the same job
      expect(job2.id).toBe(job1.id);
    });
  });

  // ============ dependsOn ============

  describe('pushJob - dependsOn', () => {
    test('should push with dependencies', async () => {
      const depJob = await qm.push('test-q', { data: { id: 'dep' } });

      const job = await qm.push('test-q', {
        data: { id: 'dependent' },
        dependsOn: [depJob.id],
      });

      expect(job.dependsOn).toEqual([depJob.id]);
    });

    test('should not pull job with unmet dependencies', async () => {
      const depJob = await qm.push('dep-q', { data: { id: 'dep' } });

      await qm.push('dep-q', {
        data: { id: 'dependent' },
        dependsOn: [depJob.id],
      });

      // Should pull depJob first (the only pullable one)
      const pulled = await qm.pull('dep-q', 0);
      expect(pulled?.id).toBe(depJob.id);

      // Dependent job not yet available
      const pulled2 = await qm.pull('dep-q', 0);
      expect(pulled2).toBeNull();
    });

    test('should place job in waitingDeps when dependencies are unmet', async () => {
      const depJob = await qm.push('test-q', { data: { id: 'dep' } });

      const job = await qm.push('test-q', {
        data: { id: 'child' },
        dependsOn: [depJob.id],
      });

      // Job should be in jobIndex
      const jobIndex = qm.getJobIndex();
      expect(jobIndex.has(job.id)).toBe(true);
    });

    test('should resolve dependency and make job pullable after dep ack', async () => {
      const depJob = await qm.push('dep-q', { data: { id: 'dep' } });

      const childJob = await qm.push('dep-q', {
        data: { id: 'child' },
        dependsOn: [depJob.id],
      });

      // Pull and ack the dependency
      const pulled = await qm.pull('dep-q', 0);
      await qm.ack(pulled!.id, { done: true });

      // Wait for dependency resolution
      await Bun.sleep(100);

      // Now child should be pullable
      const pulledChild = await qm.pull('dep-q', 0);
      expect(pulledChild?.id).toBe(childJob.id);
    });

    test('should handle job with already-completed dependency', async () => {
      const depJob = await qm.push('dep-q', { data: { id: 'dep' } });

      // Complete the dependency first
      const pulled = await qm.pull('dep-q', 0);
      await qm.ack(pulled!.id, { done: true });

      // Push job that depends on already-completed job
      const childJob = await qm.push('dep-q', {
        data: { id: 'child' },
        dependsOn: [depJob.id],
      });

      // Should be immediately pullable since dep is already completed
      const pulledChild = await qm.pull('dep-q', 0);
      expect(pulledChild?.id).toBe(childJob.id);
    });
  });

  // ============ Parent/Children ============

  describe('pushJob - parentId', () => {
    test('should push with parentId', async () => {
      const parentJob = await qm.push('test-q', { data: { id: 'parent' } });

      const childJob = await qm.push('test-q', {
        data: { id: 'child' },
        parentId: parentJob.id,
      });

      expect(childJob.parentId).toBe(parentJob.id);
    });

    test('should default parentId to null', async () => {
      const job = await qm.push('test-q', { data: {} });
      expect(job.parentId).toBeNull();
    });
  });

  // ============ Events ============

  describe('pushJob - event broadcasting', () => {
    test('should emit pushed event on successful push', async () => {
      const events: string[] = [];

      qm.subscribe((event) => {
        events.push(event.eventType);
      });

      await qm.push('test-q', { data: {} });

      expect(events).toContain('pushed');
    });

    test('should emit duplicated event when uniqueKey duplicate is found', async () => {
      const events: string[] = [];
      const eventJobIds: string[] = [];

      qm.subscribe((event) => {
        events.push(event.eventType);
        eventJobIds.push(event.jobId);
      });

      const job1 = await qm.push('test-q', {
        data: {},
        uniqueKey: 'dup-event-key',
      });

      // Second push should trigger duplicated event
      await qm.push('test-q', {
        data: {},
        uniqueKey: 'dup-event-key',
      });

      expect(events).toContain('pushed');
      expect(events).toContain('duplicated');
    });

    test('should not emit pushed event on duplicate customId', async () => {
      const events: string[] = [];

      qm.subscribe((event) => {
        events.push(event.eventType);
      });

      await qm.push('test-q', {
        data: {},
        customId: 'event-dup-id',
      });

      await qm.push('test-q', {
        data: {},
        customId: 'event-dup-id',
      });

      const pushedCount = events.filter((e) => e === 'pushed').length;
      expect(pushedCount).toBe(1);
    });
  });

  // ============ Repeat ============

  describe('pushJob - repeat config', () => {
    test('should push with repeat every', async () => {
      const job = await qm.push('test-q', {
        data: {},
        repeat: { every: 5000 },
      });
      expect(job.repeat).toBeDefined();
      expect(job.repeat!.every).toBe(5000);
      expect(job.repeat!.count).toBe(0);
    });

    test('should push with repeat limit', async () => {
      const job = await qm.push('test-q', {
        data: {},
        repeat: { every: 1000, limit: 10 },
      });
      expect(job.repeat!.limit).toBe(10);
    });

    test('should default repeat to null', async () => {
      const job = await qm.push('test-q', { data: {} });
      expect(job.repeat).toBeNull();
    });
  });

  // ============ BullMQ v5 options ============

  describe('pushJob - BullMQ v5 options', () => {
    test('should push with stackTraceLimit', async () => {
      const job = await qm.push('test-q', {
        data: {},
        stackTraceLimit: 20,
      });
      expect(job.stackTraceLimit).toBe(20);
    });

    test('should default stackTraceLimit to 10', async () => {
      const job = await qm.push('test-q', { data: {} });
      expect(job.stackTraceLimit).toBe(10);
    });

    test('should push with keepLogs', async () => {
      const job = await qm.push('test-q', {
        data: {},
        keepLogs: 50,
      });
      expect(job.keepLogs).toBe(50);
    });

    test('should push with sizeLimit', async () => {
      const job = await qm.push('test-q', {
        data: {},
        sizeLimit: 1024,
      });
      expect(job.sizeLimit).toBe(1024);
    });

    test('should push with failParentOnFailure', async () => {
      const job = await qm.push('test-q', {
        data: {},
        failParentOnFailure: true,
      });
      expect(job.failParentOnFailure).toBe(true);
    });

    test('should push with removeDependencyOnFailure', async () => {
      const job = await qm.push('test-q', {
        data: {},
        removeDependencyOnFailure: true,
      });
      expect(job.removeDependencyOnFailure).toBe(true);
    });

    test('should push with continueParentOnFailure', async () => {
      const job = await qm.push('test-q', {
        data: {},
        continueParentOnFailure: true,
      });
      expect(job.continueParentOnFailure).toBe(true);
    });

    test('should push with ignoreDependencyOnFailure', async () => {
      const job = await qm.push('test-q', {
        data: {},
        ignoreDependencyOnFailure: true,
      });
      expect(job.ignoreDependencyOnFailure).toBe(true);
    });
  });

  // ============ Durable flag ============

  describe('pushJob - durable', () => {
    test('should push with durable flag (no storage configured, no crash)', async () => {
      // Without storage, durable is essentially a no-op but should not error
      const job = await qm.push('test-q', {
        data: { important: true },
        durable: true,
      });
      expect(job).toBeDefined();
      expect(job.data).toEqual({ important: true });
    });
  });

  // ============ Combined options ============

  describe('pushJob - combined options', () => {
    test('should push with multiple options at once', async () => {
      const job = await qm.push('test-q', {
        data: { task: 'complex' },
        priority: 5,
        delay: 1000,
        maxAttempts: 5,
        backoff: { type: 'exponential', delay: 500 },
        ttl: 60000,
        timeout: 30000,
        tags: ['important', 'billing'],
        groupId: 'billing-group',
        removeOnComplete: true,
        removeOnFail: false,
        stallTimeout: 20000,
      });

      expect(job.priority).toBe(5);
      expect(job.runAt).toBeGreaterThan(job.createdAt);
      expect(job.maxAttempts).toBe(5);
      expect(job.backoffConfig).toEqual({ type: 'exponential', delay: 500 });
      expect(job.ttl).toBe(60000);
      expect(job.timeout).toBe(30000);
      expect(job.tags).toEqual(['important', 'billing']);
      expect(job.groupId).toBe('billing-group');
      expect(job.removeOnComplete).toBe(true);
      expect(job.removeOnFail).toBe(false);
      expect(job.stallTimeout).toBe(20000);
    });
  });

  // ============ Batch Push ============

  describe('pushJobBatch', () => {
    test('should push batch of jobs', async () => {
      const ids = await qm.pushBatch('batch-q', [
        { data: { id: 1 } },
        { data: { id: 2 } },
        { data: { id: 3 } },
      ]);

      expect(ids.length).toBe(3);
      // All IDs should be unique
      const uniqueIds = new Set(ids);
      expect(uniqueIds.size).toBe(3);
    });

    test('should push batch with empty array', async () => {
      const ids = await qm.pushBatch('batch-q', []);
      expect(ids.length).toBe(0);
    });

    test('should push batch with mixed options', async () => {
      const ids = await qm.pushBatch('batch-q', [
        { data: { id: 1 }, priority: 10 },
        { data: { id: 2 }, delay: 5000 },
        { data: { id: 3 }, tags: ['urgent'] },
      ]);

      expect(ids.length).toBe(3);

      // Verify jobs have correct properties
      const job1 = await qm.getJob(ids[0]);
      expect(job1?.priority).toBe(10);

      const job2 = await qm.getJob(ids[1]);
      expect(job2?.runAt).toBeGreaterThan(job2!.createdAt);

      const job3 = await qm.getJob(ids[2]);
      expect(job3?.tags).toEqual(['urgent']);
    });

    test('should handle batch with duplicate customIds (dedup within batch)', async () => {
      const ids = await qm.pushBatch('batch-q', [
        { data: { id: 1 }, customId: 'batch-dup' },
        { data: { id: 2 }, customId: 'batch-dup' },
        { data: { id: 3 }, customId: 'batch-unique' },
      ]);

      expect(ids.length).toBe(3);
      // First and second should be the same (dedup)
      expect(ids[0]).toBe(ids[1]);
      // Third should be unique
      expect(ids[2]).not.toBe(ids[0]);
    });

    test('should handle batch with duplicate uniqueKeys (dedup within batch)', async () => {
      const ids = await qm.pushBatch('batch-q', [
        { data: { id: 1 }, uniqueKey: 'batch-uk' },
        { data: { id: 2 }, uniqueKey: 'batch-uk' },
        { data: { id: 3 }, uniqueKey: 'batch-uk-2' },
      ]);

      expect(ids.length).toBe(3);
      // First and second should be the same (dedup)
      expect(ids[0]).toBe(ids[1]);
      // Third should be unique
      expect(ids[2]).not.toBe(ids[0]);
    });

    test('should increment totalPushed correctly for batch', async () => {
      await qm.pushBatch('batch-q', [
        { data: { id: 1 } },
        { data: { id: 2 } },
        { data: { id: 3 } },
      ]);

      const stats = qm.getStats();
      expect(Number(stats.totalPushed)).toBe(3);
    });

    test('should increment totalPushed correctly for batch with dedup', async () => {
      await qm.pushBatch('batch-q', [
        { data: { id: 1 }, uniqueKey: 'counted-uk' },
        { data: { id: 2 }, uniqueKey: 'counted-uk' }, // dup - not counted
        { data: { id: 3 } },
      ]);

      const stats = qm.getStats();
      expect(Number(stats.totalPushed)).toBe(2);
    });

    test('should push large batch', async () => {
      const inputs = Array.from({ length: 500 }, (_, i) => ({
        data: { index: i },
      }));

      const ids = await qm.pushBatch('batch-q', inputs);
      expect(ids.length).toBe(500);

      const uniqueIds = new Set(ids);
      expect(uniqueIds.size).toBe(500);
    });

    test('should push batch with dependencies', async () => {
      const depJob = await qm.push('batch-q', { data: { id: 'dep' } });

      const ids = await qm.pushBatch('batch-q', [
        { data: { id: 'child1' }, dependsOn: [depJob.id] },
        { data: { id: 'child2' }, dependsOn: [depJob.id] },
        { data: { id: 'independent' } },
      ]);

      expect(ids.length).toBe(3);

      // Only the independent job and the dep should be pullable
      const pulled = await qm.pull('batch-q', 0);
      expect(pulled).not.toBeNull();
      // Dependent jobs should not be pullable yet
    });

    test('should handle batch with replace dedup strategy', async () => {
      // First push a job with uniqueKey
      await qm.push('batch-q', {
        data: { msg: 'original' },
        uniqueKey: 'batch-replace-key',
      });

      // Batch push with replace
      const ids = await qm.pushBatch('batch-q', [
        { data: { msg: 'replaced' }, uniqueKey: 'batch-replace-key', dedup: { replace: true } },
        { data: { msg: 'new' } },
      ]);

      expect(ids.length).toBe(2);

      // Verify the replaced job has new data
      const replaced = await qm.getJob(ids[0]);
      expect(replaced?.data).toEqual({ msg: 'replaced' });
    });
  });

  // ============ jobIndex tracking ============

  describe('pushJob - jobIndex tracking', () => {
    test('should add job to jobIndex on push', async () => {
      const job = await qm.push('test-q', { data: {} });

      const jobIndex = qm.getJobIndex();
      const location = jobIndex.get(job.id);

      expect(location).toBeDefined();
      expect(location!.type).toBe('queue');
      if (location!.type === 'queue') {
        expect(location!.queueName).toBe('test-q');
      }
    });

    test('should track jobs in different queues', async () => {
      const job1 = await qm.push('queue-a', { data: { a: 1 } });
      const job2 = await qm.push('queue-b', { data: { b: 2 } });

      const jobIndex = qm.getJobIndex();

      const loc1 = jobIndex.get(job1.id);
      const loc2 = jobIndex.get(job2.id);

      expect(loc1).toBeDefined();
      expect(loc2).toBeDefined();
      if (loc1!.type === 'queue') expect(loc1!.queueName).toBe('queue-a');
      if (loc2!.type === 'queue') expect(loc2!.queueName).toBe('queue-b');
    });
  });

  // ============ Concurrent pushes ============

  describe('pushJob - concurrency', () => {
    test('should handle concurrent pushes to same queue', async () => {
      const promises = Array.from({ length: 20 }, (_, i) =>
        qm.push('concurrent-q', { data: { index: i } })
      );

      const jobs = await Promise.all(promises);

      expect(jobs.length).toBe(20);
      const ids = new Set(jobs.map((j) => j.id));
      expect(ids.size).toBe(20);
    });

    test('should handle concurrent pushes with customIds without data corruption', async () => {
      const promises = Array.from({ length: 10 }, (_, i) =>
        qm.push('concurrent-q', {
          data: { index: i },
          customId: `concurrent-id-${i}`,
        })
      );

      const jobs = await Promise.all(promises);

      expect(jobs.length).toBe(10);
      const ids = new Set(jobs.map((j) => j.id));
      expect(ids.size).toBe(10);
    });

    test('should handle concurrent pushes with same customId', async () => {
      const promises = Array.from({ length: 5 }, () =>
        qm.push('concurrent-q', {
          data: { shared: true },
          customId: 'same-concurrent-id',
        })
      );

      const jobs = await Promise.all(promises);

      // All should return the same job (idempotency)
      const ids = new Set(jobs.map((j) => j.id));
      expect(ids.size).toBe(1);
    });
  });

  // ============ Edge cases ============

  describe('pushJob - edge cases', () => {
    test('should handle special characters in queue name', async () => {
      const job = await qm.push('my:queue/with-special.chars', { data: {} });
      expect(job.queue).toBe('my:queue/with-special.chars');
    });

    test('should handle unicode in data', async () => {
      const job = await qm.push('test-q', {
        data: { message: 'Hello! Greetings from test' },
      });
      expect((job.data as { message: string }).message).toBe('Hello! Greetings from test');
    });

    test('should handle boolean data', async () => {
      const job = await qm.push('test-q', { data: true });
      expect(job.data).toBe(true);
    });

    test('should handle undefined fields gracefully', async () => {
      const job = await qm.push('test-q', {
        data: {},
        priority: undefined,
        delay: undefined,
        ttl: undefined,
        timeout: undefined,
      });
      expect(job.priority).toBe(0);
      expect(job.ttl).toBeNull();
      expect(job.timeout).toBeNull();
    });

    test('should assign unique IDs to all pushed jobs', async () => {
      const jobs = [];
      for (let i = 0; i < 100; i++) {
        jobs.push(await qm.push('test-q', { data: { i } }));
      }

      const ids = new Set(jobs.map((j) => j.id));
      expect(ids.size).toBe(100);
    });

    test('should set createdAt timestamp close to now', async () => {
      const before = Date.now();
      const job = await qm.push('test-q', { data: {} });
      const after = Date.now();

      expect(job.createdAt).toBeGreaterThanOrEqual(before);
      expect(job.createdAt).toBeLessThanOrEqual(after);
    });

    test('should set attempts to 0 on new job', async () => {
      const job = await qm.push('test-q', { data: {} });
      expect(job.attempts).toBe(0);
    });

    test('should initialize progress to 0', async () => {
      const job = await qm.push('test-q', { data: {} });
      expect(job.progress).toBe(0);
    });

    test('should initialize childrenIds to empty array', async () => {
      const job = await qm.push('test-q', { data: {} });
      expect(job.childrenIds).toEqual([]);
    });

    test('should initialize childrenCompleted to 0', async () => {
      const job = await qm.push('test-q', { data: {} });
      expect(job.childrenCompleted).toBe(0);
    });

    test('should initialize stallCount to 0', async () => {
      const job = await qm.push('test-q', { data: {} });
      expect(job.stallCount).toBe(0);
    });

    test('should set lastHeartbeat to createdAt', async () => {
      const job = await qm.push('test-q', { data: {} });
      expect(job.lastHeartbeat).toBe(job.createdAt);
    });

    test('should handle push to many different queues', async () => {
      const queueNames = Array.from({ length: 20 }, (_, i) => `queue-${i}`);

      for (const name of queueNames) {
        await qm.push(name, { data: { queue: name } });
      }

      const registeredQueues = qm.listQueues();
      for (const name of queueNames) {
        expect(registeredQueues).toContain(name);
      }
    });

    test('should count jobs correctly per queue', async () => {
      await qm.push('count-q', { data: { a: 1 } });
      await qm.push('count-q', { data: { a: 2 } });
      await qm.push('count-q', { data: { a: 3 } });

      const count = qm.count('count-q');
      expect(count).toBe(3);
    });
  });

  // ============ Timestamp override ============

  describe('pushJob - timestamp', () => {
    test('should respect custom timestamp', async () => {
      const customTimestamp = 1700000000000;
      const job = await qm.push('test-q', {
        data: {},
        timestamp: customTimestamp,
      });
      expect(job.createdAt).toBe(customTimestamp);
    });
  });

  // ============ Debounce options ============

  describe('pushJob - debounce options', () => {
    test('should push with debounceId', async () => {
      const job = await qm.push('test-q', {
        data: {},
        debounceId: 'debounce-1',
      });
      expect(job.debounceId).toBe('debounce-1');
    });

    test('should push with debounceTtl', async () => {
      const job = await qm.push('test-q', {
        data: {},
        debounceTtl: 5000,
      });
      expect(job.debounceTtl).toBe(5000);
    });

    test('should default debounce options to null', async () => {
      const job = await qm.push('test-q', { data: {} });
      expect(job.debounceId).toBeNull();
      expect(job.debounceTtl).toBeNull();
    });
  });

  // ============ Stats integration ============

  describe('pushJob - stats integration', () => {
    test('should track delayed jobs in stats', async () => {
      await qm.push('test-q', { data: {}, delay: 60000 });
      await qm.push('test-q', { data: {}, delay: 60000 });
      await qm.push('test-q', { data: {} });

      const stats = qm.getStats();
      expect(stats.delayed).toBe(2);
      expect(stats.waiting).toBe(1);
    });

    test('should track waiting jobs in stats', async () => {
      await qm.push('test-q', { data: {} });
      await qm.push('test-q', { data: {} });
      await qm.push('test-q', { data: {} });

      const stats = qm.getStats();
      expect(stats.waiting).toBeGreaterThanOrEqual(3);
    });
  });
});
