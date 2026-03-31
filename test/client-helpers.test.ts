/**
 * Tests for client helper modules:
 * - jobConversion.ts: createPublicJob, toPublicJob, toDlqEntry
 * - jobHelpers.ts: extractUserData, extractParent, buildRepeatOpts, buildParentOpts, buildJobOpts, buildParentKey, buildRepeatJobKey
 * - events.ts: QueueEvents class
 * - queueGroup.ts: QueueGroup class
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import {
  extractUserData,
  extractParent,
  buildRepeatOpts,
  buildParentOpts,
  buildJobOpts,
  buildParentKey,
  buildRepeatJobKey,
} from '../src/client/jobHelpers';
import { createPublicJob, toPublicJob, toDlqEntry } from '../src/client/jobConversion';
import { QueueEvents } from '../src/client/events';
import { QueueGroup } from '../src/client/queueGroup';
import { Queue, Worker, shutdownManager } from '../src/client';
import type { Job as InternalJob } from '../src/domain/types/job';
import type { DlqEntry as InternalDlqEntry } from '../src/domain/types/dlq';
import { jobId } from '../src/domain/types/job';

// ============================================================================
// Test Helpers
// ============================================================================

/** Create a minimal internal job for testing */
function makeInternalJob(overrides: Partial<InternalJob> = {}): InternalJob {
  const now = Date.now();
  return {
    id: jobId('test-job-1'),
    queue: 'test-queue',
    data: { name: 'test-task', key: 'value' },
    priority: 0,
    createdAt: now,
    lifo: false,
    runAt: now,
    startedAt: null,
    completedAt: null,
    attempts: 0,
    maxAttempts: 3,
    backoff: 1000,
    backoffConfig: null,
    ttl: null,
    timeout: null,
    uniqueKey: null,
    customId: null,
    dependsOn: [],
    parentId: null,
    childrenIds: [],
    childrenCompleted: 0,
    tags: [],
    groupId: null,
    progress: 0,
    progressMessage: null,
    removeOnComplete: false,
    removeOnFail: false,
    repeat: null,
    lastHeartbeat: now,
    stallTimeout: null,
    stallCount: 0,
    stackTraceLimit: 10,
    keepLogs: null,
    sizeLimit: null,
    failParentOnFailure: false,
    removeDependencyOnFailure: false,
    continueParentOnFailure: false,
    ignoreDependencyOnFailure: false,
    deduplicationTtl: null,
    deduplicationExtend: false,
    deduplicationReplace: false,
    debounceId: null,
    debounceTtl: null,
    timeline: [],
    ...overrides,
  };
}

// ============================================================================
// jobHelpers.ts
// ============================================================================

describe('jobHelpers', () => {
  describe('extractUserData', () => {
    test('should remove internal "name" field from object data', () => {
      const data = { name: 'task-name', email: 'user@test.com', count: 5 };
      const result = extractUserData(data);
      expect(result).toEqual({ email: 'user@test.com', count: 5 });
    });

    test('should return empty object when data only has name', () => {
      const data = { name: 'task-name' };
      const result = extractUserData(data);
      expect(result).toEqual({});
    });

    test('should return data as-is when not an object', () => {
      expect(extractUserData('hello')).toBe('hello');
      expect(extractUserData(42)).toBe(42);
      expect(extractUserData(true)).toBe(true);
    });

    test('should return null as-is', () => {
      expect(extractUserData(null)).toBeNull();
    });

    test('should return undefined as-is', () => {
      expect(extractUserData(undefined)).toBeUndefined();
    });

    test('should preserve all non-name fields', () => {
      const data = {
        name: 'ignored',
        nested: { a: 1 },
        arr: [1, 2, 3],
        bool: true,
        num: 99.5,
      };
      const result = extractUserData(data) as Record<string, unknown>;
      expect(result).toEqual({
        nested: { a: 1 },
        arr: [1, 2, 3],
        bool: true,
        num: 99.5,
      });
      expect(result).not.toHaveProperty('name');
    });

    test('should handle object without name field', () => {
      const data = { foo: 'bar', baz: 123 };
      const result = extractUserData(data);
      expect(result).toEqual({ foo: 'bar', baz: 123 });
    });
  });

  describe('extractParent', () => {
    test('should return parent when __parentId and __parentQueue are strings', () => {
      const data = { __parentId: 'parent-1', __parentQueue: 'parent-queue' };
      const result = extractParent(data);
      expect(result).toEqual({
        id: 'parent-1',
        queueQualifiedName: 'parent-queue',
      });
    });

    test('should return parent when __parentId and __parentQueue are numbers', () => {
      const data = { __parentId: 42, __parentQueue: 100 };
      const result = extractParent(data);
      expect(result).toEqual({
        id: '42',
        queueQualifiedName: '100',
      });
    });

    test('should return parent with mixed string/number types', () => {
      const data = { __parentId: 'parent-1', __parentQueue: 99 };
      const result = extractParent(data);
      expect(result).toEqual({
        id: 'parent-1',
        queueQualifiedName: '99',
      });
    });

    test('should return undefined when __parentId is missing', () => {
      const data = { __parentQueue: 'parent-queue' };
      expect(extractParent(data)).toBeUndefined();
    });

    test('should return undefined when __parentQueue is missing', () => {
      const data = { __parentId: 'parent-1' };
      expect(extractParent(data)).toBeUndefined();
    });

    test('should return undefined when data is not an object', () => {
      expect(extractParent('string')).toBeUndefined();
      expect(extractParent(42)).toBeUndefined();
      expect(extractParent(null)).toBeUndefined();
      expect(extractParent(undefined)).toBeUndefined();
    });

    test('should return undefined when parent IDs have invalid types (boolean)', () => {
      const data = { __parentId: true, __parentQueue: 'queue' };
      expect(extractParent(data)).toBeUndefined();
    });

    test('should return undefined when parent IDs have invalid types (object)', () => {
      const data = { __parentId: { nested: true }, __parentQueue: 'queue' };
      expect(extractParent(data)).toBeUndefined();
    });
  });

  describe('buildRepeatOpts', () => {
    test('should return undefined when repeat is null/undefined', () => {
      expect(buildRepeatOpts(null)).toBeUndefined();
      expect(buildRepeatOpts(undefined)).toBeUndefined();
    });

    test('should convert repeat config with "every" option', () => {
      const repeat = {
        every: 5000,
        count: 0,
      };
      const result = buildRepeatOpts(repeat);
      expect(result).toEqual({
        every: 5000,
        limit: undefined,
        pattern: undefined,
        count: 0,
        startDate: undefined,
        endDate: undefined,
        tz: undefined,
        immediately: undefined,
        prevMillis: undefined,
        offset: undefined,
        jobId: undefined,
      });
    });

    test('should convert repeat config with cron pattern', () => {
      const repeat = {
        pattern: '*/5 * * * *',
        count: 10,
        tz: 'America/New_York',
        limit: 100,
        startDate: 1700000000000,
        endDate: 1800000000000,
        immediately: true,
        prevMillis: 1700000005000,
        offset: 500,
        jobId: 'repeat-job-id',
      };
      const result = buildRepeatOpts(repeat);
      expect(result).toEqual({
        every: undefined,
        pattern: '*/5 * * * *',
        count: 10,
        tz: 'America/New_York',
        limit: 100,
        startDate: 1700000000000,
        endDate: 1800000000000,
        immediately: true,
        prevMillis: 1700000005000,
        offset: 500,
        jobId: 'repeat-job-id',
      });
    });
  });

  describe('buildParentOpts', () => {
    test('should return parent from job.parentId and data.__parentQueue', () => {
      const job = makeInternalJob({
        parentId: jobId('parent-1'),
        data: { name: 'test', __parentQueue: 'parent-queue' },
      });
      const result = buildParentOpts(job);
      expect(result).toEqual({ id: 'parent-1', queue: 'parent-queue' });
    });

    test('should return parent from data.__parentId and data.__parentQueue', () => {
      const job = makeInternalJob({
        parentId: null,
        data: { name: 'test', __parentId: 'data-parent', __parentQueue: 'data-queue' },
      });
      const result = buildParentOpts(job);
      expect(result).toEqual({ id: 'data-parent', queue: 'data-queue' });
    });

    test('should prefer job.parentId over data.__parentId', () => {
      const job = makeInternalJob({
        parentId: jobId('job-parent'),
        data: { name: 'test', __parentId: 'data-parent', __parentQueue: 'parent-queue' },
      });
      const result = buildParentOpts(job);
      expect(result).toEqual({ id: 'job-parent', queue: 'parent-queue' });
    });

    test('should return undefined when no parent info exists', () => {
      const job = makeInternalJob({ parentId: null, data: { name: 'test' } });
      expect(buildParentOpts(job)).toBeUndefined();
    });

    test('should return undefined when parentId exists but no queue', () => {
      const job = makeInternalJob({
        parentId: jobId('parent-1'),
        data: { name: 'test' },
      });
      expect(buildParentOpts(job)).toBeUndefined();
    });

    test('should handle numeric parent values', () => {
      const job = makeInternalJob({
        parentId: null,
        data: { name: 'test', __parentId: 42, __parentQueue: 99 },
      });
      const result = buildParentOpts(job);
      expect(result).toEqual({ id: '42', queue: '99' });
    });
  });

  describe('buildJobOpts', () => {
    test('should build basic job options', () => {
      const job = makeInternalJob({
        priority: 5,
        maxAttempts: 5,
        backoff: 2000,
        timeout: 30000,
        customId: 'my-custom-id',
        removeOnComplete: true,
        removeOnFail: false,
      });
      const opts = buildJobOpts(job);
      expect(opts.priority).toBe(5);
      expect(opts.attempts).toBe(5);
      expect(opts.backoff).toBe(2000);
      expect(opts.timeout).toBe(30000);
      expect(opts.jobId).toBe('my-custom-id');
      expect(opts.removeOnComplete).toBe(true);
      expect(opts.removeOnFail).toBe(false);
    });

    test('should calculate delay from runAt - createdAt', () => {
      const now = Date.now();
      const job = makeInternalJob({
        createdAt: now,
        runAt: now + 5000,
      });
      const opts = buildJobOpts(job);
      expect(opts.delay).toBe(5000);
    });

    test('should set delay to 0 when runAt <= createdAt', () => {
      const now = Date.now();
      const job = makeInternalJob({
        createdAt: now,
        runAt: now,
      });
      const opts = buildJobOpts(job);
      expect(opts.delay).toBe(0);
    });

    test('should use backoffConfig when present', () => {
      const job = makeInternalJob({
        backoffConfig: { type: 'exponential', delay: 500 },
        backoff: 1000,
      });
      const opts = buildJobOpts(job);
      expect(opts.backoff).toEqual({ type: 'exponential', delay: 500 });
    });

    test('should use numeric backoff when backoffConfig is null', () => {
      const job = makeInternalJob({
        backoffConfig: null,
        backoff: 2500,
      });
      const opts = buildJobOpts(job);
      expect(opts.backoff).toBe(2500);
    });

    test('should include repeat options when present', () => {
      const job = makeInternalJob({
        repeat: {
          every: 10000,
          count: 5,
          limit: 100,
        },
      });
      const opts = buildJobOpts(job);
      expect(opts.repeat).toBeDefined();
      expect(opts.repeat!.every).toBe(10000);
      expect(opts.repeat!.count).toBe(5);
      expect(opts.repeat!.limit).toBe(100);
    });

    test('should handle undefined repeat', () => {
      const job = makeInternalJob({ repeat: null });
      const opts = buildJobOpts(job);
      expect(opts.repeat).toBeUndefined();
    });

    test('should include BullMQ v5 options', () => {
      const job = makeInternalJob({
        lifo: true,
        stackTraceLimit: 20,
        keepLogs: 500,
        sizeLimit: 1024,
        failParentOnFailure: true,
        removeDependencyOnFailure: true,
        stallTimeout: 15000,
      });
      const opts = buildJobOpts(job);
      expect(opts.lifo).toBe(true);
      expect(opts.stackTraceLimit).toBe(20);
      expect(opts.keepLogs).toBe(500);
      expect(opts.sizeLimit).toBe(1024);
      expect(opts.failParentOnFailure).toBe(true);
      expect(opts.removeDependencyOnFailure).toBe(true);
      expect(opts.stallTimeout).toBe(15000);
    });

    test('should include deduplication options', () => {
      const job = makeInternalJob({
        customId: 'dedup-key',
        deduplicationTtl: 60000,
      });
      const opts = buildJobOpts(job);
      expect(opts.deduplication).toEqual({ id: 'dedup-key', ttl: 60000 });
    });

    test('should omit deduplication when ttl is null', () => {
      const job = makeInternalJob({
        deduplicationTtl: null,
      });
      const opts = buildJobOpts(job);
      expect(opts.deduplication).toBeUndefined();
    });

    test('should include debounce options', () => {
      const job = makeInternalJob({
        debounceId: 'debounce-1',
        debounceTtl: 5000,
      });
      const opts = buildJobOpts(job);
      expect(opts.debounce).toEqual({ id: 'debounce-1', ttl: 5000 });
    });

    test('should omit debounce when debounceId is null', () => {
      const job = makeInternalJob({
        debounceId: null,
        debounceTtl: 5000,
      });
      const opts = buildJobOpts(job);
      expect(opts.debounce).toBeUndefined();
    });

    test('should omit debounce when debounceTtl is null', () => {
      const job = makeInternalJob({
        debounceId: 'debounce-1',
        debounceTtl: null,
      });
      const opts = buildJobOpts(job);
      expect(opts.debounce).toBeUndefined();
    });
  });

  describe('buildParentKey', () => {
    test('should build parent key with parentId and __parentQueue', () => {
      const job = makeInternalJob({
        parentId: jobId('parent-abc'),
        data: { name: 'test', __parentQueue: 'billing' },
      });
      expect(buildParentKey(job)).toBe('billing:parent-abc');
    });

    test('should use "unknown" when __parentQueue is missing', () => {
      const job = makeInternalJob({
        parentId: jobId('parent-abc'),
        data: { name: 'test' },
      });
      expect(buildParentKey(job)).toBe('unknown:parent-abc');
    });

    test('should return undefined when parentId is null', () => {
      const job = makeInternalJob({ parentId: null });
      expect(buildParentKey(job)).toBeUndefined();
    });

    test('should coerce numeric __parentQueue to string', () => {
      const job = makeInternalJob({
        parentId: jobId('p1'),
        data: { name: 'test', __parentQueue: 42 },
      });
      expect(buildParentKey(job)).toBe('42:p1');
    });

    test('should use "unknown" when __parentQueue is invalid type', () => {
      const job = makeInternalJob({
        parentId: jobId('p1'),
        data: { name: 'test', __parentQueue: true },
      });
      expect(buildParentKey(job)).toBe('unknown:p1');
    });
  });

  describe('buildRepeatJobKey', () => {
    test('should build repeat key with pattern', () => {
      const job = makeInternalJob({
        queue: 'emails',
        id: jobId('job-123'),
        repeat: { pattern: '*/5 * * * *', count: 0 },
      });
      expect(buildRepeatJobKey(job)).toBe('emails:job-123:*/5 * * * *');
    });

    test('should build repeat key with "every" fallback', () => {
      const job = makeInternalJob({
        queue: 'emails',
        id: jobId('job-123'),
        repeat: { every: 5000, count: 0 },
      });
      expect(buildRepeatJobKey(job)).toBe('emails:job-123:every:5000');
    });

    test('should build repeat key with empty pattern when neither present', () => {
      const job = makeInternalJob({
        queue: 'emails',
        id: jobId('job-123'),
        repeat: { count: 0 },
      });
      expect(buildRepeatJobKey(job)).toBe('emails:job-123:');
    });

    test('should return undefined when repeat is null', () => {
      const job = makeInternalJob({ repeat: null });
      expect(buildRepeatJobKey(job)).toBeUndefined();
    });
  });
});

// ============================================================================
// jobConversion.ts
// ============================================================================

describe('jobConversion', () => {
  describe('createPublicJob', () => {
    test('should convert internal job to public job with all properties', () => {
      const now = Date.now();
      const internalJob = makeInternalJob({
        id: jobId('job-abc'),
        queue: 'my-queue',
        data: { name: 'send-email', to: 'user@example.com' },
        priority: 5,
        createdAt: now - 1000,
        runAt: now - 1000,
        attempts: 2,
        progress: 50,
        stallCount: 1,
      });

      const updateProgress = async () => {};
      const log = async () => {};

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'send-email',
        updateProgress,
        log,
      });

      expect(publicJob.id).toBe('job-abc');
      expect(publicJob.name).toBe('send-email');
      expect(publicJob.data).toEqual({ to: 'user@example.com' });
      expect(publicJob.queueName).toBe('my-queue');
      expect(publicJob.attemptsMade).toBe(2);
      expect(publicJob.timestamp).toBe(now - 1000);
      expect(publicJob.progress).toBe(50);
      expect(publicJob.stalledCounter).toBe(1);
      expect(publicJob.priority).toBe(5);
      expect(publicJob.delay).toBe(0);
      expect(publicJob.attemptsStarted).toBe(2);
    });

    test('should set delay when runAt > createdAt', () => {
      const now = Date.now();
      const internalJob = makeInternalJob({
        createdAt: now,
        runAt: now + 3000,
      });

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
      });

      expect(publicJob.delay).toBe(3000);
    });

    test('should set processedOn from startedAt', () => {
      const now = Date.now();
      const internalJob = makeInternalJob({
        startedAt: now - 500,
      });

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
      });

      expect(publicJob.processedOn).toBe(now - 500);
    });

    test('should set finishedOn from completedAt', () => {
      const now = Date.now();
      const internalJob = makeInternalJob({
        completedAt: now,
      });

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
      });

      expect(publicJob.finishedOn).toBe(now);
    });

    test('should set processedOn to undefined when startedAt is null', () => {
      const internalJob = makeInternalJob({ startedAt: null });

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
      });

      expect(publicJob.processedOn).toBeUndefined();
    });

    test('should set stacktrace from options', () => {
      const internalJob = makeInternalJob();
      const traces = ['Error: boom', '  at process (/app/worker.ts:10)'];

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
        stacktrace: traces,
      });

      expect(publicJob.stacktrace).toEqual(traces);
    });

    test('should set stacktrace to null when not provided', () => {
      const internalJob = makeInternalJob();

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
      });

      expect(publicJob.stacktrace).toBeNull();
    });

    test('should include token and processedBy', () => {
      const internalJob = makeInternalJob();

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
        token: 'lock-token-123',
        processedBy: 'worker-1',
      });

      expect(publicJob.token).toBe('lock-token-123');
      expect(publicJob.processedBy).toBe('worker-1');
    });

    test('should set deduplicationId from customId', () => {
      const internalJob = makeInternalJob({ customId: 'unique-123' });

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
      });

      expect(publicJob.deduplicationId).toBe('unique-123');
    });

    test('should set deduplicationId to undefined when customId is null', () => {
      const internalJob = makeInternalJob({ customId: null });

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
      });

      expect(publicJob.deduplicationId).toBeUndefined();
    });

    test('should extract parent info from job data', () => {
      const internalJob = makeInternalJob({
        data: { name: 'test', __parentId: 'p1', __parentQueue: 'billing' },
      });

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
      });

      expect(publicJob.parent).toEqual({
        id: 'p1',
        queueQualifiedName: 'billing',
      });
    });

    test('should coerce job id to string', () => {
      const internalJob = makeInternalJob({ id: jobId('12345') });

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
      });

      expect(publicJob.id).toBe('12345');
      expect(typeof publicJob.id).toBe('string');
    });

    // Method delegation tests

    test('updateProgress should delegate to provided callback', async () => {
      const calls: Array<{ id: string; progress: number; message?: string }> = [];
      const internalJob = makeInternalJob({ id: jobId('job-1') });

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async (id, progress, message) => {
          calls.push({ id, progress, message });
        },
        log: async () => {},
      });

      await publicJob.updateProgress(75, 'three quarters done');
      expect(calls).toHaveLength(1);
      expect(calls[0]).toEqual({ id: 'job-1', progress: 75, message: 'three quarters done' });
    });

    test('log should delegate to provided callback', async () => {
      const logs: Array<{ id: string; message: string }> = [];
      const internalJob = makeInternalJob({ id: jobId('job-1') });

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async (id, message) => {
          logs.push({ id, message });
        },
      });

      await publicJob.log('Processing step 1');
      expect(logs).toHaveLength(1);
      expect(logs[0]).toEqual({ id: 'job-1', message: 'Processing step 1' });
    });

    test('getState should return state from callback', async () => {
      const internalJob = makeInternalJob({ id: jobId('job-1') });

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
        getState: async () => 'active',
      });

      const state = await publicJob.getState();
      expect(state).toBe('active');
    });

    test('getState should return "unknown" when callback not provided', async () => {
      const internalJob = makeInternalJob();

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
      });

      const state = await publicJob.getState();
      expect(state).toBe('unknown');
    });

    test('remove should call callback', async () => {
      let removedId: string | null = null;
      const internalJob = makeInternalJob({ id: jobId('job-to-remove') });

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
        remove: async (id) => {
          removedId = id;
        },
      });

      await publicJob.remove();
      expect(removedId).toBe('job-to-remove');
    });

    test('remove should be no-op when callback not provided', async () => {
      const internalJob = makeInternalJob();

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
      });

      await expect(publicJob.remove()).resolves.toBeUndefined();
    });

    test('retry should call callback', async () => {
      let retriedId: string | null = null;
      const internalJob = makeInternalJob({ id: jobId('job-retry') });

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
        retry: async (id) => {
          retriedId = id;
        },
      });

      await publicJob.retry();
      expect(retriedId).toBe('job-retry');
    });

    // State check methods

    test('isWaiting should return true when state is waiting', async () => {
      const internalJob = makeInternalJob();

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
        getState: async () => 'waiting',
      });

      expect(await publicJob.isWaiting()).toBe(true);
      expect(await publicJob.isActive()).toBe(false);
    });

    test('isActive should return true when state is active', async () => {
      const internalJob = makeInternalJob();

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
        getState: async () => 'active',
      });

      expect(await publicJob.isActive()).toBe(true);
      expect(await publicJob.isWaiting()).toBe(false);
    });

    test('isDelayed should return true when state is delayed', async () => {
      const internalJob = makeInternalJob();

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
        getState: async () => 'delayed',
      });

      expect(await publicJob.isDelayed()).toBe(true);
    });

    test('isCompleted should return true when state is completed', async () => {
      const internalJob = makeInternalJob();

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
        getState: async () => 'completed',
      });

      expect(await publicJob.isCompleted()).toBe(true);
    });

    test('isFailed should return true when state is failed', async () => {
      const internalJob = makeInternalJob();

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
        getState: async () => 'failed',
      });

      expect(await publicJob.isFailed()).toBe(true);
    });

    test('isWaitingChildren should use getDependenciesCount', async () => {
      const internalJob = makeInternalJob();

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
        getDependenciesCount: async () => ({ processed: 2, unprocessed: 3 }),
      });

      expect(await publicJob.isWaitingChildren()).toBe(true);
    });

    test('isWaitingChildren should return false when no unprocessed dependencies', async () => {
      const internalJob = makeInternalJob();

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
        getDependenciesCount: async () => ({ processed: 5, unprocessed: 0 }),
      });

      expect(await publicJob.isWaitingChildren()).toBe(false);
    });

    test('isWaitingChildren should return false when no callback', async () => {
      const internalJob = makeInternalJob();

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
      });

      expect(await publicJob.isWaitingChildren()).toBe(false);
    });

    // Serialization methods

    test('toJSON should return proper JSON representation', () => {
      const now = Date.now();
      const internalJob = makeInternalJob({
        id: jobId('json-job'),
        queue: 'my-queue',
        data: { name: 'task', foo: 'bar' },
        priority: 3,
        createdAt: now,
        runAt: now + 2000,
        attempts: 1,
        progress: 25,
        completedAt: null,
        startedAt: now + 100,
      });

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'task',
        updateProgress: async () => {},
        log: async () => {},
      });

      const json = publicJob.toJSON();
      expect(json.id).toBe('json-job');
      expect(json.name).toBe('task');
      expect(json.data).toEqual({ foo: 'bar' });
      expect(json.delay).toBe(2000);
      expect(json.timestamp).toBe(now);
      expect(json.attemptsMade).toBe(1);
      expect(json.stacktrace).toBeNull();
      expect(json.returnvalue).toBeUndefined();
      expect(json.failedReason).toBeUndefined();
      expect(json.processedOn).toBe(now + 100);
      expect(json.finishedOn).toBeUndefined();
      expect(json.queueQualifiedName).toBe('bull:my-queue');
    });

    test('asJSON should return string-serialized representation', () => {
      const now = Date.now();
      const internalJob = makeInternalJob({
        id: jobId('raw-job'),
        queue: 'my-queue',
        data: { name: 'task', foo: 'bar' },
        createdAt: now,
        runAt: now,
        attempts: 0,
        progress: 0,
      });

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'task',
        updateProgress: async () => {},
        log: async () => {},
      });

      const raw = publicJob.asJSON();
      expect(raw.id).toBe('raw-job');
      expect(raw.name).toBe('task');
      expect(raw.data).toBe(JSON.stringify({ foo: 'bar' }));
      expect(typeof raw.opts).toBe('string');
      expect(raw.progress).toBe(JSON.stringify(0));
      expect(raw.delay).toBe('0');
      expect(raw.timestamp).toBe(String(now));
      expect(raw.attemptsMade).toBe('0');
      expect(raw.stacktrace).toBeNull();
    });

    // BullMQ v5 mutation method delegation

    test('updateData should delegate to callback', async () => {
      let calledWith: { id: string; data: unknown } | null = null;
      const internalJob = makeInternalJob({ id: jobId('ud-job') });

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
        updateData: async (id, data) => {
          calledWith = { id, data };
        },
      });

      await publicJob.updateData({ newField: true });
      expect(calledWith).toEqual({ id: 'ud-job', data: { newField: true } });
    });

    test('promote should delegate to callback', async () => {
      let promotedId: string | null = null;
      const internalJob = makeInternalJob({ id: jobId('promo-job') });

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
        promote: async (id) => {
          promotedId = id;
        },
      });

      await publicJob.promote();
      expect(promotedId).toBe('promo-job');
    });

    test('changeDelay should delegate to callback', async () => {
      let calledWith: { id: string; delay: number } | null = null;
      const internalJob = makeInternalJob({ id: jobId('cd-job') });

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
        changeDelay: async (id, delay) => {
          calledWith = { id, delay };
        },
      });

      await publicJob.changeDelay(5000);
      expect(calledWith).toEqual({ id: 'cd-job', delay: 5000 });
    });

    test('changePriority should delegate to callback', async () => {
      let calledWith: { id: string; opts: { priority: number; lifo?: boolean } } | null = null;
      const internalJob = makeInternalJob({ id: jobId('cp-job') });

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
        changePriority: async (id, opts) => {
          calledWith = { id, opts };
        },
      });

      await publicJob.changePriority({ priority: 10, lifo: true });
      expect(calledWith).toEqual({ id: 'cp-job', opts: { priority: 10, lifo: true } });
    });

    test('extendLock should delegate to callback', async () => {
      const internalJob = makeInternalJob({ id: jobId('el-job') });

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
        extendLock: async (_id, _token, _duration) => 30000,
      });

      const result = await publicJob.extendLock('tok-1', 30000);
      expect(result).toBe(30000);
    });

    test('extendLock should return 0 when no callback', async () => {
      const internalJob = makeInternalJob();

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
      });

      const result = await publicJob.extendLock('tok-1', 30000);
      expect(result).toBe(0);
    });

    test('discard should call callback synchronously', () => {
      let discardedId: string | null = null;
      const internalJob = makeInternalJob({ id: jobId('disc-job') });

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
        discard: (id) => {
          discardedId = id;
        },
      });

      publicJob.discard();
      expect(discardedId).toBe('disc-job');
    });

    test('discard should be no-op when no callback', () => {
      const internalJob = makeInternalJob();

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
      });

      // Should not throw
      publicJob.discard();
    });

    test('moveToCompleted should delegate to callback', async () => {
      const internalJob = makeInternalJob({ id: jobId('mtc-job') });

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
        moveToCompleted: async (_id, returnValue, _token) => returnValue,
      });

      const result = await publicJob.moveToCompleted({ success: true }, 'tok');
      expect(result).toEqual({ success: true });
    });

    test('moveToCompleted should return null when no callback', async () => {
      const internalJob = makeInternalJob();

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
      });

      const result = await publicJob.moveToCompleted({ success: true });
      expect(result).toBeNull();
    });

    test('moveToFailed should delegate to callback', async () => {
      let calledError: Error | null = null;
      const internalJob = makeInternalJob({ id: jobId('mtf-job') });

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
        moveToFailed: async (_id, error, _token) => {
          calledError = error;
        },
      });

      const err = new Error('test failure');
      await publicJob.moveToFailed(err);
      expect(calledError).toBe(err);
    });

    test('moveToWait should delegate and return result', async () => {
      const internalJob = makeInternalJob({ id: jobId('mtw-job') });

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
        moveToWait: async () => true,
      });

      expect(await publicJob.moveToWait('tok')).toBe(true);
    });

    test('moveToWait should return false when no callback', async () => {
      const internalJob = makeInternalJob();

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
      });

      expect(await publicJob.moveToWait()).toBe(false);
    });

    test('getDependencies should return defaults when no callback', async () => {
      const internalJob = makeInternalJob();

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
      });

      const deps = await publicJob.getDependencies();
      expect(deps).toEqual({ processed: {}, unprocessed: [] });
    });

    test('getDependenciesCount should return zeros when no callback', async () => {
      const internalJob = makeInternalJob();

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
      });

      const counts = await publicJob.getDependenciesCount();
      expect(counts).toEqual({ processed: 0, unprocessed: 0 });
    });

    test('getChildrenValues should return empty when no callback', async () => {
      const internalJob = makeInternalJob();

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
      });

      const values = await publicJob.getChildrenValues();
      expect(values).toEqual({});
    });

    test('getFailedChildrenValues should return empty when no callback', async () => {
      const internalJob = makeInternalJob();

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
      });

      expect(await publicJob.getFailedChildrenValues()).toEqual({});
    });

    test('getIgnoredChildrenFailures should return empty when no callback', async () => {
      const internalJob = makeInternalJob();

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
      });

      expect(await publicJob.getIgnoredChildrenFailures()).toEqual({});
    });

    test('removeChildDependency should return false when no callback', async () => {
      const internalJob = makeInternalJob();

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
      });

      expect(await publicJob.removeChildDependency()).toBe(false);
    });

    test('removeDeduplicationKey should return false when no callback', async () => {
      const internalJob = makeInternalJob();

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
      });

      expect(await publicJob.removeDeduplicationKey()).toBe(false);
    });

    test('removeUnprocessedChildren should resolve when no callback', async () => {
      const internalJob = makeInternalJob();

      const publicJob = createPublicJob({
        job: internalJob,
        name: 'test',
        updateProgress: async () => {},
        log: async () => {},
      });

      await expect(publicJob.removeUnprocessedChildren()).resolves.toBeUndefined();
    });
  });

  describe('toPublicJob', () => {
    test('should convert internal job to public job without core method callbacks', () => {
      const now = Date.now();
      const internalJob = makeInternalJob({
        id: jobId('simple-job'),
        queue: 'simple-queue',
        data: { name: 'task', value: 42 },
        createdAt: now,
        runAt: now,
        priority: 2,
      });

      const publicJob = toPublicJob({
        job: internalJob,
        name: 'task',
      });

      expect(publicJob.id).toBe('simple-job');
      expect(publicJob.name).toBe('task');
      expect(publicJob.data).toEqual({ value: 42 });
      expect(publicJob.queueName).toBe('simple-queue');
      expect(publicJob.priority).toBe(2);
    });

    test('updateProgress should be no-op in simple job', async () => {
      const internalJob = makeInternalJob();
      const publicJob = toPublicJob({ job: internalJob, name: 'test' });

      // Should not throw
      await publicJob.updateProgress(50);
    });

    test('log should be no-op in simple job', async () => {
      const internalJob = makeInternalJob();
      const publicJob = toPublicJob({ job: internalJob, name: 'test' });

      // Should not throw
      await publicJob.log('test message');
    });

    test('getState should return "unknown" when no callback', async () => {
      const internalJob = makeInternalJob();
      const publicJob = toPublicJob({ job: internalJob, name: 'test' });

      expect(await publicJob.getState()).toBe('unknown');
    });

    test('getState should delegate when callback provided', async () => {
      const internalJob = makeInternalJob();
      const publicJob = toPublicJob({
        job: internalJob,
        name: 'test',
        getState: async () => 'completed',
      });

      expect(await publicJob.getState()).toBe('completed');
    });

    test('toJSON should return proper representation', () => {
      const now = Date.now();
      const internalJob = makeInternalJob({
        id: jobId('json-simple'),
        queue: 'q',
        data: { name: 'n', x: 1 },
        createdAt: now,
        runAt: now,
      });

      const publicJob = toPublicJob({ job: internalJob, name: 'n' });
      const json = publicJob.toJSON();
      expect(json.id).toBe('json-simple');
      expect(json.queueQualifiedName).toBe('bull:q');
    });

    test('asJSON should have stringified values', () => {
      const now = Date.now();
      const internalJob = makeInternalJob({
        id: jobId('raw-simple'),
        data: { name: 'n', x: 1 },
        createdAt: now,
        runAt: now,
        attempts: 2,
      });

      const publicJob = toPublicJob({ job: internalJob, name: 'n' });
      const raw = publicJob.asJSON();
      expect(raw.attemptsMade).toBe('2');
      expect(typeof raw.data).toBe('string');
    });

    test('should include stacktrace when provided', () => {
      const internalJob = makeInternalJob();
      const traces = ['line1', 'line2'];

      const publicJob = toPublicJob({
        job: internalJob,
        name: 'test',
        stacktrace: traces,
      });

      expect(publicJob.stacktrace).toEqual(traces);
    });

    test('discard should be no-op when no callback', () => {
      const internalJob = makeInternalJob();
      const publicJob = toPublicJob({ job: internalJob, name: 'test' });

      // Should not throw
      publicJob.discard();
    });

    test('discard should call callback when provided', () => {
      let discardedId: string | null = null;
      const internalJob = makeInternalJob({ id: jobId('disc-simple') });

      const publicJob = toPublicJob({
        job: internalJob,
        name: 'test',
        discard: (id) => {
          discardedId = id;
        },
      });

      publicJob.discard();
      expect(discardedId).toBe('disc-simple');
    });
  });

  describe('toDlqEntry', () => {
    test('should convert internal DLQ entry to public DLQ entry', () => {
      const now = Date.now();
      const internalJob = makeInternalJob({
        id: jobId('dlq-job'),
        queue: 'failed-queue',
        data: { name: 'failed-task', payload: 'data' },
      });

      const internalEntry: InternalDlqEntry = {
        job: internalJob,
        enteredAt: now,
        reason: 'max_attempts_exceeded' as any,
        error: 'Too many attempts',
        attempts: [
          {
            attempt: 1,
            startedAt: now - 3000,
            failedAt: now - 2000,
            reason: 'explicit_fail' as any,
            error: 'First failure',
            duration: 1000,
          },
          {
            attempt: 2,
            startedAt: now - 1500,
            failedAt: now - 500,
            reason: 'timeout' as any,
            error: 'Timed out',
            duration: 1000,
          },
        ],
        retryCount: 1,
        lastRetryAt: now - 100,
        nextRetryAt: now + 3600000,
        expiresAt: now + 86400000,
      };

      const dlqEntry = toDlqEntry(internalEntry);

      expect(dlqEntry.job.id).toBe('dlq-job');
      expect(dlqEntry.job.name).toBe('failed-task');
      expect(dlqEntry.job.data).toEqual({ payload: 'data' });
      expect(dlqEntry.enteredAt).toBe(now);
      expect(dlqEntry.reason).toBe('max_attempts_exceeded');
      expect(dlqEntry.error).toBe('Too many attempts');
      expect(dlqEntry.attempts).toHaveLength(2);
      expect(dlqEntry.attempts[0].attempt).toBe(1);
      expect(dlqEntry.attempts[0].reason).toBe('explicit_fail');
      expect(dlqEntry.attempts[1].attempt).toBe(2);
      expect(dlqEntry.attempts[1].reason).toBe('timeout');
      expect(dlqEntry.retryCount).toBe(1);
      expect(dlqEntry.lastRetryAt).toBe(now - 100);
      expect(dlqEntry.nextRetryAt).toBe(now + 3600000);
      expect(dlqEntry.expiresAt).toBe(now + 86400000);
    });

    test('should use "default" name when job data has no name', () => {
      const internalJob = makeInternalJob({
        data: { foo: 'bar' },
      });

      const internalEntry: InternalDlqEntry = {
        job: internalJob,
        enteredAt: Date.now(),
        reason: 'unknown' as any,
        error: null,
        attempts: [],
        retryCount: 0,
        lastRetryAt: null,
        nextRetryAt: null,
        expiresAt: null,
      };

      const dlqEntry = toDlqEntry(internalEntry);
      expect(dlqEntry.job.name).toBe('default');
    });

    test('should handle null data in DLQ entry job', () => {
      const internalJob = makeInternalJob({ data: null });

      const internalEntry: InternalDlqEntry = {
        job: internalJob,
        enteredAt: Date.now(),
        reason: 'stalled' as any,
        error: 'No heartbeat',
        attempts: [],
        retryCount: 0,
        lastRetryAt: null,
        nextRetryAt: null,
        expiresAt: null,
      };

      const dlqEntry = toDlqEntry(internalEntry);
      expect(dlqEntry.job.name).toBe('default');
    });

    test('should handle empty attempts array', () => {
      const internalJob = makeInternalJob();

      const internalEntry: InternalDlqEntry = {
        job: internalJob,
        enteredAt: Date.now(),
        reason: 'explicit_fail' as any,
        error: 'test',
        attempts: [],
        retryCount: 0,
        lastRetryAt: null,
        nextRetryAt: null,
        expiresAt: null,
      };

      const dlqEntry = toDlqEntry(internalEntry);
      expect(dlqEntry.attempts).toEqual([]);
    });

    test('should handle null error and retry fields', () => {
      const internalJob = makeInternalJob();

      const internalEntry: InternalDlqEntry = {
        job: internalJob,
        enteredAt: Date.now(),
        reason: 'worker_lost' as any,
        error: null,
        attempts: [],
        retryCount: 0,
        lastRetryAt: null,
        nextRetryAt: null,
        expiresAt: null,
      };

      const dlqEntry = toDlqEntry(internalEntry);
      expect(dlqEntry.error).toBeNull();
      expect(dlqEntry.lastRetryAt).toBeNull();
      expect(dlqEntry.nextRetryAt).toBeNull();
      expect(dlqEntry.expiresAt).toBeNull();
    });
  });
});

// ============================================================================
// events.ts
// ============================================================================

describe('QueueEvents', () => {
  let queueEvents: QueueEvents;

  afterEach(() => {
    if (queueEvents) {
      queueEvents.close();
    }
    shutdownManager();
  });

  describe('constructor and lifecycle', () => {
    test('should create with queue name', () => {
      queueEvents = new QueueEvents('test-events-queue');
      expect(queueEvents.name).toBe('test-events-queue');
    });

    test('should be ready immediately in embedded mode', async () => {
      queueEvents = new QueueEvents('ready-test');
      await expect(queueEvents.waitUntilReady()).resolves.toBeUndefined();
    });

    test('close should stop receiving events', () => {
      queueEvents = new QueueEvents('close-test');
      queueEvents.close();

      // After close, adding a listener and emitting should still work
      // (it's an EventEmitter), but no new internal events should arrive
      let called = false;
      queueEvents.on('waiting', () => {
        called = true;
      });

      // Emit directly to verify close doesn't break EventEmitter
      queueEvents.emit('waiting', { jobId: 'test' });
      expect(called).toBe(true);
    });

    test('disconnect should resolve and close', async () => {
      queueEvents = new QueueEvents('disconnect-test');
      await expect(queueEvents.disconnect()).resolves.toBeUndefined();
    });

    test('double close should not throw', () => {
      queueEvents = new QueueEvents('double-close-test');
      queueEvents.close();
      queueEvents.close(); // Should not throw
    });
  });

  describe('event subscription', () => {
    test('should receive waiting events for matching queue', async () => {
      const queue = new Queue('events-sub-test', { embedded: true });
      queue.obliterate();
      queueEvents = new QueueEvents('events-sub-test');

      const waitingIds: string[] = [];
      queueEvents.on('waiting', ({ jobId }) => {
        waitingIds.push(jobId);
      });

      const job = await queue.add('task', { value: 1 });
      await Bun.sleep(100);

      expect(waitingIds).toContain(job.id);

      queue.close();
    });

    test('should NOT receive events for different queue', async () => {
      const queue = new Queue('events-other-queue', { embedded: true });
      queue.obliterate();
      queueEvents = new QueueEvents('events-different-name');

      let eventReceived = false;
      queueEvents.on('waiting', () => {
        eventReceived = true;
      });

      await queue.add('task', { value: 1 });
      await Bun.sleep(100);

      expect(eventReceived).toBe(false);

      queue.close();
    });

    test('should support multiple listeners on same event', async () => {
      const queue = new Queue('events-multi-listen', { embedded: true });
      queue.obliterate();
      queueEvents = new QueueEvents('events-multi-listen');

      let count1 = 0;
      let count2 = 0;
      queueEvents.on('waiting', () => {
        count1++;
      });
      queueEvents.on('waiting', () => {
        count2++;
      });

      await queue.add('task', { value: 1 });
      await Bun.sleep(100);

      expect(count1).toBeGreaterThanOrEqual(1);
      expect(count2).toBeGreaterThanOrEqual(1);

      queue.close();
    });

    test('once should fire only once', async () => {
      const queue = new Queue('events-once-test', { embedded: true });
      queue.obliterate();
      queueEvents = new QueueEvents('events-once-test');

      let callCount = 0;
      queueEvents.once('waiting', () => {
        callCount++;
      });

      await queue.add('task1', { value: 1 });
      await queue.add('task2', { value: 2 });
      await Bun.sleep(150);

      expect(callCount).toBe(1);

      queue.close();
    });

    test('should emit completed event with return value', async () => {
      const queue = new Queue<{ value: number }>('events-completed-test', { embedded: true });
      queue.obliterate();
      queueEvents = new QueueEvents('events-completed-test');

      let completedValue: unknown = null;
      queueEvents.on('completed', ({ returnvalue }) => {
        completedValue = returnvalue;
      });

      const worker = new Worker(
        'events-completed-test',
        async (job) => job.data.value * 2,
        { embedded: true, autorun: true }
      );

      await queue.add('task', { value: 21 });
      await Bun.sleep(500);

      expect(completedValue).toBe(42);

      await worker.close();
      queue.close();
    });
  });

  describe('emitError', () => {
    test('should emit error events', () => {
      queueEvents = new QueueEvents('error-test');

      let receivedError: Error | null = null;
      queueEvents.on('error', (err) => {
        receivedError = err;
      });

      const testError = new Error('custom error');
      queueEvents.emitError(testError);

      expect(receivedError).toBe(testError);
      expect(receivedError!.message).toBe('custom error');
    });
  });

  describe('event unsubscription', () => {
    test('removeListener should stop receiving events', async () => {
      const queue = new Queue('events-unsub-test', { embedded: true });
      queue.obliterate();
      queueEvents = new QueueEvents('events-unsub-test');

      let callCount = 0;
      const handler = () => {
        callCount++;
      };

      queueEvents.on('waiting', handler);

      await queue.add('task1', { value: 1 });
      await Bun.sleep(100);
      expect(callCount).toBe(1);

      queueEvents.removeListener('waiting', handler);

      await queue.add('task2', { value: 2 });
      await Bun.sleep(100);
      expect(callCount).toBe(1); // Should not have incremented

      queue.close();
    });

    test('removeAllListeners should stop all listeners on event', async () => {
      queueEvents = new QueueEvents('events-remove-all');

      let count1 = 0;
      let count2 = 0;
      queueEvents.on('waiting', () => count1++);
      queueEvents.on('waiting', () => count2++);

      // Verify both listeners fire
      queueEvents.emit('waiting', { jobId: 'test' });
      expect(count1).toBe(1);
      expect(count2).toBe(1);

      queueEvents.removeAllListeners('waiting');

      queueEvents.emit('waiting', { jobId: 'test' });
      expect(count1).toBe(1); // Should not have incremented
      expect(count2).toBe(1); // Should not have incremented
    });
  });
});

// ============================================================================
// queueGroup.ts
// ============================================================================

describe('QueueGroup', () => {
  afterEach(() => {
    shutdownManager();
  });

  describe('constructor', () => {
    test('should add colon suffix to namespace', () => {
      const group = new QueueGroup('billing');
      expect(group.prefix).toBe('billing:');
    });

    test('should not double-add colon when namespace already ends with colon', () => {
      const group = new QueueGroup('billing:');
      expect(group.prefix).toBe('billing:');
    });

    test('should handle empty string namespace', () => {
      const group = new QueueGroup('');
      expect(group.prefix).toBe(':');
    });
  });

  describe('getQueue', () => {
    test('should create queue with prefixed name', async () => {
      const group = new QueueGroup('billing');
      const queue = group.getQueue<{ amount: number }>('invoices');

      // Add a job to verify the queue works
      const job = await queue.add('create-invoice', { amount: 100 });
      expect(job.queueName).toBe('billing:invoices');
      expect(job.data).toEqual({ amount: 100 });

      queue.close();
    });

    test('should pass options to queue', async () => {
      const group = new QueueGroup('test-group');
      const queue = group.getQueue('opts-queue', {
        defaultJobOptions: { priority: 10 },
      });

      const job = await queue.add('task', {});
      expect(job.queueName).toBe('test-group:opts-queue');

      queue.close();
    });
  });

  describe('getWorker', () => {
    test('should create worker with prefixed queue name', async () => {
      const group = new QueueGroup('billing');
      const queue = group.getQueue<{ amount: number }>('payments');
      queue.obliterate();

      let processedJobQueue: string | null = null;
      const worker = group.getWorker<{ amount: number }, boolean>(
        'payments',
        async (job) => {
          processedJobQueue = job.queueName;
          return true;
        },
        { embedded: true, autorun: true }
      );

      await queue.add('pay', { amount: 50 });
      await Bun.sleep(500);

      expect(processedJobQueue).toBe('billing:payments');

      await worker.close();
      queue.close();
    });
  });

  describe('listQueues', () => {
    test('should list only queues in this group', async () => {
      const group = new QueueGroup('list-test');
      const q1 = group.getQueue('alpha');
      const q2 = group.getQueue('beta');
      const outsideQueue = new Queue('outside-queue', { embedded: true });

      // Add jobs to register queues
      await q1.add('task', {});
      await q2.add('task', {});
      await outsideQueue.add('task', {});

      const queues = group.listQueues();
      expect(queues).toContain('alpha');
      expect(queues).toContain('beta');
      expect(queues).not.toContain('outside-queue');

      q1.close();
      q2.close();
      outsideQueue.close();
    });

    test('should return empty array when no queues exist', () => {
      const group = new QueueGroup('empty-group-xyz');
      const queues = group.listQueues();
      expect(queues).toEqual([]);
    });

    test('should strip prefix from queue names', async () => {
      const group = new QueueGroup('stripped');
      const queue = group.getQueue('myqueue');
      await queue.add('task', {});

      const queues = group.listQueues();
      expect(queues).toContain('myqueue');
      expect(queues).not.toContain('stripped:myqueue');

      queue.close();
    });
  });

  describe('pauseAll / resumeAll', () => {
    test('should pause and resume all queues in group', async () => {
      const group = new QueueGroup('pause-test');
      const q1 = group.getQueue('queue1');
      const q2 = group.getQueue('queue2');

      // Add jobs to register queues
      await q1.add('task', {});
      await q2.add('task', {});

      // Pause all should not throw
      group.pauseAll();

      // Resume all should not throw
      group.resumeAll();

      q1.close();
      q2.close();
    });
  });

  describe('drainAll', () => {
    test('should drain all queues in group', async () => {
      const group = new QueueGroup('drain-test');
      const q1 = group.getQueue('queue1');
      const q2 = group.getQueue('queue2');

      await q1.add('task', { v: 1 });
      await q1.add('task', { v: 2 });
      await q2.add('task', { v: 3 });

      group.drainAll();

      const counts1 = await q1.getJobCounts();
      const counts2 = await q2.getJobCounts();
      expect(counts1.waiting).toBe(0);
      expect(counts2.waiting).toBe(0);

      q1.close();
      q2.close();
    });
  });

  describe('obliterateAll', () => {
    test('should obliterate all queues in group', async () => {
      const group = new QueueGroup('obliterate-test');
      const q1 = group.getQueue('queue1');
      const q2 = group.getQueue('queue2');

      await q1.add('task', { v: 1 });
      await q2.add('task', { v: 2 });

      group.obliterateAll();

      const counts1 = await q1.getJobCounts();
      const counts2 = await q2.getJobCounts();
      expect(counts1.waiting).toBe(0);
      expect(counts2.waiting).toBe(0);

      q1.close();
      q2.close();
    });
  });
});
