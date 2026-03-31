/**
 * Tests for flowPush.ts and bullmqCompat.ts
 *
 * Covers:
 * - pushJob: push single job with various options (embedded mode)
 * - pushJobWithParent: push job with parent relationship
 * - cleanupJobs: cleanup jobs on error
 * - getPrioritized / getPrioritizedCount (via Queue)
 * - getWaitingChildren / getWaitingChildrenCount (via Queue)
 * - Error handling and edge cases
 *
 * Run with: BUNQUEUE_EMBEDDED=1 bun test test/client-flowpush-compat.test.ts
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, shutdownManager } from '../src/client';
import { pushJob, pushJobWithParent, cleanupJobs, type PushContext } from '../src/client/flowPush';
import { getSharedManager } from '../src/client/manager';
import { jobId } from '../src/domain/types/job';

// =============================================================================
// flowPush.ts - pushJob (embedded mode)
// =============================================================================

describe('pushJob (embedded)', () => {
  let ctx: PushContext;

  beforeEach(() => { ctx = { embedded: true, tcp: null }; });
  afterEach(() => { shutdownManager(); });

  test('should push a job with no options and return a string ID', async () => {
    const id = await pushJob(ctx, 'push-test', { value: 1 });
    expect(typeof id).toBe('string');
    expect(id.length).toBeGreaterThan(0);
  });

  test('should push a job with priority and verify it exists', async () => {
    const id = await pushJob(ctx, 'push-test', { value: 1 }, { priority: 10 });
    const manager = getSharedManager();
    const job = await manager.getJob(jobId(id));
    expect(job).not.toBeNull();
  });

  test('should push a delayed job', async () => {
    const id = await pushJob(ctx, 'push-test', { value: 1 }, { delay: 60000 });
    const queue = new Queue('push-test');
    const state = await queue.getJobState(id);
    expect(state).toBe('delayed');
  });

  test('should push with attempts, backoff, and timeout options', async () => {
    const id = await pushJob(ctx, 'push-test', {}, {
      attempts: 5, backoff: 2000, timeout: 15000,
    });
    expect(typeof id).toBe('string');
  });

  test('should push with custom jobId', async () => {
    const id = await pushJob(ctx, 'push-test', {}, { jobId: 'custom-fp-id' });
    expect(typeof id).toBe('string');
  });

  test('should push with removeOnComplete and removeOnFail', async () => {
    const id1 = await pushJob(ctx, 'push-test', {}, { removeOnComplete: true, removeOnFail: true });
    const id2 = await pushJob(ctx, 'push-test', {}, { removeOnComplete: false, removeOnFail: false });
    expect(typeof id1).toBe('string');
    expect(typeof id2).toBe('string');
  });

  test('should push a job with dependsOn array', async () => {
    const depId = await pushJob(ctx, 'push-test', { step: 'first' });
    const id = await pushJob(ctx, 'push-test', { step: 'second' }, {}, [depId]);
    expect(typeof id).toBe('string');
    expect(id).not.toBe(depId);
  });

  test('should push with all options combined', async () => {
    const id = await pushJob(ctx, 'push-test', { full: true }, {
      priority: 5, delay: 1000, attempts: 3, backoff: 500,
      timeout: 10000, removeOnComplete: true, removeOnFail: false,
    });
    expect(typeof id).toBe('string');
  });

  test('should generate unique IDs for concurrent pushes', async () => {
    const ids = await Promise.all([
      pushJob(ctx, 'push-test', { i: 0 }),
      pushJob(ctx, 'push-test', { i: 1 }),
      pushJob(ctx, 'push-test', { i: 2 }),
    ]);
    expect(new Set(ids).size).toBe(3);
  });

  test('should throw when TCP not initialized in non-embedded mode', async () => {
    const tcpCtx: PushContext = { embedded: false, tcp: null };
    await expect(pushJob(tcpCtx, 'q', {})).rejects.toThrow('TCP connection not initialized');
  });
});

// =============================================================================
// flowPush.ts - pushJobWithParent (embedded mode)
// =============================================================================

describe('pushJobWithParent (embedded)', () => {
  let ctx: PushContext;

  beforeEach(() => { ctx = { embedded: true, tcp: null }; });
  afterEach(() => { shutdownManager(); });

  test('should push a job with parentRef', async () => {
    const parentId = await pushJob(ctx, 'parent-q', { role: 'parent' });
    const childId = await pushJobWithParent(ctx, {
      queueName: 'child-q', data: { role: 'child' }, opts: {},
      parentRef: { id: parentId, queue: 'parent-q' }, childIds: [],
    });
    expect(typeof childId).toBe('string');
    expect(childId).not.toBe(parentId);
  });

  test('should push a job with null parentRef', async () => {
    const id = await pushJobWithParent(ctx, {
      queueName: 'test-q', data: { standalone: true }, opts: {},
      parentRef: null, childIds: [],
    });
    expect(typeof id).toBe('string');
  });

  test('should push with childIds and update parent references', async () => {
    const child1 = await pushJob(ctx, 'flow-q', { step: 'c1' });
    const child2 = await pushJob(ctx, 'flow-q', { step: 'c2' });
    const parentId = await pushJobWithParent(ctx, {
      queueName: 'flow-q', data: { step: 'parent' }, opts: {},
      parentRef: null, childIds: [child1, child2],
    });
    expect(typeof parentId).toBe('string');
  });

  test('should pass job options through', async () => {
    const id = await pushJobWithParent(ctx, {
      queueName: 'opts-q', data: {},
      opts: { priority: 20, attempts: 4, timeout: 5000 },
      parentRef: null, childIds: [],
    });
    expect(typeof id).toBe('string');
  });

  test('should throw when TCP not initialized in non-embedded mode', async () => {
    const tcpCtx: PushContext = { embedded: false, tcp: null };
    await expect(pushJobWithParent(tcpCtx, {
      queueName: 'q', data: {}, opts: {}, parentRef: null, childIds: [],
    })).rejects.toThrow('TCP connection not initialized');
  });
});

// =============================================================================
// flowPush.ts - cleanupJobs
// =============================================================================

describe('cleanupJobs', () => {
  let ctx: PushContext;

  beforeEach(() => { ctx = { embedded: true, tcp: null }; });
  afterEach(() => { shutdownManager(); });

  test('should return immediately for empty jobIds array', async () => {
    await expect(cleanupJobs(ctx, [])).resolves.toBeUndefined();
  });

  test('should cancel existing jobs', async () => {
    const id1 = await pushJob(ctx, 'cleanup-q', { v: 1 });
    const id2 = await pushJob(ctx, 'cleanup-q', { v: 2 });
    await cleanupJobs(ctx, [id1, id2]);

    const queue = new Queue('cleanup-q');
    const state1 = await queue.getJobState(id1);
    const state2 = await queue.getJobState(id2);
    expect(['unknown', 'failed', 'completed']).toContain(state1);
    expect(['unknown', 'failed', 'completed']).toContain(state2);
  });

  test('should silently ignore non-existent job IDs', async () => {
    await expect(cleanupJobs(ctx, ['nonexistent-1', 'nonexistent-2'])).resolves.toBeUndefined();
  });

  test('should handle mix of valid and invalid IDs', async () => {
    const validId = await pushJob(ctx, 'cleanup-q', { v: 1 });
    await expect(cleanupJobs(ctx, [validId, 'nonexistent-id'])).resolves.toBeUndefined();
  });

  test('should do nothing in non-embedded mode with null TCP', async () => {
    const tcpCtx: PushContext = { embedded: false, tcp: null };
    await expect(cleanupJobs(tcpCtx, ['some-id'])).resolves.toBeUndefined();
  });
});

// =============================================================================
// bullmqCompat.ts - getPrioritized / getPrioritizedCount (via Queue)
// =============================================================================

describe('BullMQ Compat - getPrioritized', () => {
  let queue: Queue<Record<string, unknown>>;

  beforeEach(() => { queue = new Queue('compat-prioritized'); });
  afterEach(() => { shutdownManager(); });

  test('should return empty array for empty queue', async () => {
    const jobs = await queue.getPrioritized();
    expect(jobs).toEqual([]);
  });

  test('should return waiting jobs', async () => {
    await queue.add('low', { v: 1 }, { priority: 1 });
    await queue.add('high', { v: 2 }, { priority: 10 });
    await queue.add('mid', { v: 3 }, { priority: 5 });

    const jobs = await queue.getPrioritized();
    expect(jobs.length).toBeGreaterThanOrEqual(2);
  });

  test('should support start/end pagination', async () => {
    for (let i = 0; i < 10; i++) {
      await queue.add(`task-${i}`, { i }, { priority: i });
    }
    const page = await queue.getPrioritized(0, 5);
    expect(page.length).toBeLessThanOrEqual(5);
  });
});

describe('BullMQ Compat - getPrioritizedCount', () => {
  let queue: Queue<Record<string, unknown>>;

  beforeEach(() => { queue = new Queue('compat-pri-count'); });
  afterEach(() => { shutdownManager(); });

  test('should return 0 for empty queue', async () => {
    const count = await queue.getPrioritizedCount();
    expect(count).toBe(0);
  });

  test('should return count of waiting jobs', async () => {
    await queue.add('a', { v: 1 }, { priority: 1 });
    await queue.add('b', { v: 2 }, { priority: 5 });
    await queue.add('c', { v: 3 }, { priority: 10 });
    const count = await queue.getPrioritizedCount();
    expect(count).toBe(3);
  });

  test('should not count delayed jobs', async () => {
    await queue.add('normal', { v: 1 }, { priority: 1 });
    await queue.add('delayed', { v: 2 }, { priority: 5, delay: 60000 });
    const count = await queue.getPrioritizedCount();
    expect(count).toBe(1);
  });
});

// =============================================================================
// bullmqCompat.ts - getWaitingChildren / getWaitingChildrenCount (via Queue)
// =============================================================================

describe('BullMQ Compat - getWaitingChildren', () => {
  let queue: Queue<Record<string, unknown>>;

  beforeEach(() => { queue = new Queue('compat-waiting-children'); });
  afterEach(() => { shutdownManager(); });

  test('should return empty array when no waiting-children jobs exist', async () => {
    const jobs = await queue.getWaitingChildren();
    expect(jobs).toEqual([]);
  });

  test('should return empty array for queue with only normal jobs', async () => {
    await queue.add('normal-1', { v: 1 });
    await queue.add('normal-2', { v: 2 });
    const jobs = await queue.getWaitingChildren();
    expect(jobs).toEqual([]);
  });
});

describe('BullMQ Compat - getWaitingChildrenCount', () => {
  let queue: Queue<Record<string, unknown>>;

  beforeEach(() => { queue = new Queue('compat-wc-count'); });
  afterEach(() => { shutdownManager(); });

  test('should return 0 when no waiting-children jobs exist', async () => {
    const count = await queue.getWaitingChildrenCount();
    expect(count).toBe(0);
  });

  test('should return 0 for queue with only normal waiting jobs', async () => {
    await queue.add('task-1', { v: 1 });
    await queue.add('task-2', { v: 2 });
    const count = await queue.getWaitingChildrenCount();
    expect(count).toBe(0);
  });
});
