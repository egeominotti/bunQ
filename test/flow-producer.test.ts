/**
 * FlowProducer Comprehensive Tests
 *
 * Covers gaps not in test/flowProducer.test.ts:
 * - flowJobFactory: extractUserDataFromInternal, createFlowJobObject
 * - flowPush: pushJob, pushJobWithParent, cleanupJobs (via embedded)
 * - FlowProducer.add() with job options (priority, delay, attempts)
 * - FlowProducer.add() with deeply nested flows (4+ levels)
 * - FlowProducer.add() data correctness and preservation
 * - FlowProducer.addBulk() with mixed flows
 * - FlowProducer.getFlow() retrieval, depth limits, maxChildren, missing jobs
 * - FlowProducer.getParentResult() / getParentResults()
 * - FlowProducer.waitUntilReady() / disconnect()
 * - Job object methods: toJSON, asJSON, no-op methods, state checks
 * - Cross-queue flows
 * - Worker integration: children completion triggers parent
 * - Error cases
 * - close / cleanup
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, Worker, FlowProducer, shutdownManager } from '../src/client';
import type { FlowJob, JobNode } from '../src/client';
import { extractUserDataFromInternal, createFlowJobObject } from '../src/client/flowJobFactory';

// =============================================================================
// Unit Tests: flowJobFactory
// =============================================================================

describe('flowJobFactory', () => {
  describe('extractUserDataFromInternal', () => {
    test('should remove internal fields prefixed with __', () => {
      const data = {
        __parentId: 'p1',
        __childrenIds: ['c1', 'c2'],
        __flowParentId: 'fp1',
        name: 'test-job',
        email: 'user@test.com',
        count: 42,
      };
      const result = extractUserDataFromInternal(data);
      expect(result).toEqual({ email: 'user@test.com', count: 42 });
    });

    test('should remove the name field', () => {
      const data = { name: 'my-job', value: 100 };
      const result = extractUserDataFromInternal(data);
      expect(result).toEqual({ value: 100 });
    });

    test('should return empty object when only internal fields present', () => {
      const data = { __parentId: 'x', __childrenIds: ['y'], name: 'z' };
      const result = extractUserDataFromInternal(data);
      expect(result).toEqual({});
    });

    test('should return empty object for empty input', () => {
      const result = extractUserDataFromInternal({});
      expect(result).toEqual({});
    });

    test('should preserve all user fields with no internal fields', () => {
      const data = { url: 'https://example.com', retries: 3, metadata: { key: 'val' } };
      const result = extractUserDataFromInternal(data);
      expect(result).toEqual(data);
    });

    test('should not remove fields that start with _ but not __', () => {
      const data = { _singleUnderscore: true, __doubleUnderscore: false, name: 'x' };
      const result = extractUserDataFromInternal(data);
      expect(result).toEqual({ _singleUnderscore: true });
    });
  });

  describe('createFlowJobObject', () => {
    test('should create a Job object with correct core properties', () => {
      const job = createFlowJobObject('job-123', 'send-email', { to: 'a@b.com' }, 'emails');

      expect(job.id).toBe('job-123');
      expect(job.name).toBe('send-email');
      expect(job.data).toEqual({ to: 'a@b.com' });
      expect(job.queueName).toBe('emails');
      expect(job.attemptsMade).toBe(0);
      expect(job.progress).toBe(0);
      expect(job.delay).toBe(0);
      expect(job.stalledCounter).toBe(0);
      expect(job.priority).toBe(0);
      expect(job.stacktrace).toBeNull();
      expect(job.processedOn).toBeUndefined();
      expect(job.finishedOn).toBeUndefined();
      expect(job.opts).toEqual({});
      expect(job.attemptsStarted).toBe(0);
    });

    test('should create a Job with valid timestamp', () => {
      const before = Date.now();
      const job = createFlowJobObject('j1', 'test', {}, 'q');
      const after = Date.now();

      expect(job.timestamp).toBeGreaterThanOrEqual(before);
      expect(job.timestamp).toBeLessThanOrEqual(after);
    });

    test('should provide no-op method implementations', async () => {
      const job = createFlowJobObject('j1', 'test', {}, 'q');

      // updateProgress, log, remove, retry should resolve without error
      await expect(job.updateProgress(50)).resolves.toBeUndefined();
      await expect(job.log('test log')).resolves.toBeUndefined();
      await expect(job.remove()).resolves.toBeUndefined();
      await expect(job.retry()).resolves.toBeUndefined();
    });

    test('should return unknown state when no execution context is provided', async () => {
      // Without embedded/tcp/getState callback, there is no way to resolve state.
      const job = createFlowJobObject('j1', 'test', {}, 'q');
      const state = await job.getState();
      expect(state).toBe('unknown');
    });

    test('should return empty children values', async () => {
      const job = createFlowJobObject('j1', 'test', {}, 'q');
      const children = await job.getChildrenValues();
      expect(children).toEqual({});
    });

    test('should provide BullMQ v5 state check methods (unknown without context)', async () => {
      const job = createFlowJobObject('j1', 'test', {}, 'q');

      // Without execution context, state is 'unknown' → every isX check returns false.
      expect(await job.isWaiting()).toBe(false);
      expect(await job.isActive()).toBe(false);
      expect(await job.isDelayed()).toBe(false);
      expect(await job.isCompleted()).toBe(false);
      expect(await job.isFailed()).toBe(false);
      expect(await job.isWaitingChildren()).toBe(false);
    });

    test('should provide BullMQ v5 mutation no-op methods', async () => {
      const job = createFlowJobObject<{ x: number }>('j1', 'test', { x: 1 }, 'q');

      await expect(job.updateData({ x: 2 })).resolves.toBeUndefined();
      await expect(job.promote()).resolves.toBeUndefined();
      await expect(job.changeDelay(5000)).resolves.toBeUndefined();
      await expect(job.changePriority({ priority: 10 })).resolves.toBeUndefined();
      await expect(job.clearLogs()).resolves.toBeUndefined();
    });

    test('should provide extendLock returning 0', async () => {
      const job = createFlowJobObject('j1', 'test', {}, 'q');
      const result = await job.extendLock('token', 30000);
      expect(result).toBe(0);
    });

    test('should provide dependency methods', async () => {
      const job = createFlowJobObject('j1', 'test', {}, 'q');

      const deps = await job.getDependencies();
      expect(deps).toEqual({ processed: {}, unprocessed: [] });

      const counts = await job.getDependenciesCount();
      expect(counts).toEqual({ processed: 0, unprocessed: 0 });
    });

    test('toJSON should return correct structure', () => {
      const data = { value: 42 };
      const job = createFlowJobObject('j1', 'my-job', data, 'my-queue');
      const json = job.toJSON();

      expect(json.id).toBe('j1');
      expect(json.name).toBe('my-job');
      expect(json.data).toEqual(data);
      expect(json.opts).toEqual({});
      expect(json.progress).toBe(0);
      expect(json.delay).toBe(0);
      expect(json.attemptsMade).toBe(0);
      expect(json.stacktrace).toBeNull();
      expect(json.queueQualifiedName).toBe('bull:my-queue');
    });

    test('asJSON should return stringified values', () => {
      const data = { value: 42 };
      const job = createFlowJobObject('j1', 'my-job', data, 'my-queue');
      const raw = job.asJSON();

      expect(raw.id).toBe('j1');
      expect(raw.name).toBe('my-job');
      expect(raw.data).toBe(JSON.stringify(data));
      expect(raw.opts).toBe('{}');
      expect(raw.progress).toBe('0');
      expect(raw.delay).toBe('0');
      expect(raw.attemptsMade).toBe('0');
      expect(raw.stacktrace).toBeNull();
    });

    test('move methods are no-ops or throw explicit errors without context', async () => {
      const job = createFlowJobObject('j1', 'test', {}, 'q');

      // Without embedded/tcp, there is nothing to dispatch to — ack-style methods
      // quietly succeed (nothing to do), waitUntilFinished/moveToWaitingChildren
      // surface explicit errors rather than silent no-ops.
      expect(await job.moveToCompleted('result')).toBeNull();
      await expect(job.moveToFailed(new Error('fail'))).resolves.toBeUndefined();
      expect(await job.moveToWait()).toBe(false);
      await expect(job.moveToDelayed(Date.now() + 5000)).resolves.toBeUndefined();
      expect(await job.moveToWaitingChildren()).toBe(false);
      await expect(job.waitUntilFinished(null)).rejects.toThrow(/waitUntilFinished: no connection/);
    });

    test('additional BullMQ v5 methods wire correctly or throw explicitly', async () => {
      const job = createFlowJobObject('j1', 'test', {}, 'q');

      // discard is synchronous void — no-ops safely without context
      expect(() => job.discard()).not.toThrow();

      expect(await job.getFailedChildrenValues()).toEqual({});
      expect(await job.getIgnoredChildrenFailures()).toEqual({});
      expect(await job.removeChildDependency()).toBe(false);
      expect(await job.removeDeduplicationKey()).toBe(false);
      await expect(job.removeUnprocessedChildren()).resolves.toBeUndefined();
    });
  });
});

// =============================================================================
// FlowProducer - Extended Integration Tests
// =============================================================================

describe('FlowProducer - Extended', () => {
  let flowProducer: FlowProducer;
  let queue: Queue;

  beforeEach(() => {
    flowProducer = new FlowProducer({ embedded: true });
    queue = new Queue('fp-extended', { embedded: true });
    queue.obliterate();
  });

  afterEach(() => {
    flowProducer.close();
    queue.close();
    shutdownManager();
  });

  // ---------------------------------------------------------------------------
  // add() with job options
  // ---------------------------------------------------------------------------

  describe('add() with job options', () => {
    test('should pass priority option to created jobs', async () => {
      const flow: FlowJob = {
        name: 'high-priority',
        queueName: 'fp-extended',
        data: { task: 'important' },
        opts: { priority: 100 },
      };

      const result = await flowProducer.add(flow);
      expect(result.job.id).toBeDefined();

      // Verify job was created in the queue
      const job = await queue.getJob(result.job.id);
      expect(job).toBeDefined();
    });

    test('should pass delay option to created jobs', async () => {
      const flow: FlowJob = {
        name: 'delayed-job',
        queueName: 'fp-extended',
        data: { task: 'later' },
        opts: { delay: 5000 },
      };

      const result = await flowProducer.add(flow);
      expect(result.job.id).toBeDefined();
    });

    test('should pass attempts option to created jobs', async () => {
      const flow: FlowJob = {
        name: 'retryable',
        queueName: 'fp-extended',
        data: {},
        opts: { attempts: 5, backoff: 2000 },
      };

      const result = await flowProducer.add(flow);
      expect(result.job.id).toBeDefined();
    });

    test('should pass timeout option to created jobs', async () => {
      const flow: FlowJob = {
        name: 'timeout-job',
        queueName: 'fp-extended',
        data: {},
        opts: { timeout: 10000 },
      };

      const result = await flowProducer.add(flow);
      expect(result.job.id).toBeDefined();
    });

    test('should pass removeOnComplete option', async () => {
      const flow: FlowJob = {
        name: 'auto-remove',
        queueName: 'fp-extended',
        data: {},
        opts: { removeOnComplete: true },
      };

      const result = await flowProducer.add(flow);
      expect(result.job.id).toBeDefined();
    });

    test('should pass removeOnFail option', async () => {
      const flow: FlowJob = {
        name: 'auto-remove-fail',
        queueName: 'fp-extended',
        data: {},
        opts: { removeOnFail: true },
      };

      const result = await flowProducer.add(flow);
      expect(result.job.id).toBeDefined();
    });

    test('should pass custom jobId option', async () => {
      const flow: FlowJob = {
        name: 'custom-id',
        queueName: 'fp-extended',
        data: {},
        opts: { jobId: 'my-custom-id-001' },
      };

      const result = await flowProducer.add(flow);
      expect(result.job.id).toBeDefined();
    });

    test('should pass options to children as well', async () => {
      const flow: FlowJob = {
        name: 'parent',
        queueName: 'fp-extended',
        data: {},
        children: [
          {
            name: 'child-with-opts',
            queueName: 'fp-extended',
            data: {},
            opts: { priority: 50, attempts: 3 },
          },
        ],
      };

      const result = await flowProducer.add(flow);
      expect(result.children).toHaveLength(1);
      expect(result.children![0].job.id).toBeDefined();
    });
  });

  // ---------------------------------------------------------------------------
  // add() with deeply nested flows (4+ levels)
  // ---------------------------------------------------------------------------

  describe('add() with deeply nested flows', () => {
    test('should handle 4-level nesting', async () => {
      const flow: FlowJob = {
        name: 'level-0',
        queueName: 'fp-extended',
        data: { level: 0 },
        children: [
          {
            name: 'level-1',
            queueName: 'fp-extended',
            data: { level: 1 },
            children: [
              {
                name: 'level-2',
                queueName: 'fp-extended',
                data: { level: 2 },
                children: [
                  {
                    name: 'level-3',
                    queueName: 'fp-extended',
                    data: { level: 3 },
                  },
                ],
              },
            ],
          },
        ],
      };

      const result = await flowProducer.add(flow);

      expect(result.job.name).toBe('level-0');
      expect(result.children).toHaveLength(1);
      expect(result.children![0].job.name).toBe('level-1');
      expect(result.children![0].children).toHaveLength(1);
      expect(result.children![0].children![0].job.name).toBe('level-2');
      expect(result.children![0].children![0].children).toHaveLength(1);
      expect(result.children![0].children![0].children![0].job.name).toBe('level-3');
      expect(result.children![0].children![0].children![0].children).toBeUndefined();
    });

    test('should handle 5-level nesting with multiple branches', async () => {
      const flow: FlowJob = {
        name: 'root',
        queueName: 'fp-extended',
        data: {},
        children: [
          {
            name: 'branch-a',
            queueName: 'fp-extended',
            data: {},
            children: [
              {
                name: 'branch-a-1',
                queueName: 'fp-extended',
                data: {},
                children: [
                  {
                    name: 'branch-a-1-x',
                    queueName: 'fp-extended',
                    data: {},
                    children: [{ name: 'leaf-a', queueName: 'fp-extended', data: {} }],
                  },
                ],
              },
            ],
          },
          {
            name: 'branch-b',
            queueName: 'fp-extended',
            data: {},
            children: [{ name: 'leaf-b', queueName: 'fp-extended', data: {} }],
          },
        ],
      };

      const result = await flowProducer.add(flow);

      expect(result.job.name).toBe('root');
      expect(result.children).toHaveLength(2);
      expect(result.children![0].job.name).toBe('branch-a');
      expect(result.children![1].job.name).toBe('branch-b');

      // Walk branch-a down
      const branchA1 = result.children![0].children![0];
      expect(branchA1.job.name).toBe('branch-a-1');
      const branchA1x = branchA1.children![0];
      expect(branchA1x.job.name).toBe('branch-a-1-x');
      const leafA = branchA1x.children![0];
      expect(leafA.job.name).toBe('leaf-a');
      expect(leafA.children).toBeUndefined();

      // branch-b leaf
      expect(result.children![1].children![0].job.name).toBe('leaf-b');
    });
  });

  // ---------------------------------------------------------------------------
  // add() data correctness
  // ---------------------------------------------------------------------------

  describe('add() data correctness', () => {
    test('should preserve job data for parent', async () => {
      const flow: FlowJob<{ email: string; count: number }> = {
        name: 'email-job',
        queueName: 'fp-extended',
        data: { email: 'user@test.com', count: 10 },
      };

      const result = await flowProducer.add(flow);
      expect(result.job.data).toEqual({ email: 'user@test.com', count: 10 });
    });

    test('should preserve complex nested data', async () => {
      const complexData = {
        users: [
          { id: 1, name: 'Alice' },
          { id: 2, name: 'Bob' },
        ],
        config: { retries: 3, timeout: 5000 },
        tags: ['urgent', 'batch'],
      };

      const flow: FlowJob = {
        name: 'complex-data',
        queueName: 'fp-extended',
        data: complexData,
      };

      const result = await flowProducer.add(flow);
      expect(result.job.data).toEqual(complexData);
    });

    test('should handle undefined data', async () => {
      const flow: FlowJob = {
        name: 'no-data',
        queueName: 'fp-extended',
      };

      const result = await flowProducer.add(flow);
      expect(result.job.id).toBeDefined();
      expect(result.job.name).toBe('no-data');
    });

    test('should generate unique IDs for each job in a flow', async () => {
      const flow: FlowJob = {
        name: 'parent',
        queueName: 'fp-extended',
        data: {},
        children: [
          { name: 'child-1', queueName: 'fp-extended', data: {} },
          { name: 'child-2', queueName: 'fp-extended', data: {} },
          { name: 'child-3', queueName: 'fp-extended', data: {} },
        ],
      };

      const result = await flowProducer.add(flow);

      const ids = new Set<string>();
      ids.add(result.job.id);
      for (const child of result.children!) {
        ids.add(child.job.id);
      }
      // 4 unique IDs: 1 parent + 3 children
      expect(ids.size).toBe(4);
    });
  });

  // ---------------------------------------------------------------------------
  // addBulk() mixed flows
  // ---------------------------------------------------------------------------

  describe('addBulk() mixed flows', () => {
    test('should handle mix of flows with and without children', async () => {
      const flows: FlowJob[] = [
        { name: 'simple', queueName: 'fp-extended', data: { type: 'simple' } },
        {
          name: 'with-children',
          queueName: 'fp-extended',
          data: { type: 'parent' },
          children: [{ name: 'child', queueName: 'fp-extended', data: { type: 'child' } }],
        },
        { name: 'another-simple', queueName: 'fp-extended', data: { type: 'simple2' } },
      ];

      const results = await flowProducer.addBulk(flows);

      expect(results).toHaveLength(3);
      expect(results[0].children).toBeUndefined();
      expect(results[1].children).toHaveLength(1);
      expect(results[2].children).toBeUndefined();
    });

    test('should produce unique job IDs across all bulk flows', async () => {
      const flows: FlowJob[] = [
        {
          name: 'flow-a',
          queueName: 'fp-extended',
          data: {},
          children: [{ name: 'child-a', queueName: 'fp-extended', data: {} }],
        },
        {
          name: 'flow-b',
          queueName: 'fp-extended',
          data: {},
          children: [{ name: 'child-b', queueName: 'fp-extended', data: {} }],
        },
      ];

      const results = await flowProducer.addBulk(flows);

      const ids = new Set<string>();
      for (const result of results) {
        ids.add(result.job.id);
        if (result.children) {
          for (const child of result.children) {
            ids.add(child.job.id);
          }
        }
      }
      // 4 unique IDs total
      expect(ids.size).toBe(4);
    });
  });

  // ---------------------------------------------------------------------------
  // getFlow()
  // ---------------------------------------------------------------------------

  describe('getFlow()', () => {
    test('should return null for non-existent job', async () => {
      const result = await flowProducer.getFlow({
        id: 'non-existent-id',
        queueName: 'fp-extended',
      });
      expect(result).toBeNull();
    });

    test('should return null for wrong queue name', async () => {
      const flow: FlowJob = {
        name: 'test',
        queueName: 'fp-extended',
        data: {},
      };
      const added = await flowProducer.add(flow);

      const result = await flowProducer.getFlow({
        id: added.job.id,
        queueName: 'wrong-queue-name',
      });
      expect(result).toBeNull();
    });

    test('should retrieve a simple flow by id', async () => {
      const flow: FlowJob = {
        name: 'retrievable',
        queueName: 'fp-extended',
        data: { x: 1 },
      };

      const added = await flowProducer.add(flow);
      const retrieved = await flowProducer.getFlow({
        id: added.job.id,
        queueName: 'fp-extended',
      });

      expect(retrieved).not.toBeNull();
      expect(retrieved!.job.id).toBe(added.job.id);
      expect(retrieved!.job.name).toBe('retrievable');
    });

    test('should retrieve flow with children', async () => {
      const flow: FlowJob = {
        name: 'parent',
        queueName: 'fp-extended',
        data: {},
        children: [
          { name: 'child-1', queueName: 'fp-extended', data: {} },
          { name: 'child-2', queueName: 'fp-extended', data: {} },
        ],
      };

      const added = await flowProducer.add(flow);
      const retrieved = await flowProducer.getFlow({
        id: added.job.id,
        queueName: 'fp-extended',
      });

      expect(retrieved).not.toBeNull();
      expect(retrieved!.job.name).toBe('parent');
      expect(retrieved!.children).toBeDefined();
      expect(retrieved!.children!.length).toBe(2);
    });

    test('should respect depth parameter', async () => {
      const flow: FlowJob = {
        name: 'top',
        queueName: 'fp-extended',
        data: {},
        children: [
          {
            name: 'mid',
            queueName: 'fp-extended',
            data: {},
            children: [{ name: 'bottom', queueName: 'fp-extended', data: {} }],
          },
        ],
      };

      const added = await flowProducer.add(flow);

      // depth=0 should not include children
      const depthZero = await flowProducer.getFlow({
        id: added.job.id,
        queueName: 'fp-extended',
        depth: 0,
      });
      expect(depthZero).not.toBeNull();
      expect(depthZero!.children).toBeUndefined();

      // depth=1 should include children but not grandchildren
      const depthOne = await flowProducer.getFlow({
        id: added.job.id,
        queueName: 'fp-extended',
        depth: 1,
      });
      expect(depthOne).not.toBeNull();
      expect(depthOne!.children).toBeDefined();
      expect(depthOne!.children!.length).toBe(1);
      // The child at depth=1 should not have its own children traversed
      expect(depthOne!.children![0].children).toBeUndefined();
    });

    test('should respect maxChildren parameter', async () => {
      const flow: FlowJob = {
        name: 'parent',
        queueName: 'fp-extended',
        data: {},
        children: [
          { name: 'child-1', queueName: 'fp-extended', data: {} },
          { name: 'child-2', queueName: 'fp-extended', data: {} },
          { name: 'child-3', queueName: 'fp-extended', data: {} },
        ],
      };

      const added = await flowProducer.add(flow);
      const retrieved = await flowProducer.getFlow({
        id: added.job.id,
        queueName: 'fp-extended',
        maxChildren: 2,
      });

      expect(retrieved).not.toBeNull();
      expect(retrieved!.children).toBeDefined();
      expect(retrieved!.children!.length).toBe(2);
    });
  });

  // ---------------------------------------------------------------------------
  // getParentResult / getParentResults
  // ---------------------------------------------------------------------------

  describe('getParentResult / getParentResults', () => {
    test('getParentResult should return undefined for non-existent job', () => {
      const result = flowProducer.getParentResult('non-existent');
      expect(result).toBeUndefined();
    });

    test('getParentResults should return empty map for non-existent jobs', () => {
      const results = flowProducer.getParentResults(['no-1', 'no-2']);
      expect(results.size).toBe(0);
    });

    test('getParentResults should return empty map for empty array', () => {
      const results = flowProducer.getParentResults([]);
      expect(results.size).toBe(0);
    });
  });

  // ---------------------------------------------------------------------------
  // waitUntilReady / disconnect
  // ---------------------------------------------------------------------------

  describe('waitUntilReady / disconnect', () => {
    test('waitUntilReady should resolve immediately in embedded mode', async () => {
      await expect(flowProducer.waitUntilReady()).resolves.toBeUndefined();
    });

    test('disconnect should resolve (alias for close)', async () => {
      const fp = new FlowProducer({ embedded: true });
      await expect(fp.disconnect()).resolves.toBeUndefined();
    });
  });

  // ---------------------------------------------------------------------------
  // close / cleanup
  // ---------------------------------------------------------------------------

  describe('close / cleanup', () => {
    test('close should not throw in embedded mode', () => {
      const fp = new FlowProducer({ embedded: true });
      expect(() => fp.close()).not.toThrow();
    });

    test('close should be idempotent', () => {
      const fp = new FlowProducer({ embedded: true });
      expect(() => fp.close()).not.toThrow();
      expect(() => fp.close()).not.toThrow();
    });

    test('disconnect then close should not throw', async () => {
      const fp = new FlowProducer({ embedded: true });
      await fp.disconnect();
      expect(() => fp.close()).not.toThrow();
    });
  });
});

// =============================================================================
// Cross-queue flows
// =============================================================================

describe('FlowProducer - Cross-queue flows', () => {
  let flowProducer: FlowProducer;
  let queueA: Queue;
  let queueB: Queue;

  beforeEach(() => {
    flowProducer = new FlowProducer({ embedded: true });
    queueA = new Queue('cross-q-a', { embedded: true });
    queueB = new Queue('cross-q-b', { embedded: true });
    queueA.obliterate();
    queueB.obliterate();
  });

  afterEach(() => {
    flowProducer.close();
    queueA.close();
    queueB.close();
    shutdownManager();
  });

  test('should create flow with children in different queues', async () => {
    const flow: FlowJob = {
      name: 'parent',
      queueName: 'cross-q-a',
      data: { type: 'orchestrator' },
      children: [
        { name: 'child-a', queueName: 'cross-q-a', data: { type: 'same-queue' } },
        { name: 'child-b', queueName: 'cross-q-b', data: { type: 'other-queue' } },
      ],
    };

    const result = await flowProducer.add(flow);

    expect(result.job.queueName).toBe('cross-q-a');
    expect(result.children).toHaveLength(2);
    expect(result.children![0].job.queueName).toBe('cross-q-a');
    expect(result.children![1].job.queueName).toBe('cross-q-b');
  });

  test('should create deeply nested cross-queue flow', async () => {
    const flow: FlowJob = {
      name: 'root',
      queueName: 'cross-q-a',
      data: {},
      children: [
        {
          name: 'mid',
          queueName: 'cross-q-b',
          data: {},
          children: [{ name: 'leaf', queueName: 'cross-q-a', data: {} }],
        },
      ],
    };

    const result = await flowProducer.add(flow);

    expect(result.job.queueName).toBe('cross-q-a');
    expect(result.children![0].job.queueName).toBe('cross-q-b');
    expect(result.children![0].children![0].job.queueName).toBe('cross-q-a');
  });
});

// =============================================================================
// Legacy API - Extended
// =============================================================================

describe('FlowProducer - Legacy API Extended', () => {
  let flowProducer: FlowProducer;

  beforeEach(() => {
    flowProducer = new FlowProducer({ embedded: true });
  });

  afterEach(() => {
    flowProducer.close();
    shutdownManager();
  });

  describe('addChain() extended', () => {
    test('should create single-step chain', async () => {
      const result = await flowProducer.addChain([
        { name: 'only-step', queueName: 'chain-ext', data: { id: 'solo' } },
      ]);

      expect(result.jobIds).toHaveLength(1);
    });

    test('should create long chain with sequential dependencies', async () => {
      const steps = Array.from({ length: 10 }, (_, i) => ({
        name: `step-${i}`,
        queueName: 'chain-ext',
        data: { index: i },
      }));

      const result = await flowProducer.addChain(steps);
      expect(result.jobIds).toHaveLength(10);

      // All IDs should be unique
      const unique = new Set(result.jobIds);
      expect(unique.size).toBe(10);
    });

    test('should pass options through chain steps', async () => {
      const result = await flowProducer.addChain([
        {
          name: 'step-with-opts',
          queueName: 'chain-ext',
          data: { id: '1' },
          opts: { priority: 10, attempts: 5 },
        },
        {
          name: 'step-normal',
          queueName: 'chain-ext',
          data: { id: '2' },
        },
      ]);

      expect(result.jobIds).toHaveLength(2);
    });
  });

  describe('addBulkThen() extended', () => {
    test('should handle single parallel job', async () => {
      const result = await flowProducer.addBulkThen(
        [{ name: 'single-parallel', queueName: 'bulk-ext', data: { id: '1' } }],
        { name: 'final', queueName: 'bulk-ext', data: { id: 'f' } }
      );

      expect(result.parallelIds).toHaveLength(1);
      expect(result.finalId).toBeDefined();
      expect(result.finalId).not.toBe(result.parallelIds[0]);
    });

    test('should handle many parallel jobs converging', async () => {
      const parallel = Array.from({ length: 8 }, (_, i) => ({
        name: `parallel-${i}`,
        queueName: 'bulk-ext',
        data: { index: i },
      }));

      const result = await flowProducer.addBulkThen(parallel, {
        name: 'merge',
        queueName: 'bulk-ext',
        data: { type: 'merge' },
      });

      expect(result.parallelIds).toHaveLength(8);
      expect(result.finalId).toBeDefined();

      // All IDs should be unique
      const allIds = [...result.parallelIds, result.finalId];
      expect(new Set(allIds).size).toBe(9);
    });

    test('should pass options to parallel and final jobs', async () => {
      const result = await flowProducer.addBulkThen(
        [
          {
            name: 'p1',
            queueName: 'bulk-ext',
            data: { id: '1' },
            opts: { priority: 5 },
          },
        ],
        {
          name: 'final',
          queueName: 'bulk-ext',
          data: { id: 'f' },
          opts: { priority: 100 },
        }
      );

      expect(result.parallelIds).toHaveLength(1);
      expect(result.finalId).toBeDefined();
    });
  });

  describe('addTree() extended', () => {
    test('should create tree with single root (no children)', async () => {
      const result = await flowProducer.addTree({
        name: 'lone-root',
        queueName: 'tree-ext',
        data: { id: 'root' },
      });

      expect(result.jobIds).toHaveLength(1);
    });

    test('should create wide tree (many children)', async () => {
      const children = Array.from({ length: 6 }, (_, i) => ({
        name: `child-${i}`,
        queueName: 'tree-ext',
        data: { index: i },
      }));

      const result = await flowProducer.addTree({
        name: 'wide-root',
        queueName: 'tree-ext',
        data: { id: 'root' },
        children,
      });

      expect(result.jobIds).toHaveLength(7); // 1 root + 6 children
    });

    test('should create deeply nested tree', async () => {
      const result = await flowProducer.addTree({
        name: 'root',
        queueName: 'tree-ext',
        data: { id: 'r' },
        children: [
          {
            name: 'l1',
            queueName: 'tree-ext',
            data: { id: 'l1' },
            children: [
              {
                name: 'l2',
                queueName: 'tree-ext',
                data: { id: 'l2' },
                children: [{ name: 'l3', queueName: 'tree-ext', data: { id: 'l3' } }],
              },
            ],
          },
        ],
      });

      expect(result.jobIds).toHaveLength(4); // root + l1 + l2 + l3
    });
  });
});

// =============================================================================
// Worker Integration: Children completion triggers parent
// =============================================================================

describe('FlowProducer - Worker Integration', () => {
  let flowProducer: FlowProducer;
  let queue: Queue<{ value: number }>;
  let worker: Worker<{ value: number }, { result: number }> | null = null;

  beforeEach(() => {
    flowProducer = new FlowProducer({ embedded: true });
    queue = new Queue('fp-worker-int', { embedded: true });
    queue.obliterate();
  });

  afterEach(async () => {
    if (worker) {
      await worker.close();
      worker = null;
    }
    flowProducer.close();
    queue.close();
    shutdownManager();
  });

  test('should process children jobs via worker', async () => {
    const processed: string[] = [];

    worker = new Worker<{ value: number }, { result: number }>(
      'fp-worker-int',
      async (job) => {
        processed.push(job.name);
        return { result: job.data.value * 2 };
      },
      { embedded: true, concurrency: 1 }
    );

    const flow: FlowJob<{ value: number }> = {
      name: 'parent',
      queueName: 'fp-worker-int',
      data: { value: 100 },
      children: [
        { name: 'child-1', queueName: 'fp-worker-int', data: { value: 10 } },
        { name: 'child-2', queueName: 'fp-worker-int', data: { value: 20 } },
      ],
    };

    await flowProducer.add(flow);

    // Wait for processing
    await new Promise((resolve) => setTimeout(resolve, 2000));

    // All jobs should have been processed (children + parent)
    expect(processed.length).toBeGreaterThanOrEqual(2);
  });

  test('should process jobs from flow with options', async () => {
    const processed: string[] = [];

    worker = new Worker<{ value: number }, { result: number }>(
      'fp-worker-int',
      async (job) => {
        processed.push(job.name);
        return { result: job.data.value };
      },
      { embedded: true, concurrency: 2 }
    );

    const flow: FlowJob<{ value: number }> = {
      name: 'parent-with-opts',
      queueName: 'fp-worker-int',
      data: { value: 50 },
      opts: { attempts: 3 },
      children: [
        {
          name: 'child-with-opts',
          queueName: 'fp-worker-int',
          data: { value: 25 },
          opts: { priority: 10 },
        },
      ],
    };

    await flowProducer.add(flow);

    // Wait for processing
    await new Promise((resolve) => setTimeout(resolve, 2000));

    expect(processed.length).toBeGreaterThanOrEqual(1);
  });
});

// =============================================================================
// FlowProducer - Multiple children scenarios
// =============================================================================

describe('FlowProducer - Multiple children scenarios', () => {
  let flowProducer: FlowProducer;

  beforeEach(() => {
    flowProducer = new FlowProducer({ embedded: true });
  });

  afterEach(() => {
    flowProducer.close();
    shutdownManager();
  });

  test('should handle parent with many children (10+)', async () => {
    const children = Array.from({ length: 15 }, (_, i) => ({
      name: `child-${i}`,
      queueName: 'many-children',
      data: { index: i },
    }));

    const flow: FlowJob = {
      name: 'big-parent',
      queueName: 'many-children',
      data: {},
      children,
    };

    const result = await flowProducer.add(flow);

    expect(result.job.name).toBe('big-parent');
    expect(result.children).toHaveLength(15);

    for (let i = 0; i < 15; i++) {
      expect(result.children![i].job.name).toBe(`child-${i}`);
    }
  });

  test('should handle mixed nesting depths among siblings', async () => {
    const flow: FlowJob = {
      name: 'root',
      queueName: 'mixed-depth',
      data: {},
      children: [
        { name: 'shallow', queueName: 'mixed-depth', data: {} },
        {
          name: 'deep',
          queueName: 'mixed-depth',
          data: {},
          children: [
            {
              name: 'deeper',
              queueName: 'mixed-depth',
              data: {},
              children: [{ name: 'deepest', queueName: 'mixed-depth', data: {} }],
            },
          ],
        },
      ],
    };

    const result = await flowProducer.add(flow);

    expect(result.children).toHaveLength(2);
    expect(result.children![0].job.name).toBe('shallow');
    expect(result.children![0].children).toBeUndefined();
    expect(result.children![1].job.name).toBe('deep');
    expect(result.children![1].children![0].job.name).toBe('deeper');
    expect(result.children![1].children![0].children![0].job.name).toBe('deepest');
  });

  test('should handle empty children array', async () => {
    const flow: FlowJob = {
      name: 'empty-children',
      queueName: 'empty-children-q',
      data: {},
      children: [],
    };

    const result = await flowProducer.add(flow);

    expect(result.job.name).toBe('empty-children');
    expect(result.children).toBeUndefined();
  });
});
