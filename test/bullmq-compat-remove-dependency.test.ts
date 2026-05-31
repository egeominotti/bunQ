/**
 * BullMQ v5 Compatibility: remove dependency flow options
 *
 * Tests for:
 * - removeDependencyOnFailure
 * - ignoreDependencyOnFailure + getIgnoredChildrenFailures()
 * - continueParentOnFailure + getFailedChildrenValues()
 * - removeChildDependency()
 * - removeUnprocessedChildren()
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, FlowProducer, shutdownManager } from '../src/client';
import { getSharedManager } from '../src/client/manager';
import { jobId } from '../src/domain/types/job';

// Helper: wait up to `ms` milliseconds for a predicate to become true
async function waitFor(predicate: () => Promise<boolean> | boolean, ms = 4000): Promise<void> {
  const deadline = Date.now() + ms;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((r) => setTimeout(r, 30));
  }
  throw new Error(`waitFor timed out after ${ms}ms`);
}

describe('BullMQ v5 - removeDependencyOnFailure', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('parent is promoted when failing child has removeDependencyOnFailure', async () => {
    const queueName = 'remove-dep-on-fail-test';
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue(queueName, { embedded: true });
    queue.obliterate();

    let childJobId: string | undefined;
    let parentJobId: string | undefined;
    let resolveChildFailed: () => void;
    const childFailed = new Promise<void>((r) => (resolveChildFailed = r));

    const worker = new Worker(
      queueName,
      async (job) => {
        const data = job.data as { role: string };
        if (data.role === 'child') {
          throw new Error('Child intentionally failed');
        }
        // parent processes successfully after being promoted
        return { done: true };
      },
      { embedded: true, concurrency: 5 }
    );

    worker.on('failed', (job) => {
      if (job && job.id === childJobId) {
        resolveChildFailed();
      }
    });

    const result = await flow.add({
      name: 'parent',
      queueName,
      data: { role: 'parent' },
      children: [
        {
          name: 'child',
          queueName,
          data: { role: 'child' },
          opts: {
            removeDependencyOnFailure: true,
            attempts: 1, // terminal failure immediately
          },
        },
      ],
    });

    parentJobId = result.job.id;
    childJobId = result.children?.[0]?.job?.id;

    expect(parentJobId).toBeDefined();
    expect(childJobId).toBeDefined();

    // Wait for child to fail
    await childFailed;

    // Parent should be promoted to queue (waiting state) after child fails
    await waitFor(async () => {
      const state = await queue.getJobState(parentJobId!);
      return state === 'waiting' || state === 'active' || state === 'completed';
    });

    const parentState = await queue.getJobState(parentJobId!);
    expect(['waiting', 'active', 'completed']).toContain(parentState);

    await worker.close();
  });
});

describe('BullMQ v5 - ignoreDependencyOnFailure', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('parent is promoted and failure is recorded in getIgnoredChildrenFailures', async () => {
    const queueName = 'ignore-dep-on-fail-test';
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue(queueName, { embedded: true });
    queue.obliterate();

    let childJobId: string | undefined;
    let parentJobId: string | undefined;
    let resolveChildFailed: () => void;
    const childFailed = new Promise<void>((r) => (resolveChildFailed = r));

    let resolveParentCompleted: () => void;
    const parentCompleted = new Promise<void>((r) => (resolveParentCompleted = r));

    // Capture the ignored-children failures while the parent is PROCESSING — this is
    // the BullMQ-documented read point (job.getIgnoredChildrenFailures()). The tracking
    // entry is released once the parent completes (AUDIT H8), so we read it here.
    let capturedFailures: Record<string, string> = {};

    const worker = new Worker(
      queueName,
      async (job) => {
        const data = job.data as { role: string };
        if (data.role === 'child') {
          throw new Error('Child ignored failure');
        }
        // parent processes — read failure metadata before it completes
        capturedFailures = await getSharedManager().getIgnoredChildrenFailures(jobId(job.id));
        return { parentDone: true };
      },
      { embedded: true, concurrency: 5 }
    );

    worker.on('failed', (job) => {
      if (job && job.id === childJobId) {
        resolveChildFailed();
      }
    });
    worker.on('completed', (job) => {
      if (job && job.id === parentJobId) {
        resolveParentCompleted();
      }
    });

    const result = await flow.add({
      name: 'parent',
      queueName,
      data: { role: 'parent' },
      children: [
        {
          name: 'child',
          queueName,
          data: { role: 'child' },
          opts: {
            ignoreDependencyOnFailure: true,
            attempts: 1,
          },
        },
      ],
    });

    parentJobId = result.job.id;
    childJobId = result.children?.[0]?.job?.id;

    await childFailed;
    await parentCompleted;

    // getIgnoredChildrenFailures should have had the child's failure during processing
    expect(Object.keys(capturedFailures).length).toBeGreaterThan(0);

    const childKey = Object.keys(capturedFailures)[0];
    expect(childKey).toContain(childJobId!);
    expect(capturedFailures[childKey]).toContain('Child ignored failure');

    await worker.close();
  });
});

describe('BullMQ v5 - continueParentOnFailure', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('parent is immediately promoted and getFailedChildrenValues returns failure info', async () => {
    const queueName = 'continue-parent-on-fail-test';
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue(queueName, { embedded: true });
    queue.obliterate();

    let childJobId: string | undefined;
    let parentJobId: string | undefined;
    let resolveChildFailed: () => void;
    const childFailed = new Promise<void>((r) => (resolveChildFailed = r));

    let resolveParentCompleted: () => void;
    const parentCompleted = new Promise<void>((r) => (resolveParentCompleted = r));

    // Capture the failed-children values while the parent is PROCESSING — this is the
    // BullMQ-documented read point (job.getFailedChildrenValues()). The tracking entry
    // is released once the parent completes (AUDIT H8), so we read it here.
    let capturedFailedValues: Record<string, string> = {};

    const worker = new Worker(
      queueName,
      async (job) => {
        const data = job.data as { role: string };
        if (data.role === 'child') {
          throw new Error('Child continue-parent failure');
        }
        // parent processes — read failure metadata before it completes
        capturedFailedValues = await getSharedManager().getFailedChildrenValues(jobId(job.id));
        return { parentDone: true };
      },
      { embedded: true, concurrency: 5 }
    );

    worker.on('failed', (job) => {
      if (job && job.id === childJobId) {
        resolveChildFailed();
      }
    });
    worker.on('completed', (job) => {
      if (job && job.id === parentJobId) {
        resolveParentCompleted();
      }
    });

    const result = await flow.add({
      name: 'parent',
      queueName,
      data: { role: 'parent' },
      children: [
        {
          name: 'child',
          queueName,
          data: { role: 'child' },
          opts: {
            continueParentOnFailure: true,
            attempts: 1,
          },
        },
      ],
    });

    parentJobId = result.job.id;
    childJobId = result.children?.[0]?.job?.id;

    await childFailed;
    await parentCompleted;

    // getFailedChildrenValues should have had the child's failure during processing
    expect(Object.keys(capturedFailedValues).length).toBeGreaterThan(0);

    const childKey = Object.keys(capturedFailedValues)[0];
    expect(childKey).toContain(childJobId!);
    expect(capturedFailedValues[childKey]).toContain('Child continue-parent failure');

    await worker.close();
  });

  test('getFailedChildrenValues returns empty object when no failures', async () => {
    const queueName = 'no-failed-children-test';
    const queue = new Queue(queueName, { embedded: true });
    queue.obliterate();

    const job = await queue.add('test', { hello: 'world' });
    const manager = getSharedManager();
    const values = await manager.getFailedChildrenValues(jobId(job.id));
    expect(values).toEqual({});

    queue.obliterate();
  });
});

describe('BullMQ v5 - removeChildDependency()', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('removeChildDependency promotes parent when it is the last dependency', async () => {
    const queueName = 'remove-child-dep-test';
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue(queueName, { embedded: true });
    queue.obliterate();

    let childJobId: string | undefined;
    let parentJobId: string | undefined;
    let resolveChildActive: () => void;
    const childActive = new Promise<void>((r) => (resolveChildActive = r));

    const worker = new Worker(
      queueName,
      async (job) => {
        const data = job.data as { role: string };
        if (data.role === 'child') {
          resolveChildActive();
          // Hold briefly so we can call removeChildDependency
          await new Promise((r) => setTimeout(r, 200));
          return { childDone: true };
        }
        return { parentDone: true };
      },
      { embedded: true, concurrency: 5 }
    );

    const result = await flow.add({
      name: 'parent',
      queueName,
      data: { role: 'parent' },
      children: [
        {
          name: 'child',
          queueName,
          data: { role: 'child' },
          opts: { attempts: 1 },
        },
      ],
    });

    parentJobId = result.job.id;
    childJobId = result.children?.[0]?.job?.id;

    await childActive;

    // Manually remove child dependency via manager
    const manager = getSharedManager();
    const removed = await manager.removeChildDependency(jobId(childJobId!));
    expect(removed).toBe(true);

    // After removing, parent should eventually become waiting or active
    await waitFor(async () => {
      const state = await queue.getJobState(parentJobId!);
      return state === 'waiting' || state === 'active' || state === 'completed';
    });

    const finalParentState = await queue.getJobState(parentJobId!);
    expect(['waiting', 'active', 'completed']).toContain(finalParentState);

    await worker.close();
  });

  test('removeChildDependency throws when job has no parent', async () => {
    const queueName = 'remove-dep-no-parent-test';
    const queue = new Queue(queueName, { embedded: true });
    queue.obliterate();

    const job = await queue.add('orphan', { x: 1 });

    const manager = getSharedManager();
    await expect(manager.removeChildDependency(jobId(job.id))).rejects.toThrow(/no parent/i);

    queue.obliterate();
  });
});

describe('BullMQ v5 - removeUnprocessedChildren()', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('removes waiting children of a parent job', async () => {
    const queueName = 'remove-unprocessed-children-test';
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue(queueName, { embedded: true });
    queue.obliterate();

    const result = await flow.add({
      name: 'parent',
      queueName,
      data: { role: 'parent' },
      children: [
        {
          name: 'child1',
          queueName,
          data: { role: 'child' },
          opts: { attempts: 1 },
        },
        {
          name: 'child2',
          queueName,
          data: { role: 'child' },
          opts: { attempts: 1 },
        },
      ],
    });

    const parentJobId = result.job.id;
    const child1Id = result.children?.[0]?.job?.id;
    const child2Id = result.children?.[1]?.job?.id;

    expect(child1Id).toBeDefined();
    expect(child2Id).toBeDefined();

    // Children should be in queue (waiting)
    const child1State = await queue.getJobState(child1Id!);
    const child2State = await queue.getJobState(child2Id!);
    expect(child1State).toBe('waiting');
    expect(child2State).toBe('waiting');

    // Call removeUnprocessedChildren via manager
    const manager = getSharedManager();
    await manager.removeUnprocessedChildren(jobId(parentJobId));

    // Children should be cancelled/removed now
    await waitFor(async () => {
      const s1 = await queue.getJobState(child1Id!);
      const s2 = await queue.getJobState(child2Id!);
      return s1 === 'unknown' && s2 === 'unknown';
    });

    const finalState1 = await queue.getJobState(child1Id!);
    const finalState2 = await queue.getJobState(child2Id!);
    expect(finalState1).toBe('unknown');
    expect(finalState2).toBe('unknown');

    queue.obliterate();
  });
});
