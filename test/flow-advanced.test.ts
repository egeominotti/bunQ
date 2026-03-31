/**
 * Advanced Flow Tests (Embedded Mode)
 *
 * Real-world scenarios testing flow chains, parent-child relationships,
 * fan-out/fan-in patterns, result passing, progress tracking, and
 * concurrent flow execution.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, FlowProducer, shutdownManager } from '../src/client';

describe('Advanced Flow - Embedded', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('chain: each step receives parent result', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-chain-results', { embedded: true });
    queue.obliterate();

    const collectedResults: Array<{ step: number; parentResult: unknown }> = [];
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const worker = new Worker(
      'flow-chain-results',
      async (job) => {
        const data = job.data as { step: number; __flowParentId?: string };
        let parentResult: unknown = null;

        if (data.__flowParentId) {
          parentResult = flow.getParentResult(data.__flowParentId);
        }

        collectedResults.push({ step: data.step, parentResult });

        if (data.step === 2) resolve!();
        return { computed: (data.step + 1) * 100 };
      },
      { embedded: true, concurrency: 1 }
    );

    await flow.addChain([
      { name: 'step-0', queueName: 'flow-chain-results', data: { step: 0 } },
      { name: 'step-1', queueName: 'flow-chain-results', data: { step: 1 } },
      { name: 'step-2', queueName: 'flow-chain-results', data: { step: 2 } },
    ]);

    await done;
    await Bun.sleep(200);

    expect(collectedResults).toHaveLength(3);
    // step 0 has no parent
    expect(collectedResults[0].parentResult).toBeNull();
    // step 1 gets step 0's result
    expect(collectedResults[1].parentResult).toEqual({ computed: 100 });
    // step 2 gets step 1's result
    expect(collectedResults[2].parentResult).toEqual({ computed: 200 });

    await worker.close();
    flow.close();
    queue.close();
  }, 15000);

  test('parent-child: parent waits for children, getChildrenValues works', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-parent-child', { embedded: true });
    queue.obliterate();

    let parentChildrenValues: Record<string, unknown> = {};
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const worker = new Worker(
      'flow-parent-child',
      async (job) => {
        const data = job.data as { role: string; value?: number };

        if (data.role === 'child') {
          await Bun.sleep(50); // simulate work
          return { childResult: (data.value ?? 0) * 10 };
        }

        if (data.role === 'parent') {
          parentChildrenValues = await queue.getChildrenValues(job.id);
          resolve!();
          return { summary: 'all children done' };
        }

        return {};
      },
      { embedded: true, concurrency: 5 }
    );

    await flow.add({
      name: 'parent',
      queueName: 'flow-parent-child',
      data: { role: 'parent' },
      children: [
        { name: 'child-a', queueName: 'flow-parent-child', data: { role: 'child', value: 3 } },
        { name: 'child-b', queueName: 'flow-parent-child', data: { role: 'child', value: 7 } },
        { name: 'child-c', queueName: 'flow-parent-child', data: { role: 'child', value: 5 } },
      ],
    });

    await done;
    await Bun.sleep(200);

    const values = Object.values(parentChildrenValues).map((v: any) => v.childResult).sort();
    expect(values).toEqual([30, 50, 70]);

    await worker.close();
    flow.close();
    queue.close();
  }, 15000);

  test('fan-out/fan-in: parallel tasks converge to final step', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-fanout', { embedded: true });
    queue.obliterate();

    const executionOrder: string[] = [];
    let finalExecuted = false;
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const worker = new Worker(
      'flow-fanout',
      async (job) => {
        const data = job.data as { task: string };
        executionOrder.push(data.task);

        if (data.task === 'merge') {
          finalExecuted = true;
          resolve!();
        }

        await Bun.sleep(30);
        return { result: data.task };
      },
      { embedded: true, concurrency: 5 }
    );

    await flow.addBulkThen(
      [
        { name: 'fetch-api', queueName: 'flow-fanout', data: { task: 'fetch-1' } },
        { name: 'fetch-db', queueName: 'flow-fanout', data: { task: 'fetch-2' } },
        { name: 'fetch-cache', queueName: 'flow-fanout', data: { task: 'fetch-3' } },
      ],
      { name: 'merge-results', queueName: 'flow-fanout', data: { task: 'merge' } }
    );

    await done;
    await Bun.sleep(200);

    expect(finalExecuted).toBe(true);
    // Merge must be last (depends on all 3 parallel)
    expect(executionOrder.indexOf('merge')).toBe(executionOrder.length - 1);
    // All 3 parallel tasks executed before merge
    expect(executionOrder.filter((t) => t.startsWith('fetch-'))).toHaveLength(3);

    await worker.close();
    flow.close();
    queue.close();
  }, 15000);

  test('deep tree: grandchildren -> children -> root execution order', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-tree', { embedded: true });
    queue.obliterate();

    const executionOrder: string[] = [];
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const worker = new Worker(
      'flow-tree',
      async (job) => {
        const data = job.data as { node: string };
        executionOrder.push(data.node);

        if (data.node === 'root') resolve!();
        return { node: data.node };
      },
      { embedded: true, concurrency: 1 }
    );

    // Tree: root -> [branch-A -> [leaf-1, leaf-2], branch-B]
    await flow.add({
      name: 'root',
      queueName: 'flow-tree',
      data: { node: 'root' },
      children: [
        {
          name: 'branch-A',
          queueName: 'flow-tree',
          data: { node: 'branch-A' },
          children: [
            { name: 'leaf-1', queueName: 'flow-tree', data: { node: 'leaf-1' } },
            { name: 'leaf-2', queueName: 'flow-tree', data: { node: 'leaf-2' } },
          ],
        },
        {
          name: 'branch-B',
          queueName: 'flow-tree',
          data: { node: 'branch-B' },
        },
      ],
    });

    await done;
    await Bun.sleep(200);

    expect(executionOrder).toHaveLength(5);
    // Leaves before branch-A, branch-A and branch-B before root
    expect(executionOrder.indexOf('leaf-1')).toBeLessThan(executionOrder.indexOf('branch-A'));
    expect(executionOrder.indexOf('leaf-2')).toBeLessThan(executionOrder.indexOf('branch-A'));
    expect(executionOrder.indexOf('branch-A')).toBeLessThan(executionOrder.indexOf('root'));
    expect(executionOrder.indexOf('branch-B')).toBeLessThan(executionOrder.indexOf('root'));

    await worker.close();
    flow.close();
    queue.close();
  }, 15000);

  test('chain with retry: failed step retries and chain completes', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-retry', { embedded: true });
    queue.obliterate();

    let step1Attempts = 0;
    let chainCompleted = false;
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const worker = new Worker(
      'flow-retry',
      async (job) => {
        const data = job.data as { step: number };

        if (data.step === 1) {
          step1Attempts++;
          if (step1Attempts < 3) {
            throw new Error(`Transient failure attempt ${step1Attempts}`);
          }
        }

        if (data.step === 2) {
          chainCompleted = true;
          resolve!();
        }

        return { step: data.step, ok: true };
      },
      { embedded: true, concurrency: 1 }
    );

    await flow.addChain([
      { name: 'step-0', queueName: 'flow-retry', data: { step: 0 } },
      { name: 'step-1', queueName: 'flow-retry', data: { step: 1 }, opts: { attempts: 5, backoff: 100 } },
      { name: 'step-2', queueName: 'flow-retry', data: { step: 2 } },
    ]);

    await done;
    await Bun.sleep(200);

    expect(step1Attempts).toBe(3);
    expect(chainCompleted).toBe(true);

    await worker.close();
    flow.close();
    queue.close();
  }, 30000);

  test('progress tracking inside flow steps', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-progress', { embedded: true });
    queue.obliterate();

    const progressUpdates: Array<{ step: number; progress: number }> = [];
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const worker = new Worker(
      'flow-progress',
      async (job) => {
        const data = job.data as { step: number };

        for (let p = 25; p <= 100; p += 25) {
          await job.updateProgress(p);
          progressUpdates.push({ step: data.step, progress: p });
        }

        if (data.step === 1) resolve!();
        return { done: true };
      },
      { embedded: true, concurrency: 1 }
    );

    worker.on('progress', () => {
      // just confirm events fire
    });

    await flow.addChain([
      { name: 'download', queueName: 'flow-progress', data: { step: 0 } },
      { name: 'process', queueName: 'flow-progress', data: { step: 1 } },
    ]);

    await done;
    await Bun.sleep(200);

    // Each step reports 4 progress updates (25, 50, 75, 100)
    expect(progressUpdates).toHaveLength(8);
    // Step 0 updates
    expect(progressUpdates.filter((u) => u.step === 0).map((u) => u.progress)).toEqual([25, 50, 75, 100]);
    // Step 1 updates
    expect(progressUpdates.filter((u) => u.step === 1).map((u) => u.progress)).toEqual([25, 50, 75, 100]);

    await worker.close();
    flow.close();
    queue.close();
  }, 15000);

  test('flow across multiple queues', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queueA = new Queue('flow-multi-a', { embedded: true });
    const queueB = new Queue('flow-multi-b', { embedded: true });
    queueA.obliterate();
    queueB.obliterate();

    const results: Array<{ queue: string; step: string }> = [];
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const workerA = new Worker(
      'flow-multi-a',
      async (job) => {
        const data = job.data as { step: string };
        results.push({ queue: 'a', step: data.step });
        return { from: 'queue-a', step: data.step };
      },
      { embedded: true, concurrency: 2 }
    );

    const workerB = new Worker(
      'flow-multi-b',
      async (job) => {
        const data = job.data as { step: string };
        results.push({ queue: 'b', step: data.step });
        if (data.step === 'aggregate') resolve!();
        return { from: 'queue-b', step: data.step };
      },
      { embedded: true, concurrency: 2 }
    );

    // Chain across two queues: A:fetch -> B:transform -> A:validate -> B:aggregate
    await flow.addChain([
      { name: 'fetch', queueName: 'flow-multi-a', data: { step: 'fetch' } },
      { name: 'transform', queueName: 'flow-multi-b', data: { step: 'transform' } },
      { name: 'validate', queueName: 'flow-multi-a', data: { step: 'validate' } },
      { name: 'aggregate', queueName: 'flow-multi-b', data: { step: 'aggregate' } },
    ]);

    await done;
    await Bun.sleep(200);

    expect(results).toHaveLength(4);
    expect(results[0]).toEqual({ queue: 'a', step: 'fetch' });
    expect(results[1]).toEqual({ queue: 'b', step: 'transform' });
    expect(results[2]).toEqual({ queue: 'a', step: 'validate' });
    expect(results[3]).toEqual({ queue: 'b', step: 'aggregate' });

    await workerA.close();
    await workerB.close();
    flow.close();
    queueA.close();
    queueB.close();
  }, 15000);

  test('concurrent flows do not interfere with each other', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-concurrent', { embedded: true });
    queue.obliterate();

    const flowResults = new Map<string, string[]>();
    let completedFlows = 0;
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const worker = new Worker(
      'flow-concurrent',
      async (job) => {
        const data = job.data as { flowId: string; step: number };
        const key = data.flowId;

        if (!flowResults.has(key)) flowResults.set(key, []);
        flowResults.get(key)!.push(`step-${data.step}`);

        await Bun.sleep(20); // simulate work

        if (data.step === 2) {
          completedFlows++;
          if (completedFlows === 3) resolve!();
        }

        return { flowId: data.flowId, step: data.step };
      },
      { embedded: true, concurrency: 10 }
    );

    // Launch 3 chains simultaneously
    await Promise.all([
      flow.addChain([
        { name: 's0', queueName: 'flow-concurrent', data: { flowId: 'alpha', step: 0 } },
        { name: 's1', queueName: 'flow-concurrent', data: { flowId: 'alpha', step: 1 } },
        { name: 's2', queueName: 'flow-concurrent', data: { flowId: 'alpha', step: 2 } },
      ]),
      flow.addChain([
        { name: 's0', queueName: 'flow-concurrent', data: { flowId: 'beta', step: 0 } },
        { name: 's1', queueName: 'flow-concurrent', data: { flowId: 'beta', step: 1 } },
        { name: 's2', queueName: 'flow-concurrent', data: { flowId: 'beta', step: 2 } },
      ]),
      flow.addChain([
        { name: 's0', queueName: 'flow-concurrent', data: { flowId: 'gamma', step: 0 } },
        { name: 's1', queueName: 'flow-concurrent', data: { flowId: 'gamma', step: 1 } },
        { name: 's2', queueName: 'flow-concurrent', data: { flowId: 'gamma', step: 2 } },
      ]),
    ]);

    await done;
    await Bun.sleep(300);

    // Each flow executed all 3 steps
    for (const id of ['alpha', 'beta', 'gamma']) {
      const steps = flowResults.get(id)!;
      expect(steps).toHaveLength(3);
      expect(steps).toContain('step-0');
      expect(steps).toContain('step-1');
      expect(steps).toContain('step-2');
    }

    await worker.close();
    flow.close();
    queue.close();
  }, 20000);

  test('getFlow returns correct tree structure', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-getflow', { embedded: true });
    queue.obliterate();

    let rootJobId = '';
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const worker = new Worker(
      'flow-getflow',
      async (job) => {
        const data = job.data as { node: string };
        if (data.node === 'root') resolve!();
        return { node: data.node };
      },
      { embedded: true, concurrency: 5 }
    );

    const result = await flow.add({
      name: 'root',
      queueName: 'flow-getflow',
      data: { node: 'root' },
      children: [
        { name: 'child-1', queueName: 'flow-getflow', data: { node: 'child-1' } },
        { name: 'child-2', queueName: 'flow-getflow', data: { node: 'child-2' } },
      ],
    });

    rootJobId = result.job.id;

    await done;
    await Bun.sleep(200);

    // Retrieve the flow tree
    const tree = await flow.getFlow({
      id: rootJobId,
      queueName: 'flow-getflow',
    });

    expect(tree).not.toBeNull();
    expect(tree!.job.name).toBe('root');
    expect(tree!.children).toHaveLength(2);

    const childNames = tree!.children!.map((c) => c.job.name).sort();
    expect(childNames).toEqual(['child-1', 'child-2']);

    await worker.close();
    flow.close();
    queue.close();
  }, 15000);

  test('pipeline: ETL with data transformation across steps', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-etl', { embedded: true });
    queue.obliterate();

    let finalResult: unknown = null;
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const worker = new Worker(
      'flow-etl',
      async (job) => {
        const data = job.data as { stage: string; __flowParentId?: string; records?: number[] };

        if (data.stage === 'extract') {
          // Simulate extracting raw data
          return { records: [10, 20, 30, 40, 50] };
        }

        if (data.stage === 'transform') {
          const parentResult = flow.getParentResult<{ records: number[] }>(data.__flowParentId!);
          // Double each value
          const transformed = parentResult!.records.map((r) => r * 2);
          return { records: transformed };
        }

        if (data.stage === 'load') {
          const parentResult = flow.getParentResult<{ records: number[] }>(data.__flowParentId!);
          // Sum all values
          const total = parentResult!.records.reduce((a, b) => a + b, 0);
          finalResult = { total, count: parentResult!.records.length };
          resolve!();
          return finalResult;
        }

        return {};
      },
      { embedded: true, concurrency: 1 }
    );

    await flow.addChain([
      { name: 'extract', queueName: 'flow-etl', data: { stage: 'extract' } },
      { name: 'transform', queueName: 'flow-etl', data: { stage: 'transform' } },
      { name: 'load', queueName: 'flow-etl', data: { stage: 'load' } },
    ]);

    await done;
    await Bun.sleep(200);

    // [10,20,30,40,50] * 2 = [20,40,60,80,100], sum = 300
    expect(finalResult).toEqual({ total: 300, count: 5 });

    await worker.close();
    flow.close();
    queue.close();
  }, 15000);

  test('worker events fire correctly during flow execution', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('flow-events', { embedded: true });
    queue.obliterate();

    const events: Array<{ type: string; jobName: string }> = [];
    let resolve: () => void;
    const done = new Promise<void>((r) => (resolve = r));

    const worker = new Worker(
      'flow-events',
      async (job) => {
        const data = job.data as { step: number };
        if (data.step === 1) resolve!();
        return { step: data.step };
      },
      { embedded: true, concurrency: 1 }
    );

    worker.on('active', (job) => {
      events.push({ type: 'active', jobName: job.name });
    });

    worker.on('completed', (job) => {
      events.push({ type: 'completed', jobName: job.name });
    });

    await flow.addChain([
      { name: 'first', queueName: 'flow-events', data: { step: 0 } },
      { name: 'second', queueName: 'flow-events', data: { step: 1 } },
    ]);

    await done;
    await Bun.sleep(300);

    // Should have active + completed for each step
    const activeEvents = events.filter((e) => e.type === 'active');
    const completedEvents = events.filter((e) => e.type === 'completed');

    expect(activeEvents.length).toBeGreaterThanOrEqual(2);
    expect(completedEvents.length).toBeGreaterThanOrEqual(2);
    expect(activeEvents.map((e) => e.jobName)).toContain('first');
    expect(activeEvents.map((e) => e.jobName)).toContain('second');

    await worker.close();
    flow.close();
    queue.close();
  }, 15000);
});
