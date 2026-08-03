/**
 * Executable proof for /guide/flow/patterns/
 * ("Flow Patterns: Chains, Fan-In and Trees").
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { Queue } from '../../src/client';
import { TcpConnectionPool } from '../../src/client/tcpPool';
import {
  type CoreE2eHarness,
  MODES,
  closeHarness,
  startHarness,
  waitForState,
  waitUntil,
} from '../docs-guide-support';

let harness: CoreE2eHarness | null = null;

afterEach(async () => {
  await closeHarness(harness);
  harness = null;
});

interface FlowJobData {
  __flowParentId?: string;
  __flowParentIds?: string[];
  __parentId?: string;
  __parentQueue?: string;
  __childrenIds?: string[];
  source?: string;
  level?: number;
}

for (const mode of MODES) {
  describe(`flow guide · patterns [${mode}]`, () => {
    test('addChain runs the steps strictly in order', async () => {
      harness = await startHarness('flow-patterns', mode);
      const queue = harness.queue<FlowJobData>('pipeline');
      const flow = harness.flow();
      const order: string[] = [];

      harness.worker<FlowJobData, unknown>(queue.name, async (job) => {
        order.push(job.name);
        await Bun.sleep(30);
        return { step: job.name };
      });

      const { jobIds } = await flow.addChain([
        { name: 'fetch', queueName: queue.name, data: { url: 'https://api.example.com' } },
        { name: 'process', queueName: queue.name, data: {} },
        { name: 'store', queueName: queue.name, data: {} },
      ]);

      expect(jobIds).toHaveLength(3);
      await waitUntil(() => order.length === 3, 'every chained step', 30_000);
      expect(order).toEqual(['fetch', 'process', 'store']);
    }, 60_000);

    test('addBulkThen runs the batch in parallel and then the final job', async () => {
      harness = await startHarness('flow-patterns', mode);
      const queue = harness.queue<FlowJobData>('parallel');
      const flow = harness.flow();
      const order: string[] = [];

      harness.worker<FlowJobData, unknown>(
        queue.name,
        async (job) => {
          order.push(job.name);
          return { from: job.name };
        },
        { concurrency: 4 }
      );

      const { parallelIds, finalId } = await flow.addBulkThen(
        [
          { name: 'fetch-api-1', queueName: queue.name, data: { source: 'api1' } },
          { name: 'fetch-api-2', queueName: queue.name, data: { source: 'api2' } },
          { name: 'fetch-api-3', queueName: queue.name, data: { source: 'api3' } },
        ],
        { name: 'merge-results', queueName: queue.name, data: {} }
      );

      expect(parallelIds).toHaveLength(3);
      expect(finalId).toBeString();
      await waitUntil(() => order.length === 4, 'the batch and the merge', 30_000);
      expect(order.at(-1)).toBe('merge-results');
      expect(order.slice(0, 3).sort()).toEqual(['fetch-api-1', 'fetch-api-2', 'fetch-api-3']);
    }, 60_000);

    test('addTree runs the parent before its children', async () => {
      harness = await startHarness('flow-patterns', mode);
      const queue = harness.queue<FlowJobData>('tree');
      const flow = harness.flow();
      const order: string[] = [];

      harness.worker<FlowJobData, unknown>(
        queue.name,
        async (job) => {
          order.push(job.name);
          return { node: job.name };
        },
        { concurrency: 4 }
      );

      const { jobIds } = await flow.addTree({
        name: 'root',
        queueName: queue.name,
        data: { level: 0 },
        children: [
          {
            name: 'branch-1',
            queueName: queue.name,
            data: { level: 1 },
            children: [
              { name: 'leaf-1a', queueName: queue.name, data: { level: 2 } },
              { name: 'leaf-1b', queueName: queue.name, data: { level: 2 } },
            ],
          },
          { name: 'branch-2', queueName: queue.name, data: { level: 1 } },
        ],
      });

      expect(jobIds).toHaveLength(5);
      await waitUntil(() => order.length === 5, 'every tree node', 30_000);
      expect(order[0]).toBe('root');
      expect(order.indexOf('branch-1')).toBeLessThan(order.indexOf('leaf-1a'));
      expect(order.indexOf('branch-1')).toBeLessThan(order.indexOf('leaf-1b'));
    }, 60_000);

    test('chain and fan-in inject the documented parent id fields', async () => {
      harness = await startHarness('flow-patterns', mode);
      const queue = harness.queue<FlowJobData>('pipeline');
      const flow = harness.flow();
      const seen = new Map<string, FlowJobData>();

      harness.worker<FlowJobData, unknown>(
        queue.name,
        async (job) => {
          seen.set(job.name, job.data);
          return { processed: true };
        },
        { concurrency: 4 }
      );

      const chain = await flow.addChain([
        { name: 'chain-first', queueName: queue.name, data: {} },
        { name: 'chain-second', queueName: queue.name, data: {} },
      ]);
      const fanIn = await flow.addBulkThen(
        [
          { name: 'merge-a', queueName: queue.name, data: {} },
          { name: 'merge-b', queueName: queue.name, data: {} },
        ],
        { name: 'merge-final', queueName: queue.name, data: {} }
      );

      await waitUntil(() => seen.size === 5, 'every injected job', 30_000);
      // A chained step carries the flow parent id; the first step has none.
      expect(seen.get('chain-second')?.__flowParentId).toBe(chain.jobIds[0]);
      expect(seen.get('chain-first')?.__flowParentId ?? null).toBeNull();
      // The fan-in final job carries the list of parents and its children.
      expect(seen.get('merge-final')?.__flowParentIds).toEqual(fanIn.parallelIds);
      expect(seen.get('merge-final')?.__childrenIds).toEqual(fanIn.parallelIds);
      // The parallel batch carries the BullMQ-compatible parent link.
      expect(seen.get('merge-a')?.__parentId).toBe(fanIn.finalId);
      expect(seen.get('merge-a')?.__parentQueue).toBe(queue.name);
    }, 60_000);

    test('a cross-queue chain carries exact BullMQ-compatible parent fields', async () => {
      harness = await startHarness('flow-patterns', mode);
      const firstQueue = harness.queue<FlowJobData>('pipeline-bullmq-first');
      const secondQueue = harness.queue<FlowJobData>('pipeline-bullmq-second');
      const flow = harness.flow();
      const seen = new Map<string, FlowJobData>();

      for (const queue of [firstQueue, secondQueue]) {
        harness.worker<FlowJobData, unknown>(queue.name, async (job) => {
          seen.set(job.name, job.data);
          return { processed: true };
        });
      }
      const { jobIds } = await flow.addChain([
        { name: 'chain-first', queueName: firstQueue.name, data: {} },
        { name: 'chain-second', queueName: secondQueue.name, data: {} },
      ]);

      await waitUntil(() => seen.size === 2, 'both chained steps', 30_000);
      expect(seen.get('chain-first')?.__parentId).toBeUndefined();
      expect(seen.get('chain-first')?.__parentQueue).toBeUndefined();
      expect(seen.get('chain-second')?.__parentId).toBe(jobIds[0]);
      expect(seen.get('chain-second')?.__parentQueue).toBe(firstQueue.name);

      await waitForState(secondQueue, jobIds[1], 'completed', 30_000);
      const queried = await secondQueue.getJob(jobIds[1]);
      expect(queried?.data.__parentId).toBe(jobIds[0]);
      expect(queried?.data.__parentQueue).toBe(firstQueue.name);
      const listed = (await secondQueue.getCompletedAsync(0, -1)).find(
        (job) => job.id === jobIds[1]
      );
      expect(listed?.data.__parentId).toBe(jobIds[0]);
      expect(listed?.data.__parentQueue).toBe(firstQueue.name);
    }, 60_000);

    test('a cross-queue tree carries parent metadata at every non-root level', async () => {
      harness = await startHarness('flow-patterns', mode);
      const rootQueue = harness.queue<FlowJobData>('tree-bullmq-root');
      const branchQueue = harness.queue<FlowJobData>('tree-bullmq-branch');
      const leafQueue = harness.queue<FlowJobData>('tree-bullmq-leaf');
      const flow = harness.flow();
      const seen = new Map<string, FlowJobData>();

      for (const queue of [rootQueue, branchQueue, leafQueue]) {
        harness.worker<FlowJobData, unknown>(queue.name, async (job) => {
          seen.set(job.name, job.data);
          return { processed: true };
        });
      }
      const { jobIds } = await flow.addTree({
        name: 'tree-root',
        queueName: rootQueue.name,
        data: {},
        children: [
          {
            name: 'tree-branch',
            queueName: branchQueue.name,
            data: {},
            children: [{ name: 'tree-leaf', queueName: leafQueue.name, data: {} }],
          },
        ],
      });

      await waitUntil(() => seen.size === 3, 'every metadata tree node', 30_000);
      expect(seen.get('tree-root')?.__parentId).toBeUndefined();
      expect(seen.get('tree-root')?.__parentQueue).toBeUndefined();
      expect(seen.get('tree-branch')?.__parentId).toBe(jobIds[0]);
      expect(seen.get('tree-branch')?.__parentQueue).toBe(rootQueue.name);
      expect(seen.get('tree-leaf')?.__parentId).toBe(jobIds[1]);
      expect(seen.get('tree-leaf')?.__parentQueue).toBe(branchQueue.name);
    }, 60_000);

    test('legacy flow metadata survives a broker restart', async () => {
      harness = await startHarness('flow-patterns', mode);
      const firstQueue = harness.queue<FlowJobData>('restart-first');
      const secondQueue = harness.queue<FlowJobData>('restart-second');
      const flow = harness.flow();
      const { jobIds } = await flow.addChain([
        {
          name: 'restart-first',
          queueName: firstQueue.name,
          data: {},
          opts: { durable: true },
        },
        {
          name: 'restart-second',
          queueName: secondQueue.name,
          data: {},
          opts: { durable: true },
        },
      ]);

      const beforeRestart = await secondQueue.getJob(jobIds[1]);
      await beforeRestart?.updateData({ name: 'user-name-after-update', source: 'updated' });

      await flow.close();
      await firstQueue.close();
      await secondQueue.close();
      await harness.restartBroker();

      const reopened = new Queue<FlowJobData>(secondQueue.name, harness.queueOptions());
      harness.addCleanup(() => reopened.close());
      const second = await reopened.getJob(jobIds[1]);
      expect(second?.name).toBe('restart-second');
      expect(second?.data.name).toBe('user-name-after-update');
      expect(second?.data.source).toBe('updated');
      expect(second?.data.__flowParentId).toBe(jobIds[0]);
      expect(second?.data.__parentId).toBe(jobIds[0]);
      expect(second?.data.__parentQueue).toBe(firstQueue.name);
    }, 60_000);

    test('updateData preserves topology and rejects reserved metadata collisions', async () => {
      harness = await startHarness('flow-patterns', mode);
      const firstQueue = harness.queue<FlowJobData>('update-first');
      const secondQueue = harness.queue<FlowJobData>('update-second');
      const flow = harness.flow();
      const { jobIds } = await flow.addChain([
        { name: 'update-first', queueName: firstQueue.name, data: { source: 'before' } },
        { name: 'update-second', queueName: secondQueue.name, data: { source: 'before' } },
      ]);
      const second = await secondQueue.getJob(jobIds[1]);
      expect(second).not.toBeNull();

      await second?.updateData({ name: 'user-name', source: 'after' });
      const updated = await secondQueue.getJob(jobIds[1]);
      expect(updated?.name).toBe('update-second');
      expect(updated?.data).toEqual({
        name: 'user-name',
        source: 'after',
        __flowParentId: jobIds[0],
        __parentId: jobIds[0],
        __parentQueue: firstQueue.name,
      });

      await expect(
        updated?.updateData({ source: 'forged', __parentId: 'forged-parent' })
      ).rejects.toThrow('flow job data key is reserved: __parentId');
      expect((await secondQueue.getJob(jobIds[1]))?.data).toEqual(updated?.data);

      if (mode === 'tcp') {
        const raw = new TcpConnectionPool(harness.connection());
        try {
          const response = await raw.send({
            cmd: 'Update',
            id: jobIds[1],
            data: { source: 'raw-forge', __parentQueue: 'forged-queue' },
          });
          expect(response.ok).toBe(false);
          expect(response.error).toContain('flow job data key is reserved: __parentQueue');
        } finally {
          raw.close();
        }
        expect((await secondQueue.getJob(jobIds[1]))?.data).toEqual(updated?.data);
      }

      const ordinary = await secondQueue.add('ordinary', { source: 'ordinary' });
      await ordinary.updateData({ source: 'ordinary-updated', __custom: 'allowed' });
      expect((await secondQueue.getJob(ordinary.id))?.data).toEqual({
        source: 'ordinary-updated',
        __custom: 'allowed',
      });

      const fanIn = await flow.addBulkThen(
        [
          { name: 'fan-in-a', queueName: firstQueue.name, data: { source: 'a' } },
          { name: 'fan-in-b', queueName: firstQueue.name, data: { source: 'b' } },
        ],
        { name: 'fan-in-final', queueName: secondQueue.name, data: { source: 'final' } }
      );
      const final = await secondQueue.getJob(fanIn.finalId);
      await final?.updateData({ source: 'final-updated' });
      expect((await secondQueue.getJob(fanIn.finalId))?.data).toEqual({
        source: 'final-updated',
        __flowParentIds: fanIn.parallelIds,
        __childrenIds: fanIn.parallelIds,
      });

      const modern = await flow.add({
        name: 'modern-parent',
        queueName: secondQueue.name,
        data: { source: 'modern-parent' },
        children: [
          { name: 'modern-child', queueName: firstQueue.name, data: { source: 'modern-child' } },
        ],
      });
      const modernChildId = modern.children?.[0]?.job.id;
      if (!modernChildId) throw new Error('modern flow child was not returned');
      expect(modern.job.data).toEqual({ source: 'modern-parent' });
      const modernParent = await secondQueue.getJob(modern.job.id);
      expect(modernParent?.data).toEqual({
        source: 'modern-parent',
        __childrenIds: [modernChildId],
      });
      await modernParent?.updateData({ source: 'modern-parent-updated' });
      expect((await secondQueue.getJob(modern.job.id))?.data).toEqual({
        source: 'modern-parent-updated',
        __childrenIds: [modernChildId],
      });
    }, 60_000);

    test('legacy flow APIs reject user data that collides with reserved metadata', async () => {
      harness = await startHarness('flow-patterns', mode);
      const queue = harness.queue<FlowJobData>('reserved-metadata');
      const flow = harness.flow();

      await expect(
        flow.addChain([
          {
            name: 'forged-chain',
            queueName: queue.name,
            data: { __parentId: 'forged' },
          },
        ])
      ).rejects.toThrow('flow job data key is reserved: __parentId');
      await expect(
        flow.addTree({
          name: 'forged-tree',
          queueName: queue.name,
          data: { __parentQueue: 'forged' },
        })
      ).rejects.toThrow('flow job data key is reserved: __parentQueue');
    }, 20_000);

    test('getParentResult and getParentResults read completed parents', async () => {
      harness = await startHarness('flow-patterns', mode);
      const queue = harness.queue<FlowJobData>('pipeline');
      const flow = harness.flow();
      const chainResults: unknown[] = [];
      const mergeResults: Array<Map<string, unknown>> = [];

      harness.worker<FlowJobData, unknown>(
        queue.name,
        async (job) => {
          if (job.data.__flowParentId) {
            chainResults.push(await flow.getParentResult(job.data.__flowParentId));
          }
          if (job.data.__flowParentIds) {
            mergeResults.push(await flow.getParentResults(job.data.__flowParentIds));
          }
          return { from: job.name };
        },
        { concurrency: 4 }
      );

      await flow.addChain([
        { name: 'first', queueName: queue.name, data: {} },
        { name: 'second', queueName: queue.name, data: {} },
      ]);
      await waitUntil(() => chainResults.length === 1, 'the chained parent result', 30_000);
      expect(chainResults[0]).toEqual({ from: 'first' });

      await flow.addBulkThen(
        [
          { name: 'p1', queueName: queue.name, data: {} },
          { name: 'p2', queueName: queue.name, data: {} },
        ],
        { name: 'final', queueName: queue.name, data: {} }
      );
      await waitUntil(() => mergeResults.length === 1, 'the merged parent results', 30_000);
      expect(
        [...mergeResults[0].values()].sort((a, b) =>
          JSON.stringify(a).localeCompare(JSON.stringify(b))
        )
      ).toEqual([{ from: 'p1' }, { from: 'p2' }]);
    }, 60_000);

    test('queuesOptions set per-queue defaults and per-job opts win', async () => {
      harness = await startHarness('flow-patterns', mode);
      const api = harness.queue('api');
      const cpu = harness.queue('cpu');
      const reports = harness.queue('reports');
      const flow = harness.flow();

      const node = await flow.add(
        {
          name: 'report',
          queueName: reports.name,
          data: {},
          children: [
            { name: 'fetch', queueName: api.name, data: {}, opts: { priority: 10 } },
            { name: 'render', queueName: cpu.name, data: {} },
          ],
        },
        {
          queuesOptions: {
            [api.name]: { attempts: 5, backoff: 2000 },
            [cpu.name]: { timeout: 60_000, attempts: 2 },
          },
        }
      );

      const fetchJob = await api.getJob(node.children?.[0].job.id ?? '');
      const renderJob = await cpu.getJob(node.children?.[1].job.id ?? '');
      expect(fetchJob?.opts.attempts).toBe(5);
      expect(fetchJob?.opts.backoff).toBe(2000);
      expect(fetchJob?.opts.priority).toBe(10); // per-job option preserved
      expect(renderJob?.opts.timeout).toBe(60_000);
      expect(renderJob?.opts.attempts).toBe(2);
    }, 30_000);

    test('jobId is rejected inside queuesOptions', async () => {
      harness = await startHarness('flow-patterns', mode);
      const queue = harness.queue('reports');
      const flow = harness.flow();

      await expect(
        flow.add(
          {
            name: 'report',
            queueName: queue.name,
            data: {},
            children: [{ name: 'fetch', queueName: queue.name, data: {} }],
          },
          { queuesOptions: { [queue.name]: { jobId: 'shared-id' } } }
        )
      ).rejects.toThrow();
    }, 20_000);

    test('a delayed chained step still waits for its dependency', async () => {
      harness = await startHarness('flow-patterns', mode);
      const queue = harness.queue<FlowJobData>('pipeline');
      const flow = harness.flow();
      const order: string[] = [];

      harness.worker<FlowJobData, unknown>(
        queue.name,
        async (job) => {
          order.push(job.name);
          if (job.name === 'slow-first') await Bun.sleep(800);
          return { step: job.name };
        },
        { concurrency: 4 }
      );

      await flow.addChain([
        { name: 'slow-first', queueName: queue.name, data: {} },
        { name: 'delayed-second', queueName: queue.name, data: {}, opts: { delay: 50 } },
      ]);

      await waitUntil(() => order.length === 2, 'both chained steps', 30_000);
      expect(order).toEqual(['slow-first', 'delayed-second']);
    }, 60_000);

    test('getFlow traverses a graph with depth and maxChildren bounds', async () => {
      harness = await startHarness('flow-patterns', mode);
      const queue = harness.queue('reports');
      const flow = harness.flow();

      const node = await flow.add({
        name: 'parent',
        queueName: queue.name,
        data: {},
        children: [
          { name: 'child-a', queueName: queue.name, data: {} },
          { name: 'child-b', queueName: queue.name, data: {} },
          { name: 'child-c', queueName: queue.name, data: {} },
        ],
      });

      const full = await flow.getFlow({ id: node.job.id, queueName: queue.name, depth: 2 });
      expect(full?.job.id).toBe(node.job.id);
      expect(full?.children).toHaveLength(3);

      const capped = await flow.getFlow({
        id: node.job.id,
        queueName: queue.name,
        depth: 2,
        maxChildren: 2,
      });
      expect(capped?.children).toHaveLength(2);

      const rootOnly = await flow.getFlow({
        id: node.job.id,
        queueName: queue.name,
        maxChildren: 0,
      });
      expect(rootOnly?.job.id).toBe(node.job.id);
      expect(rootOnly?.children ?? []).toHaveLength(0);
    }, 30_000);

    test('getFlow returns null for a missing root or a queue mismatch', async () => {
      harness = await startHarness('flow-patterns', mode);
      const queue = harness.queue('reports');
      const other = harness.queue('other');
      const flow = harness.flow();

      const node = await flow.add({
        name: 'parent',
        queueName: queue.name,
        data: {},
        children: [{ name: 'child', queueName: queue.name, data: {} }],
      });

      expect(await flow.getFlow({ id: 'does-not-exist', queueName: queue.name })).toBeNull();
      expect(await flow.getFlow({ id: node.job.id, queueName: other.name })).toBeNull();
    }, 30_000);

    test('cross-queue dependency keys use the child queue', async () => {
      harness = await startHarness('flow-patterns', mode);
      const parentQueue = harness.queue('reports');
      const childQueue = harness.queue('api');
      const flow = harness.flow();

      const node = await flow.add({
        name: 'parent',
        queueName: parentQueue.name,
        data: {},
        children: [{ name: 'child', queueName: childQueue.name, data: {} }],
      });

      const dependencies = await parentQueue.getJobDependencies(node.job.id);
      const childId = node.children?.[0].job.id ?? '';
      expect(dependencies.unprocessed).toContain(`${childQueue.name}:${childId}`);
    }, 30_000);
  });
}
