/**
 * Executable counterparts of the Bun examples in the public FlowProducer guide.
 * Provider/network work is replaced with deterministic in-memory values; the
 * public FlowProducer, Worker, Job, option, traversal, and failure APIs are real.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { FlowProducer, Queue, Worker, shutdownManager } from '../src/client';
import { getSharedManager } from '../src/client/manager';
import { closeAllSharedPools } from '../src/client/tcpPool';
import { jobId } from '../src/domain/types/job';

afterEach(() => {
  shutdownManager();
  closeAllSharedPools();
});

async function pullEventually(queue: string, timeoutMs = 2_000) {
  const manager = getSharedManager();
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const job = await manager.pull(queue);
    if (job) return job;
    await Bun.sleep(5);
  }
  throw new Error(`No job became ready on ${queue}`);
}

describe('docs: FlowProducer', () => {
  test('Quick Start runs children first and the parent reads both results', async () => {
    type ReportData = {
      month?: string;
      source?: 'sales' | 'costs';
    };
    const queueName = 'docs-flow-reports';
    const flow = new FlowProducer({ embedded: true });
    const rowsBySource = { sales: [120, 80], costs: [50, 20] };
    const order: string[] = [];
    const worker = new Worker<ReportData>(
      queueName,
      async (job) => {
        order.push(job.name);
        if (job.name === 'build-report') {
          const values = await job.getChildrenValues<{ rows: number[] }>();
          return { report: Object.values(values) };
        }
        if (!job.data.source) throw new Error('source is required');
        return { rows: rowsBySource[job.data.source] };
      },
      { embedded: true }
    );

    try {
      const node = await flow.add<ReportData>({
        name: 'build-report',
        queueName,
        data: { month: '2026-01' },
        children: [
          { name: 'fetch-sales', queueName, data: { source: 'sales' } },
          { name: 'fetch-costs', queueName, data: { source: 'costs' } },
        ],
      });
      const result = (await node.job.waitUntilFinished(null, 10_000)) as {
        report: Array<{ rows: number[] }>;
      };

      expect(result.report.map((entry) => entry.rows).sort()).toEqual(
        [
          [120, 80],
          [50, 20],
        ].sort()
      );
      expect(order.at(-1)).toBe('build-report');
      expect(new Set(order.slice(0, -1))).toEqual(new Set(['fetch-sales', 'fetch-costs']));
    } finally {
      await worker.close();
      await flow.close();
    }
  }, 15_000);

  test('chain, fan-in, and parent-first tree execute in their documented order', async () => {
    const flow = new FlowProducer({ embedded: true });
    const manager = getSharedManager();
    try {
      const chain = await flow.addChain([
        { name: 'fetch', queueName: 'docs-flow-chain', data: {} },
        { name: 'process', queueName: 'docs-flow-chain', data: {} },
        { name: 'store', queueName: 'docs-flow-chain', data: {} },
      ]);
      for (const expectedId of chain.jobIds) {
        const job = await pullEventually('docs-flow-chain');
        expect(String(job.id)).toBe(expectedId);
        await manager.ack(job.id, { step: expectedId });
      }

      const fanIn = await flow.addBulkThen(
        [
          { name: 'api-1', queueName: 'docs-flow-parallel', data: {} },
          { name: 'api-2', queueName: 'docs-flow-parallel', data: {} },
        ],
        { name: 'merge', queueName: 'docs-flow-final', data: {} }
      );
      const parallel = [
        await pullEventually('docs-flow-parallel'),
        await pullEventually('docs-flow-parallel'),
      ];
      expect(new Set(parallel.map((job) => String(job.id)))).toEqual(new Set(fanIn.parallelIds));
      for (const job of parallel) await manager.ack(job.id, String(job.id));
      expect(String((await pullEventually('docs-flow-final')).id)).toBe(fanIn.finalId);

      const tree = await flow.addTree({
        name: 'root',
        queueName: 'docs-flow-tree',
        data: {},
        children: [{ name: 'leaf', queueName: 'docs-flow-tree', data: {} }],
      });
      const root = await pullEventually('docs-flow-tree');
      expect(String(root.id)).toBe(tree.jobIds[0]);
      await manager.ack(root.id, 'root-result');
      expect(String((await pullEventually('docs-flow-tree')).id)).toBe(tree.jobIds[1]);
    } finally {
      await flow.close();
    }
  });

  test('queue defaults, getFlow bounds, and embedded parent results are observable', async () => {
    const flow = new FlowProducer({ embedded: true });
    const manager = getSharedManager();
    try {
      const node = await flow.add(
        {
          name: 'report',
          queueName: 'docs-flow-parent',
          data: {},
          children: [
            {
              name: 'fetch',
              queueName: 'docs-flow-api',
              data: {},
              opts: { priority: 10 },
            },
          ],
        },
        { queuesOptions: { 'docs-flow-api': { attempts: 5, backoff: 2_000 } } }
      );
      expect(node.children![0].job.priority).toBe(10);
      expect(node.children![0].job.opts.attempts).toBe(5);
      expect(node.children![0].job.opts.backoff).toBe(2_000);

      const rootOnly = await flow.getFlow({
        id: node.job.id,
        queueName: 'docs-flow-parent',
        depth: 2,
        maxChildren: 0,
      });
      expect(rootOnly?.children).toBeUndefined();

      const child = await pullEventually('docs-flow-api');
      await manager.ack(child.id, { rows: [1, 2] });
      expect(flow.getParentResult<{ rows: number[] }>(String(child.id))).toEqual({
        rows: [1, 2],
      });
    } finally {
      await flow.close();
    }
  });

  test('continue and ignore failure policies expose durable-shaped error maps', async () => {
    const flow = new FlowProducer({ embedded: true });
    const manager = getSharedManager();
    try {
      const continued = await flow.add({
        name: 'pipeline',
        queueName: 'docs-flow-main',
        data: {},
        children: [
          {
            name: 'step-a',
            queueName: 'docs-flow-workers',
            data: {},
            opts: { continueParentOnFailure: true, attempts: 1 },
          },
          { name: 'step-b', queueName: 'docs-flow-workers', data: {} },
        ],
      });
      const failed = await pullEventually('docs-flow-workers');
      await manager.fail(failed.id, 'step-a failed', undefined, true);
      const parent = await pullEventually('docs-flow-main');
      const parentNode = parent.id === jobId(continued.job.id) ? continued.job : null;
      expect(parentNode).not.toBeNull();
      expect(await parentNode!.getFailedChildrenValues()).toEqual({
        [`docs-flow-workers:${String(failed.id)}`]: 'step-a failed',
      });
      await parentNode!.removeUnprocessedChildren();

      const ignored = await flow.add({
        name: 'optional-parent',
        queueName: 'docs-flow-ignore-parent',
        data: {},
        children: [
          {
            name: 'optional-child',
            queueName: 'docs-flow-ignore-child',
            data: {},
            opts: { ignoreDependencyOnFailure: true, attempts: 1 },
          },
        ],
      });
      const optional = await pullEventually('docs-flow-ignore-child');
      await manager.fail(optional.id, 'enrichment timeout', undefined, true);
      await pullEventually('docs-flow-ignore-parent');
      expect(await ignored.job.getIgnoredChildrenFailures()).toEqual({
        [`docs-flow-ignore-child:${String(optional.id)}`]: 'enrichment timeout',
      });
    } finally {
      await flow.close();
    }
  });
});
