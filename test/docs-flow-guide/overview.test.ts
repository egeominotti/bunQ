/**
 * Executable proof for /guide/flow/ ("Flow Producer: Parent and Child Jobs").
 */

import { afterEach, describe, expect, test } from 'bun:test';
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

type ReportData = { month?: string; source?: 'sales' | 'costs' };

for (const mode of MODES) {
  describe(`flow guide · overview [${mode}]`, () => {
    test('the quick start runs children first and hands their results to the parent', async () => {
      harness = await startHarness('flow-overview', mode);
      const queue = harness.queue<ReportData>('reports');
      const flow = harness.flow();
      const rowsBySource = { sales: [120, 80], costs: [50, 20] };

      harness.worker<ReportData, unknown>(queue.name, async (job) => {
        if (job.name === 'build-report') {
          const values = await job.getChildrenValues<{ rows: number[] }>();
          return { report: Object.values(values) };
        }
        if (!job.data.source) throw new Error('source is required');
        return { rows: rowsBySource[job.data.source] };
      });

      const node = await flow.add<ReportData>({
        name: 'build-report',
        queueName: queue.name,
        data: { month: '2026-01' },
        children: [
          { name: 'fetch-sales', queueName: queue.name, data: { source: 'sales' } },
          { name: 'fetch-costs', queueName: queue.name, data: { source: 'costs' } },
        ],
      });

      const result = (await node.job.waitUntilFinished(null, 20_000)) as {
        report: Array<{ rows: number[] }>;
      };
      expect(result.report).toHaveLength(2);
      expect(
        result.report
          .map((entry) => entry.rows)
          .flat()
          .sort((a, b) => a - b)
      ).toEqual([20, 50, 80, 120]);
    }, 40_000);

    test('flow.add returns a JobNode tree', async () => {
      harness = await startHarness('flow-overview', mode);
      const queue = harness.queue('reports');
      const flow = harness.flow();

      const node = await flow.add({
        name: 'parent',
        queueName: queue.name,
        data: {},
        children: [
          { name: 'child-a', queueName: queue.name, data: {} },
          { name: 'child-b', queueName: queue.name, data: {} },
        ],
      });

      expect(node.job.id).toBeString();
      expect(node.job.name).toBe('parent');
      expect(node.children).toHaveLength(2);
      expect(node.children?.map((child) => child.job.name)).toEqual(['child-a', 'child-b']);
      expect(node.children?.[0].job.id).not.toBe(node.job.id);
    }, 20_000);

    test('the parent waits in waiting-children until every child finishes', async () => {
      harness = await startHarness('flow-overview', mode);
      const queue = harness.queue('reports');
      const flow = harness.flow();
      let release = (): void => undefined;
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });

      harness.worker(queue.name, async (job) => {
        if (job.name === 'child-slow') await held;
        return { done: job.name };
      });
      const node = await flow.add({
        name: 'parent',
        queueName: queue.name,
        data: {},
        children: [
          { name: 'child-fast', queueName: queue.name, data: {} },
          { name: 'child-slow', queueName: queue.name, data: {} },
        ],
      });

      await waitUntil(
        async () => (await queue.getJobState(node.job.id)) === 'waiting-children',
        'the parent to be parked'
      );
      await Bun.sleep(300);
      expect(await queue.getJobState(node.job.id)).toBe('waiting-children');

      release();
      await waitForState(queue, node.job.id, 'completed', 20_000);
    }, 40_000);

    test('addBulk commits every tree atomically', async () => {
      harness = await startHarness('flow-overview', mode);
      const queue = harness.queue('reports');
      const flow = harness.flow();

      const nodes = await flow.addBulk([
        {
          name: 'parent-1',
          queueName: queue.name,
          data: {},
          children: [{ name: 'child-1', queueName: queue.name, data: {} }],
        },
        {
          name: 'parent-2',
          queueName: queue.name,
          data: {},
          children: [{ name: 'child-2', queueName: queue.name, data: {} }],
        },
      ]);

      expect(nodes).toHaveLength(2);
      const counts = await queue.getJobCountsAsync();
      expect(counts.waiting + counts['waiting-children']).toBe(4);
    }, 20_000);

    test('a rejected graph leaves nothing behind', async () => {
      harness = await startHarness('flow-overview', mode);
      const queue = harness.queue('reports');
      const flow = harness.flow();

      await expect(
        flow.add({
          name: 'parent',
          queueName: queue.name,
          data: {},
          children: [
            { name: 'ok', queueName: queue.name, data: {} },
            // `:` is rejected inside a flow jobId
            { name: 'bad', queueName: queue.name, data: {}, opts: { jobId: 'has:colon' } },
          ],
        })
      ).rejects.toThrow();

      await Bun.sleep(200);
      expect(await queue.countAsync()).toBe(0);
      expect((await queue.getJobCountsAsync())['waiting-children']).toBe(0);
    }, 20_000);

    test('an empty jobId is rejected', async () => {
      harness = await startHarness('flow-overview', mode);
      const queue = harness.queue('reports');
      const flow = harness.flow();

      await expect(
        flow.add({
          name: 'parent',
          queueName: queue.name,
          data: {},
          opts: { jobId: '' },
          children: [{ name: 'child', queueName: queue.name, data: {} }],
        })
      ).rejects.toThrow();
    }, 20_000);

    test('reusing an existing job id rejects the whole request', async () => {
      harness = await startHarness('flow-overview', mode);
      const queue = harness.queue('reports');
      const flow = harness.flow();
      const id = `taken-${crypto.randomUUID().slice(0, 8)}`;
      await queue.add('existing', {}, { jobId: id, durable: true });

      await expect(
        flow.add({
          name: 'parent',
          queueName: queue.name,
          data: {},
          children: [{ name: 'child', queueName: queue.name, data: {}, opts: { jobId: id } }],
        })
      ).rejects.toThrow();

      expect(await queue.countAsync()).toBe(1);
    }, 20_000);

    test('repeat, deduplication, debounce and parent are rejected inside a flow', async () => {
      harness = await startHarness('flow-overview', mode);
      const queue = harness.queue('reports');
      const flow = harness.flow();

      const rejected = [
        { repeat: { every: 1_000 } },
        { deduplication: { id: 'dedup-key', ttl: 1_000 } },
        { debounce: { id: 'debounce-key', ttl: 1_000 } },
        { parent: { id: 'some-id', queue: queue.name } },
      ];

      for (const opts of rejected) {
        await expect(
          flow.add({
            name: 'parent',
            queueName: queue.name,
            data: {},
            children: [{ name: 'child', queueName: queue.name, data: {}, opts }],
          }),
          JSON.stringify(opts)
        ).rejects.toThrow();
      }
      expect(await queue.countAsync()).toBe(0);
    }, 30_000);

    test('the four child-failure policies are mutually exclusive', async () => {
      harness = await startHarness('flow-overview', mode);
      const queue = harness.queue('reports');
      const flow = harness.flow();

      await expect(
        flow.add({
          name: 'parent',
          queueName: queue.name,
          data: {},
          children: [
            {
              name: 'child',
              queueName: queue.name,
              data: {},
              opts: { failParentOnFailure: true, ignoreDependencyOnFailure: true },
            },
          ],
        })
      ).rejects.toThrow();
    }, 20_000);

    test('data.name is preserved while reserved flow metadata is rejected atomically', async () => {
      harness = await startHarness('flow-overview', mode);
      const queue = harness.queue('reports');
      const flow = harness.flow();

      const accepted = await flow.add({
        name: 'parent',
        queueName: queue.name,
        data: {},
        children: [{ name: 'child', queueName: queue.name, data: { name: 'user-name' } }],
      });
      const childId = accepted.children?.[0]?.job.id ?? '';
      expect(accepted.children?.[0]?.job.data).toEqual({ name: 'user-name' });
      expect((await queue.getJob(childId))?.data).toMatchObject({ name: 'user-name' });
      const countBeforeRejection = await queue.countAsync();

      for (const data of [{ __parentId: 'nope' }, { __custom: 'reserved-in-flow' }]) {
        await expect(
          flow.add({
            name: 'rejected-parent',
            queueName: queue.name,
            data: {},
            children: [{ name: 'rejected-child', queueName: queue.name, data }],
          }),
          JSON.stringify(data)
        ).rejects.toThrow();
        expect(await queue.countAsync()).toBe(countBeforeRejection);
      }
    }, 20_000);

    test('the depth limit is enforced', async () => {
      harness = await startHarness('flow-overview', mode);
      const queue = harness.queue('reports');
      const flow = harness.flow();

      type Node = { name: string; queueName: string; data: unknown; children?: Node[] };
      const build = (depth: number): Node => ({
        name: `level-${depth}`,
        queueName: queue.name,
        data: {},
        children: depth > 0 ? [build(depth - 1)] : undefined,
      });

      // 100 edges below the root is the documented ceiling.
      await expect(flow.add(build(150))).rejects.toThrow();
    }, 30_000);
  });
}
