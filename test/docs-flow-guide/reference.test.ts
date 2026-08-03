/**
 * Executable proof for /guide/flow/reference/
 * ("Flow Producer Reference: Methods and Shapes").
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

const PRODUCER_METHODS = [
  'add',
  'addBulk',
  'addBulkThen',
  'addChain',
  'addTree',
  'close',
  'disconnect',
  'getFlow',
  'getParentResult',
  'getParentResults',
  'waitUntilReady',
];

const JOB_HELPERS = [
  'getChildrenValues',
  'getFailedChildrenValues',
  'getIgnoredChildrenFailures',
  'removeChildDependency',
  'removeUnprocessedChildren',
];

for (const mode of MODES) {
  describe(`flow guide · reference [${mode}]`, () => {
    test('every documented FlowProducer method exists', async () => {
      harness = await startHarness('flow-reference', mode);
      const flow = harness.flow() as unknown as Record<string, unknown>;

      for (const method of PRODUCER_METHODS) {
        expect(typeof flow[method], method).toBe('function');
      }
      expect(typeof (flow as { on?: unknown }).on).toBe('function'); // EventEmitter
      expect('closing' in flow).toBe(true);
    });

    test('waitUntilReady resolves and close() is idempotent', async () => {
      harness = await startHarness('flow-reference', mode);
      const flow = harness.flow();

      await flow.waitUntilReady();
      await flow.close();
      await flow.close(); // documented alias/idempotent shutdown
      await flow.disconnect();
    }, 20_000);

    test('closing exposes the one shutdown promise only after close starts', async () => {
      harness = await startHarness('flow-reference', mode);
      const flow = harness.flow();
      const state = flow as unknown as { closing: Promise<void> | null };

      const before = state.closing;
      const first = flow.close();
      const during = state.closing;
      const second = flow.disconnect();

      expect(before).toBeNull();
      expect(during).toBe(first);
      expect(second).toBe(first);
      await first;
      expect(state.closing).toBe(first);
    }, 20_000);

    test('every documented job helper exists inside a processor', async () => {
      harness = await startHarness('flow-reference', mode);
      const queue = harness.queue('reports');
      const flow = harness.flow();
      let helpers: string[] = [];

      harness.worker(queue.name, async (job) => {
        const candidate = job as unknown as Record<string, unknown>;
        helpers = JOB_HELPERS.filter((name) => typeof candidate[name] === 'function');
        return { ok: true };
      });
      const node = await flow.add({
        name: 'parent',
        queueName: queue.name,
        data: {},
        children: [{ name: 'child', queueName: queue.name, data: {} }],
      });
      await waitForState(queue, node.job.id, 'completed', 20_000);

      expect(helpers.sort()).toEqual([...JOB_HELPERS].sort());
    }, 30_000);

    test('getParentResults preserves 0, false, empty string and null', async () => {
      harness = await startHarness('flow-reference', mode);
      const queue = harness.queue<{ kind: string }>('values');
      const flow = harness.flow();

      harness.worker<{ kind: string }, unknown>(
        queue.name,
        async (job) => {
          switch (job.data.kind) {
            case 'zero':
              return 0;
            case 'false':
              return false;
            case 'empty':
              return '';
            case 'null':
              return null;
            default:
              return { merged: true };
          }
        },
        { concurrency: 4 }
      );

      const { parallelIds } = await flow.addBulkThen(
        [
          { name: 'zero', queueName: queue.name, data: { kind: 'zero' } },
          { name: 'false', queueName: queue.name, data: { kind: 'false' } },
          { name: 'empty', queueName: queue.name, data: { kind: 'empty' } },
          { name: 'null', queueName: queue.name, data: { kind: 'null' } },
        ],
        { name: 'merge', queueName: queue.name, data: { kind: 'merge' } }
      );

      await waitUntil(
        async () => (await queue.getCompletedCount()) >= 4,
        'every parent to complete',
        30_000
      );
      const results = await flow.getParentResults(parallelIds);
      const values = parallelIds.map((id) => results.get(id));
      expect(values).toContain(0);
      expect(values).toContain(false);
      expect(values).toContain('');
      expect(values).toContain(null);
    }, 60_000);

    test('an unknown id is omitted from getParentResults and undefined alone', async () => {
      harness = await startHarness('flow-reference', mode);
      const flow = harness.flow();

      expect(await flow.getParentResult('does-not-exist')).toBeUndefined();
      const results = await flow.getParentResults(['does-not-exist', 'also-missing']);
      expect(results.size).toBe(0);
    }, 20_000);

    test('a FlowStep accepts name, queueName, data, opts and children', async () => {
      harness = await startHarness('flow-reference', mode);
      const queue = harness.queue('steps');
      const flow = harness.flow();

      const { jobIds } = await flow.addTree({
        name: 'root',
        queueName: queue.name,
        data: { level: 0 },
        opts: { priority: 5 },
        children: [
          { name: 'leaf', queueName: queue.name, data: { level: 1 }, opts: { delay: 10 } },
        ],
      });

      expect(jobIds).toHaveLength(2);
      const root = await queue.getJob(jobIds[0]);
      expect(root?.opts.priority).toBe(5);
    }, 30_000);

    test('a FlowJob may omit data', async () => {
      harness = await startHarness('flow-reference', mode);
      const queue = harness.queue('optional-data');
      const flow = harness.flow();

      const node = await flow.add({
        name: 'parent',
        queueName: queue.name,
        children: [{ name: 'child', queueName: queue.name }],
      });

      expect(node.job.id).toBeString();
      expect(node.children).toHaveLength(1);
    }, 20_000);

    test('removeChildDependency detaches a child and promotes the parent', async () => {
      harness = await startHarness('flow-reference', mode);
      const queue = harness.queue('detach');
      const flow = harness.flow();
      let detached = false;

      harness.worker(queue.name, async (job) => {
        if (job.name === 'child') {
          detached = await job.removeChildDependency();
          return { detached };
        }
        return { parent: true };
      });
      const node = await flow.add({
        name: 'parent',
        queueName: queue.name,
        data: {},
        children: [{ name: 'child', queueName: queue.name, data: {} }],
      });

      await waitForState(queue, node.job.id, 'completed', 30_000);
      expect(detached).toBe(true);
    }, 60_000);
  });
}
