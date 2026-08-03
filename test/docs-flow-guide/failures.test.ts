/**
 * Executable proof for /guide/flow/failures/
 * ("Flow Failure Handling: When a Child Fails").
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

type StepData = { fail?: boolean; slow?: boolean };

for (const mode of MODES) {
  describe(`flow guide · child failures [${mode}]`, () => {
    test('by default a parent keeps waiting when a child fails', async () => {
      harness = await startHarness('flow-failures', mode);
      const queue = harness.queue<StepData>('main');
      const flow = harness.flow();

      harness.worker<StepData, unknown>(queue.name, async (job) => {
        if (job.data.fail) throw new Error(`${job.name} failed`);
        return { ok: job.name };
      });
      const node = await flow.add({
        name: 'parent',
        queueName: queue.name,
        data: {},
        children: [
          { name: 'bad', queueName: queue.name, data: { fail: true }, opts: { attempts: 1 } },
          { name: 'good', queueName: queue.name, data: {} },
        ],
      });

      await waitUntil(
        async () => (await queue.getFailedCount()) === 1,
        'the child to fail',
        30_000
      );
      await Bun.sleep(500);
      expect(await queue.getJobState(node.job.id)).toBe('waiting-children');
    }, 60_000);

    test('failParentOnFailure fails the parent immediately', async () => {
      harness = await startHarness('flow-failures', mode);
      const queue = harness.queue<StepData>('main');
      const flow = harness.flow();
      let release = (): void => undefined;
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });

      harness.worker<StepData, unknown>(
        queue.name,
        async (job) => {
          if (job.data.fail) throw new Error(`${job.name} failed`);
          if (job.data.slow) await held;
          return { ok: job.name };
        },
        { concurrency: 4 }
      );
      const node = await flow.add({
        name: 'parent',
        queueName: queue.name,
        data: {},
        children: [
          {
            name: 'bad',
            queueName: queue.name,
            data: { fail: true },
            opts: { attempts: 1, failParentOnFailure: true },
          },
          { name: 'still-running', queueName: queue.name, data: { slow: true } },
        ],
      });

      await waitForState(queue, node.job.id, 'failed', 30_000);
      release();
    }, 60_000);

    test('removeDependencyOnFailure drops the failed child silently', async () => {
      harness = await startHarness('flow-failures', mode);
      const queue = harness.queue<StepData>('main');
      const flow = harness.flow();
      let ignored: Record<string, unknown> = { untouched: true };

      harness.worker<StepData, unknown>(
        queue.name,
        async (job) => {
          if (job.data.fail) throw new Error(`${job.name} failed`);
          if (job.name === 'parent') ignored = await job.getIgnoredChildrenFailures();
          return { ok: job.name };
        },
        { concurrency: 4 }
      );
      const node = await flow.add({
        name: 'parent',
        queueName: queue.name,
        data: {},
        children: [
          {
            name: 'bad',
            queueName: queue.name,
            data: { fail: true },
            opts: { attempts: 1, removeDependencyOnFailure: true },
          },
          { name: 'good', queueName: queue.name, data: {} },
        ],
      });

      await waitForState(queue, node.job.id, 'completed', 30_000);
      expect(ignored).toEqual({}); // dropped, not recorded
    }, 60_000);

    test('ignoreDependencyOnFailure records the failure for the parent', async () => {
      harness = await startHarness('flow-failures', mode);
      const queue = harness.queue<StepData>('reports');
      const flow = harness.flow();
      let ignored: Record<string, unknown> = {};

      harness.worker<StepData, unknown>(
        queue.name,
        async (job) => {
          if (job.data.fail) throw new Error('enrichment API timeout');
          if (job.name === 'parent') ignored = await job.getIgnoredChildrenFailures();
          return { ok: job.name };
        },
        { concurrency: 4 }
      );
      const node = await flow.add({
        name: 'parent',
        queueName: queue.name,
        data: {},
        children: [
          {
            name: 'enrich',
            queueName: queue.name,
            data: { fail: true },
            opts: { attempts: 1, ignoreDependencyOnFailure: true },
          },
          { name: 'good', queueName: queue.name, data: {} },
        ],
      });

      await waitForState(queue, node.job.id, 'completed', 30_000);
      const keys = Object.keys(ignored);
      expect(keys).toHaveLength(1);
      expect(keys[0]).toContain(`${queue.name}:`);
      expect(String(Object.values(ignored)[0])).toContain('enrichment API timeout');
    }, 60_000);

    test('continueParentOnFailure promotes the parent and exposes the failures', async () => {
      harness = await startHarness('flow-failures', mode);
      const queue = harness.queue<StepData>('main');
      const flow = harness.flow();
      let failedValues: Record<string, unknown> = {};
      let release = (): void => undefined;
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });

      harness.worker<StepData, unknown>(
        queue.name,
        async (job) => {
          if (job.data.fail) throw new Error(`${job.name} failed`);
          if (job.data.slow) await held;
          if (job.name === 'pipeline') {
            failedValues = await job.getFailedChildrenValues();
            if (Object.keys(failedValues).length > 0) {
              await job.removeUnprocessedChildren();
              return { status: 'partial', failedSteps: failedValues };
            }
            return { status: 'complete' };
          }
          return { ok: job.name };
        },
        { concurrency: 4 }
      );
      const node = await flow.add({
        name: 'pipeline',
        queueName: queue.name,
        data: {},
        children: [
          {
            name: 'step-a',
            queueName: queue.name,
            data: { fail: true },
            opts: { attempts: 1, continueParentOnFailure: true },
          },
          { name: 'step-c', queueName: queue.name, data: { slow: true } },
        ],
      });

      const result = (await node.job.waitUntilFinished(null, 30_000)) as { status: string };
      expect(result.status).toBe('partial');
      expect(Object.keys(failedValues)).toHaveLength(1);
      expect(String(Object.values(failedValues)[0])).toContain('step-a failed');
      release();
    }, 60_000);

    test('removeUnprocessedChildren cancels only the pending children', async () => {
      harness = await startHarness('flow-failures', mode);
      const queue = harness.queue<StepData>('main');
      const flow = harness.flow();
      const processed: string[] = [];

      harness.worker<StepData, unknown>(
        queue.name,
        async (job) => {
          processed.push(job.name);
          if (job.data.fail) throw new Error(`${job.name} failed`);
          if (job.name === 'pipeline') {
            await job.removeUnprocessedChildren();
            return { done: true };
          }
          return { ok: job.name };
        },
        { concurrency: 1 } // one at a time, so the third child is still waiting
      );
      const node = await flow.add({
        name: 'pipeline',
        queueName: queue.name,
        data: {},
        children: [
          {
            name: 'failing',
            queueName: queue.name,
            data: { fail: true },
            opts: { attempts: 1, continueParentOnFailure: true },
          },
          { name: 'pending-1', queueName: queue.name, data: {} },
          { name: 'pending-2', queueName: queue.name, data: {} },
        ],
      });

      await waitForState(queue, node.job.id, 'completed', 30_000);
      await Bun.sleep(400);
      expect(processed).toContain('pipeline');
    }, 60_000);

    test('two failure policies on one child are rejected', async () => {
      harness = await startHarness('flow-failures', mode);
      const queue = harness.queue<StepData>('main');
      const flow = harness.flow();

      const pairs: Array<Record<string, boolean>> = [
        { failParentOnFailure: true, removeDependencyOnFailure: true },
        { removeDependencyOnFailure: true, ignoreDependencyOnFailure: true },
        { ignoreDependencyOnFailure: true, continueParentOnFailure: true },
        { failParentOnFailure: true, continueParentOnFailure: true },
      ];
      for (const opts of pairs) {
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
    }, 30_000);
  });
}
