/**
 * Executable proof for /guide/queue/options/ ("JobOptions Reference").
 *
 * Every row of the reference table is checked: the documented default when the
 * option is omitted, and the documented effect when it is set.
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

for (const mode of MODES) {
  describe(`queue guide · job options reference [${mode}]`, () => {
    test('the documented defaults apply when no options are passed', async () => {
      harness = await startHarness('options', mode);
      const queue = harness.queue('defaults');
      await queue.pauseAsync();

      const job = await queue.add('report', { key: 'value' }, { durable: true });
      const opts = (await queue.getJob(job.id))?.opts;

      expect(opts?.priority ?? 0).toBe(0);
      expect(opts?.delay ?? 0).toBe(0);
      expect(opts?.attempts).toBe(3);
      expect(opts?.backoff).toBe(1000);
      expect(opts?.removeOnComplete ?? false).toBe(false);
      expect(opts?.removeOnFail ?? false).toBe(false);
      expect(opts?.lifo ?? false).toBe(false);
      expect(opts?.timeout).toBeUndefined();
      expect(opts?.deduplication).toBeUndefined();
      expect(opts?.repeat).toBeUndefined();
      expect(opts?.stallTimeout).toBeUndefined();
      expect(opts?.parent).toBeUndefined();
    });

    test('the documented example is accepted and reflected', async () => {
      harness = await startHarness('options', mode);
      const queue = harness.queue('example');
      const customId = `report-42-${crypto.randomUUID().slice(0, 8)}`;

      const job = await queue.add(
        'report',
        { key: 'value' },
        { priority: 10, delay: 5000, attempts: 5, jobId: customId, durable: true }
      );

      expect(job.id).toBe(customId);
      const opts = (await queue.getJob(customId))?.opts;
      expect(opts?.priority).toBe(10);
      expect(opts?.delay).toBe(5000);
      expect(opts?.attempts).toBe(5);
    });

    test('removeOnFail deletes the job once it exhausts its attempts', async () => {
      harness = await startHarness('options', mode);
      const queue = harness.queue('remove-on-fail');
      harness.worker(queue.name, async () => {
        throw new Error('boom');
      });

      const kept = await queue.add('kept', {}, { attempts: 1, durable: true });
      const dropped = await queue.add(
        'dropped',
        {},
        { attempts: 1, removeOnFail: true, durable: true }
      );

      await waitForState(queue, kept.id, 'failed', 15_000);
      await waitUntil(
        async () => (await queue.getJob(dropped.id)) === null,
        'the removeOnFail job to disappear',
        15_000
      );
      expect(await queue.getJob(kept.id)).not.toBeNull();
    }, 30_000);

    test('lifo reverses the order when the queue is processed newest-first', async () => {
      harness = await startHarness('options', mode);
      const queue = harness.queue<{ label: string }>('lifo');
      await queue.pauseAsync();

      for (const label of ['first', 'second', 'newest']) {
        await queue.add(label, { label }, { lifo: true, durable: true });
      }

      const order: string[] = [];
      harness.worker<{ label: string }, boolean>(
        queue.name,
        async (job) => {
          order.push(job.data.label);
          return true;
        },
        { concurrency: 1, batchSize: 1 }
      );
      await queue.resumeAsync();

      await waitUntil(() => order.length === 3, 'all three jobs to be processed', 15_000);
      expect(order).toEqual(['newest', 'second', 'first']);
    }, 30_000);

    // Documented: "lifo | boolean | false | Process newest first". At equal
    // priority, LIFO jobs form the newest-first partition ahead of FIFO jobs.
    test('a single lifo job jumps ahead of the FIFO jobs', async () => {
      harness = await startHarness('options', mode);
      const queue = harness.queue<{ label: string }>('lifo-mixed');
      await queue.pauseAsync();

      await queue.add('first', { label: 'first' }, { durable: true });
      await queue.add('second', { label: 'second' }, { durable: true });
      await queue.add('newest', { label: 'newest' }, { lifo: true, durable: true });

      const order: string[] = [];
      harness.worker<{ label: string }, boolean>(
        queue.name,
        async (job) => {
          order.push(job.data.label);
          return true;
        },
        { concurrency: 1, batchSize: 1 }
      );
      await queue.resumeAsync();

      await waitUntil(() => order.length === 3, 'all three jobs to be processed', 15_000);
      expect(order[0]).toBe('newest');
    }, 30_000);

    test('parent records the parent reference on the child', async () => {
      harness = await startHarness('options', mode);
      const queue = harness.queue<{ role: string }>('parenting');
      await queue.pauseAsync();

      const parent = await queue.add('parent', { role: 'parent' }, { durable: true });
      const child = await queue.add(
        'child',
        { role: 'child' },
        { parent: { id: parent.id, queue: queue.name }, durable: true }
      );

      const stored = await queue.getJob(child.id);
      expect(stored?.parent?.id).toBe(parent.id);
      expect(stored?.parent?.queueQualifiedName).toBe(queue.name);
    });

    // A standalone parent option creates the same durable dependency edge used
    // by FlowProducer for an existing, still-pending parent.
    test('a parent reference holds the parent until the child finishes', async () => {
      harness = await startHarness('options', mode);
      const queue = harness.queue<{ role: string }>('parenting-dependency');
      await queue.pauseAsync();

      const parent = await queue.add('parent', { role: 'parent' }, { durable: true });
      const child = await queue.add(
        'child',
        { role: 'child' },
        { parent: { id: parent.id, queue: queue.name }, durable: true }
      );

      await waitUntil(
        async () => (await queue.getJobState(parent.id)) === 'waiting-children',
        'the parent to wait for its child'
      );

      const pending = await queue.getJobDependencies(parent.id);
      expect(pending.unprocessed).toEqual([`${queue.name}:${child.id}`]);

      const order: string[] = [];
      harness.worker<{ role: string }, string>(
        queue.name,
        async (job) => {
          order.push(job.data.role);
          return job.data.role;
        },
        { concurrency: 1, batchSize: 1 }
      );
      await queue.resumeAsync();

      await waitForState(queue, child.id, 'completed', 15_000);
      await waitForState(queue, parent.id, 'completed', 15_000);
      expect(order).toEqual(['child', 'parent']);
      expect(await parent.getChildrenValues<string>()).toEqual({
        [`${queue.name}:${child.id}`]: 'child',
      });
    }, 20_000);

    test('a parent reference applies the child failure policy', async () => {
      harness = await startHarness('options', mode);
      const queue = harness.queue<{ role: string }>('parenting-failure');
      await queue.pauseAsync();

      const parent = await queue.add('parent', { role: 'parent' }, { durable: true });
      const child = await queue.add(
        'child',
        { role: 'child' },
        {
          parent: { id: parent.id, queue: queue.name },
          failParentOnFailure: true,
          attempts: 1,
          durable: true,
        }
      );
      const processed: string[] = [];
      harness.worker<{ role: string }, boolean>(queue.name, async (job) => {
        processed.push(job.data.role);
        if (job.id === child.id) throw new Error('child failed as expected');
        return true;
      });
      await queue.resumeAsync();

      await waitForState(queue, child.id, 'failed', 15_000);
      await waitForState(queue, parent.id, 'failed', 15_000);
      expect(processed).toEqual(['child']);
    }, 30_000);

    test('stallTimeout overrides the queue stall interval for one job', async () => {
      harness = await startHarness('options', mode);
      const impatient = harness.queue('stall-override');
      const patient = harness.queue('stall-default');
      const stalled: Record<string, string[]> = { override: [], control: [] };

      for (const [label, queue] of [
        ['override', impatient],
        ['control', patient],
      ] as const) {
        await queue.setStallConfigAsync({
          enabled: true,
          stallInterval: 120_000,
          maxStalls: 5,
          gracePeriod: 0,
        });
        const worker = harness.worker(
          queue.name,
          async () => await new Promise<never>(() => undefined),
          { heartbeatInterval: 0, useLocks: true }
        );
        worker.on('error', () => undefined);
        worker.on('stalled', (jobId: string) => stalled[label].push(jobId));
      }

      const override = await impatient.add('slow', {}, { stallTimeout: 50, durable: true });
      const control = await patient.add('slow', {}, { durable: true });
      await waitForState(impatient, override.id, 'active', 10_000);
      await waitForState(patient, control.id, 'active', 10_000);

      // The broker sweeps for stalls every 5s, so the per-job override fires
      // within a few sweeps while the control job keeps the 120s queue default.
      await waitUntil(
        async () =>
          stalled.override.includes(override.id) ||
          (await impatient.getJobState(override.id)) !== 'active',
        'the job with stallTimeout to be recovered as stalled',
        40_000
      );
      expect(stalled.control).toEqual([]);
    }, 60_000);
  });
}
