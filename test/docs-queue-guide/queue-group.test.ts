/**
 * Executable proof for /guide/queue-group/
 * ("QueueGroup: Namespace Related Queues").
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { QueueGroup } from '../../src/client';
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

function groupName(label: string): string {
  return `${label}-${crypto.randomUUID().slice(0, 8)}`;
}

for (const mode of MODES) {
  describe(`queue guide · queue group [${mode}]`, () => {
    test('queues created through a group carry the group prefix', async () => {
      harness = await startHarness('queue-group', mode);
      const billing = new QueueGroup(groupName('billing'));
      const invoices = billing.getQueue('invoices', harness.queueOptions());
      const payments = billing.getQueue('payments', harness.queueOptions());
      harness.addCleanup(() => invoices.close());
      harness.addCleanup(() => payments.close());

      expect(billing.prefix).toBe(`${billing.prefix.slice(0, -1)}:`);
      expect(invoices.name).toBe(`${billing.prefix}invoices`);
      expect(payments.name).toBe(`${billing.prefix}payments`);

      const job = await invoices.add('create', { amount: 100 }, { durable: true });
      await payments.add('process', { orderId: '123' }, { durable: true });

      expect(await invoices.countAsync()).toBe(1);
      expect(await payments.countAsync()).toBe(1);
      expect(await invoices.getJobState(job.id)).toBe('waiting');
    });

    test('getWorker consumes the prefixed queue', async () => {
      harness = await startHarness('queue-group', mode);
      const billing = new QueueGroup(groupName('billing'));
      const invoices = billing.getQueue<{ amount: number }>('invoices', harness.queueOptions());
      harness.addCleanup(() => invoices.close());

      const seen: number[] = [];
      const worker = billing.getWorker<{ amount: number }, { processed: boolean }>(
        'invoices',
        async (job) => {
          seen.push(job.data.amount);
          return { processed: true };
        },
        harness.workerOptions()
      );
      harness.addCleanup(() => worker.close(true));

      const job = await invoices.add('create', { amount: 100 }, { durable: true });
      await waitForState(invoices, job.id, 'completed', 10_000);
      expect(seen).toEqual([100]);
    }, 20_000);

    test('two groups isolate the same logical queue name', async () => {
      harness = await startHarness('queue-group', mode);
      const tenantA = new QueueGroup(groupName('tenant-a'));
      const tenantB = new QueueGroup(groupName('tenant-b'));
      const tasksA = tenantA.getQueue('tasks', harness.queueOptions());
      const tasksB = tenantB.getQueue('tasks', harness.queueOptions());
      harness.addCleanup(() => tasksA.close());
      harness.addCleanup(() => tasksB.close());

      expect(tasksA.name).not.toBe(tasksB.name);
      await tasksA.add('only-a', {}, { durable: true });

      expect(await tasksA.countAsync()).toBe(1);
      expect(await tasksB.countAsync()).toBe(0);
    });

    test('the awaitable group operations work in this mode', async () => {
      harness = await startHarness('queue-group', mode);
      const billing = new QueueGroup(groupName('billing'));
      const invoices = billing.getQueue('invoices', harness.queueOptions());
      const payments = billing.getQueue('payments', harness.queueOptions());
      harness.addCleanup(() => invoices.close());
      harness.addCleanup(() => payments.close());

      expect(await billing.listQueuesAsync()).toEqual(['invoices', 'payments']);

      await billing.pauseAllAsync();
      expect(await invoices.isPausedAsync()).toBe(true);
      expect(await payments.isPausedAsync()).toBe(true);

      await billing.resumeAllAsync();
      expect(await invoices.isPausedAsync()).toBe(false);
      expect(await payments.isPausedAsync()).toBe(false);

      await invoices.add('a', {}, { durable: true });
      await invoices.add('b', {}, { durable: true });
      await payments.add('c', {}, { durable: true });
      expect(await billing.drainAllAsync()).toBe(3);
      expect(await invoices.countAsync()).toBe(0);
      expect(await payments.countAsync()).toBe(0);

      await invoices.add('d', {}, { durable: true });
      await billing.obliterateAllAsync();
      expect(await invoices.countAsync()).toBe(0);
    });
  });
}

describe('queue guide · queue group [embedded-only helpers]', () => {
  test('listQueues, pauseAll, resumeAll, drainAll and obliterateAll', async () => {
    harness = await startHarness('queue-group', 'embedded');
    const billing = new QueueGroup(groupName('billing'));
    const invoices = billing.getQueue('invoices', harness.queueOptions());
    const payments = billing.getQueue('payments', harness.queueOptions());
    harness.addCleanup(() => invoices.close());
    harness.addCleanup(() => payments.close());

    await invoices.add('a', {}, { durable: true });
    await payments.add('b', {}, { durable: true });
    await waitUntil(
      () => billing.listQueues().length === 2,
      'both group queues to be registered on the manager'
    );

    expect([...billing.listQueues()].sort()).toEqual(['invoices', 'payments']);

    billing.pauseAll();
    expect(invoices.isPaused()).toBe(true);
    expect(payments.isPaused()).toBe(true);

    billing.resumeAll();
    expect(invoices.isPaused()).toBe(false);
    expect(payments.isPaused()).toBe(false);

    billing.drainAll();
    expect(invoices.count()).toBe(0);
    expect(payments.count()).toBe(0);

    await invoices.add('c', {}, { durable: true });
    billing.obliterateAll();
    await waitUntil(
      async () => (await invoices.countAsync()) === 0,
      'the group data to be removed'
    );
  }, 20_000);
});
