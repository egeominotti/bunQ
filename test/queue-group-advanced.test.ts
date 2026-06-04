/**
 * QueueGroup Advanced Tests (Embedded Mode)
 *
 * Tests namespace isolation, group-wide operations,
 * and independent group management.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, QueueGroup, shutdownManager } from '../src/client';

describe('QueueGroup Advanced - Embedded', () => {
  afterEach(() => {
    shutdownManager();
  });

  test('namespace isolation - two groups each with two queues', async () => {
    const billing = new QueueGroup('billing');
    const shipping = new QueueGroup('shipping');

    const bInvoices = billing.getQueue<{ type: string }>('invoices');
    const bPayments = billing.getQueue<{ type: string }>('payments');
    const sOrders = shipping.getQueue<{ type: string }>('orders');
    const sTracking = shipping.getQueue<{ type: string }>('tracking');

    await bInvoices.add('inv', { type: 'billing-invoice' });
    await bPayments.add('pay', { type: 'billing-payment' });
    await sOrders.add('ord', { type: 'shipping-order' });
    await sTracking.add('trk', { type: 'shipping-tracking' });

    const received: Record<string, string[]> = {
      'billing:invoices': [],
      'billing:payments': [],
      'shipping:orders': [],
      'shipping:tracking': [],
    };

    const w1 = billing.getWorker<{ type: string }>('invoices', async (job) => {
      received['billing:invoices'].push(job.data.type);
      return {};
    });
    const w2 = billing.getWorker<{ type: string }>('payments', async (job) => {
      received['billing:payments'].push(job.data.type);
      return {};
    });
    const w3 = shipping.getWorker<{ type: string }>('orders', async (job) => {
      received['shipping:orders'].push(job.data.type);
      return {};
    });
    const w4 = shipping.getWorker<{ type: string }>('tracking', async (job) => {
      received['shipping:tracking'].push(job.data.type);
      return {};
    });

    for (let i = 0; i < 30; i++) await Bun.sleep(100);

    await w1.close();
    await w2.close();
    await w3.close();
    await w4.close();

    expect(received['billing:invoices']).toEqual(['billing-invoice']);
    expect(received['billing:payments']).toEqual(['billing-payment']);
    expect(received['shipping:orders']).toEqual(['shipping-order']);
    expect(received['shipping:tracking']).toEqual(['shipping-tracking']);
  }, 20000);

  test('listQueues returns correct names without prefix', async () => {
    const group = new QueueGroup('list-test');

    const q1 = group.getQueue('alpha');
    const q2 = group.getQueue('beta');
    const q3 = group.getQueue('gamma');

    await q1.add('task', { v: 1 });
    await q2.add('task', { v: 2 });
    await q3.add('task', { v: 3 });

    const queues = group.listQueues();

    expect(queues.length).toBe(3);
    expect(queues).toContain('alpha');
    expect(queues).toContain('beta');
    expect(queues).toContain('gamma');

    // Verify no prefix leaks through
    for (const name of queues) {
      expect(name.includes(':')).toBe(false);
    }
  }, 20000);

  test('pauseAll / resumeAll', async () => {
    const group = new QueueGroup('pause-resume');

    const q1 = group.getQueue<{ idx: number }>('q1');
    const q2 = group.getQueue<{ idx: number }>('q2');

    await q1.add('task', { idx: 1 });
    await q2.add('task', { idx: 2 });

    group.pauseAll();

    // Verify both queues are paused
    expect(q1.isPaused()).toBe(true);
    expect(q2.isPaused()).toBe(true);

    // Start workers while paused
    const processed: number[] = [];
    const w1 = group.getWorker<{ idx: number }>('q1', async (job) => {
      processed.push(job.data.idx);
      return {};
    });
    const w2 = group.getWorker<{ idx: number }>('q2', async (job) => {
      processed.push(job.data.idx);
      return {};
    });

    // Wait a bit - nothing should process while paused
    for (let i = 0; i < 10; i++) await Bun.sleep(100);
    expect(processed.length).toBe(0);

    // Resume and verify processing
    group.resumeAll();
    expect(q1.isPaused()).toBe(false);
    expect(q2.isPaused()).toBe(false);

    for (let i = 0; i < 30; i++) await Bun.sleep(100);

    await w1.close();
    await w2.close();

    expect(processed.length).toBe(2);
    expect(processed).toContain(1);
    expect(processed).toContain(2);
  }, 20000);

  test('drainAll removes all waiting jobs', async () => {
    const group = new QueueGroup('drain-grp');

    const q1 = group.getQueue<{ idx: number }>('q1');
    const q2 = group.getQueue<{ idx: number }>('q2');

    for (let i = 0; i < 10; i++) {
      await q1.add('task', { idx: i });
      await q2.add('task', { idx: i + 10 });
    }

    const counts1Before = q1.getJobCounts();
    const counts2Before = q2.getJobCounts();
    expect(counts1Before.waiting).toBe(10);
    expect(counts2Before.waiting).toBe(10);

    group.drainAll();

    const counts1After = q1.getJobCounts();
    const counts2After = q2.getJobCounts();
    expect(counts1After.waiting).toBe(0);
    expect(counts2After.waiting).toBe(0);
  }, 20000);

  test('obliterateAll removes all queue data', async () => {
    const group = new QueueGroup('obliterate-grp');

    const q1 = group.getQueue<{ v: number }>('q1');
    const q2 = group.getQueue<{ v: number }>('q2');

    await q1.add('task', { v: 1 });
    await q1.add('task', { v: 2 });
    await q2.add('task', { v: 3 });
    await q2.add('task', { v: 4 });

    expect(q1.getJobCounts().waiting).toBe(2);
    expect(q2.getJobCounts().waiting).toBe(2);

    group.obliterateAll();

    expect(q1.getJobCounts().waiting).toBe(0);
    expect(q2.getJobCounts().waiting).toBe(0);

    // Queues should no longer appear in listQueues
    const remaining = group.listQueues();
    expect(remaining.length).toBe(0);
  }, 20000);

  test('multiple groups are independent - pausing one does not affect others', async () => {
    const groupA = new QueueGroup('indep-a');
    const groupB = new QueueGroup('indep-b');
    const groupC = new QueueGroup('indep-c');

    const qA = groupA.getQueue<{ src: string }>('work');
    const qB = groupB.getQueue<{ src: string }>('work');
    const qC = groupC.getQueue<{ src: string }>('work');

    await qA.add('task', { src: 'a' });
    await qB.add('task', { src: 'b' });
    await qC.add('task', { src: 'c' });

    // Pause only group A
    groupA.pauseAll();

    const processedB: string[] = [];
    const processedC: string[] = [];

    const wB = groupB.getWorker<{ src: string }>('work', async (job) => {
      processedB.push(job.data.src);
      return {};
    });
    const wC = groupC.getWorker<{ src: string }>('work', async (job) => {
      processedC.push(job.data.src);
      return {};
    });

    for (let i = 0; i < 30; i++) await Bun.sleep(100);

    await wB.close();
    await wC.close();

    // Group B and C should have processed their jobs
    expect(processedB).toEqual(['b']);
    expect(processedC).toEqual(['c']);

    // Group A should still be paused with its job queued. BullMQ semantics (#92):
    // a paused queue reports its job under `paused`, not `waiting`.
    expect(qA.isPaused()).toBe(true);
    const countsA = qA.getJobCounts();
    expect(countsA.waiting).toBe(0);
    expect(countsA.paused).toBe(1);
  }, 20000);

  test('getWorker creates worker for prefixed queue', async () => {
    const group = new QueueGroup('worker-test');

    const q = group.getQueue<{ msg: string }>('emails');
    await q.add('send', { msg: 'hello' });
    await q.add('send', { msg: 'world' });

    const results: string[] = [];
    const worker = group.getWorker<{ msg: string }>('emails', async (job) => {
      results.push(job.data.msg);
      return { sent: true };
    });

    for (let i = 0; i < 30; i++) await Bun.sleep(100);

    await worker.close();

    expect(results.length).toBe(2);
    expect(results).toContain('hello');
    expect(results).toContain('world');

    // Verify worker was created for the correctly prefixed queue
    const counts = q.getJobCounts();
    expect(counts.waiting).toBe(0);
  }, 20000);
});
