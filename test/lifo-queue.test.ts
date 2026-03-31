/**
 * LIFO Queue Mode Tests
 * Validates FIFO/LIFO ordering, priority precedence, delayed jobs, and bulk push.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';

describe('LIFO Queue Mode', () => {
  let qm: QueueManager;

  beforeEach(() => { qm = new QueueManager(); });
  afterEach(() => { qm.shutdown(); });

  /** Push N jobs with a small delay between each so UUID7 timestamps differ. */
  async function pushJobs(
    queue: string, count: number,
    opts: { priority?: number; lifo?: boolean; delay?: number } = {}
  ): Promise<string[]> {
    const ids: string[] = [];
    for (let i = 0; i < count; i++) {
      const job = await qm.push(queue, {
        data: { seq: i }, priority: opts.priority ?? 0,
        lifo: opts.lifo ?? false, delay: opts.delay,
      });
      ids.push(job.id);
      if (i < count - 1) await Bun.sleep(2);
    }
    return ids;
  }

  /** Pull up to N jobs and return their IDs in pull order. */
  async function pullAll(queue: string, count: number): Promise<string[]> {
    const ids: string[] = [];
    for (let i = 0; i < count; i++) {
      const job = await qm.pull(queue, 0);
      if (!job) break;
      ids.push(job.id);
    }
    return ids;
  }

  test('1. default mode is FIFO (first in, first out)', async () => {
    const pushOrder = await pushJobs('fifo-default', 5);
    const pullOrder = await pullAll('fifo-default', 5);
    expect(pullOrder).toEqual(pushOrder);
  });

  test('lifo=false is equivalent to default FIFO', async () => {
    const pushOrder = await pushJobs('fifo-explicit', 3, { lifo: false });
    const pullOrder = await pullAll('fifo-explicit', 3);
    expect(pullOrder).toEqual(pushOrder);
  });

  test('2. jobs with same priority follow FIFO order', async () => {
    const pushOrder = await pushJobs('same-prio-fifo', 4, { priority: 5 });
    const pullOrder = await pullAll('same-prio-fifo', 4);
    expect(pullOrder).toEqual(pushOrder);
  });

  test('same priority LIFO reverses order', async () => {
    const pushOrder = await pushJobs('same-prio-lifo', 4, { priority: 5, lifo: true });
    const pullOrder = await pullAll('same-prio-lifo', 4);
    expect(pullOrder).toEqual([...pushOrder].reverse());
  });

  test('3. higher priority comes first regardless of insert order', async () => {
    const lowJob = await qm.push('prio-first', { data: { label: 'low' }, priority: 1 });
    await Bun.sleep(2);
    const highJob = await qm.push('prio-first', { data: { label: 'high' }, priority: 10 });

    const first = await qm.pull('prio-first', 0);
    const second = await qm.pull('prio-first', 0);

    expect(first!.id).toBe(highJob.id);
    expect(second!.id).toBe(lowJob.id);
  });

  test('4. priority ordering combined with LIFO insert order', async () => {
    const q = 'prio-lifo-combined';
    const low1 = await qm.push(q, { data: { l: 'low1' }, priority: 1, lifo: true });
    await Bun.sleep(2);
    const low2 = await qm.push(q, { data: { l: 'low2' }, priority: 1, lifo: true });
    await Bun.sleep(2);
    const high1 = await qm.push(q, { data: { l: 'high1' }, priority: 10, lifo: true });
    await Bun.sleep(2);
    const high2 = await qm.push(q, { data: { l: 'high2' }, priority: 10, lifo: true });

    const pullOrder = await pullAll(q, 4);

    // High priority first (LIFO within), then low priority (LIFO within)
    expect(pullOrder[0]).toBe(high2.id);
    expect(pullOrder[1]).toBe(high1.id);
    expect(pullOrder[2]).toBe(low2.id);
    expect(pullOrder[3]).toBe(low1.id);
  });

  test('three priority levels with FIFO within each', async () => {
    const q = 'three-prio';
    const jobs: { id: string; prio: number }[] = [];
    for (const prio of [1, 5, 10]) {
      for (let s = 0; s < 2; s++) {
        const job = await qm.push(q, { data: { prio, s }, priority: prio });
        jobs.push({ id: job.id, prio });
        await Bun.sleep(2);
      }
    }
    const pullOrder = await pullAll(q, 6);
    const p10 = jobs.filter((j) => j.prio === 10);
    const p5 = jobs.filter((j) => j.prio === 5);
    const p1 = jobs.filter((j) => j.prio === 1);
    expect(pullOrder).toEqual([p10[0].id, p10[1].id, p5[0].id, p5[1].id, p1[0].id, p1[1].id]);
  });

  test('5. delayed jobs not pulled before ready', async () => {
    await qm.push('delayed-nr', { data: { l: 'delayed' }, priority: 0, delay: 60000 });
    const imm = await qm.push('delayed-nr', { data: { l: 'immediate' }, priority: 0 });

    const first = await qm.pull('delayed-nr', 0);
    expect(first).not.toBeNull();
    expect(first!.id).toBe(imm.id);

    const second = await qm.pull('delayed-nr', 0);
    expect(second).toBeNull();
  });

  test('delayed jobs respect priority after delay expires', async () => {
    const high = await qm.push('delayed-p', { data: { l: 'dh' }, priority: 10, delay: 10 });
    const low = await qm.push('delayed-p', { data: { l: 'il' }, priority: 1 });
    await Bun.sleep(20);

    const first = await qm.pull('delayed-p', 0);
    const second = await qm.pull('delayed-p', 0);
    expect(first!.id).toBe(high.id);
    expect(second!.id).toBe(low.id);
  });

  test('6. bulk push maintains FIFO order within same priority', async () => {
    const inputs = Array.from({ length: 5 }, (_, i) => ({ data: { seq: i }, priority: 0 }));
    const ids = await qm.pushBatch('bulk-fifo', inputs);
    const pullOrder = await pullAll('bulk-fifo', 5);
    expect(pullOrder).toEqual(ids);
  });

  test('bulk push with mixed priorities respects priority order', async () => {
    const ids = await qm.pushBatch('bulk-mp', [
      { data: { l: 'low' }, priority: 1 },
      { data: { l: 'high' }, priority: 10 },
      { data: { l: 'med' }, priority: 5 },
    ]);
    const pullOrder = await pullAll('bulk-mp', 3);
    expect(pullOrder[0]).toBe(ids[1]);
    expect(pullOrder[1]).toBe(ids[2]);
    expect(pullOrder[2]).toBe(ids[0]);
  });

  test('bulk push with lifo contains all pushed jobs', async () => {
    const inputs = Array.from({ length: 4 }, (_, i) => ({
      data: { seq: i }, priority: 0, lifo: true,
    }));
    const ids = await qm.pushBatch('bulk-lifo', inputs);
    const pullOrder = await pullAll('bulk-lifo', 4);
    // Within a single batch all jobs share timestamp; verify all present
    expect(pullOrder.sort()).toEqual([...ids].sort());
  });

  test('single LIFO job works correctly', async () => {
    const job = await qm.push('lifo-single', { data: { val: 1 }, lifo: true });
    const pulled = await qm.pull('lifo-single', 0);
    expect(pulled).not.toBeNull();
    expect(pulled!.id).toBe(job.id);
  });

  test('lifo=true reverses order within same priority', async () => {
    const pushOrder = await pushJobs('lifo-basic', 5, { lifo: true });
    const pullOrder = await pullAll('lifo-basic', 5);
    expect(pullOrder).toEqual([...pushOrder].reverse());
  });
});
