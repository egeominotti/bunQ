/**
 * Flow + Stall Detection Interaction Tests (Embedded Mode)
 * Tests flow/dependency jobs combined with stall detection using QueueManager.
 */
import { describe, test, expect, afterEach } from 'bun:test';
import { Queue, Worker, FlowProducer, shutdownManager } from '../src/client';
import { getSharedManager } from '../src/client/manager';
import type { JobId } from '../src/domain/types/job';

type Closeable = { close: () => void | Promise<void> };
const STALL_CFG = { stallInterval: 400, maxStalls: 2, gracePeriod: 100 };

describe('Flow + Stall Detection Interaction', () => {
  const res: Closeable[] = [];
  const use = <T extends Closeable>(r: T): T => { res.push(r); return r; };
  const setup = (q: string, cfg = STALL_CFG) => {
    const flow = use(new FlowProducer({ embedded: true }));
    const queue = use(new Queue(q, { embedded: true }));
    queue.obliterate();
    queue.setStallConfig(cfg);
    return { flow, queue };
  };
  const deferred = () => { let r: () => void; return { p: new Promise<void>(f => r = f), r: () => r() }; };

  afterEach(async () => {
    for (const r of res.reverse()) await r.close();
    res.length = 0;
    shutdownManager();
  });

  test('parent job stalls while children are processing', async () => {
    const { flow } = setup('fs-parent');
    const executed: string[] = [];
    const { p, r } = deferred();
    use(new Worker('fs-parent', async (job) => {
      const d = job.data as { role: string };
      executed.push(d.role);
      if (d.role === 'child') { await Bun.sleep(50); return { ok: true }; }
      if (d.role === 'parent') { r(); return { ok: true }; }
    }, { embedded: true, concurrency: 5 }));

    await flow.add({
      name: 'parent', queueName: 'fs-parent', data: { role: 'parent' },
      children: [
        { name: 'c-a', queueName: 'fs-parent', data: { role: 'child' } },
        { name: 'c-b', queueName: 'fs-parent', data: { role: 'child' } },
      ],
    });
    await p; await Bun.sleep(200);
    expect(executed.filter(r => r === 'child').length).toBe(2);
    expect(executed.indexOf('parent')).toBeGreaterThan(0);
  }, 15000);

  test('child job stalls - parent should not complete prematurely', async () => {
    const { flow } = setup('fs-child-stall', { stallInterval: 400, maxStalls: 3, gracePeriod: 100 });
    const mgr = getSharedManager();
    use(new Worker('fs-child-stall', async (job) => {
      const d = job.data as { role: string; stall?: boolean };
      if (d.role === 'child' && d.stall) {
        await Bun.sleep(900); return { late: true };
      }
      if (d.role === 'child') return { fast: true };
      if (d.role === 'parent') return { done: true };
    }, { embedded: true, concurrency: 1 }));

    const result = await flow.add({
      name: 'parent', queueName: 'fs-child-stall', data: { role: 'parent' },
      children: [
        { name: 'fast', queueName: 'fs-child-stall', data: { role: 'child' } },
        { name: 'slow', queueName: 'fs-child-stall', data: { role: 'child', stall: true } },
      ],
    });
    await Bun.sleep(700);
    const state = await mgr.getJobState(result.job.id as unknown as JobId);
    expect(['waiting', 'waiting-children', 'delayed']).toContain(state);
  }, 20000);

  test('stalled child is requeued and parent eventually completes', async () => {
    const { flow } = setup('fs-requeue', { stallInterval: 300, maxStalls: 3, gracePeriod: 100 });
    let childAttempts = 0, parentDone = false;
    const { p, r } = deferred();
    use(new Worker('fs-requeue', async (job) => {
      const d = job.data as { role: string };
      if (d.role === 'child') {
        childAttempts++;
        if (childAttempts === 1) await Bun.sleep(800);
        return { attempt: childAttempts };
      }
      if (d.role === 'parent') { parentDone = true; r(); return { ok: true }; }
    }, { embedded: true, concurrency: 1 }));

    await flow.add({
      name: 'parent', queueName: 'fs-requeue', data: { role: 'parent' },
      children: [{ name: 'child', queueName: 'fs-requeue', data: { role: 'child' } }],
    });
    await Promise.race([p, Bun.sleep(10000)]);
    await Bun.sleep(300);
    expect(parentDone).toBe(true);
    expect(childAttempts).toBeGreaterThanOrEqual(1);
  }, 15000);

  test('flow with multiple children, one stalls', async () => {
    const { flow } = setup('fs-multi', { stallInterval: 300, maxStalls: 3, gracePeriod: 100 });
    const done_c: string[] = [];
    let parentDone = false, stallAttempts = 0;
    const { p, r } = deferred();
    use(new Worker('fs-multi', async (job) => {
      const d = job.data as { role: string; tag?: string };
      if (d.role === 'child') {
        if (d.tag === 'staller' && ++stallAttempts === 1) await Bun.sleep(800);
        done_c.push(d.tag ?? '?');
        return { child: d.tag };
      }
      if (d.role === 'parent') { parentDone = true; r(); return { ok: true }; }
    }, { embedded: true, concurrency: 5 }));

    await flow.add({
      name: 'parent', queueName: 'fs-multi', data: { role: 'parent' },
      children: [
        { name: 'f1', queueName: 'fs-multi', data: { role: 'child', tag: 'fast-1' } },
        { name: 'f2', queueName: 'fs-multi', data: { role: 'child', tag: 'fast-2' } },
        { name: 'st', queueName: 'fs-multi', data: { role: 'child', tag: 'staller' } },
      ],
    });
    await Promise.race([p, Bun.sleep(12000)]);
    await Bun.sleep(300);
    expect(parentDone).toBe(true);
    expect(done_c).toContain('fast-1');
    expect(done_c).toContain('fast-2');
    expect(done_c).toContain('staller');
  }, 15000);

  test('stall detection respects grace period in flow jobs', async () => {
    // Long grace period: child work exceeds stallInterval but is within gracePeriod
    const { flow } = setup('fs-grace', { stallInterval: 200, maxStalls: 1, gracePeriod: 2000 });
    let childOk = false, parentOk = false;
    const { p, r } = deferred();
    use(new Worker('fs-grace', async (job) => {
      const d = job.data as { role: string };
      if (d.role === 'child') { await Bun.sleep(500); childOk = true; return { ok: true }; }
      if (d.role === 'parent') { parentOk = true; r(); return { ok: true }; }
    }, { embedded: true, concurrency: 2 }));

    await flow.add({
      name: 'parent', queueName: 'fs-grace', data: { role: 'parent' },
      children: [{ name: 'child', queueName: 'fs-grace', data: { role: 'child' } }],
    });
    await Promise.race([p, Bun.sleep(8000)]);
    await Bun.sleep(200);
    expect(childOk).toBe(true);
    expect(parentOk).toBe(true);
  }, 15000);

  test('heartbeat keeps child alive during long processing in flow', async () => {
    const { flow } = setup('fs-hb', { stallInterval: 300, maxStalls: 1, gracePeriod: 100 });
    const mgr = getSharedManager();
    let parentDone = false;
    const { p, r } = deferred();
    use(new Worker('fs-hb', async (job) => {
      const d = job.data as { role: string };
      if (d.role === 'child') {
        for (let i = 0; i < 4; i++) {
          await Bun.sleep(100);
          mgr.jobHeartbeat(job.id as unknown as JobId);
        }
        return { ok: true };
      }
      if (d.role === 'parent') { parentDone = true; r(); return { ok: true }; }
    }, { embedded: true, concurrency: 3 }));

    await flow.add({
      name: 'parent', queueName: 'fs-hb', data: { role: 'parent' },
      children: [{ name: 'child', queueName: 'fs-hb', data: { role: 'child' } }],
    });
    await Promise.race([p, Bun.sleep(8000)]);
    await Bun.sleep(200);
    expect(parentDone).toBe(true);
  }, 15000);

  test('nested flow: grandchild stalls, chain eventually completes', async () => {
    const { flow } = setup('fs-nested', { stallInterval: 300, maxStalls: 3, gracePeriod: 100 });
    const order: string[] = [];
    let gcAttempts = 0, rootDone = false;
    const { p, r } = deferred();
    use(new Worker('fs-nested', async (job) => {
      const d = job.data as { node: string };
      if (d.node === 'grandchild') {
        if (++gcAttempts === 1) await Bun.sleep(800);
        order.push('grandchild');
        return { ok: true };
      }
      order.push(d.node);
      if (d.node === 'root') { rootDone = true; r(); }
      return { ok: true };
    }, { embedded: true, concurrency: 1 }));

    await flow.add({
      name: 'root', queueName: 'fs-nested', data: { node: 'root' },
      children: [{
        name: 'branch', queueName: 'fs-nested', data: { node: 'branch' },
        children: [{ name: 'gc', queueName: 'fs-nested', data: { node: 'grandchild' } }],
      }],
    });
    await Promise.race([p, Bun.sleep(12000)]);
    await Bun.sleep(300);
    expect(rootDone).toBe(true);
    expect(order.indexOf('grandchild')).toBeLessThan(order.indexOf('branch'));
    expect(order.indexOf('branch')).toBeLessThan(order.indexOf('root'));
    expect(gcAttempts).toBeGreaterThanOrEqual(1);
  }, 15000);
});
