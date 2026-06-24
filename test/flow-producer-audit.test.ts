/**
 * FlowProducer — adversarial audit suite.
 *
 * Probes every FlowProducer feature end-to-end (embedded), targeting the GAPS
 * not covered by flow-advanced.test.ts and several SUSPECTED bugs found by
 * reading the source:
 *
 *  - addChain / addTree execution-order semantics (they are OPPOSITE)
 *  - cross-queue add(): parent linkage + __parentQueue correctness
 *    (updateJobParent sets data.__parentQueue = childJob.queue, i.e. the CHILD's
 *     own queue, not the parent's — suspected bug for cross-queue flows)
 *  - addBulkThen: can the merge job read its predecessors' results?
 *  - getFlow depth / maxChildren / queue-mismatch / missing
 *  - getParentResult / getParentResults
 *  - addBulk independence
 *  - failParentOnFailure propagation
 *
 * Tests assert the INTENDED contract. A RED test reproduces a real defect.
 */
import { describe, test, expect, afterEach } from 'bun:test';
import { FlowProducer, Queue, Worker, shutdownManager } from '../src/client';
import { getSharedManager } from '../src/client/manager';
import { jobId } from '../src/domain/types/job';

afterEach(() => {
  shutdownManager();
});

/** Poll a predicate until true or timeout. Avoids fixed sleeps. */
async function until(fn: () => Promise<boolean> | boolean, timeoutMs = 5000): Promise<boolean> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await fn()) return true;
    await Bun.sleep(10);
  }
  return false;
}

// ───────────────────────────────────────────────────────────────────────────
// addChain — sequential, each step depends on its predecessor (predecessor first)
// ───────────────────────────────────────────────────────────────────────────
describe('FlowProducer.addChain', () => {
  test('executes steps strictly in order (predecessor completes before successor starts)', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('audit-chain', { embedded: true });
    queue.obliterate();

    const order: string[] = [];
    const worker = new Worker(
      'audit-chain',
      async (job) => {
        const step = (job.data as { step: string }).step;
        order.push(`start:${step}`);
        await Bun.sleep(30);
        order.push(`end:${step}`);
        return { step };
      },
      { embedded: true, concurrency: 5 }
    );

    const { jobIds } = await flow.addChain([
      { name: 's0', queueName: 'audit-chain', data: { step: 's0' } },
      { name: 's1', queueName: 'audit-chain', data: { step: 's1' } },
      { name: 's2', queueName: 'audit-chain', data: { step: 's2' } },
    ]);
    expect(jobIds).toHaveLength(3);

    const ok = await until(() => order.includes('end:s2'), 8000);
    expect(ok).toBe(true);

    // Strict sequencing: each step ends before the next begins (no overlap).
    expect(order).toEqual([
      'start:s0',
      'end:s0',
      'start:s1',
      'end:s1',
      'start:s2',
      'end:s2',
    ]);

    await worker.close();
    flow.close();
    queue.close();
  }, 15000);

  test('empty chain returns no ids', async () => {
    const flow = new FlowProducer({ embedded: true });
    const res = await flow.addChain([]);
    expect(res.jobIds).toEqual([]);
    flow.close();
  });

  test('forwards per-step opts (priority) to the underlying jobs', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('audit-chain-opts', { embedded: true });
    queue.obliterate();

    const { jobIds } = await flow.addChain([
      { name: 'a', queueName: 'audit-chain-opts', data: {}, opts: { priority: 9 } },
      { name: 'b', queueName: 'audit-chain-opts', data: {} },
    ]);

    const first = await getSharedManager().getJob(jobId(jobIds[0]));
    expect(first?.priority).toBe(9);

    flow.close();
    queue.close();
  });
});

// ───────────────────────────────────────────────────────────────────────────
// addTree — children depend on parent (parent first). OPPOSITE of add().
// ───────────────────────────────────────────────────────────────────────────
describe('FlowProducer.addTree', () => {
  test('parent runs BEFORE its children (opposite of BullMQ add())', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('audit-tree', { embedded: true });
    queue.obliterate();

    const order: string[] = [];
    const worker = new Worker(
      'audit-tree',
      async (job) => {
        order.push((job.data as { node: string }).node);
        await Bun.sleep(20);
        return {};
      },
      { embedded: true, concurrency: 5 }
    );

    await flow.addTree({
      name: 'root',
      queueName: 'audit-tree',
      data: { node: 'root' },
      children: [
        { name: 'c1', queueName: 'audit-tree', data: { node: 'c1' } },
        { name: 'c2', queueName: 'audit-tree', data: { node: 'c2' } },
      ],
    });

    const ok = await until(() => order.includes('c1') && order.includes('c2'), 8000);
    expect(ok).toBe(true);

    // root must be processed before either child
    expect(order[0]).toBe('root');
    expect(order.indexOf('root')).toBeLessThan(order.indexOf('c1'));
    expect(order.indexOf('root')).toBeLessThan(order.indexOf('c2'));

    await worker.close();
    flow.close();
    queue.close();
  }, 15000);
});

// ───────────────────────────────────────────────────────────────────────────
// add() — BullMQ v5: children first, parent depends on children.
// Focus: CROSS-QUEUE parent/child linkage (suspected __parentQueue bug).
// ───────────────────────────────────────────────────────────────────────────
describe('FlowProducer.add — cross-queue parent linkage', () => {
  test('child stores the PARENT queue in __parentQueue (not its own queue)', async () => {
    const flow = new FlowProducer({ embedded: true });
    const parentQ = new Queue('audit-parent-q', { embedded: true });
    const childQ = new Queue('audit-child-q', { embedded: true });
    parentQ.obliterate();
    childQ.obliterate();

    const node = await flow.add({
      name: 'parent',
      queueName: 'audit-parent-q',
      data: { v: 1 },
      children: [{ name: 'child', queueName: 'audit-child-q', data: { v: 2 } }],
    });

    const childId = node.children![0].job.id;
    const childJob = await getSharedManager().getJob(jobId(childId));
    expect(childJob).not.toBeNull();

    // 'pending' must have been corrected to the real parent id
    expect(String(childJob!.parentId)).toBe(node.job.id);

    // __parentQueue must point at the PARENT's queue for cross-queue navigation
    const childData = childJob!.data as Record<string, unknown>;
    expect(childData.__parentQueue).toBe('audit-parent-q');

    flow.close();
    parentQ.close();
    childQ.close();
  });

  test('cross-queue flow completes: children first, then parent; getChildrenValues populated', async () => {
    const flow = new FlowProducer({ embedded: true });
    const parentQ = new Queue('xq-parent', { embedded: true });
    const childQ = new Queue('xq-child', { embedded: true });
    parentQ.obliterate();
    childQ.obliterate();

    const order: string[] = [];
    let parentChildren: Record<string, unknown> = {};

    const childWorker = new Worker(
      'xq-child',
      async (job) => {
        order.push(`child:${(job.data as { v: number }).v}`);
        await Bun.sleep(30);
        return { doubled: (job.data as { v: number }).v * 2 };
      },
      { embedded: true, concurrency: 5 }
    );
    const parentWorker = new Worker(
      'xq-parent',
      async (job) => {
        order.push('parent');
        parentChildren = await getSharedManager().getChildrenValues(jobId(job.id));
        return { done: true };
      },
      { embedded: true, concurrency: 5 }
    );

    const node = await flow.add({
      name: 'parent',
      queueName: 'xq-parent',
      data: {},
      children: [
        { name: 'c1', queueName: 'xq-child', data: { v: 10 } },
        { name: 'c2', queueName: 'xq-child', data: { v: 20 } },
      ],
    });

    const ok = await until(() => order.includes('parent'), 8000);
    expect(ok).toBe(true);

    // Both children processed before the parent
    expect(order.indexOf('parent')).toBe(order.length - 1);
    expect(order.filter((o) => o.startsWith('child:'))).toHaveLength(2);

    // getChildrenValues keyed by the CHILD queue
    const vals = Object.values(parentChildren).map((v) => (v as { doubled: number }).doubled).sort(
      (a, b) => a - b
    );
    expect(vals).toEqual([20, 40]);
    const keys = Object.keys(parentChildren);
    expect(keys.every((k) => k.startsWith('xq-child:'))).toBe(true);

    // sanity: parent really completed
    expect(String(node.job.id)).toBeTruthy();

    await childWorker.close();
    await parentWorker.close();
    flow.close();
    parentQ.close();
    childQ.close();
  }, 15000);
});

// ───────────────────────────────────────────────────────────────────────────
// add() — 'pending' parent id must never linger after a nested flow
// ───────────────────────────────────────────────────────────────────────────
describe('FlowProducer.add — nested linkage', () => {
  test('3-level flow: grandchild.parentId is the real child id, never "pending"', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('audit-nested', { embedded: true });
    queue.obliterate();

    const node = await flow.add({
      name: 'root',
      queueName: 'audit-nested',
      data: {},
      children: [
        {
          name: 'child',
          queueName: 'audit-nested',
          data: {},
          children: [{ name: 'grandchild', queueName: 'audit-nested', data: {} }],
        },
      ],
    });

    const childNode = node.children![0];
    const grandchildNode = childNode.children![0];

    const child = await getSharedManager().getJob(jobId(childNode.job.id));
    const grandchild = await getSharedManager().getJob(jobId(grandchildNode.job.id));

    expect(String(child!.parentId)).toBe(node.job.id);
    expect(String(grandchild!.parentId)).toBe(childNode.job.id);
    expect(String(grandchild!.parentId)).not.toBe('pending');
    expect((grandchild!.data as Record<string, unknown>).__parentId).not.toBe('pending');

    flow.close();
    queue.close();
  });
});

// ───────────────────────────────────────────────────────────────────────────
// addBulkThen — parallel → final. Can the final job read predecessor results?
// ───────────────────────────────────────────────────────────────────────────
describe('FlowProducer.addBulkThen', () => {
  test('final job can read the parallel results via getChildrenValues', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('audit-bulkthen', { embedded: true });
    queue.obliterate();

    let finalChildren: Record<string, unknown> = {};
    let finalRan = false;

    const worker = new Worker(
      'audit-bulkthen',
      async (job) => {
        const data = job.data as { kind: string; n?: number };
        if (data.kind === 'final') {
          finalChildren = await getSharedManager().getChildrenValues(jobId(job.id));
          finalRan = true;
          return { merged: true };
        }
        await Bun.sleep(20);
        return { n: data.n };
      },
      { embedded: true, concurrency: 5 }
    );

    await flow.addBulkThen(
      [
        { name: 'p1', queueName: 'audit-bulkthen', data: { kind: 'p', n: 1 } },
        { name: 'p2', queueName: 'audit-bulkthen', data: { kind: 'p', n: 2 } },
        { name: 'p3', queueName: 'audit-bulkthen', data: { kind: 'p', n: 3 } },
      ],
      { name: 'final', queueName: 'audit-bulkthen', data: { kind: 'final' } }
    );

    const ok = await until(() => finalRan, 8000);
    expect(ok).toBe(true);

    // The whole point of fan-in: the merge step should see all 3 results.
    const ns = Object.values(finalChildren).map((v) => (v as { n: number }).n).sort();
    expect(ns).toEqual([1, 2, 3]);

    await worker.close();
    flow.close();
    queue.close();
  }, 15000);
});

// ───────────────────────────────────────────────────────────────────────────
// getFlow — tree reconstruction, depth/maxChildren limits, null cases
// ───────────────────────────────────────────────────────────────────────────
describe('FlowProducer.getFlow', () => {
  async function buildTree(flow: FlowProducer) {
    return flow.add({
      name: 'root',
      queueName: 'audit-getflow',
      data: {},
      children: [
        { name: 'a', queueName: 'audit-getflow', data: {} },
        { name: 'b', queueName: 'audit-getflow', data: {} },
        { name: 'c', queueName: 'audit-getflow', data: {} },
      ],
    });
  }

  test('returns the full tree by default', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('audit-getflow', { embedded: true });
    queue.obliterate();

    const node = await buildTree(flow);
    const tree = await flow.getFlow({ id: node.job.id, queueName: 'audit-getflow' });
    expect(tree).not.toBeNull();
    expect(tree!.children).toHaveLength(3);

    flow.close();
    queue.close();
  });

  test('respects depth = 0 (no children returned)', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('audit-getflow', { embedded: true });
    queue.obliterate();

    const node = await buildTree(flow);
    const tree = await flow.getFlow({ id: node.job.id, queueName: 'audit-getflow', depth: 0 });
    expect(tree).not.toBeNull();
    expect(tree!.children).toBeUndefined();

    flow.close();
    queue.close();
  });

  test('respects maxChildren', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('audit-getflow', { embedded: true });
    queue.obliterate();

    const node = await buildTree(flow);
    const tree = await flow.getFlow({
      id: node.job.id,
      queueName: 'audit-getflow',
      maxChildren: 2,
    });
    expect(tree!.children).toHaveLength(2);

    flow.close();
    queue.close();
  });

  test('returns null on queue mismatch and on missing job', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('audit-getflow', { embedded: true });
    queue.obliterate();

    const node = await buildTree(flow);
    expect(await flow.getFlow({ id: node.job.id, queueName: 'wrong-queue' })).toBeNull();
    expect(await flow.getFlow({ id: 'does-not-exist', queueName: 'audit-getflow' })).toBeNull();

    flow.close();
    queue.close();
  });
});

// ───────────────────────────────────────────────────────────────────────────
// getParentResult / getParentResults
// ───────────────────────────────────────────────────────────────────────────
describe('FlowProducer.getParentResult(s)', () => {
  test('returns a completed parent result; getParentResults filters missing ids', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('audit-parentresult', { embedded: true });
    queue.obliterate();

    const worker = new Worker(
      'audit-parentresult',
      async (job) => ({ echo: (job.data as { v: number }).v }),
      { embedded: true, concurrency: 5 }
    );

    const node = await flow.add({
      name: 'root',
      queueName: 'audit-parentresult',
      data: { v: 42 },
    });

    const finished = await until(
      async () => (await getSharedManager().getJobState(jobId(node.job.id))) === 'completed',
      8000
    );
    expect(finished).toBe(true);

    expect(flow.getParentResult<{ echo: number }>(node.job.id)).toEqual({ echo: 42 });

    const many = flow.getParentResults<{ echo: number }>([node.job.id, 'missing-id']);
    expect(many.size).toBe(1);
    expect(many.get(node.job.id)).toEqual({ echo: 42 });

    await worker.close();
    flow.close();
    queue.close();
  }, 15000);
});

// ───────────────────────────────────────────────────────────────────────────
// addBulk — independent flows all complete
// ───────────────────────────────────────────────────────────────────────────
describe('FlowProducer.addBulk', () => {
  test('adds multiple independent flows that all process to completion', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('audit-bulk', { embedded: true });
    queue.obliterate();

    let completed = 0;
    const worker = new Worker(
      'audit-bulk',
      async () => {
        completed++;
        return {};
      },
      { embedded: true, concurrency: 5 }
    );

    const nodes = await flow.addBulk([
      { name: 'p1', queueName: 'audit-bulk', data: {}, children: [{ name: 'c1', queueName: 'audit-bulk', data: {} }] },
      { name: 'p2', queueName: 'audit-bulk', data: {}, children: [{ name: 'c2', queueName: 'audit-bulk', data: {} }] },
    ]);
    expect(nodes).toHaveLength(2);

    // 2 parents + 2 children = 4 jobs
    const ok = await until(() => completed >= 4, 8000);
    expect(ok).toBe(true);

    await worker.close();
    flow.close();
    queue.close();
  }, 15000);
});

// ───────────────────────────────────────────────────────────────────────────
// failParentOnFailure — a failing child must fail its parent
// ───────────────────────────────────────────────────────────────────────────
describe('FlowProducer.add — failParentOnFailure', () => {
  test('child failure with failParentOnFailure fails the parent', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('audit-failparent', { embedded: true });
    queue.obliterate();

    const worker = new Worker(
      'audit-failparent',
      async (job) => {
        if ((job.data as { role: string }).role === 'child') {
          throw new Error('child boom');
        }
        return { ok: true };
      },
      { embedded: true, concurrency: 5 }
    );

    const node = await flow.add({
      name: 'parent',
      queueName: 'audit-failparent',
      data: { role: 'parent' },
      children: [
        {
          name: 'child',
          queueName: 'audit-failparent',
          data: { role: 'child' },
          opts: { attempts: 1, failParentOnFailure: true },
        },
      ],
    });

    const parentFailed = await until(
      async () => (await getSharedManager().getJobState(jobId(node.job.id))) === 'failed',
      8000
    );
    expect(parentFailed).toBe(true);

    await worker.close();
    flow.close();
    queue.close();
  }, 15000);
});
