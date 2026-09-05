/** E2E: FlowProducer — atomic trees, chains, fan-in and flow reads. */

import { FlowProducer, type Job } from '../dist/legacy.js';
import {
  assert,
  assertEq,
  getPort,
  makeWorker,
  namedQueue,
  qname,
  test,
  waitFor,
} from './harness.ts';

function makeFlow(): FlowProducer {
  return new FlowProducer({ host: '127.0.0.1', port: getPort() });
}

test('flow: tree children complete before parent + getChildrenValues', async () => {
  const name = qname('flowtree');
  const queue = namedQueue(name);
  const flow = makeFlow();
  const order: string[] = [];
  const worker = makeWorker(
    name,
    async (job) => {
      order.push(job.name ?? '?');
      return { step: job.name };
    },
    { concurrency: 4 }
  );
  try {
    const node = await flow.add({
      name: 'parent',
      queueName: name,
      data: { kind: 'root' },
      children: [
        { name: 'child-a', queueName: name, data: {} },
        { name: 'child-b', queueName: name, data: {} },
      ],
    });
    assertEq(node.children?.length, 2, 'two children in node');
    await waitFor(async () => (await queue.getJobState(node.job.id)) === 'completed', 20_000);
    assertEq(order[order.length - 1], 'parent', `parent ran last: ${order.join(',')}`);
    assertEq(new Set(order.slice(0, 2)).size, 2, 'children ran first');
    const values = await queue.getChildrenValues(node.job.id);
    assertEq(Object.keys(values).length, 2, 'children values');
  } finally {
    await worker.close();
    flow.close();
    queue.close();
  }
});

test('flow: addChain executes sequentially', async () => {
  const name = qname('chain');
  const queue = namedQueue(name);
  const flow = makeFlow();
  const order: string[] = [];
  const worker = makeWorker(
    name,
    async (job) => {
      order.push(job.name ?? '?');
      return job.name;
    },
    { concurrency: 4 }
  );
  try {
    const { jobIds } = await flow.addChain([
      { name: 'step-1', queueName: name },
      { name: 'step-2', queueName: name },
      { name: 'step-3', queueName: name },
    ]);
    assertEq(jobIds.length, 3, 'three chained jobs');
    await waitFor(async () => (await queue.getJobState(jobIds[2])) === 'completed', 20_000);
    assertEq(order.join(','), 'step-1,step-2,step-3', 'chain order');
  } finally {
    await worker.close();
    flow.close();
    queue.close();
  }
});

test('flow: addBulkThen fan-in reads children values', async () => {
  const name = qname('fanin');
  const queue = namedQueue(name);
  const flow = makeFlow();
  let childrenSeen: Record<string, unknown> | null = null;
  const worker = makeWorker(
    name,
    async (job: Job) => {
      if (job.name === 'merge') {
        childrenSeen = await job.getChildrenValues();
        return { merged: true };
      }
      return { part: job.name };
    },
    { concurrency: 4 }
  );
  try {
    const { parallelIds, finalId } = await flow.addBulkThen(
      [
        { name: 'part-a', queueName: name },
        { name: 'part-b', queueName: name },
        { name: 'part-c', queueName: name },
      ],
      { name: 'merge', queueName: name }
    );
    assertEq(parallelIds.length, 3, 'three parallel jobs');
    await waitFor(async () => (await queue.getJobState(finalId)) === 'completed', 20_000);
    assert(childrenSeen !== null, 'merge read children values');
    assertEq(Object.keys(childrenSeen ?? {}).length, 3, 'three children values');
  } finally {
    await worker.close();
    flow.close();
    queue.close();
  }
});

test('flow: getFlow reconstructs nested tree', async () => {
  const name = qname('gettree');
  const queue = namedQueue(name);
  const flow = makeFlow();
  try {
    await queue.pause(); // keep jobs in place while we inspect
    const node = await flow.add({
      name: 'root',
      queueName: name,
      children: [
        {
          name: 'mid',
          queueName: name,
          children: [{ name: 'leaf', queueName: name }],
        },
      ],
    });
    const tree = await flow.getFlow({ id: node.job.id });
    assert(tree !== null, 'tree fetched');
    assertEq(tree?.children?.length, 1, 'one mid child');
    assertEq(tree?.children?.[0].children?.length, 1, 'one leaf child');
    await queue.obliterate();
  } finally {
    flow.close();
    queue.close();
  }
});

test('flow: custom IDs and reciprocal links survive the atomic snapshot', async () => {
  const name = qname('customflow');
  const rootId = `root-${name}`;
  const childId = `child-${name}`;
  const queue = namedQueue(name);
  const flow = makeFlow();
  try {
    await queue.pause();
    const node = await flow.add({
      name: 'root',
      queueName: name,
      opts: { jobId: rootId },
      children: [{ name: 'child', queueName: name, opts: { jobId: childId } }],
    });
    assertEq(node.job.id, rootId, 'root planned custom ID');
    assertEq(node.job.customId, rootId, 'root customId snapshot');
    assertEq(node.children?.[0].job.id, childId, 'child planned custom ID');
    assertEq(node.children?.[0].job.parentId, rootId, 'child points to root');
    assertEq(node.job.childrenIds[0], childId, 'root owns child');
    await queue.obliterate();
  } finally {
    flow.close();
    queue.close();
  }
});

test('flow: invalid batch commits no jobs', async () => {
  const name = qname('rollback');
  const queue = namedQueue(name);
  const flow = makeFlow();
  try {
    let threw = false;
    try {
      await flow.addChain([
        { name: 'ok-step', queueName: name },
        { name: 'bad-step', queueName: 'invalid queue name!!' },
      ]);
    } catch {
      threw = true;
    }
    assert(threw, 'chain with invalid queue must throw');
    assertEq(await queue.count(), 0, 'failed atomic batch created no jobs');
  } finally {
    flow.close();
    queue.close();
  }
});
