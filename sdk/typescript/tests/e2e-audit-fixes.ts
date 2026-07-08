/**
 * E2E repro tests for the SDK-audit fixes (RED against the pre-fix dist,
 * GREEN after rebuild). Covers H1 addBulk custom-id, H2 command-timeout
 * teardown, M1 waitForJob timeout, M2 getWaitingCount excludes prioritized,
 * L2 worker FAIL keeps the stack message, plus the addJobLog level parity.
 */

import { createServer } from 'node:net';
import { FlowProducer, Queue } from '../dist/index.js';
import {
  assert,
  assertEq,
  getPort,
  makeQueue,
  makeWorker,
  namedQueue,
  qname,
  test,
  waitFor,
} from './harness.ts';

test('audit H4: getFlow returns null on a missing job instead of throwing', async () => {
  const flow = new FlowProducer({ host: '127.0.0.1', port: getPort() });
  try {
    const tree = await flow.getFlow({ id: 'does-not-exist-h4-ts' });
    assertEq(tree, null, 'getFlow(missing) must return null, not throw');
  } finally {
    flow.close();
  }
});

test('audit H4: getFlow survives a removed child (partial tree)', async () => {
  const name = qname('h4tree');
  const queue = namedQueue(name);
  const flow = new FlowProducer({ host: '127.0.0.1', port: getPort() });
  try {
    await queue.pause();
    const node = await flow.add({
      name: 'root',
      queueName: name,
      children: [{ name: 'child', queueName: name }],
    });
    await queue.remove(node.children?.[0].job.id ?? ''); // root.childrenIds still lists it
    const tree = await flow.getFlow({ id: node.job.id }); // must not throw
    assert(tree !== null, 'getFlow returns the partial tree');
    await queue.obliterate();
  } finally {
    flow.close();
    queue.close();
  }
});

test('audit H1: addBulk preserves the custom job id', async () => {
  const queue = makeQueue('h1');
  try {
    await queue.addBulk([{ name: 't', data: { x: 1 }, opts: { jobId: 'cid-h1-ts' } }]);
    await waitFor(async () => (await queue.getJobByCustomId('cid-h1-ts')) !== null, 10_000);
    const found = await queue.getJobByCustomId('cid-h1-ts');
    assert(found !== null, 'addBulk must preserve customId on PUSHB');
  } finally {
    await queue.obliterate();
    queue.close();
  }
});

test('audit M1: waitForJob throws on timeout instead of resolving undefined', async () => {
  const queue = makeQueue('m1');
  try {
    const job = await queue.add('t', { x: 1 }); // no worker → never completes
    let threw = false;
    try {
      await queue.waitForJob(job.id, 500);
    } catch {
      threw = true;
    }
    assert(threw, 'waitForJob must reject on timeout, not resolve undefined');
  } finally {
    await queue.obliterate();
    queue.close();
  }
});

test('audit M1: waitForJob rejects a failed job with a failure error (not a timeout)', async () => {
  const name = qname('m1f');
  const queue = namedQueue(name);
  const worker = makeWorker(name, async () => {
    throw new Error('processor boom');
  });
  try {
    const job = await queue.add('t', { x: 1 });
    await waitFor(async () => (await queue.getJobState(job.id)) === 'failed', 15_000);
    let message = '';
    try {
      await queue.waitForJob(job.id, 500);
    } catch (err) {
      message = err instanceof Error ? err.message : String(err);
    }
    assert(/failed/i.test(message), `failed job must reject with a failure error, got: ${message}`);
  } finally {
    await worker.close();
    await queue.obliterate();
    queue.close();
  }
});

test('audit M2: getWaitingCount excludes prioritized jobs', async () => {
  const queue = makeQueue('m2');
  try {
    await queue.add('t', { x: 1 }, { priority: 5 }); // lands in the prioritized bucket
    await waitFor(async () => (await queue.getJobCounts()).prioritized >= 1, 10_000);
    const counts = await queue.getJobCounts();
    assert(counts.prioritized >= 1, 'a prioritized job exists');
    assertEq(
      await queue.getWaitingCount(),
      counts.waiting,
      'getWaitingCount must equal the waiting bucket, excluding prioritized'
    );
  } finally {
    await queue.obliterate();
    queue.close();
  }
});

test('audit parity: addJobLog level is sent and getJobLogs formats [level] message', async () => {
  const queue = makeQueue('log');
  try {
    const job = await queue.add('t', { x: 1 });
    await queue.addJobLog(job.id, 'boom', 'error');
    await waitFor(async () => (await queue.getJobLogs(job.id)).length > 0, 10_000);
    const logs = await queue.getJobLogs(job.id);
    assert(
      logs.some((l) => l === '[error] boom'),
      `expected "[error] boom" in ${JSON.stringify(logs)}`
    );
  } finally {
    await queue.obliterate();
    queue.close();
  }
});

test('audit L2: worker FAIL keeps the stack message (first lines, deep stack)', async () => {
  const name = qname('l2');
  const queue = namedQueue(name);
  // A deep stack (>MAX_STACK_LINES frames) so slice(-N) would drop the message.
  function deep(n: number): number {
    if (n <= 0) throw new Error('BOOM-L2');
    return deep(n - 1);
  }
  const worker = makeWorker(name, async () => deep(40));
  try {
    const job = await queue.add('t', {});
    await waitFor(async () => (await queue.getJobState(job.id)) === 'failed', 20_000);
    const failed = await queue.getJob(job.id);
    const stack = failed?.stacktrace ?? [];
    assert(stack.length > 0, 'stack persisted');
    assert(
      stack[0].includes('BOOM-L2'),
      `first persisted stack line must carry the message, got: ${stack[0]}`
    );
  } finally {
    await worker.close();
    queue.close();
  }
});

test('audit H2: connection tears down after repeated command timeouts', async () => {
  const holds: import('node:net').Socket[] = [];
  const srv = createServer((sock) => {
    holds.push(sock); // accept but never reply (half-open simulation)
  });
  await new Promise<void>((resolve) => srv.listen(0, '127.0.0.1', () => resolve()));
  const port = (srv.address() as { port: number }).port;
  const queue = new Queue('blackhole', { host: '127.0.0.1', port, commandTimeoutMs: 300 });
  try {
    // 3 consecutive timeouts (maxCommandTimeouts) must tear the socket down;
    // a 4th call would reconnect (counter resets), so stop at the threshold.
    for (let i = 0; i < 3; i++) {
      try {
        await queue.connection.call({ cmd: 'Ping' });
      } catch {
        /* expected timeout */
      }
    }
    assert(!queue.connection.isConnected, 'connection must tear down after repeated timeouts');
  } finally {
    queue.close();
    srv.close();
    for (const s of holds) s.destroy();
  }
});
