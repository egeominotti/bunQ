/**
 * E2E repro tests for the 2026-07-14 spec-alignment audit (RED against the
 * pre-fix dist, GREEN after rebuild). Covers the batchSize server cap,
 * heartbeatIntervalS 0 = disabled (no interval storm), Simple Mode
 * cron()/every() execution `limit` (wire maxLimit) and the protocol version
 * advertised by hello().
 */

import { PROTOCOL_VERSION } from '../dist/frame.js';
import { Bunqueue, Worker } from '../dist/index.js';
import {
  assert,
  assertEq,
  getPort,
  makeQueue,
  namedQueue,
  qname,
  sleep,
  test,
  waitFor,
} from './harness.ts';

test('spec: worker batchSize clamps to the server PULLB max (1000)', async () => {
  const name = qname('cap');
  const queue = namedQueue(name);
  const done: string[] = [];
  // concurrency > 1000 so an unclamped batchSize would send PULLB count>1000,
  // which the server rejects — wedging the pull loop in an error cycle.
  const worker = new Worker(
    name,
    async () => {
      done.push('x');
      return 'ok';
    },
    { host: '127.0.0.1', port: getPort(), pollTimeoutMs: 300, concurrency: 1200, batchSize: 1200 }
  );
  try {
    assertEq(worker.batchSize, 1000, 'batchSize must clamp to the server max (1000)');
    await queue.add('t', { x: 1 });
    await waitFor(async () => done.length >= 1, 15_000);
  } finally {
    await worker.close();
    await queue.obliterate();
    queue.close();
  }
});

test('spec: worker batchSize 0 clamps to 1 and still processes', async () => {
  const name = qname('bz');
  const queue = namedQueue(name);
  const done: string[] = [];
  const worker = new Worker(
    name,
    async (job: { id: string }) => {
      done.push(job.id);
      return 'ok';
    },
    { host: '127.0.0.1', port: getPort(), pollTimeoutMs: 300, batchSize: 0 }
  );
  try {
    assertEq(worker.batchSize, 1, 'batchSize 0 must clamp to 1');
    await queue.add('t', { x: 1 });
    await waitFor(async () => done.length >= 1, 15_000);
  } finally {
    await worker.close();
    await queue.obliterate();
    queue.close();
  }
});

test('spec: heartbeatIntervalS 0 disables heartbeats (no interval storm)', async () => {
  const name = qname('hb0');
  const queue = namedQueue(name);
  let heartbeats = 0;
  const worker = new Worker(name, async () => 'ok', {
    host: '127.0.0.1',
    port: getPort(),
    pollTimeoutMs: 300,
    heartbeatIntervalS: 0,
    onTelemetry: (e: { type: string; cmd?: string }) => {
      if (e.type === 'command' && e.cmd === 'Heartbeat') heartbeats += 1;
    },
  });
  try {
    await worker.waitUntilReady();
    await queue.add('t', { x: 1 });
    await waitFor(async () => (await queue.getJobCounts()).completed >= 1, 15_000);
    await sleep(500); // setInterval(fn, 0) would have fired hundreds of times
    assertEq(heartbeats, 0, 'heartbeatIntervalS: 0 must send no Heartbeat commands');
  } finally {
    await worker.close();
    await queue.obliterate();
    queue.close();
  }
});

test('spec: Simple Mode cron()/every() forward the execution limit (wire maxLimit)', async () => {
  const app = new Bunqueue(qname('lim'), {
    connection: { host: '127.0.0.1', port: getPort() },
    pollTimeout: 300,
    processor: async () => 'ok',
  });
  const cronId = qname('limc');
  const everyId = qname('lime');
  try {
    const info = await app.cron(cronId, '0 9 * * *', { t: 1 }, { limit: 3 });
    assertEq(info?.maxLimit, 3, 'cron() limit must reach the scheduler as maxLimit');
    await app.every(everyId, 60_000, { t: 1 }, { limit: 2 });
    const crons = await app.listCrons();
    const ev = crons.find((c) => String(c.name) === everyId);
    assert(ev !== undefined, 'every() scheduler listed');
    assertEq(ev?.maxLimit, 2, 'every() limit must reach the scheduler as maxLimit');
  } finally {
    await app.removeCron(cronId).catch(() => {});
    await app.removeCron(everyId).catch(() => {});
    await app.close();
  }
});

test('spec: waitForJob clamps ttlMs beyond the server cap (600000)', async () => {
  const name = qname('wclamp');
  const queue = namedQueue(name);
  const worker = new Worker(name, async () => ({ done: true }), {
    host: '127.0.0.1',
    port: getPort(),
    pollTimeoutMs: 300,
  });
  try {
    const job = await queue.add('t', { x: 1 });
    await waitFor(async () => (await queue.getJobState(job.id)) === 'completed', 15_000);
    // pre-fix: the server rejects timeout > 600000 with a CommandError
    const result = await queue.waitForJob<{ done: boolean }>(job.id, 700_000);
    assertEq(result.done, true, 'completed job must resolve immediately');
  } finally {
    await worker.close();
    await queue.obliterate();
    queue.close();
  }
});

test('spec: client PROTOCOL_VERSION matches the server hello', async () => {
  const queue = makeQueue('hello');
  try {
    const resp = (await queue.connection.hello()) as { protocolVersion?: number };
    assertEq(
      PROTOCOL_VERSION,
      resp.protocolVersion,
      'client protocol version must match the server'
    );
  } finally {
    queue.close();
  }
});
