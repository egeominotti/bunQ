/** E2E: edge cases — payload limits, unicode, pipelining, reconnect, isolation. */

import { type ChildProcess, spawn } from 'node:child_process';
import { mkdtempSync } from 'node:fs';
import { connect } from 'node:net';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { Queue } from '../dist/legacy.js';
import {
  assert,
  assertEq,
  getPort,
  makeQueue,
  makeWorker,
  qname,
  sleep,
  test,
  waitFor,
} from './harness.ts';

const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..', '..');

/** Spawn a bunqueue server on a FIXED port and wait until it accepts TCP. */
async function spawnFixedServer(port: number): Promise<ChildProcess> {
  const dataDir = mkdtempSync(join(tmpdir(), 'bunqueue-edge-'));
  const proc = spawn('bun', ['src/main.ts'], {
    cwd: REPO_ROOT,
    env: {
      ...process.env,
      TCP_PORT: String(port),
      HTTP_PORT: '0',
      BUNQUEUE_DATA_PATH: join(dataDir, 'bunq.db'),
    },
    stdio: 'ignore',
  });
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    const up = await new Promise<boolean>((res) => {
      const s = connect({ host: '127.0.0.1', port }, () => {
        s.destroy();
        res(true);
      });
      s.on('error', () => res(false));
      s.setTimeout(500, () => {
        s.destroy();
        res(false);
      });
    });
    if (up) return proc;
    await sleep(100);
  }
  proc.kill();
  throw new Error('fixed-port bunqueue server did not start within 15s');
}

test('edge: 2MB payload roundtrip', async () => {
  const queue = makeQueue<{ blob: string }>('edge-big');
  const blob = 'x'.repeat(2 * 1024 * 1024);
  const job = await queue.add('big', { blob });
  const fetched = await queue.getJob(job.id);
  assert(fetched !== null, 'job found');
  assertEq((fetched!.data as { blob: string }).blob.length, blob.length, 'payload intact');
  queue.close();
});

test('edge: unicode + nested payload survives msgpack', async () => {
  const queue = makeQueue('edge-unicode');
  const data = {
    emoji: '🚀🔥 “quotes” — dash',
    jp: 'こんにちは世界',
    nested: { list: [1, 2.5, null, true, 'x'], deep: { a: { b: { c: 'd' } } } },
    negative: -42,
    float: 12.34567,
  };
  const job = await queue.add('uni', data);
  const fetched = await queue.getJob(job.id);
  const got = fetched?.data as typeof data & { name: string };
  assertEq(got.emoji, data.emoji, 'emoji intact');
  assertEq(got.jp, data.jp, 'japanese intact');
  assertEq(got.nested.deep.a.b.c, 'd', 'deep nesting intact');
  assertEq(got.negative, -42, 'negative int intact');
  assertEq(got.float, 12.34567, 'float intact');
  queue.close();
});

test('edge: 200 concurrent adds pipeline on one socket', async () => {
  const queue = makeQueue<{ i: number }>('edge-pipe');
  const jobs = await Promise.all(Array.from({ length: 200 }, (_, i) => queue.add('p', { i })));
  const ids = new Set(jobs.map((j) => j.id));
  assertEq(ids.size, 200, 'all 200 ids unique');
  await waitFor(async () => (await queue.count()) === 200);
  queue.close();
});

test('edge: addBulk 1000 jobs in one frame', async () => {
  const queue = makeQueue<{ i: number }>('edge-bulk');
  const jobs = await queue.addBulk(
    Array.from({ length: 1000 }, (_, i) => ({ name: 'b', data: { i } }))
  );
  assertEq(jobs.length, 1000, '1000 ids returned');
  await waitFor(async () => (await queue.count()) === 1000);
  queue.close();
});

test('edge: multi-queue isolation on a shared connection', async () => {
  const a = makeQueue('edge-iso-a');
  const b = new Queue(qname('edge-iso-b'), { host: '127.0.0.1', port: getPort() });
  await a.add('only-a', { q: 'a' });
  await b.add('only-b', { q: 'b' });
  assertEq(await a.count(), 1, 'queue a sees 1');
  assertEq(await b.count(), 1, 'queue b sees 1');
  await a.obliterate();
  assertEq(await b.count(), 1, 'obliterating a leaves b intact');
  a.close();
  b.close();
});

test('edge: long-poll PULL on empty queue returns null after timeout', async () => {
  const queue = makeQueue('edge-poll');
  const started = Date.now();
  const response = await queue.connection.call(
    { cmd: 'PULL', queue: queue.name, owner: 'edge', timeout: 800 },
    10_000
  );
  assert(Date.now() - started >= 700, 'server held the long-poll');
  assertEq(response.job ?? null, null, 'no job on empty queue');
  queue.close();
});

test('edge: getJobs pagination is stable', async () => {
  const queue = makeQueue<{ i: number }>('edge-page');
  await queue.addBulk(Array.from({ length: 25 }, (_, i) => ({ name: 'p', data: { i } })));
  await waitFor(async () => (await queue.getJobs({ state: 'waiting' })).length === 25);
  const page1 = await queue.getJobs({ state: 'waiting', start: 0, end: 10 });
  const page2 = await queue.getJobs({ state: 'waiting', start: 10, end: 20 });
  const page3 = await queue.getJobs({ state: 'waiting', start: 20, end: 30 });
  assertEq(page1.length, 10, 'page1 = 10');
  assertEq(page2.length, 10, 'page2 = 10');
  assertEq(page3.length, 5, 'page3 = 5');
  const all = new Set([...page1, ...page2, ...page3].map((j) => j.id));
  assertEq(all.size, 25, 'pages do not overlap');
  queue.close();
});

test('edge: worker survives server crash + restart on the same port', async () => {
  // Dedicated server on a FIXED port so we can kill it and bring it back —
  // exactly what a production deploy/crash looks like to a connected client.
  const port = 21_000 + Math.floor(Math.random() * 5_000);
  const boot = () => spawnFixedServer(port);
  const first = await boot();
  const name = qname('edge-restart');
  const queue = new Queue<{ n: number }>(name, { host: '127.0.0.1', port });
  const seen: number[] = [];
  const { Worker } = await import('../dist/legacy.js');
  const worker = new Worker<{ n: number }, string>(
    name,
    (job) => {
      seen.push(job.data.n);
      return 'ok';
    },
    { host: '127.0.0.1', port, pollTimeoutMs: 300 }
  );
  worker.on('error', () => {}); // connection errors during the outage are expected
  await queue.add('before', { n: 1 });
  await waitFor(() => seen.includes(1));

  first.kill();
  await sleep(500);
  const second = await boot(); // same port: clients must reconnect lazily
  try {
    await waitFor(async () => {
      try {
        await queue.add('after', { n: 2 });
        return true;
      } catch {
        return false;
      }
    }, 20_000);
    await waitFor(() => seen.includes(2), 20_000);
    assert(seen.includes(1) && seen.includes(2), 'jobs before and after restart processed');
  } finally {
    await worker.close();
    queue.close();
    second.kill();
  }
});

test('edge: duplicate custom ids across addBulk are idempotent', async () => {
  const queue = makeQueue<{ i: number }>('edge-dup');
  const first = await queue.add('one', { i: 1 }, { jobId: 'edge-dup-key' });
  const again = await queue.add('one', { i: 2 }, { jobId: 'edge-dup-key' });
  assertEq(again.id, first.id, 're-add with same jobId returns the same job');
  assertEq(await queue.count(), 1, 'no duplicate enqueued');
  queue.close();
});

test('edge: results preserved per job under concurrency', async () => {
  const queue = makeQueue<{ i: number }>('edge-results');
  const ids: string[] = [];
  for (let i = 0; i < 20; i += 1) {
    ids.push((await queue.add('r', { i })).id);
  }
  const worker = makeWorker<{ i: number }, { double: number }>(
    queue.name,
    (job) => ({
      double: job.data.i * 2,
    }),
    { concurrency: 5 }
  );
  try {
    await waitFor(async () => (await queue.getJobCounts()).completed === 20, 20_000);
    for (let i = 0; i < 20; i += 1) {
      const result = (await queue.getResult(ids[i])) as { double: number };
      assertEq(result.double, i * 2, `result for job ${i} matches its input`);
    }
  } finally {
    await worker.close();
    queue.close();
  }
});
