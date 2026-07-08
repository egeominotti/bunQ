/**
 * Integration smoke suite — mirrors sdk/python/tests/test_integration.py.
 *
 * Run with any runtime:
 *   bun tests/integration.ts
 *   node --experimental-strip-types tests/integration.ts
 *   deno run -A tests/integration.ts
 */

import { CommandError, Connection, type Job } from '../dist/index.js';
import {
  assert,
  assertEq,
  getPort,
  makeQueue,
  makeWorker,
  runSuite,
  test,
  waitFor,
} from './harness.ts';

test('ping + hello', async () => {
  const conn = new Connection({ host: '127.0.0.1', port: getPort() });
  assert(await conn.ping(), 'ping should return true');
  const hello = await conn.hello();
  assert(Number(hello.protocolVersion) >= 1, 'protocolVersion >= 1');
  assert(typeof hello.server === 'string', 'server name present');
  conn.close();
});

test('add + query + name + counts', async () => {
  const queue = makeQueue<{ to: string }>('add');
  try {
    const job = await queue.add('send', { to: 'a@b.c' }, { priority: 5 });
    assert(job.id.length > 0, 'job id assigned');
    const state = await queue.getJobState(job.id);
    assert(state === 'waiting' || state === 'prioritized', `queued state (${state})`);
    const fetched = await queue.getJob(job.id);
    assert(fetched !== null, 'job fetched');
    assertEq(fetched?.name, 'send', 'job name');
    assertEq((fetched?.data as Record<string, unknown>).to, 'a@b.c', 'job data');
    const counts = await queue.getJobCounts();
    assertEq(counts.waiting + counts.prioritized, 1, 'one job queued');
  } finally {
    queue.close();
  }
});

test('addBulk 50 + count', async () => {
  const queue = makeQueue('bulk');
  try {
    const jobs = await queue.addBulk(
      Array.from({ length: 50 }, (_, i) => ({ name: 'task', data: { i } }))
    );
    assertEq(jobs.length, 50, 'bulk returned 50 jobs');
    assertEq(await queue.count(), 50, 'count is 50');
  } finally {
    queue.close();
  }
});

test('worker roundtrip: 10 jobs, concurrency 3', async () => {
  const queue = makeQueue<{ value: number }>('worker');
  const results = new Map<string, unknown>();
  const jobs = await Promise.all(
    Array.from({ length: 10 }, (_, i) => queue.add('double', { value: i }))
  );
  const worker = makeWorker<{ value: number }, { doubled: number }>(
    queue.name,
    async (job) => ({ doubled: job.data.value * 2 }),
    { concurrency: 3, pollTimeoutMs: 1000 }
  );
  worker.on('completed', (job: Job, result: unknown) => results.set(job.id, result));
  try {
    await waitFor(() => results.size === 10);
    for (const job of jobs) {
      assertEq(await queue.getJobState(job.id), 'completed', `job ${job.id} completed`);
      const result = await queue.getResult<{ doubled: number }>(job.id);
      assert(result.doubled % 2 === 0, 'result is doubled');
    }
  } finally {
    await worker.close();
    queue.close();
  }
});

test('worker failure attempts=1 → failed + DLQ', async () => {
  const queue = makeQueue('fail');
  const failures: string[] = [];
  const worker = makeWorker(queue.name, async () => {
    throw new Error('kaboom');
  });
  worker.on('failed', (_job: Job, err: Error) => failures.push(err.message));
  try {
    const job = await queue.add('boom', { x: 1 }, { attempts: 1 });
    await waitFor(() => failures.length > 0);
    assert(failures[0].includes('kaboom'), 'failure message propagated');
    await waitFor(async () => (await queue.getJobState(job.id)) === 'failed');
    assertEq((await queue.getDlq()).length, 1, 'one DLQ entry');
  } finally {
    await worker.close();
    queue.close();
  }
});

test('priority ordering via PULL', async () => {
  const queue = makeQueue('prio');
  try {
    await queue.add('low', { p: 'low' }, { priority: 1 });
    await queue.add('high', { p: 'high' }, { priority: 100 });
    const response = await queue.connection.call({
      cmd: 'PULL',
      queue: queue.name,
      owner: 'integration',
      timeout: 1000,
    });
    const pulled = response.job as Record<string, unknown>;
    assertEq((pulled.data as Record<string, unknown>).p, 'high', 'high priority first');
  } finally {
    queue.close();
  }
});

test('pause / resume / isPaused', async () => {
  const queue = makeQueue('pause');
  try {
    await queue.pause();
    assertEq(await queue.isPaused(), true, 'paused');
    await queue.resume();
    assertEq(await queue.isPaused(), false, 'resumed');
  } finally {
    queue.close();
  }
});

test('nonexistent job id handled', async () => {
  const queue = makeQueue('missing');
  try {
    assertEq(await queue.getJob('nonexistent-job-id'), null, 'getJob returns null');
    try {
      await queue.connection.call({ cmd: 'GetJob', id: 'nonexistent-job-id' });
    } catch (err) {
      assert(err instanceof CommandError, 'raw command raises CommandError');
    }
  } finally {
    queue.close();
  }
});

await runSuite();
