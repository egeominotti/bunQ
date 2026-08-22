/**
 * Integration smoke suite — mirrors sdk/python/tests/test_integration.py.
 *
 * Run with any runtime:
 *   bun tests/integration.ts
 *   node --experimental-strip-types tests/integration.ts
 *   deno run -A tests/integration.ts
 */

import { frame } from '../dist/frame.js';
import {
  CommandError,
  Connection,
  type Job,
  MAX_FRAME_SIZE,
  SerializationError,
} from '../dist/index.js';
import {
  assert,
  assertEq,
  getPort,
  makeQueue,
  makeWorker,
  runSuite,
  sleep,
  test,
  waitFor,
} from './harness.ts';

test('outgoing frames reject payloads above the 64 MiB protocol cap', () => {
  let error: unknown;
  try {
    frame(new Uint8Array(MAX_FRAME_SIZE + 1));
  } catch (caught) {
    error = caught;
  }
  assert(error instanceof SerializationError, 'oversized frame must fail before allocation/write');
});

test('serialization failure does not retain an in-flight slot', async () => {
  const telemetry: import('../dist/index.js').TelemetryEvent[] = [];
  const conn = new Connection({
    host: '127.0.0.1',
    port: getPort(),
    commandTimeoutMs: 1000,
    maxInFlight: 1,
    onTelemetry: (event) => telemetry.push(event),
  });
  try {
    assert(await conn.ping(), 'connection should be ready before the regression');
    let error: unknown;
    try {
      await conn.call({ cmd: 'Ping', payload: new Uint8Array(MAX_FRAME_SIZE) });
    } catch (caught) {
      error = caught;
    }
    assert(error instanceof SerializationError, 'oversized command must be typed');
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    error = undefined;
    try {
      await conn.call({ cmd: 'Ping', payload: cyclic });
    } catch (caught) {
      error = caught;
    }
    assert(error instanceof SerializationError, 'MessagePack encoder failures must be typed');
    error = undefined;
    try {
      await conn.call({
        cmd: 'Ping',
        payload: { nested: 2n ** 62n, secret: 'must-not-reach-telemetry' },
      });
    } catch (caught) {
      error = caught;
    }
    assert(error instanceof SerializationError, 'BigInt must never reach the JavaScript broker');
    const serializationEvent = telemetry.find(
      (event) => event.type === 'error' && event.operation === 'serialization'
    );
    assert(serializationEvent?.type === 'error', 'serialization failure emits error telemetry');
    assert(
      serializationEvent?.type === 'error' &&
        serializationEvent.message === 'command serialization failed' &&
        !JSON.stringify(serializationEvent).includes('must-not-reach-telemetry'),
      'serialization error telemetry is sanitized'
    );
    error = undefined;
    try {
      await conn.call({ cmd: 'Ping', payload: new Map([[1, 'non-string key']]) });
    } catch (caught) {
      error = caught;
    }
    assert(error instanceof SerializationError, 'MessagePack map keys must be strings');
    const supported = await conn.call({
      cmd: 'Ping',
      payload: {
        binary: new Uint8Array([1, 2, 3]),
        date: new Date(0),
        map: new Map([['key', ['plain', { nested: true }]]]),
      },
    });
    assert(supported.ok, 'portable binary, Date, Map, object, and array values must remain valid');
    await Promise.race([
      conn.ping(),
      sleep(250).then(() => {
        throw new Error('serialization retained the only in-flight slot');
      }),
    ]);
  } finally {
    conn.close();
  }
});

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
    assertEq((fetched?.data as Record<string, unknown> | undefined)?.to, 'a@b.c', 'job data');
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
