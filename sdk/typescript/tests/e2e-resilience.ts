/** E2E: resilience + throughput — backpressure, ACKB batching, connection pool. */

import { ConnectionPool, Queue, type TelemetryEvent, Worker } from '../dist/legacy.js';
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

interface AckBatcherProbe {
  add(item: {
    id: string;
    token: string;
    result: unknown;
    onSettled: (err?: unknown, applied?: boolean) => void;
  }): void;
  flush(): Promise<void>;
}

async function makeAckBatcher(connection: { call: (command: unknown) => Promise<unknown> }) {
  const { AckBatcher } = (await import('../dist/ack-batcher.js')) as {
    AckBatcher: new (
      conn: { call: (command: unknown) => Promise<unknown> },
      maxSize: number,
      maxDelayMs: number
    ) => AckBatcherProbe;
  };
  return new AckBatcher(connection, 100, 1000);
}

test('resilience: maxInFlight applies backpressure and still delivers every command', async () => {
  const events: TelemetryEvent[] = [];
  const queue = new Queue(qname('bp'), {
    host: '127.0.0.1',
    port: getPort(),
    maxInFlight: 1,
    onTelemetry: (e) => events.push(e),
  });
  try {
    await queue.waitUntilReady();
    // Fire many concurrent adds; with a 1-command ceiling most must park.
    const jobs = await Promise.all(Array.from({ length: 12 }, (_, i) => queue.add('t', { i })));
    assertEq(jobs.length, 12, 'all concurrent adds resolved under backpressure');
    assert(
      events.some((e) => e.type === 'backpressure'),
      'a backpressure telemetry event fired'
    );
  } finally {
    await queue.obliterate();
    queue.close();
  }
});

test('resilience: worker ackBatch (ACKB) completes every job', async () => {
  const name = qname('ackb');
  const queue = namedQueue(name);
  const completed: string[] = [];
  const worker = makeWorker(name, async (job) => ({ echoed: (job.data as { i: number }).i }), {
    concurrency: 8,
    ackBatch: { enabled: true, maxSize: 5, maxDelayMs: 5 },
  });
  worker.on('completed', (job) => completed.push(job.id));
  try {
    const jobs = await Promise.all(Array.from({ length: 25 }, (_, i) => queue.add('t', { i })));
    await waitFor(() => completed.length === 25, 20_000);
    assertEq(completed.length, 25, 'every job completed via batched ACK');
    // and the server actually recorded the acks (state completed + result stored)
    const state = await queue.getJobState(jobs[0].id);
    assertEq(state, 'completed', 'server persisted the batched ack');
  } finally {
    await worker.close();
    queue.close();
  }
});

test('resilience: ConnectionPool fans producer commands across N sockets', async () => {
  const queue = new Queue(qname('pool'), { host: '127.0.0.1', port: getPort(), poolSize: 3 });
  try {
    assert(queue.connection instanceof ConnectionPool, 'queue uses a ConnectionPool');
    assertEq((queue.connection as ConnectionPool).size, 3, 'pool has 3 connections');
    const jobs = await Promise.all(Array.from({ length: 15 }, (_, i) => queue.add('t', { i })));
    assertEq(jobs.length, 15, 'all adds via the pool resolved');
    assert(queue.connection.isConnected, 'pool reports connected');
    await waitFor(async () => (await queue.getJobCounts()).waiting >= 15, 10_000);
  } finally {
    await queue.obliterate();
    queue.close();
  }
});

test('resilience: ackBatch failed ACKB settles every item exactly once and frees slots', async () => {
  // Unit-level: a stub connection whose ACKB always rejects. Every item's
  // onSettled must fire exactly once with the error — a throwing callback
  // (unhandled 'error' emit) must not starve the remaining items.
  const failing = { call: () => Promise.reject(new Error('ACKB down')) };
  const batcher = await makeAckBatcher(failing);
  const settled: Record<string, number> = { a: 0, b: 0, c: 0 };
  const errs: unknown[] = [];
  batcher.add({
    id: 'a',
    token: 't',
    result: 1,
    onSettled: (e) => {
      settled.a++;
      errs.push(e);
    },
  });
  batcher.add({
    id: 'b',
    token: 't',
    result: 2,
    onSettled: (e) => {
      settled.b++;
      errs.push(e);
      throw new Error('listener boom'); // must not affect c
    },
  });
  batcher.add({
    id: 'c',
    token: 't',
    result: 3,
    onSettled: (e) => {
      settled.c++;
      errs.push(e);
    },
  });
  await batcher.flush();
  assertEq(settled.a, 1, 'a settled exactly once');
  assertEq(settled.b, 1, 'b settled exactly once (no double settle)');
  assertEq(settled.c, 1, 'c settled despite b throwing');
  assert(
    errs.every((e) => e instanceof Error),
    'every callback received the ACKB error'
  );
});

test('resilience: ACKB ignoredIndices settle duplicate IDs by position', async () => {
  const batcher = await makeAckBatcher({
    call: async () => ({
      ok: true,
      data: { ignoredIds: ['same'], ignoredIndices: [1] },
    }),
  });
  const outcomes: Array<{ error: unknown; applied: boolean | undefined }> = [];
  for (const id of ['same', 'same', 'other']) {
    batcher.add({
      id,
      token: `token-${outcomes.length}`,
      result: null,
      onSettled: (error, applied) => outcomes.push({ error, applied }),
    });
  }
  await batcher.flush();
  assertEq(outcomes.length, 3, 'every ACKB position settled');
  assert(
    outcomes.every(({ error }) => error === undefined),
    'ignored ACKs are not errors'
  );
  assertEq(
    JSON.stringify(outcomes.map(({ applied }) => applied)),
    JSON.stringify([true, false, true]),
    'ignoredIndices selects the second duplicate ID only'
  );
});

test('resilience: ACKB never infers duplicate positions from ignoredIds alone', async () => {
  const batcher = await makeAckBatcher({
    call: async () => ({ ok: true, data: { ignoredIds: ['same'] } }),
  });
  const outcomes: Array<{ error: unknown; applied: boolean | undefined }> = [];
  for (const token of ['first', 'second']) {
    batcher.add({
      id: 'same',
      token,
      result: null,
      onSettled: (error, applied) => outcomes.push({ error, applied }),
    });
  }
  await batcher.flush();
  assertEq(outcomes.length, 2, 'every ambiguous ACKB position settled');
  assert(
    outcomes.every(({ error }) => error instanceof Error),
    'ambiguity is observable'
  );
  assert(
    outcomes.every(({ applied }) => applied === undefined),
    'no position was fabricated'
  );
});

test('resilience: ackBatch with a throwing completed-listener keeps processing (no slot leak)', async () => {
  const name = qname('ackb-throw');
  const queue = namedQueue(name);
  const seen: string[] = [];
  const worker = makeWorker(name, async (job) => ({ i: (job.data as { i: number }).i }), {
    concurrency: 1, // a single leaked slot would wedge the worker entirely
    ackBatch: { enabled: true, maxSize: 1, maxDelayMs: 1 },
  });
  worker.on('error', () => {}); // observe errors; 'completed' below still throws
  worker.on('completed', (job) => {
    seen.push(job.id);
    throw new Error('completed listener boom');
  });
  try {
    await Promise.all([
      queue.add('t', { i: 1 }),
      queue.add('t', { i: 2 }),
      queue.add('t', { i: 3 }),
    ]);
    // If the slot leaked on the first throw, jobs 2 and 3 would never run.
    await waitFor(() => seen.length === 3, 20_000);
    assertEq(seen.length, 3, 'all three jobs completed despite the throwing listener');
  } finally {
    await worker.close();
    await queue.obliterate();
    queue.close();
  }
});

test('resilience: Worker rejects a pool (single-connection ownership) — pool is producer-side', async () => {
  // Documented contract: Worker keeps a single Connection. Constructing one
  // never yields a pool; this guards against a future regression wiring a pool
  // into the worker's pull/ack path.
  const worker = new Worker(qname('wpool'), async () => ({}), {
    host: '127.0.0.1',
    port: getPort(),
    autorun: false,
  });
  try {
    assert(!(worker.connection instanceof ConnectionPool), 'worker uses a single connection');
  } finally {
    await worker.close();
  }
});
