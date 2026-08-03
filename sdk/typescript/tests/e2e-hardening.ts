/** Hardening: races, generated invariants, malformed-input fuzz corpus. */

import { Queue, SerializationError } from '../dist/index.js';
import { assert, assertEq, getPort, namedQueue, qname, test, waitFor } from './harness.ts';

function generator(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (Math.imul(state, 1_664_525) + 1_013_904_223) >>> 0;
    return state;
  };
}

test('hardening/race: concurrent retries of one custom id enqueue exactly once', async () => {
  const name = qname('idempotency-race');
  const queues = Array.from(
    { length: 24 },
    () => new Queue<{ attempt: number }>(name, { host: '127.0.0.1', port: getPort() })
  );
  try {
    const jobs = await Promise.all(
      queues.map((queue, attempt) =>
        queue.add('charge', { attempt }, { jobId: 'same-operation-id' })
      )
    );
    assertEq(new Set(jobs.map((job) => job.id)).size, 1, 'all retries return one job id');
    assertEq(await queues[0].count(), 1, 'the broker contains exactly one job');
  } finally {
    await queues[0].obliterate();
    queues.forEach((queue) => {
      queue.close();
    });
  }
});

test('hardening/race: simultaneous dequeues lease a single job to one owner', async () => {
  const name = qname('double-dequeue');
  const producer = namedQueue(name);
  const consumers = Array.from(
    { length: 12 },
    () => new Queue(name, { host: '127.0.0.1', port: getPort() })
  );
  try {
    const expected = await producer.add('only-once', { value: 1 });
    const responses = await Promise.all(
      consumers.map((queue, index) =>
        queue.call({
          cmd: 'PULL',
          queue: name,
          owner: `contender-${index}`,
          timeout: 250,
        })
      )
    );
    const leased = responses
      .map((response) => response.job as { id?: string } | null)
      .filter((job): job is { id?: string } => job !== null);
    assertEq(leased.length, 1, 'only one contender receives a lease');
    assertEq(String(leased[0].id), expected.id, 'the unique lease belongs to the queued job');
  } finally {
    await producer.obliterate();
    producer.close();
    consumers.forEach((queue) => {
      queue.close();
    });
  }
});

test('hardening/property: generated portable payloads preserve all invariants', async () => {
  const queue = namedQueue<Record<string, unknown>>(qname('generated-payloads'));
  const next = generator(0xbadc0de);
  const payloads = Array.from({ length: 64 }, (_, index) => {
    const sample = next();
    return {
      index,
      signed: (sample % 2_000_001) - 1_000_000,
      flag: (sample & 1) === 1,
      text: `case-${sample.toString(36)}-🧪`,
      nullable: index % 3 === 0 ? null : `value-${index}`,
      nested: [sample % 97, { checksum: (sample ^ index) >>> 0 }],
    };
  });
  try {
    const jobs = await queue.addBulk(
      payloads.map((data, index) => ({ name: `generated-${index % 7}`, data }))
    );
    assertEq(jobs.length, payloads.length, 'every generated input is accepted');
    await waitFor(async () => (await queue.count()) === payloads.length);
    for (let index = 0; index < jobs.length; index++) {
      const fetched = await queue.getJob(jobs[index].id);
      assert(fetched !== null, `generated job ${index} remains queryable`);
      assertEq(fetched.name, `generated-${index % 7}`, `generated job ${index} keeps its name`);
      assertEq(
        JSON.stringify(fetched.data),
        JSON.stringify(payloads[index]),
        `generated payload ${index} round-trips exactly`
      );
    }
  } finally {
    await queue.obliterate();
    queue.close();
  }
});

test('hardening/fuzz: malformed mutations fail typed and leave the socket usable', async () => {
  const queue = namedQueue(qname('mutation-fuzz'));
  const invalid: unknown[] = [];
  for (let depth = 1; depth <= 12; depth++) {
    let value: unknown = 2n ** BigInt(32 + depth);
    for (let level = 0; level < depth; level++) value = { nested: [value] };
    invalid.push(value);
  }
  const cycle: Record<string, unknown> = {};
  cycle.self = cycle;
  invalid.push(cycle, new Map([[1, 'non-string key']]));
  try {
    for (const payload of invalid) {
      let error: unknown;
      try {
        await queue.call({ cmd: 'Ping', payload });
      } catch (caught) {
        error = caught;
      }
      assert(error instanceof SerializationError, 'malformed mutation returns SerializationError');
    }
    assert(await queue.ping(), 'the connection remains usable after the entire fuzz corpus');
  } finally {
    queue.close();
  }
});
