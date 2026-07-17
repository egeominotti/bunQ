/**
 * E2E: realistic production scenarios against a real server — a flaky order
 * pipeline with retries and poison jobs landing in the DLQ (zero-loss
 * accounting), a limit-bounded Simple Mode interval digest, a 1500-job burst
 * through a >1000-slot worker (server-clamped PULLB), and a long job that
 * outlives its lock TTL thanks to heartbeat lock renewal. The invoice burst
 * verifies that concurrent processing never crosses persisted results.
 */

import { Bunqueue, UnrecoverableError } from '../dist/index.js';
import {
  assert,
  assertEq,
  getPort,
  makeWorker,
  namedQueue,
  qname,
  sleep,
  test,
  waitFor,
} from './harness.ts';

test('realistic: order pipeline — transient retries, poison → DLQ, zero loss', async () => {
  const name = qname('orders');
  const queue = namedQueue<{ i?: number; poison?: boolean }>(name);
  const invocations = new Map<string, number>();
  const completedIds = new Set<string>();
  const worker = makeWorker<{ i?: number; poison?: boolean }, unknown>(
    name,
    async (job) => {
      const n = (invocations.get(job.id) ?? 0) + 1;
      invocations.set(job.id, n);
      if (job.data.poison) throw new UnrecoverableError('malformed order payload');
      if ((job.data.i ?? 1) % 10 === 0 && n === 1) throw new Error('transient upstream glitch');
      return { charged: true };
    },
    { concurrency: 8 }
  );
  worker.on('completed', (job: { id: string }) => completedIds.add(job.id));
  worker.on('failed', () => {}); // poison + transient failures are expected
  worker.on('error', () => {});
  try {
    const orders = Array.from({ length: 120 }, (_, i) => ({
      name: 'order',
      data: { i },
      opts: { attempts: 3, backoff: 50 },
    }));
    await queue.addBulk(orders);
    for (let p = 0; p < 3; p++) {
      await queue.add('order', { poison: true }, { attempts: 3, backoff: 50 });
    }
    await waitFor(async () => completedIds.size >= 120, 60_000);
    await waitFor(async () => (await queue.getDlq()).length >= 3, 30_000);
    assertEq(completedIds.size, 120, 'every order completed (and only once: id set)');
    assertEq((await queue.getDlq()).length, 3, 'every poison order landed in the DLQ');
    const retried = [...invocations.values()].filter((n) => n > 1).length;
    assert(retried >= 12, `transient glitches really were retried (got ${retried})`);
  } finally {
    await worker.close();
    await queue.obliterate();
    queue.close();
  }
});

test('realistic: Simple Mode limit-bounded digest fires exactly N times', async () => {
  let fired = 0;
  const app = new Bunqueue(qname('digest'), {
    connection: { host: '127.0.0.1', port: getPort() },
    pollTimeout: 300,
    processor: async () => {
      fired += 1;
      return 'sent';
    },
  });
  const schedId = qname('dig');
  try {
    await app.every(schedId, 300, { kind: 'digest' }, { limit: 3 });
    await waitFor(async () => fired >= 3, 20_000);
    await sleep(1500); // ~5 more fires would land here without the limit
    assertEq(fired, 3, 'the scheduler must stop at its execution limit');
  } finally {
    await app.removeCron(schedId).catch(() => {});
    await app.close();
  }
});

test('realistic: 1500-job burst drains through a >1000-slot worker', async () => {
  const name = qname('burst');
  const queue = namedQueue<{ i: number }>(name);
  let completed = 0;
  const pullErrors: string[] = [];
  const worker = makeWorker<{ i: number }, string>(name, async () => 'ok', {
    concurrency: 1100,
    batchSize: 5000, // clamps to the server max (1000)
    pollTimeoutMs: 300,
  });
  worker.on('completed', () => {
    completed += 1;
  });
  worker.on('error', (err: Error) => pullErrors.push(err.message));
  try {
    const jobs = Array.from({ length: 1500 }, (_, i) => ({ name: 'evt', data: { i } }));
    for (let off = 0; off < jobs.length; off += 500) {
      await queue.addBulk(jobs.slice(off, off + 500));
    }
    await waitFor(async () => completed >= 1500, 60_000);
    assertEq(completed, 1500, 'the whole burst must drain');
    assertEq(
      pullErrors.filter((m) => /count/i.test(m)).length,
      0,
      `no PULLB count rejections, got: ${pullErrors.slice(0, 3).join(' | ')}`
    );
  } finally {
    await worker.close();
    await queue.obliterate();
    queue.close();
  }
});

test('realistic: heartbeats keep a long job leased past its lock TTL', async () => {
  const name = qname('long');
  const queue = namedQueue<{ size: string }>(name);
  let invocations = 0;
  const worker = makeWorker<{ size: string }, string>(
    name,
    async () => {
      invocations += 1;
      await sleep(5500); // far beyond the 2s lock TTL
      return 'done';
    },
    { lockTtlMs: 2000, heartbeatIntervalS: 1 }
  );
  try {
    const job = await queue.add('crunch', { size: 'xl' }, { attempts: 3 });
    await waitFor(async () => (await queue.getJobState(job.id)) === 'completed', 30_000);
    assertEq(invocations, 1, 'lock renewal must prevent a stall retry (single execution)');
  } finally {
    await worker.close();
    await queue.obliterate();
    queue.close();
  }
});

test('realistic: concurrent invoice burst preserves every persisted result', async () => {
  const name = qname('invoices');
  const queue = namedQueue<{ invoice: number; cents: number }>(name);
  const worker = makeWorker<{ invoice: number; cents: number }, { invoice: number; total: number }>(
    name,
    async (job) => ({ invoice: job.data.invoice, total: job.data.cents * 2 }),
    { concurrency: 12, batchSize: 32, pollTimeoutMs: 300 }
  );
  try {
    const jobs = await queue.addBulk(
      Array.from({ length: 64 }, (_, invoice) => ({
        name: 'reconcile',
        data: { invoice, cents: 101 + invoice },
      }))
    );
    await waitFor(async () => (await queue.getJobCounts()).completed === jobs.length, 30_000);

    let checksum = 0;
    for (let invoice = 0; invoice < jobs.length; invoice++) {
      const result = await queue.getResult<{ invoice: number; total: number }>(jobs[invoice].id);
      assertEq(result.invoice, invoice, `result ${invoice} belongs to its originating invoice`);
      assertEq(result.total, (101 + invoice) * 2, `result ${invoice} preserves its amount`);
      checksum += result.total;
    }
    assertEq(checksum, 16_960, 'all 64 persisted results contribute exactly once');
  } finally {
    await worker.close();
    await queue.obliterate();
    queue.close();
  }
});
