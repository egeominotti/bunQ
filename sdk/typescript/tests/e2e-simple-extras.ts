/** E2E: Simple Mode (Bunqueue) — triggers, TTL, cancel, dedup, cron, events. */

import { Bunqueue } from '../dist/legacy.js';
import { assert, assertEq, getPort, qname, sleep, test, waitFor } from './harness.ts';

function makeApp<T, R>(name: string, opts: Record<string, unknown>): Bunqueue<T, R> {
  return new Bunqueue<T, R>(name, {
    connection: { host: '127.0.0.1', port: getPort() },
    pollTimeout: 300,
    ...opts,
  });
}

test('simple: triggers create follow-up jobs, honor condition, and chain', async () => {
  const alerted: number[] = [];
  const receipts: string[] = [];
  const chain: string[] = [];
  const app = makeApp<Record<string, unknown>, Record<string, unknown>>(qname('trig'), {
    concurrency: 4,
    routes: {
      'place-order': (job: { data: { id: string; total: number } }) => ({
        orderId: job.data.id,
        total: job.data.total,
      }),
      'send-receipt': (job: { data: { id: string } }) => {
        receipts.push(job.data.id);
        return { sent: true };
      },
      'fraud-alert': (job: { data: { amount: number } }) => {
        alerted.push(job.data.amount);
        return { alerted: true };
      },
      'step-1': () => {
        chain.push('step-1');
        return { next: true };
      },
      'step-2': () => {
        chain.push('step-2');
        return { next: true };
      },
      'step-3': () => {
        chain.push('step-3');
        return { next: true };
      },
    },
  });
  app
    .trigger({
      on: 'place-order',
      create: 'send-receipt',
      data: (result) => ({ id: (result as { orderId: string }).orderId }),
    })
    .trigger({
      on: 'place-order',
      create: 'fraud-alert',
      data: (result) => ({ amount: (result as { total: number }).total }),
      condition: (result) => (result as { total: number }).total > 1000,
    })
    .trigger({ on: 'step-1', create: 'step-2', data: (r) => r as Record<string, unknown> })
    .trigger({ on: 'step-2', create: 'step-3', data: (r) => r as Record<string, unknown> });
  try {
    await app.add('place-order', { id: 'small', total: 99 });
    await app.add('place-order', { id: 'big', total: 5000 });
    await app.add('step-1', {});
    await waitFor(() => receipts.length === 2 && alerted.length === 1 && chain.length === 3);
    assert(receipts.includes('small') && receipts.includes('big'), 'receipts for both orders');
    assertEq(alerted[0], 5000, 'fraud alert only for the big order');
    assertEq(chain.join(','), 'step-1,step-2,step-3', 'trigger chain executed in order');
  } finally {
    await app.close();
  }
});

test('simple: expired TTL jobs are rejected at pickup', async () => {
  let processedExpired = 0;
  const app = makeApp<{ v: number }, string>(qname('ttl'), {
    processor: (job: { name: string | null }) => {
      if (job.name === 'short-lived') processedExpired += 1;
      return 'ok';
    },
    ttl: { defaultTtl: 0, perName: { 'short-lived': 200 } },
    autorun: false, // let the job age past its TTL before the worker starts
  });
  const failed: string[] = [];
  app.on('failed', ((_j: unknown, e: Error) => failed.push(e.message)) as never);
  try {
    await app.add('short-lived', { v: 1 }, { attempts: 1 });
    await app.add('long-lived', { v: 2 }, { attempts: 1 });
    await sleep(400); // short-lived is now expired
    app.worker.run();
    await waitFor(() => failed.length === 1);
    assert(failed[0].includes('expired'), `TTL failure reason: ${failed[0]}`);
    assertEq(processedExpired, 0, 'expired job never reached the processor');
    await waitFor(async () => (await app.getJobCounts()).completed === 1);
  } finally {
    await app.close();
  }
});

test('simple: getSignal cancellation aborts a running job', async () => {
  const holder: { app?: Bunqueue<{ file: string }, string> } = {};
  const app = makeApp<{ file: string }, string>(qname('cancel'), {
    processor: async (job: { id: string }) => {
      const signal = holder.app?.getSignal(job.id);
      for (let i = 0; i < 100; i += 1) {
        if (signal?.aborted) throw new Error('Cancelled');
        await sleep(50);
      }
      return 'never';
    },
  });
  holder.app = app;
  const failed: string[] = [];
  const activeIds: string[] = [];
  app.on('active', ((job: { id: string }) => activeIds.push(job.id)) as never);
  app.on('failed', ((_j: unknown, e: Error) => failed.push(e.message)) as never);
  try {
    const job = await app.add('encode', { file: 'big.mp4' }, { attempts: 1 });
    await waitFor(() => activeIds.includes(job.id));
    assert(app.getSignal(job.id) !== null, 'signal available while active');
    app.cancel(job.id);
    await waitFor(() => app.isCancelled(job.id));
    await waitFor(() => failed.length === 1, 20_000);
    assertEq(failed[0], 'Cancelled', 'processor aborted via signal');
  } finally {
    await app.close();
  }
});

test('simple: dedup defaults derive the id from name+data', async () => {
  const app = makeApp<{ event: string }, string>(qname('dedup'), {
    processor: () => 'ok',
    deduplication: { ttl: 60_000 },
    autorun: false, // keep jobs waiting so counts are stable
  });
  try {
    const first = await app.add('hook', { event: 'user.created' });
    const dup = await app.add('hook', { event: 'user.created' });
    const other = await app.add('hook', { event: 'user.updated' });
    assertEq(dup.id, first.id, 'same name+data deduplicated to the same job');
    assert(other.id !== first.id, 'different data creates a new job');
    assertEq(await app.count(), 2, 'two distinct jobs enqueued');
  } finally {
    await app.close();
  }
});

test('simple: cron + every + listCrons + removeCron', async () => {
  const app = makeApp<{ type: string }, string>(qname('cron'), {
    processor: () => 'ok',
    autorun: false,
  });
  const cronId = qname('daily');
  const everyId = qname('tick');
  try {
    const info = await app.cron(cronId, '0 9 * * *', { type: 'report' }, { timezone: 'UTC' });
    assert(info !== null && info.schedule === '0 9 * * *', 'cron upserted with pattern');
    await app.every(everyId, 300, { type: 'ping' });
    const crons = await app.listCrons();
    const names = crons.map((c) => String(c.name));
    assert(names.includes(cronId) && names.includes(everyId), 'both schedulers listed');
    await waitFor(async () => (await app.count()) >= 1, 10_000); // every() fired
    await app.removeCron(cronId);
    await app.removeCron(everyId);
    const after = (await app.listCrons()).map((c) => String(c.name));
    assert(!after.includes(cronId) && !after.includes(everyId), 'both schedulers removed');
  } finally {
    await app.close();
  }
});

test('simple: lifecycle events and controls proxy to queue + worker', async () => {
  const events: string[] = [];
  const app = makeApp<{ v: number }, string>(qname('events'), {
    processor: () => 'ok',
    concurrency: 2,
  });
  app.on('ready', (() => events.push('ready')) as never);
  app.on('drained', (() => events.push('drained')) as never);
  app.on('closed', (() => events.push('closed')) as never);
  try {
    await app.add('e', { v: 1 });
    await waitFor(async () => (await app.getJobCounts()).completed === 1);
    await waitFor(() => events.includes('drained'));
    assert(events.includes('ready'), 'ready fired (or replayed)');

    app.pause();
    assert(app.isPaused(), 'paused');
    app.resume();
    assert(!app.isPaused(), 'resumed');
    assert(app.isRunning(), 'running');
  } finally {
    await app.close();
  }
  assert(app.isClosed(), 'closed after close()');
  assert(events.includes('closed'), 'closed event fired');
});

test('simple: worker rate limit gates job starts per window', async () => {
  const startTimes: number[] = [];
  const app = makeApp<{ i: number }, string>(qname('rate'), {
    concurrency: 10,
    processor: () => {
      startTimes.push(Date.now());
      return 'ok';
    },
    rateLimit: { max: 2, duration: 500 },
  });
  try {
    await Promise.all(Array.from({ length: 6 }, (_, i) => app.add('r', { i })));
    await waitFor(async () => (await app.getJobCounts()).completed === 6, 20_000);
    startTimes.sort((a, b) => a - b);
    // 6 jobs at max 2 per 500ms need at least ~2 extra windows
    const span = startTimes[5] - startTimes[0];
    assert(span >= 900, `starts spread over rate windows (span ${span}ms)`);
    for (let i = 0; i + 2 < startTimes.length; i += 1) {
      const windowSpan = startTimes[i + 2] - startTimes[i];
      assert(windowSpan >= 400, `no 3 starts inside one window (got ${windowSpan}ms)`);
    }
  } finally {
    await app.close();
  }
});
