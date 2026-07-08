/** E2E: Simple Mode (Bunqueue) — routes, middleware, retry, circuit breaker, batch. */

import { Bunqueue } from '../dist/index.js';
import { assert, assertEq, getPort, qname, sleep, test, waitFor } from './harness.ts';

function makeApp<T, R>(name: string, opts: Record<string, unknown>): Bunqueue<T, R> {
  return new Bunqueue<T, R>(name, {
    connection: { host: '127.0.0.1', port: getPort() },
    pollTimeout: 300,
    ...opts,
  });
}

test('simple: constructor guards (no mode, two modes, embedded)', async () => {
  const conn = { connection: { host: '127.0.0.1', port: getPort() } };
  let threw = 0;
  try {
    new Bunqueue(qname('sg'), { ...conn });
  } catch {
    threw += 1;
  }
  try {
    new Bunqueue(qname('sg'), { ...conn, processor: () => 1, routes: { a: () => 1 } });
  } catch {
    threw += 1;
  }
  try {
    new Bunqueue(qname('sg'), { ...conn, processor: () => 1, embedded: true } as never);
  } catch {
    threw += 1;
  }
  assertEq(threw, 3, 'all three invalid configs throw');
});

test('simple: routes dispatch by job name, unknown route fails', async () => {
  const app = makeApp<{ to: string }, { channel: string }>(qname('routes'), {
    routes: {
      'send-email': (job: { data: { to: string } }) => ({ channel: `email:${job.data.to}` }),
      'send-sms': (job: { data: { to: string } }) => ({ channel: `sms:${job.data.to}` }),
    },
  });
  const results: string[] = [];
  const failures: string[] = [];
  app.on('completed', ((_job: unknown, result: { channel: string }) =>
    results.push(result.channel)) as never);
  app.on('failed', ((job: { name: string }) => failures.push(job.name)) as never);
  try {
    await app.add('send-email', { to: 'alice' });
    await app.add('send-sms', { to: 'bob' });
    await app.add('no-such-route', { to: 'x' }, { attempts: 1 });
    await waitFor(() => results.length === 2 && failures.length === 1);
    assert(results.includes('email:alice') && results.includes('sms:bob'), 'both routes ran');
    assertEq(failures[0], 'no-such-route', 'unknown route failed');
  } finally {
    await app.close();
  }
});

test('simple: middleware runs onion-style around the processor', async () => {
  const order: string[] = [];
  const app = makeApp<Record<string, never>, string>(qname('mw'), {
    processor: () => {
      order.push('processor');
      return 'done';
    },
  });
  app.use(async (_job, next) => {
    order.push('mw1-in');
    const result = await next();
    order.push('mw1-out');
    return result;
  });
  app.use(async (_job, next) => {
    order.push('mw2-in');
    const result = await next();
    order.push('mw2-out');
    return result;
  });
  try {
    await app.add('task', {});
    await waitFor(() => order.length === 5);
    assertEq(
      order.join(','),
      'mw1-in,mw2-in,processor,mw2-out,mw1-out',
      'onion order mw1 -> mw2 -> processor -> mw2 -> mw1'
    );
  } finally {
    await app.close();
  }
});

test('simple: in-process retry with retryIf recovers without re-queueing', async () => {
  let attempts = 0;
  let nonRetryableAttempts = 0;
  const app = makeApp<{ kind: string }, string>(qname('retry'), {
    processor: async (job: { data: { kind: string } }) => {
      if (job.data.kind === 'flaky') {
        attempts += 1;
        if (attempts < 3) throw new Error('transient 503');
        return 'recovered';
      }
      nonRetryableAttempts += 1;
      throw new Error('fatal 400');
    },
    retry: {
      maxAttempts: 5,
      delay: 20,
      strategy: 'jitter',
      retryIf: (error: Error) => error.message.includes('503'),
    },
  });
  const completed: string[] = [];
  const failed: string[] = [];
  app.on('completed', ((_j: unknown, r: string) => completed.push(r)) as never);
  app.on('failed', ((_j: unknown, e: Error) => failed.push(e.message)) as never);
  try {
    await app.add('flaky-job', { kind: 'flaky' }, { attempts: 1 });
    await app.add('fatal-job', { kind: 'fatal' }, { attempts: 1 });
    await waitFor(() => completed.length === 1 && failed.length === 1);
    assertEq(attempts, 3, 'flaky job took exactly 3 in-process attempts');
    assertEq(nonRetryableAttempts, 1, 'retryIf=false error was not retried');
    assertEq(completed[0], 'recovered', 'flaky job recovered');
  } finally {
    await app.close();
  }
});

test('simple: circuit breaker opens after threshold and recovers half-open', async () => {
  const transitions: string[] = [];
  const app = makeApp<{ boom: boolean }, string>(qname('cb'), {
    concurrency: 1,
    processor: async (job: { data: { boom: boolean } }) => {
      if (job.data.boom) throw new Error('gateway down');
      return 'ok';
    },
    circuitBreaker: {
      threshold: 3,
      resetTimeout: 700,
      onOpen: () => transitions.push('open'),
      onHalfOpen: () => transitions.push('half-open'),
      onClose: () => transitions.push('closed'),
    },
  });
  const completed: string[] = [];
  app.on('completed', ((_j: unknown, r: string) => completed.push(r)) as never);
  app.on('error', (() => {}) as never);
  try {
    for (let i = 0; i < 3; i += 1) await app.add('boom', { boom: true }, { attempts: 1 });
    await waitFor(() => app.getCircuitState() === 'open', 10_000);
    assert(transitions.includes('open'), 'onOpen fired');
    assert(app.worker.isPaused(), 'worker paused while open');

    // After resetTimeout the breaker half-opens and resumes the worker;
    // a successful job then closes it.
    await app.add('recover', { boom: false }, { attempts: 1 });
    await waitFor(() => completed.includes('ok'), 15_000);
    await waitFor(() => app.getCircuitState() === 'closed', 10_000);
    assertEq(
      transitions.join(','),
      'open,half-open,closed',
      'state sequence open -> half-open -> closed'
    );
  } finally {
    await app.close();
  }
});

test('simple: batch flushes on size, on timeout, and on close', async () => {
  const flushes: number[] = [];
  const app = makeApp<{ i: number }, { done: boolean }>(qname('batch'), {
    concurrency: 10,
    batch: {
      size: 3,
      timeout: 400,
      processor: async (jobs: Array<{ data: { i: number } }>) => {
        flushes.push(jobs.length);
        return jobs.map(() => ({ done: true }));
      },
    },
  });
  let completedCount = 0;
  app.on('completed', (() => {
    completedCount += 1;
  }) as never);
  try {
    // Size flush: 3 jobs together
    await Promise.all([0, 1, 2].map((i) => app.add('b', { i })));
    await waitFor(() => flushes.length === 1);
    assertEq(flushes[0], 3, 'first flush by size');

    // Timeout flush: single job flushed after ~400ms
    await app.add('b', { i: 3 });
    await waitFor(() => flushes.length === 2);
    assertEq(flushes[1], 1, 'second flush by timeout');

    await waitFor(() => completedCount === 4);
  } finally {
    await app.close();
  }
  assertEq(flushes.length, 2, 'no spurious flushes');
});

test('simple: close flushes the remaining batch buffer', async () => {
  const flushes: number[] = [];
  const app = makeApp<{ i: number }, string>(qname('batchclose'), {
    concurrency: 5,
    batch: {
      size: 50,
      timeout: 60_000, // neither size nor timeout will flush
      processor: async (jobs: Array<{ data: { i: number } }>) => {
        flushes.push(jobs.length);
        return jobs.map(() => 'flushed');
      },
    },
  });
  try {
    await app.add('b', { i: 1 });
    await app.add('b', { i: 2 });
    // wait until both jobs are pulled and sitting in the accumulator
    await sleep(700);
  } finally {
    await app.close(); // destroy() flushes the partial buffer
  }
  assertEq(flushes.length, 1, 'close flushed once');
  assertEq(flushes[0], 2, 'both buffered jobs flushed on close');
});
