/** E2E: worker — events, concurrency, pause/resume, unrecoverable, locks. */

import { type Job, UnrecoverableError } from '../dist/index.js';
import { assert, assertEq, makeQueue, makeWorker, sleep, test, waitFor } from './harness.ts';

test('worker: full event lifecycle (ready/active/completed/drained/closed)', async () => {
  const queue = makeQueue<{ v: number }>('events');
  const seen = {
    ready: 0,
    active: [] as string[],
    completed: [] as Array<[string, unknown]>,
    drained: 0,
    closed: 0,
  };
  const worker = makeWorker<{ v: number }, number>(queue.name, async (job) => job.data.v * 2);
  worker.on('ready', () => {
    seen.ready += 1;
  });
  worker.on('active', (job: Job) => seen.active.push(job.id));
  worker.on('completed', (job: Job, result: unknown) => seen.completed.push([job.id, result]));
  worker.on('drained', () => {
    seen.drained += 1;
  });
  worker.on('closed', () => {
    seen.closed += 1;
  });
  try {
    await worker.waitUntilReady();
    assertEq(seen.ready, 1, 'ready fired once');
    const job = await queue.add('t', { v: 21 });
    await waitFor(() => seen.completed.length === 1);
    assertEq(seen.completed[0][0], job.id, 'completed job id');
    assertEq(seen.completed[0][1], 42, 'completed result');
    assert(seen.active.includes(job.id), 'active fired');
    await waitFor(() => seen.drained >= 1, 10_000);
    await worker.close();
    assertEq(seen.closed, 1, 'closed fired');
    assert(worker.isClosed(), 'isClosed true');
  } finally {
    if (!worker.isClosed()) await worker.close();
    queue.close();
  }
});

test('worker: concurrency cap observed (max 3, parallelism ≥ 2)', async () => {
  const queue = makeQueue('conc');
  let now = 0;
  let max = 0;
  const worker = makeWorker(
    queue.name,
    async () => {
      now += 1;
      max = Math.max(max, now);
      await sleep(150);
      now -= 1;
      return 'ok';
    },
    { concurrency: 3 }
  );
  try {
    await queue.addBulk(Array.from({ length: 12 }, (_, i) => ({ name: 't', data: { i } })));
    await waitFor(async () => (await queue.getJobCounts()).completed === 12, 30_000);
    assert(max <= 3, `concurrency exceeded: ${max}`);
    assert(max >= 2, `no parallelism observed: ${max}`);
  } finally {
    await worker.close();
    queue.close();
  }
});

test('worker: pause / resume', async () => {
  const queue = makeQueue('wpause');
  const done: string[] = [];
  const worker = makeWorker(queue.name, async (job) => {
    done.push(job.id);
    return 'ok';
  });
  try {
    await worker.waitUntilReady();
    worker.pause();
    assert(worker.isPaused(), 'isPaused');
    await sleep(500); // let in-flight polls settle
    await queue.add('t', {});
    await sleep(1000);
    assertEq(done.length, 0, 'paused worker must not process');
    worker.resume();
    await waitFor(() => done.length === 1);
  } finally {
    await worker.close();
    queue.close();
  }
});

test('worker: failure attempts=1 → failed + DLQ entry', async () => {
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
    const dlq = await queue.getDlq();
    assertEq(dlq.length, 1, 'one DLQ entry');
  } finally {
    await worker.close();
    queue.close();
  }
});

test('worker: UnrecoverableError skips retries despite attempts=5', async () => {
  const queue = makeQueue('unrec');
  let attempts = 0;
  const worker = makeWorker(queue.name, async () => {
    attempts += 1;
    throw new UnrecoverableError('fatal, do not retry');
  });
  try {
    const job = await queue.add('t', {}, { attempts: 5, backoff: 50 });
    await waitFor(async () => (await queue.getJobState(job.id)) === 'failed');
    await sleep(1000); // would-be retry window
    assertEq(attempts, 1, 'processor ran exactly once');
  } finally {
    await worker.close();
    queue.close();
  }
});

test('worker: normal error retries then recovers', async () => {
  const queue = makeQueue('retry');
  let attempts = 0;
  const worker = makeWorker(queue.name, async () => {
    attempts += 1;
    if (attempts < 3) throw new Error('transient');
    return 'recovered';
  });
  try {
    const job = await queue.add('t', {}, { attempts: 5, backoff: 100 });
    await waitFor(async () => (await queue.getJobState(job.id)) === 'completed', 20_000);
    assertEq(attempts, 3, 'three attempts');
    assertEq(await queue.getResult(job.id), 'recovered', 'result');
  } finally {
    await worker.close();
    queue.close();
  }
});

test('worker: progress event + stacktrace persisted on failure', async () => {
  const queue = makeQueue('stack');
  const progressEvents: Array<[string, number]> = [];
  const worker = makeWorker(queue.name, async (job) => {
    await job.updateProgress(50, 'halfway');
    throw new Error('with stack');
  });
  worker.on('progress', (job: Job, p: number) => progressEvents.push([job.id, p]));
  try {
    const job = await queue.add('t', {}, { attempts: 1 });
    await waitFor(async () => (await queue.getJobState(job.id)) === 'failed');
    assert(
      progressEvents.some(([id, p]) => id === job.id && p === 50),
      'progress event emitted'
    );
    const fetched = await queue.getJob(job.id);
    assert(fetched !== null && fetched.stacktrace !== null, 'stacktrace persisted');
    assert(
      fetched?.stacktrace?.some((line) => line.includes('with stack') || line.includes('Error')),
      'stacktrace content'
    );
  } finally {
    await worker.close();
    queue.close();
  }
});

test('worker: job longer than lockTtl survives via heartbeat renewal', async () => {
  const queue = makeQueue('longjob');
  const worker = makeWorker(
    queue.name,
    async () => {
      await sleep(3500);
      return 'survived';
    },
    { lockTtlMs: 1500, heartbeatIntervalS: 0.5 }
  );
  try {
    const job = await queue.add('slow', {}, { attempts: 1 });
    await waitFor(async () => (await queue.getJobState(job.id)) === 'completed', 20_000);
    assertEq(await queue.getResult(job.id), 'survived', 'long job result');
  } finally {
    await worker.close();
    queue.close();
  }
});

test('worker: registration visible in getWorkers', async () => {
  const queue = makeQueue('reg');
  const worker = makeWorker(queue.name, async () => 'ok', { name: 'e2e-ts-worker' });
  try {
    await worker.waitUntilReady();
    await waitFor(async () => {
      const workers = await queue.getWorkers();
      return workers.some((w) => w.name === 'e2e-ts-worker');
    });
  } finally {
    await worker.close();
    queue.close();
  }
});
