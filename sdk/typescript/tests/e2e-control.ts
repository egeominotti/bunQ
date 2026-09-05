/** E2E: control surface — delay/promote, moves, updates, clean, retry, lifo. */

import type { Job } from '../dist/legacy.js';
import { assert, assertEq, makeQueue, makeWorker, sleep, test, waitFor } from './harness.ts';

test('control: delayed + promote single', async () => {
  const queue = makeQueue('delay');
  try {
    const job = await queue.add('later', { x: 1 }, { delay: 60_000 });
    assertEq(await queue.getJobState(job.id), 'delayed', 'job is delayed');
    await queue.promoteJob(job.id);
    const state = await queue.getJobState(job.id);
    assert(state === 'waiting' || state === 'prioritized', `promoted (${state})`);
  } finally {
    queue.close();
  }
});

test('control: promoteJobs bulk returns count', async () => {
  const queue = makeQueue('promoteb');
  try {
    for (let i = 0; i < 3; i++) await queue.add('t', { i }, { delay: 60_000 });
    // PromoteJobs scans persisted delayed jobs: wait for write-buffer flush
    await waitFor(async () => (await queue.getDelayed()).length === 3, 10_000);
    const promoted = await queue.promoteJobs();
    assertEq(promoted, 3, 'promoted count');
    assertEq(await queue.getWaitingCount(), 3, 'all waiting');
  } finally {
    queue.close();
  }
});

test('control: moveToDelayed + changeDelay', async () => {
  const queue = makeQueue('movedelay');
  try {
    const job = await queue.add('t', {});
    await queue.moveJobToDelayed(job.id, 60_000);
    assertEq(await queue.getJobState(job.id), 'delayed', 'moved to delayed');
    await queue.changeJobDelay(job.id, 1);
    await waitFor(async () => {
      const state = await queue.getJobState(job.id);
      return state === 'waiting' || state === 'prioritized';
    }, 10_000);
  } finally {
    queue.close();
  }
});

test('control: updateJobData + changeJobPriority', async () => {
  const queue = makeQueue('mutate');
  try {
    const job = await queue.add('t', { v: 1 });
    await queue.updateJobData(job.id, { name: 't', v: 2 });
    const fetched = await queue.getJob(job.id);
    assertEq((fetched?.data as Record<string, unknown> | undefined)?.v, 2, 'data updated');
    await queue.changeJobPriority(job.id, { priority: 99 });
    const again = await queue.getJob(job.id);
    assertEq(again?.priority, 99, 'priority changed');
  } finally {
    queue.close();
  }
});

test('control: remove job', async () => {
  const queue = makeQueue('remove');
  try {
    const job = await queue.add('t', {});
    await queue.remove(job.id);
    assertEq(await queue.getJob(job.id), null, 'job gone');
    assertEq(await queue.count(), 0, 'count 0');
  } finally {
    queue.close();
  }
});

test('control: drain removes waiting', async () => {
  const queue = makeQueue('drain');
  try {
    await queue.addBulk(Array.from({ length: 10 }, (_, i) => ({ name: 't', data: { i } })));
    assertEq(await queue.count(), 10, 'ten queued');
    await queue.drain();
    assertEq(await queue.count(), 0, 'drained');
  } finally {
    queue.close();
  }
});

test('control: obliterate wipes queue and resets pause', async () => {
  const queue = makeQueue('oblit');
  try {
    await queue.addBulk(Array.from({ length: 5 }, () => ({ name: 't', data: {} })));
    await queue.pause();
    await queue.obliterate();
    assertEq(await queue.count(), 0, 'wiped');
    assertEq(await queue.isPaused(), false, 'obliterate resets paused state');
  } finally {
    queue.close();
  }
});

test('control: pause / resume / isPaused', async () => {
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

test('control: retryJob failed → waiting', async () => {
  const queue = makeQueue('retryjob');
  const worker = makeWorker(queue.name, async () => {
    throw new Error('nope');
  });
  try {
    const job = await queue.add('boom', {}, { attempts: 1 });
    await waitFor(async () => (await queue.getJobState(job.id)) === 'failed');
    await worker.close();
    await queue.retryJob(job.id);
    const state = await queue.getJobState(job.id);
    assert(state === 'waiting' || state === 'prioritized', `retried to waiting (${state})`);
  } finally {
    if (!worker.isClosed()) await worker.close();
    queue.close();
  }
});

test('control: clean completed returns count', async () => {
  const queue = makeQueue('clean');
  const done: string[] = [];
  const worker = makeWorker(queue.name, async () => 'ok');
  worker.on('completed', (job: Job) => done.push(job.id));
  try {
    for (let i = 0; i < 4; i++) await queue.add('t', { i });
    await waitFor(() => done.length === 4);
    await worker.close();
    await sleep(200);
    const removed = await queue.clean(0, undefined, 'completed');
    assertEq(removed, 4, 'cleaned count');
  } finally {
    if (!worker.isClosed()) await worker.close();
    queue.close();
  }
});

test('control: lifo ordering among lifo jobs', async () => {
  const queue = makeQueue('lifo');
  try {
    // LIFO applies among lifo jobs (mixed with FIFO jobs falls back to FIFO)
    await queue.add('first', { o: 1 }, { lifo: true });
    await queue.add('second', { o: 2 }, { lifo: true });
    const response = await queue.connection.call({
      cmd: 'PULL',
      queue: queue.name,
      owner: 'e2e',
      timeout: 1000,
    });
    const pulled = response.job as Record<string, unknown>;
    assertEq((pulled.data as Record<string, unknown>).o, 2, 'newest lifo job first');
  } finally {
    queue.close();
  }
});

test('control: priority ordering via PULL', async () => {
  const queue = makeQueue('prio');
  try {
    await queue.add('low', { p: 'low' }, { priority: 1 });
    await queue.add('high', { p: 'high' }, { priority: 100 });
    const response = await queue.connection.call({
      cmd: 'PULL',
      queue: queue.name,
      owner: 'e2e',
      timeout: 1000,
    });
    const pulled = response.job as Record<string, unknown>;
    assertEq((pulled.data as Record<string, unknown>).p, 'high', 'high priority first');
  } finally {
    queue.close();
  }
});
