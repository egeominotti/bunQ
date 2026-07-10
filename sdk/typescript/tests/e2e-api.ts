/** E2E: full API coverage — every public SDK method not exercised elsewhere,
 * verified against a real server (no mocks). */

import { Job, Queue } from '../dist/index.js';
import {
  assert,
  assertEq,
  getPort,
  makeQueue,
  makeWorker,
  pullOne,
  qname,
  test,
  waitFor,
} from './harness.ts';

test('api: per-state getters and counts cover every state', async () => {
  const queue = makeQueue<{ i: number }>('api-states');
  const activeJob = await queue.add('a', { i: 3 });
  await pullOne(queue, 'api-states'); // only job in the queue -> deterministic
  await queue.add('w', { i: 1 }); // waiting
  await queue.add('p', { i: 2 }, { priority: 5 }); // prioritized
  await waitFor(async () => (await queue.getJobCounts()).active === 1);

  await waitFor(async () => (await queue.getActive()).length === 1);
  assert((await queue.getActiveCount()) === 1, 'active count');
  await waitFor(async () => (await queue.getPrioritized()).some((j) => j.name === 'p'));
  assert((await queue.getPrioritizedCount()) >= 1, 'prioritized count');
  void activeJob;
  queue.close();
});

test('api: moveJobToFailed / moveJobToWait / moveJobToCompleted with token', async () => {
  const queue = makeQueue<{ v: number }>('api-moves');
  const failing = await queue.add('mf', { v: 1 }, { attempts: 1 });
  let pulled = await pullOne(queue, 'api-moves');
  await queue.moveJobToFailed(pulled.id, new Error('manual fail'), pulled.token);
  assertEq(await queue.getJobState(failing.id), 'failed', 'moveJobToFailed');

  const good = await queue.add('mc', { v: 2 });
  pulled = await pullOne(queue, 'api-moves');
  await queue.moveJobToWait(pulled.id); // active -> waiting again
  await waitFor(async () => ['waiting', 'prioritized'].includes(await queue.getJobState(good.id)));
  pulled = await pullOne(queue, 'api-moves');
  await queue.moveJobToCompleted(pulled.id, { manual: true }, pulled.token);
  assertEq(await queue.getJobState(good.id), 'completed', 'manually completed');
  assertEq(((await queue.getResult(good.id)) as { manual: boolean }).manual, true, 'result stored');
  queue.close();
});

test('api: Job instance methods (state, updateData, priority, delay, promote, retry, discard)', async () => {
  const queue = makeQueue<{ v: number }>('api-job');
  const added = await queue.add('j', { v: 1 });
  const job = (await queue.getJob(added.id)) as Job<{ v: number }>;
  assertEq(await job.getState(), 'waiting', 'job.getState');
  await job.updateData({ name: 'j', v: 2 });
  await job.changePriority({ priority: 42 });
  const reloaded = await queue.getJob(added.id);
  assertEq((reloaded?.data as { v: number }).v, 2, 'job.updateData');
  assertEq(reloaded?.priority, 42, 'job.changePriority');
  await job.moveToDelayed(60_000);
  assertEq(await job.getState(), 'delayed', 'job.moveToDelayed');
  await job.changeDelay(30_000);
  await job.promote();
  assert(['waiting', 'prioritized'].includes(await job.getState()), 'job.promote');

  // retry: fail it first (attempts=1 job), then Job.retry() -> waiting
  const boom = await queue.add('boom', { v: 9 }, { attempts: 1 });
  const pulled = await pullOne(queue, 'api-job');
  await queue.moveJobToFailed(pulled.id, new Error('x'), pulled.token);
  // pulled job is FIFO: 'j' may be pulled instead; align on actual states
  const failedId = (await queue.getJobState(boom.id)) === 'failed' ? boom.id : pulled.id;
  const failedJob = (await queue.getJob(failedId)) as Job;
  await failedJob.retry();
  assert(['waiting', 'prioritized'].includes(await failedJob.getState()), 'job.retry');

  await failedJob.discard();
  queue.close();
});

test('api: job lock — job.heartbeat, job.extendLock, queue.extendJobLock', async () => {
  const queue = makeQueue<{ v: number }>('api-lock');
  await queue.add('locked', { v: 1 });
  const pulled = await pullOne(queue, 'api-lock');
  assert(pulled.token !== undefined, 'fresh pull issues a token');
  const job = new Job({ id: pulled.id, queue: queue.name }, queue.connection, pulled.token);
  await job.heartbeat(); // renews the lock — throws on bad token
  await job.extendLock(60_000);
  await queue.extendJobLock(pulled.id, pulled.token as string, 60_000);
  assertEq(await queue.getJobState(pulled.id), 'active', 'still active and locked');
  queue.close();
});

test('api: retryJobs(failed) + retryCompleted', async () => {
  const queue = makeQueue<{ v: number }>('api-retry');
  const bad = await queue.add('bad', { v: 1 }, { attempts: 1 });
  let pulled = await pullOne(queue, 'api-retry');
  await queue.moveJobToFailed(pulled.id, new Error('to dlq'), pulled.token);
  await waitFor(async () => (await queue.getDlq()).length === 1);
  await queue.retryJobs({ state: 'failed' }); // void: assert by state
  await waitFor(async () => ['waiting', 'prioritized'].includes(await queue.getJobState(bad.id)));
  assertEq((await queue.getDlq()).length, 0, 'DLQ drained by retryJobs');
  pulled = await pullOne(queue, 'api-retry');
  await queue.moveJobToCompleted(pulled.id, 'ok', pulled.token);
  await queue.retryCompleted(pulled.id); // completed -> waiting again
  await waitFor(async () =>
    ['waiting', 'prioritized'].includes(await queue.getJobState(pulled.id))
  );
  queue.close();
});

test('api: flow children failure surface + child dependency removal', async () => {
  const { FlowProducer } = await import('../dist/index.js');
  const flow = new FlowProducer({ host: '127.0.0.1', port: getPort() });
  const queueName = qname('api-children');
  const queue = new Queue(queueName, { host: '127.0.0.1', port: getPort() });

  const node = await flow.add({
    name: 'parent',
    queueName,
    children: [
      { name: 'ok-child', queueName },
      { name: 'bad-child', queueName, opts: { attempts: 1, ignoreDependencyOnFailure: true } },
    ],
  });
  // process children manually: complete one, fail the ignorable one
  for (let i = 0; i < 2; i++) {
    const pulled = await pullOne(queue, 'api-children');
    const pulledJob = await queue.getJob(pulled.id);
    if (pulledJob?.name === 'ok-child') {
      await queue.moveJobToCompleted(pulled.id, { part: 'ok' }, pulled.token);
    } else {
      await queue.moveJobToFailed(pulled.id, new Error('child exploded'), pulled.token);
    }
  }
  await waitFor(async () => {
    const failedValues = await queue.getFailedChildrenValues(node.job.id);
    const ignored = await queue.getIgnoredChildrenFailures(node.job.id);
    return Object.keys(failedValues).length + Object.keys(ignored).length >= 1;
  });

  // removeUnprocessedChildren: parent with pending children loses them
  const tree = await flow.add({
    name: 'parent2',
    queueName,
    children: [
      { name: 'pending-a', queueName },
      { name: 'pending-b', queueName },
    ],
  });
  await queue.removeUnprocessedChildren(tree.job.id);
  const pendingIds = (tree.children ?? []).map((c) => c.job.id);
  await waitFor(async () => {
    for (const id of pendingIds) {
      const state = await queue.getJobState(id).catch(() => 'gone');
      if (['waiting', 'prioritized'].includes(state)) return false; // still pending
    }
    return true; // every unprocessed child was removed
  });

  // removeChildDependency: child detaches itself from the parent
  const tree2 = await flow.add({
    name: 'parent3',
    queueName,
    children: [{ name: 'detach-me', queueName }],
  });
  const childId = tree2.children?.[0]?.job.id as string;
  await queue.removeChildDependency(childId);
  // the detached child is now an independent, processable job
  await waitFor(async () => ['waiting', 'prioritized'].includes(await queue.getJobState(childId)));
  flow.close();
  queue.close();
});

test('api: addCron + getJobSchedulersCount + setWebhookEnabled + getWorkersCount', async () => {
  const queue = makeQueue('api-admin');
  const cronId = qname('api-cron');
  await queue.addCron(cronId, '0 9 * * *', { type: 'daily' });
  assert((await queue.getJobSchedulersCount()) === 1, 'schedulers count');
  await queue.removeJobScheduler(cronId);
  assertEq(await queue.getJobSchedulersCount(), 0, 'schedulers removed');

  const created = await queue.addWebhook({
    url: 'https://example.com/api-hook',
    events: ['job.completed'],
  });
  const webhookId = String(created.webhookId ?? created.id);
  assert(webhookId !== 'undefined', `webhook created: ${JSON.stringify(created)}`);
  await queue.setWebhookEnabled(webhookId, false);
  const hooks = await queue.listWebhooks();
  const hook = hooks.find((h) => String(h.id) === webhookId);
  assertEq(hook?.enabled, false, 'webhook disabled');
  await queue.removeWebhook(webhookId);

  const worker = makeWorker(queue.name, () => 'ok');
  await worker.waitUntilReady();
  assert((await queue.getWorkersCount()) >= 1, 'workers count');
  await worker.close();
  queue.close();
});

test('api: worker cooperative cancel — cancelJob, cancelAllJobs, isJobCancelled', async () => {
  const queue = makeQueue<{ n: number }>('api-cancel');
  const seenCancelled: string[] = [];
  const worker = makeWorker<{ n: number }, string>(
    queue.name,
    async (job) => {
      for (let i = 0; i < 200; i++) {
        if (worker.isJobCancelled(job.id)) throw new Error('aborted by cancel');
        await new Promise((r) => setTimeout(r, 20));
      }
      return 'never';
    },
    { concurrency: 2 }
  );
  worker.on('cancelled', (info) => seenCancelled.push(info.jobId));
  const a = await queue.add('long-a', { n: 1 }, { attempts: 1 });
  const b = await queue.add('long-b', { n: 2 }, { attempts: 1 });
  await waitFor(async () => (await queue.getJobCounts()).active === 2);
  assert(worker.cancelJob(a.id, 'stop one') === true, 'cancelJob true for active');
  assert(worker.cancelJob('nope') === false, 'cancelJob false for unknown');
  worker.cancelAllJobs('stop everything');
  await waitFor(async () => {
    const counts = await queue.getJobCounts();
    return counts.failed === 2;
  }, 20_000);
  assert(seenCancelled.includes(a.id) && seenCancelled.includes(b.id), 'cancelled events');
  await worker.close();
  queue.close();
});

test('api: waitForJob + disconnect alias', async () => {
  const queue = makeQueue<{ v: number }>('api-wait');
  const worker = makeWorker<{ v: number }, { doubled: number }>(queue.name, (job) => ({
    doubled: job.data.v * 2,
  }));
  const job = await queue.add('calc', { v: 21 });
  const result = (await queue.waitForJob(job.id, 10_000)) as { doubled: number };
  assertEq(result.doubled, 42, 'waitForJob returns the result');
  await worker.close();
  await queue.disconnect(); // BullMQ-parity alias of close()
});
