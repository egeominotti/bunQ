import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { Queue, QueueGroup, FlowProducer, TcpConnectionPool } from '../dist/index.js';
import { withBroker } from './canonical-harness.mjs';

await withBroker(async (connection) => {
  const name = `canonical-${randomUUID()}`;
  const queue = new Queue(name, {
    embedded: false,
    connection,
    prefixKey: 'tenant:',
    defaultJobOptions: { priority: 4, attempts: 1 },
  });
  const wire = new TcpConnectionPool(connection);
  const flow = new FlowProducer({ embedded: false, connection });
  const group = new QueueGroup(`namespace-${randomUUID()}`);
  const groupedQueue = group.getQueue('registered', { embedded: false, connection });
  const queueKey = `tenant:${name}`;
  try {
    assert.deepEqual(await group.listQueuesAsync(), ['registered']);
    const jobs = await Promise.all([
      queue.add('first', { n: 1 }, { deduplication: { id: 'unique' }, group: { id: 'group' } }),
      queue.add('second', { n: 2 }, { delay: 60_000 }),
    ]);
    assert.equal(jobs[0].priority, 4);
    assert.equal(jobs[0].opts.attempts, 1);
    assert.equal(await queue.getDeduplicationJobId('unique'), jobs[0].id);
    assert.equal(await queue.countAsync(), 2);
    assert.equal((await wire.send({ cmd: 'Count', queue: name })).count, 0);
    assert.equal((await queue.getJobsAsync({ end: -1 })).length, 2);
    assert.equal((await queue.getJobsAsync({ start: 0, end: 1, asc: true })).length, 1);
    assert.equal(await queue.getGroupJobsCount('group'), 1);
    await queue.setGroupConcurrency('group', 2);
    assert.equal(await queue.getGroupConcurrency('group'), 2);
    await queue.setGroupRateLimit('group', 3, 2_000);
    assert.deepEqual(await queue.getGroupRateLimit('group'), { max: 3, duration: 2_000 });
    assert.equal(await queue.pauseGroup('group'), true);
    assert.equal(await queue.isGroupPaused('group'), true);
    assert.equal(await queue.resumeGroup('group'), true);
    assert.equal(await queue.removeGroupConcurrency('group'), 1);
    assert.equal(await queue.removeGroupRateLimit('group'), 1);
    await queue.setGlobalConcurrencyAsync(3);
    assert.equal(await queue.getGlobalConcurrency(), 3);
    await queue.removeGlobalConcurrencyAsync();
    await queue.setGlobalRateLimitAsync(5, 2_000);
    assert.deepEqual(await queue.getGlobalRateLimit(), { max: 5, duration: 2_000 });
    await queue.removeGlobalRateLimitAsync();
    await queue.pauseAsync();
    assert.equal(await queue.isPausedAsync(), true);
    await queue.resumeAsync();
    assert.equal(await queue.isPausedAsync(), false);
    assert.equal(await queue.addJobLog(jobs[0].id, 'portable'), 1);
    assert.deepEqual(await queue.getJobLogs(jobs[0].id), { logs: ['[info] portable'], count: 1 });

    const scheduler = await queue.upsertJobScheduler(
      'schedule',
      { every: 3_600_000 },
      {
        name: 'scheduled',
        data: { n: 3 },
      }
    );
    assert.deepEqual(scheduler, await queue.getJobScheduler('schedule'));
    assert.equal((await queue.getJobSchedulers(0, 0, true)).length, 1);
    assert.equal(await queue.removeJobScheduler('schedule'), true);

    const pulled = await wire.send({
      cmd: 'PULL',
      queue: queueKey,
      owner: 'canonical',
      timeout: 100,
    });
    assert.equal(pulled.job.id, jobs[0].id);
    await queue.moveJobToFailed(jobs[0].id, new Error('portable failure'), pulled.token);
    const entries = await queue.getDlqAsync({ limit: 1 });
    assert.equal(entries.length, 1);
    assert.equal(entries[0].job.id, jobs[0].id);
    assert.equal(entries[0].error, 'portable failure');
    assert.equal(await entries[0].job.getState(), 'failed');
    assert.equal((await queue.getDlqStatsAsync()).total, 1);
    assert.equal((await queue.getMetrics('failed', 0, 0)).meta.count, 1);
    assert.equal(await queue.removeDlqJob(jobs[0].id), true);
    assert.equal((await queue.getDlqStatsAsync()).total, 0);

    const tree = await flow.add({
      name: 'parent',
      queueName: queueKey,
      data: {},
      children: [{ name: 'child', queueName: queueKey, data: { n: 4 } }],
    });
    assert.equal(tree.children.length, 1);
    assert.equal((await flow.getFlow({ id: tree.job.id, queueName: queueKey })).children.length, 1);
    assert.equal(await queue.getJobState(tree.job.id), 'waiting-children');
    assert.equal((await queue.getJobDependencies(tree.job.id)).unprocessed.length, 1);
    await queue.obliterateAsync();
    assert.equal(await queue.countAsync(), 0);
    console.log(
      'PASS canonical Queue: options, batching, groups, limits, logs, schedulers, DLQ, metrics, flows'
    );
  } finally {
    await queue.disconnect();
    await flow.close();
    await groupedQueue.disconnect();
    wire.close();
  }
});
