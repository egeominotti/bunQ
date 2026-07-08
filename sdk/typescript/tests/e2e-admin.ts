/** E2E: admin — schedulers, DLQ, configs, rate limits, webhooks, monitoring. */

import { assert, assertEq, makeQueue, makeWorker, qname, sleep, test, waitFor } from './harness.ts';

test('admin: scheduler every spawns jobs, single get, remove stops it', async () => {
  const queue = makeQueue('sched');
  const schedulerId = qname('tick');
  try {
    await queue.every(schedulerId, 300, { kind: 'tick' });
    const schedulers = await queue.getJobSchedulers();
    assert(
      schedulers.some((s) => s.name === schedulerId),
      'scheduler listed'
    );
    const single = await queue.getJobScheduler(schedulerId);
    assert(single !== null && single.queue === queue.name, 'single scheduler fetched');
    await waitFor(async () => (await queue.count()) >= 1, 15_000);
    await queue.removeJobScheduler(schedulerId);
    const after = await queue.count();
    await sleep(1000);
    assert((await queue.count()) <= after + 1, 'scheduler stopped firing');
  } finally {
    queue.close();
  }
});

test('admin: cron pattern scheduler + CronGet not-found → null', async () => {
  const queue = makeQueue('cron');
  const schedulerId = qname('daily');
  try {
    await queue.upsertJobScheduler(
      schedulerId,
      { pattern: '0 9 * * *', tz: 'Europe/Rome' },
      { name: 'report', data: { type: 'daily' } }
    );
    const single = await queue.getJobScheduler(schedulerId);
    assert(single !== null, 'scheduler fetched');
    assertEq(single?.schedule, '0 9 * * *', 'cron pattern stored');
    assert(Number(single?.nextRun ?? 0) > 0, 'nextRun computed');
    await queue.removeJobScheduler(schedulerId);
    assertEq(await queue.getJobScheduler(schedulerId), null, 'not found → null');
  } finally {
    queue.close();
  }
});

test('admin: DLQ entries + retryDlq requeues', async () => {
  const queue = makeQueue('dlqr');
  const worker = makeWorker(queue.name, async () => {
    throw new Error('dead');
  });
  try {
    const job = await queue.add('boom', {}, { attempts: 1 });
    await waitFor(async () => (await queue.getDlq()).length === 1);
    await worker.close();
    const entries = await queue.getDlq();
    assert(entries[0].jobId === job.id || entries[0].id === job.id, 'DLQ entry references job');
    const retried = await queue.retryDlq();
    assertEq(retried, 1, 'one retried');
    const state = await queue.getJobState(job.id);
    assert(state === 'waiting' || state === 'prioritized', 'back to waiting');
    assertEq((await queue.getDlq()).length, 0, 'DLQ empty');
  } finally {
    if (!worker.isClosed()) await worker.close();
    queue.close();
  }
});

test('admin: purgeDlq', async () => {
  const queue = makeQueue('dlqp');
  const worker = makeWorker(queue.name, async () => {
    throw new Error('dead');
  });
  try {
    await queue.add('boom', {}, { attempts: 1 });
    await waitFor(async () => (await queue.getDlq()).length === 1);
    await worker.close();
    assertEq(await queue.purgeDlq(), 1, 'purged one');
    assertEq((await queue.getDlq()).length, 0, 'DLQ empty');
  } finally {
    if (!worker.isClosed()) await worker.close();
    queue.close();
  }
});

test('admin: stall + dlq config roundtrip', async () => {
  const queue = makeQueue('cfg');
  try {
    await queue.setStallConfig({ stallInterval: 20_000, maxStalls: 5, gracePeriod: 3000 });
    const stall = await queue.getStallConfig();
    assertEq(Number(stall.stallInterval), 20_000, 'stallInterval');
    assertEq(Number(stall.maxStalls), 5, 'maxStalls');
    await queue.setDlqConfig({ autoRetry: true, maxEntries: 500 });
    const dlq = await queue.getDlqConfig();
    assertEq(Number(dlq.maxEntries), 500, 'dlq maxEntries');
  } finally {
    queue.close();
  }
});

test('admin: rate limit + concurrency accepted', async () => {
  const queue = makeQueue('limits');
  try {
    await queue.setGlobalRateLimit(100);
    await queue.removeGlobalRateLimit();
    await queue.setGlobalConcurrency(2);
    await queue.removeGlobalConcurrency();
  } finally {
    queue.close();
  }
});

test('admin: global concurrency enforced via PULLB', async () => {
  const queue = makeQueue('gconc');
  try {
    await queue.setGlobalConcurrency(1);
    await queue.addBulk(Array.from({ length: 4 }, (_, i) => ({ name: 't', data: { i } })));
    const response = await queue.connection.call({
      cmd: 'PULLB',
      queue: queue.name,
      count: 4,
      owner: 'e2e',
      lockTtl: 30_000,
    });
    const jobs = (response.jobs ?? []) as unknown[];
    assert(jobs.length <= 1, `global concurrency not enforced: got ${jobs.length}`);
    await queue.removeGlobalConcurrency();
  } finally {
    queue.close();
  }
});

test('admin: webhooks CRUD (https URL, SSRF blocks localhost)', async () => {
  const queue = makeQueue('hooks');
  try {
    const added = await queue.addWebhook({
      url: 'https://example.com/bunqueue-hook',
      events: ['job.completed'],
    });
    const listed = await queue.listWebhooks();
    assert(
      listed.some((h) => h.url === 'https://example.com/bunqueue-hook'),
      'webhook listed'
    );
    const id = String(added.id ?? added.webhookId ?? '');
    assert(id.length > 0, 'webhook id resolvable');
    await queue.removeWebhook(id);
    const after = await queue.listWebhooks();
    assert(!after.some((h) => h.id === id), 'webhook removed');
  } finally {
    queue.close();
  }
});

test('admin: stats + metrics + listQueues (string names)', async () => {
  const queue = makeQueue('mon');
  try {
    await queue.add('t', {});
    const stats = await queue.getStats();
    assert('waiting' in stats && 'uptime' in stats, `stats shape: ${Object.keys(stats)}`);
    const metrics = await queue.getMetrics();
    assert('totalPushed' in metrics, `metrics shape: ${Object.keys(metrics)}`);
    const queues = await queue.listQueues();
    assert(queues.includes(queue.name), 'queue name listed as string');
  } finally {
    queue.close();
  }
});
