/** E2E: basics + query surface — custom ids, states, counts, logs, progress. */

import { CommandError, Connection } from '../dist/legacy.js';
import { assert, assertEq, getPort, makeQueue, makeWorker, test, waitFor } from './harness.ts';

test('ping + hello', async () => {
  const conn = new Connection({ host: '127.0.0.1', port: getPort() });
  assert(await conn.ping(), 'ping should return true');
  const hello = await conn.hello();
  assert(Number(hello.protocolVersion) >= 1, 'protocolVersion >= 1');
  assert(typeof hello.server === 'string', 'server name present');
  conn.close();
});

test('add + getJob + name + state', async () => {
  const queue = makeQueue<{ to: string }>('add');
  try {
    const job = await queue.add('send', { to: 'a@b.c' }, { priority: 5 });
    assert(job.id.length > 0, 'job id assigned');
    const state = await queue.getJobState(job.id);
    assert(state === 'waiting' || state === 'prioritized', `state waiting/prioritized (${state})`);
    const fetched = await queue.getJob(job.id);
    assert(fetched !== null, 'job fetched');
    assertEq(fetched!.name, 'send', 'job name');
    assertEq((fetched!.data as Record<string, unknown>).to, 'a@b.c', 'job data');
  } finally {
    queue.close();
  }
});

test('addBulk 50 + count', async () => {
  const queue = makeQueue('bulk');
  try {
    const jobs = await queue.addBulk(
      Array.from({ length: 50 }, (_, i) => ({ name: 'task', data: { i } }))
    );
    assertEq(jobs.length, 50, 'bulk returned 50 jobs');
    assertEq(await queue.count(), 50, 'count is 50');
  } finally {
    queue.close();
  }
});

test('query: custom id idempotent + getJobByCustomId', async () => {
  const queue = makeQueue('customid');
  try {
    const first = await queue.add('task', { v: 1 }, { jobId: 'my-custom-id' });
    const second = await queue.add('task', { v: 2 }, { jobId: 'my-custom-id' });
    assertEq(second.id, first.id, 'idempotent re-add returns same id');
    const fetched = await queue.getJobByCustomId('my-custom-id');
    assert(fetched !== null && fetched.id === first.id, 'fetched by custom id');
    const dedupId = await queue.getDeduplicationJobId('my-custom-id');
    assertEq(dedupId, first.id, 'getDeduplicationJobId');
  } finally {
    queue.close();
  }
});

test('query: getJobs by state (multi-state, write-buffer flush)', async () => {
  const queue = makeQueue('states');
  try {
    await queue.addBulk(Array.from({ length: 5 }, (_, i) => ({ name: 't', data: { i } })));
    await queue.add('later', {}, { delay: 60_000 });
    // GetJobs reads persisted rows: allow the 10ms write buffer to flush
    await waitFor(async () => (await queue.getWaiting()).length === 5, 10_000);
    await waitFor(async () => (await queue.getDelayed()).length === 1, 10_000);
    assertEq(await queue.getWaitingCount(), 5, 'waiting count');
    assertEq(await queue.getDelayedCount(), 1, 'delayed count');
    const both = await queue.getJobs({ state: ['waiting', 'delayed'] });
    assertEq(both.length, 6, 'multi-state getJobs');
  } finally {
    queue.close();
  }
});

test('query: counts per priority', async () => {
  const queue = makeQueue('perprio');
  try {
    await queue.add('a', {}, { priority: 1 });
    await queue.add('b', {}, { priority: 1 });
    await queue.add('c', {}, { priority: 5 });
    const counts = await queue.getCountsPerPriority();
    const total = Object.values(counts).reduce((a, b) => a + Number(b), 0);
    assertEq(total, 3, 'per-priority totals add up');
  } finally {
    queue.close();
  }
});

test('query: progress roundtrip (job must be active)', async () => {
  const queue = makeQueue('progress');
  try {
    const job = await queue.add('t', {});
    // Progress requires the job to be ACTIVE: pull it first
    await queue.connection.call({ cmd: 'PULL', queue: queue.name, owner: 'e2e', timeout: 1000 });
    await queue.updateJobProgress(job.id, 42, 'almost');
    const progress = await queue.getProgress(job.id);
    assertEq(progress.progress, 42, 'progress value');
    assertEq(progress.message, 'almost', 'progress message');
  } finally {
    queue.close();
  }
});

test('query: job logs add / get / clear', async () => {
  const queue = makeQueue('logs');
  try {
    const job = await queue.add('t', {});
    await queue.addJobLog(job.id, 'step one');
    await queue.addJobLog(job.id, 'step two');
    const logs = await queue.getJobLogs(job.id);
    assertEq(logs.length, 2, 'two log rows');
    assert(logs[0].includes('step one'), 'first log row content');
    await queue.clearJobLogs(job.id);
    assertEq((await queue.getJobLogs(job.id)).length, 0, 'logs cleared');
  } finally {
    queue.close();
  }
});

test('query: tags + groupId + priority roundtrip', async () => {
  const queue = makeQueue('tags');
  try {
    const job = await queue.add(
      't',
      { x: 1 },
      { tags: ['alpha', 'beta'], groupId: 'g1', priority: 7 }
    );
    const fetched = await queue.getJob(job.id);
    assert(fetched !== null, 'job fetched');
    assertEq(fetched?.tags.join(','), 'alpha,beta', 'tags roundtrip');
    assertEq(fetched?.groupId, 'g1', 'groupId roundtrip');
    assertEq(fetched?.priority, 7, 'priority roundtrip');
    assertEq(fetched?.name, 't', 'name roundtrip');
  } finally {
    queue.close();
  }
});

test('query: waitForJob returns worker result', async () => {
  const queue = makeQueue('waitjob');
  const worker = makeWorker(queue.name, async () => ({ answer: 42 }));
  try {
    const job = await queue.add('t', {});
    const result = await queue.waitForJob<{ answer: number }>(job.id, 10_000);
    assertEq(result.answer, 42, 'waitForJob result');
  } finally {
    await worker.close();
    queue.close();
  }
});

test('query: nonexistent job → getJob null, getJobState tolerated', async () => {
  const queue = makeQueue('missing');
  try {
    assertEq(await queue.getJob('no-such-id'), null, 'getJob returns null');
    try {
      const state = await queue.getJobState('no-such-id');
      assert(['unknown', 'not_found'].includes(state), `unexpected state ${state}`);
    } catch (err) {
      assert(err instanceof CommandError, 'CommandError acceptable');
    }
  } finally {
    queue.close();
  }
});
