import type { QueueManager } from '../../application/queueManager';
import type { TestResults } from './harness';

export async function testBasicApis(
  queueManager: QueueManager,
  results: TestResults
): Promise<void> {
  results.section('1. BASIC APIs (Embedded Mode)');
  const queue = 'basic-test-' + Date.now();
  const job = await queueManager.push(queue, { data: { test: 'data' } });
  results.assert(!!job.id, 'push() returns job with id');
  const pulled = await queueManager.pull(queue, 100);
  results.assert(pulled?.id === job.id, 'pull() returns correct job');
  await queueManager.ack(job.id, { result: 'done' });
  const result = queueManager.getResult(job.id) as { result?: string } | undefined;
  results.assert(result?.result === 'done', 'ack() stores result');
  const failedJob = await queueManager.push(queue, { data: { willFail: true }, maxAttempts: 1 });
  const failedPulled = await queueManager.pull(queue, 100);
  results.assert(failedPulled !== null, 'Failed job pulled', failedPulled ? 'ok' : 'null');
  if (failedPulled) {
    await queueManager.fail(failedJob.id, 'test error');
    await Bun.sleep(50);
  }
  const dlq = queueManager.getDlq(queue);
  results.assert(
    dlq.length > 0,
    'fail() moves to DLQ after maxAttempts',
    `dlq=${dlq.length}, attempts=${failedJob.attempts}`
  );
  queueManager.purgeDlq(queue);
  results.assert(queueManager.getDlq(queue).length === 0, 'purgeDlq() clears DLQ');
}

export async function testBatchOperations(
  queueManager: QueueManager,
  results: TestResults
): Promise<void> {
  results.section('2. BATCH OPERATIONS');
  const queue = 'batch-test-' + Date.now();
  const count = 1000;
  const start = performance.now();
  const jobs = await queueManager.pushBatch(
    queue,
    Array.from({ length: count }, (_, index) => ({
      data: { index },
      removeOnComplete: true,
    }))
  );
  const pushTime = performance.now() - start;
  results.assert(
    jobs.length === count,
    `pushBatch() created ${count} jobs`,
    `${Math.round(count / (pushTime / 1000))}/s`
  );
  let processed = 0;
  const processingStart = performance.now();
  while (processed < count) {
    const job = await queueManager.pull(queue, 50);
    if (!job) break;
    await queueManager.ack(job.id, {});
    processed++;
  }
  const processingTime = performance.now() - processingStart;
  results.assert(
    processed === count,
    `Processed all ${count} jobs`,
    `${Math.round(count / (processingTime / 1000))}/s`
  );
}

export async function testPriorities(
  queueManager: QueueManager,
  results: TestResults
): Promise<void> {
  results.section('3. PRIORITY QUEUE');
  const queue = 'priority-test-' + Date.now();
  const lowJob = await queueManager.push(queue, { data: { priority: 'low' }, priority: 1 });
  await Bun.sleep(10);
  const highJob = await queueManager.push(queue, { data: { priority: 'high' }, priority: 100 });
  await Bun.sleep(10);
  await queueManager.push(queue, { data: { priority: 'medium' }, priority: 50 });
  await Bun.sleep(100);
  const jobs: Array<{ id: string }> = [];
  for (let index = 0; index < 3; index++) {
    const job = await queueManager.pull(queue, 100);
    if (job) {
      jobs.push(job);
      await queueManager.ack(job.id, {});
    }
  }
  results.assert(jobs.length === 3, 'All 3 jobs pulled');
  const highIndex = jobs.findIndex((job) => job.id === highJob.id);
  const lowIndex = jobs.findIndex((job) => job.id === lowJob.id);
  results.assert(
    highIndex < lowIndex,
    'High priority before low',
    `high@${highIndex}, low@${lowIndex}`
  );
  results.assert(
    jobs[0]?.id === highJob.id,
    'First is high priority',
    `first=${jobs[0]?.id}, high=${highJob.id}`
  );
}

export async function testDelayedJobs(
  queueManager: QueueManager,
  results: TestResults
): Promise<void> {
  results.section('4. DELAYED JOBS');
  const queue = 'delay-test-' + Date.now();
  const now = Date.now();
  const job = await queueManager.push(queue, { data: { delayed: true }, delay: 1000 });
  results.assert(job.runAt > now, 'Job has future runAt', `runAt=${job.runAt}, now=${now}`);
  results.assert(
    (await queueManager.pull(queue, 50)) === null,
    'Delayed job not immediately available'
  );
  await Bun.sleep(1200);
  const delayed = await queueManager.pull(queue, 100);
  results.assert(
    delayed?.id === job.id,
    'Delayed job available after delay',
    delayed ? 'ok' : 'null'
  );
  if (delayed) await queueManager.ack(delayed.id, {});
}

export async function testDependencies(
  queueManager: QueueManager,
  results: TestResults
): Promise<void> {
  results.section('5. JOB DEPENDENCIES');
  const queue = 'deps-test-' + Date.now();
  const parent = await queueManager.push(queue, { data: { role: 'parent' } });
  const child = await queueManager.push(queue, {
    data: { role: 'child' },
    dependsOn: [parent.id],
  });
  results.assert(
    (await queueManager.pull(queue, 100))?.id === parent.id,
    'Parent pulled first (child blocked)'
  );
  await queueManager.ack(parent.id, {});
  await Bun.sleep(500);
  const childPulled = await queueManager.pull(queue, 500);
  results.assert(childPulled?.id === child.id, 'Child available after parent completes');
  if (childPulled) await queueManager.ack(childPulled.id, {});
}

export async function testCronJobs(
  queueManager: QueueManager,
  results: TestResults
): Promise<void> {
  results.section('6. CRON JOBS');
  const queue = 'cron-test-' + Date.now();
  const cronName = 'test-cron-' + Date.now();
  queueManager.addCron({
    name: cronName,
    queue,
    data: { cron: true },
    repeatEvery: 500,
    maxLimit: 3,
  });
  results.assert(
    queueManager.listCrons().some((cron) => cron.name === cronName),
    'Cron job added'
  );
  await Bun.sleep(1500);
  let cronJobs = 0;
  while (true) {
    const job = await queueManager.pull(queue, 100);
    if (!job) break;
    await queueManager.ack(job.id, {});
    cronJobs++;
  }
  results.assert(cronJobs >= 1, 'Cron created jobs', `${cronJobs} jobs`);
  queueManager.removeCron(cronName);
  results.assert(
    !queueManager.listCrons().some((cron) => cron.name === cronName),
    'Cron job deleted'
  );
}

export async function testUniqueKeys(
  queueManager: QueueManager,
  results: TestResults
): Promise<void> {
  results.section('7. UNIQUE KEYS');
  const queue = 'unique-test-' + Date.now();
  const first = await queueManager.push(queue, { data: { first: true }, uniqueKey: 'unique-123' });
  results.assert(!!first.id, 'First job with uniqueKey created');
  const duplicate = await queueManager.push(queue, {
    data: { second: true },
    uniqueKey: 'unique-123',
  });
  results.assert(duplicate.id === first.id, 'Duplicate uniqueKey returns existing job');
  const pulled = await queueManager.pull(queue, 100);
  if (pulled) await queueManager.ack(pulled.id, {});
  const reused = await queueManager.push(queue, { data: { third: true }, uniqueKey: 'unique-123' });
  results.assert(reused.id !== first.id, 'UniqueKey can be reused after completion');
  const cleanup = await queueManager.pull(queue, 100);
  if (cleanup) await queueManager.ack(cleanup.id, {});
}

export async function testQueueControl(
  queueManager: QueueManager,
  results: TestResults
): Promise<void> {
  results.section('8. QUEUE CONTROL');
  const queue = 'control-test-' + Date.now();
  await queueManager.push(queue, { data: { num: 1 } });
  await queueManager.push(queue, { data: { num: 2 } });
  queueManager.pause(queue);
  results.assert(queueManager.isPaused(queue) === true, 'Queue paused');
  results.assert((await queueManager.pull(queue, 100)) === null, 'Cannot pull from paused queue');
  queueManager.resume(queue);
  results.assert(queueManager.isPaused(queue) === false, 'Queue resumed');
  const resumed = await queueManager.pull(queue, 100);
  results.assert(resumed !== null, 'Can pull after resume');
  queueManager.drain(queue);
  results.assert(queueManager.count(queue) === 0, 'Queue drained');
  if (resumed) await queueManager.ack(resumed.id, {});
}
