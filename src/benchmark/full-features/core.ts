import { queueManager as qm, section, test } from './harness';

export function retryWaitMs(deadline: number, now = Date.now()): number {
  return Math.max(25, deadline - now + 25);
}

export async function runCoreFeatureSections(): Promise<void> {
  await section('Basic Operations', async () => {
    const job1 = await qm.push('basic', { data: { msg: 'hello' } });
    await test('push returns job with id', () => !!job1.id);
    await test('push returns correct queue', () => job1.queue === 'basic');
    await test('push returns correct data', () => (job1.data as { msg: string }).msg === 'hello');
    const pulled = await qm.pull('basic', 0);
    await test('pull returns job', () => !!pulled);
    await test('pull returns correct id', () => pulled?.id === job1.id);
    await test('pull sets startedAt', () => !!pulled?.startedAt);
    await qm.ack(pulled!.id, { result: 'done' });
    const result = qm.getResult(pulled!.id);
    await test('ack stores result', () => (result as { result?: string })?.result === 'done');
    await qm.push('basic', { data: { test: 'fail' }, maxAttempts: 1 });
    const pulled2 = await qm.pull('basic', 0);
    await qm.fail(pulled2!.id, 'Test error');
    await test('fail moves to DLQ when maxAttempts reached', () => qm.getStats().dlq >= 1);
  });

  await section('Priority Queue', async () => {
    await qm.push('priority', { data: { id: 'low' }, priority: 1 });
    await qm.push('priority', { data: { id: 'high' }, priority: 100 });
    await qm.push('priority', { data: { id: 'medium' }, priority: 50 });
    const first = await qm.pull('priority', 0);
    const second = await qm.pull('priority', 0);
    const third = await qm.pull('priority', 0);
    await test('high priority pulled first', () =>
      (first?.data as { id: string } | undefined)?.id === 'high');
    await test('medium priority pulled second', () =>
      (second?.data as { id: string } | undefined)?.id === 'medium');
    await test('low priority pulled third', () =>
      (third?.data as { id: string } | undefined)?.id === 'low');
    await qm.ack(first!.id);
    await qm.ack(second!.id);
    await qm.ack(third!.id);
  });

  await section('Delayed Jobs', async () => {
    const delayed = await qm.push('delayed', { data: { msg: 'delayed' }, delay: 100 });
    await test('delayed job has future runAt', () => delayed.runAt > Date.now());
    await test('delayed job not immediately available', async () =>
      (await qm.pull('delayed', 0)) === null);
    await Bun.sleep(150);
    const ready = await qm.pull('delayed', 0);
    await test('delayed job available after delay', () => !!ready);
    if (ready) await qm.ack(ready.id);
  });

  await section('Batch Operations', async () => {
    const jobs = await qm.pushBatch('batch', [
      { data: { i: 1 } },
      { data: { i: 2 } },
      { data: { i: 3 } },
    ]);
    await test('pushBatch returns all job ids', () => jobs.length === 3);
    let count = 0;
    for (let index = 0; index < 3; index++) {
      const job = await qm.pull('batch', 0);
      if (job) {
        count++;
        await qm.ack(job.id);
      }
    }
    await test('all batch jobs processed', () => count === 3);
  });

  await section('Job Dependencies', async () => {
    const parent = await qm.push('deps', { data: { type: 'parent' } });
    await qm.push('deps', { data: { type: 'child' }, dependsOn: [parent.id] });
    const childBefore = await qm.pull('deps', 0);
    await test('parent pulled first (child blocked)', () =>
      (childBefore?.data as { type: string } | undefined)?.type === 'parent');
    await qm.ack(childBefore!.id);
    await Bun.sleep(1200);
    const childAfter = await qm.pull('deps', 0);
    await test('child available after parent completes', () =>
      !!childAfter && (childAfter.data as { type: string }).type === 'child');
    if (childAfter) await qm.ack(childAfter.id);
  });

  await section('Retry with Backoff', async () => {
    await qm.push('retry', { data: { test: 'retry' }, maxAttempts: 3, backoff: 50 });
    let pulled = await qm.pull('retry', 0);
    await test('job has maxAttempts=3', () => pulled?.maxAttempts === 3);
    await qm.fail(pulled!.id, 'First failure');
    await Bun.sleep(retryWaitMs(pulled!.runAt));
    pulled = await qm.pull('retry', 0);
    await test('attempts incremented after fail', () => pulled?.attempts === 1);
    await qm.fail(pulled!.id, 'Second failure');
    await Bun.sleep(retryWaitMs(pulled!.runAt));
    pulled = await qm.pull('retry', 0);
    await test('job retried correctly', () => !!pulled && pulled.attempts === 2);
    if (pulled) await qm.ack(pulled.id);
  });

  await section('DLQ Operations', async () => {
    await qm.push('dlq-test', { data: { fail: true }, maxAttempts: 1 });
    const job = await qm.pull('dlq-test', 0);
    await qm.fail(job!.id, 'DLQ test failure');
    await test('getDlq returns failed jobs', () => qm.getDlq('dlq-test').length >= 1);
    await test('retryDlq moves jobs back to queue', () => qm.retryDlq('dlq-test') >= 1);
    const retriedJob = await qm.pull('dlq-test', 0);
    if (retriedJob) await qm.ack(retriedJob.id);
  });
}
