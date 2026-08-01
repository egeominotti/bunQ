import { queueManager as qm, section, test } from './harness';

export async function runSchedulingFeatureSections(): Promise<void> {
  await section('Cron Jobs', async () => {
    qm.addCron({
      name: 'test-cron',
      queue: 'cron-queue',
      data: { source: 'cron' },
      repeatEvery: 500,
    });
    await test('cron job added', () => qm.listCrons().some((cron) => cron.name === 'test-cron'));
    await Bun.sleep(1500);
    const cronJob = await qm.pull('cron-queue', 0);
    await test('cron job creates jobs', () => !!cronJob);
    if (cronJob) await qm.ack(cronJob.id);
    qm.removeCron('test-cron');
    await test('cron job deleted', () => !qm.listCrons().some((cron) => cron.name === 'test-cron'));
  });

  await section('Events/Subscriptions', async () => {
    const events: string[] = [];
    const unsubscribe = qm.subscribe((event) => events.push(event.eventType));
    await qm.push('events', { data: { test: 'events' } });
    await test('pushed event emitted', () => events.includes('pushed'));
    const job = await qm.pull('events', 0);
    await test('pulled event emitted', () => events.includes('pulled'));
    await qm.ack(job!.id);
    await test('completed event emitted', () => events.includes('completed'));
    unsubscribe();
    events.length = 0;
    await qm.push('events', { data: { after: 'unsubscribe' } });
    await test('unsubscribe stops events', () => events.length === 0);
    const cleanup = await qm.pull('events', 0);
    if (cleanup) await qm.ack(cleanup.id);
  });

  await section('Unique Keys', async () => {
    const first = await qm.push('unique', { data: { id: 1 }, uniqueKey: 'unique-1' });
    const duplicate = await qm.push('unique', { data: { id: 2 }, uniqueKey: 'unique-1' });
    await test('duplicate uniqueKey returns existing job', () => duplicate.id === first.id);
    const job = await qm.pull('unique', 0);
    await qm.ack(job!.id);
    await Bun.sleep(50);
    const reused = await qm.push('unique', { data: { id: 3 }, uniqueKey: 'unique-1' });
    await test('uniqueKey can be reused after completion', () => reused.id !== first.id);
    const reusedJob = await qm.pull('unique', 0);
    if (reusedJob) await qm.ack(reusedJob.id);
  });

  await section('Queue Concurrency', async () => {
    qm.setConcurrency('concurrency-q', 1);
    await qm.push('concurrency-q', { data: { n: 1 } });
    await qm.push('concurrency-q', { data: { n: 2 } });
    const first = await qm.pull('concurrency-q', 0);
    await test('first job pulled', () => !!first);
    await test('second job blocked by concurrency', async () =>
      (await qm.pull('concurrency-q', 0)) === null);
    if (first) await qm.ack(first.id);
    await Bun.sleep(50);
    const second = await qm.pull('concurrency-q', 0);
    await test('second job available after first completes', () => !!second);
    if (second) await qm.ack(second.id);
    qm.clearConcurrency('concurrency-q');
  });

  await section('Progress Tracking', async () => {
    await qm.push('progress', { data: { task: 'upload' } });
    const pulled = await qm.pull('progress', 0);
    await qm.updateProgress(pulled!.id, 50, 'Halfway done');
    const progress = qm.getProgress(pulled!.id);
    await test('progress updated', () => progress?.progress === 50);
    await test('progress message set', () => progress?.message === 'Halfway done');
    await qm.ack(pulled!.id);
  });

  await section('Job Logs', async () => {
    await qm.push('logs', { data: { task: 'process' } });
    const pulled = await qm.pull('logs', 0);
    qm.addLog(pulled!.id, 'Processing started', 'info');
    qm.addLog(pulled!.id, 'Step 1 complete', 'info');
    qm.addLog(pulled!.id, 'Minor issue detected', 'warn');
    const logs = qm.getLogs(pulled!.id);
    await test('logs recorded', () => logs.length === 3);
    await test('log levels correct', () => logs.some((log) => log.level === 'warn'));
    await qm.ack(pulled!.id);
  });

  await section('TTL (Time To Live)', async () => {
    const ttlJob = await qm.push('ttl', { data: { temp: true }, ttl: 100 });
    await test('job has TTL set', () => ttlJob.ttl === 100);
    const pulled = await qm.pull('ttl', 0);
    await test('job TTL preserved', () => pulled?.ttl === 100);
    if (pulled) await qm.ack(pulled.id);
  });

  await section('LIFO Mode', async () => {
    await qm.push('lifo', { data: { n: 1 }, lifo: true });
    await Bun.sleep(50);
    await qm.push('lifo', { data: { n: 2 }, lifo: true });
    await Bun.sleep(50);
    await qm.push('lifo', { data: { n: 3 }, lifo: true });
    const first = await qm.pull('lifo', 0);
    const second = await qm.pull('lifo', 0);
    const third = await qm.pull('lifo', 0);
    await test('LIFO: last in first out', () => (first?.data as { n?: number })?.n === 3);
    await test('LIFO: second correct', () => (second?.data as { n?: number })?.n === 2);
    await test('LIFO: third correct', () => (third?.data as { n?: number })?.n === 1);
    if (first) await qm.ack(first.id);
    if (second) await qm.ack(second.id);
    if (third) await qm.ack(third.id);
  });
}
