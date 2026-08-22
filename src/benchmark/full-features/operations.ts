import { jobId } from '../../domain/types/job';
import { queueManager as qm, section, test } from './harness';

export async function runOperationalFeatureSections(): Promise<void> {
  await section('Queue Control', async () => {
    await qm.push('control', { data: { n: 1 } });
    await qm.push('control', { data: { n: 2 } });
    qm.pause('control');
    await test('queue paused', () => qm.isPaused('control') === true);
    await test('cannot pull from paused queue', async () => (await qm.pull('control', 0)) === null);
    qm.resume('control');
    await test('queue resumed', () => qm.isPaused('control') === false);
    const afterResume = await qm.pull('control', 0);
    await test('can pull after resume', () => !!afterResume);
    if (afterResume) await qm.ack(afterResume.id);
    await test('count returns correct number', () => qm.count('control') === 1);
    await test('drain removes waiting jobs', () => qm.drain('control') === 1);
    await test('listQueues returns queues', () => qm.listQueues().length > 0);
  });

  await section('Stats/Metrics', async () => {
    const stats = qm.getStats();
    await test('stats has waiting count', () => typeof stats.waiting === 'number');
    await test('stats has active count', () => typeof stats.active === 'number');
    await test('stats has completed count', () => typeof stats.completed === 'number');
    await test('stats has totalPushed', () => typeof stats.totalPushed === 'bigint');
    await test('stats has totalCompleted', () => typeof stats.totalCompleted === 'bigint');
    const prometheus = qm.getPrometheusMetrics();
    await test('prometheus metrics generated', () => prometheus.includes('bunqueue_'));
    await test('prometheus has job counts', () => prometheus.includes('bunqueue_jobs_waiting'));
  });

  await section('Custom ID', async () => {
    const customJob = await qm.push('custom-id', {
      data: { custom: true },
      customId: 'my-custom-id-123',
    });
    await test('job created with customId', () => !!customJob);
    const found = await qm.getJobByCustomId('my-custom-id-123');
    await test('getJobByCustomId finds job', () => found?.id === customJob.id);
    const pulled = await qm.pull('custom-id', 0);
    if (pulled) await qm.ack(pulled.id);
  });

  await section('Remove on Complete', async () => {
    const job = await qm.push('remove-complete', {
      data: { temp: true },
      removeOnComplete: true,
    });
    const pulled = await qm.pull('remove-complete', 0);
    await qm.ack(pulled!.id);
    await test('job removed after completion', async () => (await qm.getJob(job.id)) === null);
  });

  await section('Workers/Webhooks (via internal managers)', async () => {
    const worker = qm.workerManager.register('test-worker', ['work-queue']);
    await test('worker registered', () => !!worker.id);
    await test('worker in list', () =>
      qm.workerManager.list().some((item) => item.id === worker.id));
    qm.workerManager.heartbeat(worker.id);
    await test('heartbeat works', () => true);
    qm.workerManager.unregister(worker.id);
    await test('worker unregistered', () =>
      !qm.workerManager.list().some((item) => item.id === worker.id));
    const webhook = qm.webhookManager.add('http://example.com/webhook', ['completed', 'failed']);
    await test('webhook added', () => !!webhook.id);
    await test('webhook in list', () =>
      qm.webhookManager.list().some((item) => item.id === webhook.id));
    qm.webhookManager.remove(webhook.id);
    await test('webhook removed', () =>
      !qm.webhookManager.list().some((item) => item.id === webhook.id));
  });

  await section('Rate Limiting', async () => {
    qm.setRateLimit('rate-test', 2);
    await qm.push('rate-test', { data: { n: 1 } });
    await qm.push('rate-test', { data: { n: 2 } });
    await qm.push('rate-test', { data: { n: 3 } });
    const first = await qm.pull('rate-test', 0);
    const second = await qm.pull('rate-test', 0);
    const third = await qm.pull('rate-test', 0);
    await test('rate limit allows first 2 jobs', () => !!first && !!second);
    await test('rate limit blocks third job', () => third === null);
    if (first) await qm.ack(first.id);
    if (second) await qm.ack(second.id);
    qm.clearRateLimit('rate-test');
    const thirdAfter = await qm.pull('rate-test', 0);
    await test('jobs available after rate limit cleared', () => !!thirdAfter);
    if (thirdAfter) await qm.ack(thirdAfter.id);
  });

  await section('Get Job', async () => {
    const created = await qm.push('get-job', { data: { fetch: true } });
    const fetched = await qm.getJob(created.id);
    await test('getJob returns job', () => fetched?.id === created.id);
    await test('getJob returns correct data', () =>
      (fetched?.data as { fetch?: boolean } | undefined)?.fetch === true);
    await test('getJob returns null for unknown id', async () =>
      (await qm.getJob(jobId('non-existent-id'))) === null);
    const pulled = await qm.pull('get-job', 0);
    if (pulled) await qm.ack(pulled.id);
  });
}
