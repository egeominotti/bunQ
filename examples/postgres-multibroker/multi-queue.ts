import { Queue, QueueEvents, Worker } from 'bunqueue/client';
import { connection, invariant, settleCleanup, uniqueQueue, waitFor } from './shared';

interface EmailJob {
  orderId: string;
  to: string;
  transient?: boolean;
}

interface MediaJob {
  assetId: string;
  width: number;
}

interface AuditJob {
  action: string;
  orderId: string;
}

export async function runMultiQueueExample(): Promise<void> {
  const emailName = uniqueQueue('emails');
  const mediaName = uniqueQueue('media');
  const auditName = uniqueQueue('audit');
  const emails = new Queue<EmailJob>(emailName, { connection: connection('a') });
  const media = new Queue<MediaJob>(mediaName, { connection: connection('a') });
  const audit = new Queue<AuditJob>(auditName, { connection: connection('a') });
  const emailObserver = new Queue<EmailJob>(emailName, { connection: connection('c') });
  const events = new QueueEvents<{ delivered: string }, { stage: string }>(emailName, {
    connection: connection('c'),
  });
  const retried = new Set<string>();
  const completedEvents = new Set<string>();
  const progressEvents = new Set<string>();
  const emailStarts: string[] = [];
  const workerErrors: Error[] = [];

  events.on('completed', ({ jobId }) => completedEvents.add(jobId));
  events.on('progress', ({ jobId }) => progressEvents.add(jobId));

  const emailWorker = new Worker<EmailJob, { delivered: string }>(
    emailName,
    async (job) => {
      emailStarts.push(job.id);
      await job.updateProgress(50, 'rendering-template');
      await job.log(`sending order ${job.data.orderId}`);
      if (job.data.transient && !retried.has(job.id)) {
        retried.add(job.id);
        throw new Error('simulated provider timeout');
      }
      await job.updateProgress(100, 'delivered');
      return { delivered: job.data.to };
    },
    { batchSize: 1, concurrency: 1, connection: connection('b') }
  );
  const mediaWorker = new Worker<MediaJob, { output: string }>(
    mediaName,
    (job) => ({ output: `${job.data.assetId}-${job.data.width}.webp` }),
    { concurrency: 2, connection: connection('c') }
  );
  const auditWorker = new Worker<AuditJob, { recorded: boolean }>(
    auditName,
    () => ({ recorded: true }),
    { concurrency: 2, connection: connection('b') }
  );
  for (const worker of [emailWorker, mediaWorker, auditWorker]) {
    worker.on('error', (error) => workerErrors.push(error));
  }

  try {
    await Promise.all([
      emails.waitUntilReady(),
      media.waitUntilReady(),
      audit.waitUntilReady(),
      events.waitUntilReady(),
      emailWorker.waitUntilReady(),
      mediaWorker.waitUntilReady(),
      auditWorker.waitUntilReady(),
    ]);
    await emails.pauseAsync();
    await waitFor('the email pause to reach broker C', () => emailObserver.isPausedAsync());

    const emailJobs = await emails.addBulk([
      {
        name: 'order-confirmation',
        data: { orderId: 'ord-100', to: 'customer@example.com' },
        opts: { durable: true, jobId: `${emailName}-normal` },
      },
      {
        name: 'vip-confirmation',
        data: { orderId: 'ord-101', to: 'vip@example.com' },
        opts: { durable: true, jobId: `${emailName}-vip`, priority: 100 },
      },
      {
        name: 'provider-retry',
        data: { orderId: 'ord-102', to: 'retry@example.com', transient: true },
        opts: { attempts: 3, backoff: 25, durable: true, jobId: `${emailName}-retry` },
      },
      {
        name: 'delayed-follow-up',
        data: { orderId: 'ord-103', to: 'later@example.com' },
        opts: {
          delay: 300_000,
          durable: true,
          jobId: `${emailName}-delayed`,
          priority: 200,
        },
      },
    ]);
    const mediaJob = await media.add(
      'thumbnail',
      { assetId: 'asset-7', width: 640 },
      { durable: true }
    );
    const auditJob = await audit.add(
      'order-created',
      { action: 'created', orderId: 'ord-100' },
      { durable: true }
    );

    const delayedJob = emailJobs[3];
    invariant(
      (await emailObserver.getJobState(delayedJob.id)) === 'delayed',
      'the delayed email became eligible before promotion'
    );
    invariant(emailStarts.length === 0, 'the paused email queue started work');
    await emailObserver.resumeAsync();

    await waitFor('all ready emails to complete in priority order', async () => {
      const states = await Promise.all(emailJobs.slice(0, 3).map((job) => job.getState()));
      return states.every((state) => state === 'completed');
    });
    invariant(emailStarts[0] === `${emailName}-vip`, 'the highest-priority ready email ran late');
    invariant(!emailStarts.includes(delayedJob.id), 'the delayed email ran before promotion');
    invariant(
      (await emailObserver.getJobState(delayedJob.id)) === 'delayed',
      'the delayed email did not remain delayed'
    );
    const beforePromotion = await emailObserver.getJobCountsAsync();
    invariant(
      beforePromotion.completed === 3 && beforePromotion.delayed === 1,
      'ready and delayed email counts diverged before promotion'
    );
    await delayedJob.promote();

    await waitFor('all queues to complete across three brokers', async () => {
      const [emailCounts, mediaCounts, auditCounts] = await Promise.all([
        emailObserver.getJobCountsAsync(),
        media.getJobCountsAsync(),
        audit.getJobCountsAsync(),
      ]);
      return (
        emailCounts.completed === emailJobs.length &&
        mediaCounts.completed === 1 &&
        auditCounts.completed === 1
      );
    });
    await waitFor(
      'cross-broker completion and progress events',
      () => completedEvents.size === emailJobs.length && progressEvents.size === emailJobs.length
    );

    invariant(retried.has(`${emailName}-retry`), 'the transient email was not retried');
    invariant(workerErrors.length === 0, `worker error: ${workerErrors[0]?.message}`);
    invariant(await mediaJob.isCompleted(), 'the media job did not complete');
    invariant(await auditJob.isCompleted(), 'the audit job did not complete');
    invariant((await emailObserver.getWorkersCount()) >= 1, 'no email worker was registered');

    const logs = await emailObserver.getJobLogs(`${emailName}-normal`);
    invariant(logs.logs.includes('[info] sending order ord-100'), 'the email log was not retained');
    const retained = await emailObserver.getJob(`${emailName}-normal`);
    invariant(
      retained?.returnvalue &&
        (retained.returnvalue as { delivered: string }).delivered === 'customer@example.com',
      'the cross-broker result was not retained'
    );
  } finally {
    await settleCleanup(
      [
        () => emailWorker.close(true),
        () => mediaWorker.close(true),
        () => auditWorker.close(true),
        () => events.close(),
      ],
      [
        () => emails.obliterateAsync(),
        () => media.obliterateAsync(),
        () => audit.obliterateAsync(),
      ],
      [() => emails.close(), () => media.close(), () => audit.close(), () => emailObserver.close()]
    );
  }
}
