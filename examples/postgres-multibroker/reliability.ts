import { Queue, Worker } from 'bunqueue/client';
import { connection, invariant, settleCleanup, uniqueQueue, waitFor, withTimeout } from './shared';

interface PaymentJob {
  amount: number;
  failUntilRetried?: boolean;
  mode?: 'concurrency' | 'rate';
}

type WorkerBroker = 'b' | 'c';

export async function runReliabilityExample(): Promise<void> {
  const queueName = uniqueQueue('payments');
  const primary = new Queue<PaymentJob>(queueName, {
    autoBatch: { enabled: false },
    connection: connection('a'),
  });
  const peer = new Queue<PaymentJob>(queueName, {
    autoBatch: { enabled: false },
    connection: connection('c'),
  });
  const attempts = new Map<string, number>();
  const startedBy = new Map<string, WorkerBroker>();
  let allowRetry = false;
  let releaseConcurrency = (): void => undefined;
  const concurrencyGate = new Promise<void>((resolve) => {
    releaseConcurrency = resolve;
  });

  const processor = (broker: WorkerBroker) => async (job: { id: string; data: PaymentJob }) => {
    attempts.set(job.id, (attempts.get(job.id) ?? 0) + 1);
    startedBy.set(job.id, broker);
    if (job.data.mode === 'concurrency') await concurrencyGate;
    if (job.data.failUntilRetried && !allowRetry) throw new Error('payment provider refused');
    return { charged: job.data.amount };
  };
  const workerB = new Worker<PaymentJob, { charged: number }>(queueName, processor('b'), {
    autorun: false,
    batchSize: 1,
    concurrency: 1,
    connection: connection('b'),
  });
  const workerC = new Worker<PaymentJob, { charged: number }>(queueName, processor('c'), {
    autorun: false,
    batchSize: 1,
    concurrency: 1,
    connection: connection('c'),
  });

  try {
    await Promise.all([primary.waitUntilReady(), peer.waitUntilReady(), workerB.waitUntilReady()]);
    await workerC.waitUntilReady();

    await primary.setGlobalConcurrencyAsync(1);
    await waitFor(
      'the concurrency cap to reach broker C',
      async () => (await peer.getGlobalConcurrency()) === 1
    );
    const concurrencyJobs = await primary.addBulk([
      { name: 'concurrency-a', data: { amount: 1, mode: 'concurrency' }, opts: { durable: true } },
      { name: 'concurrency-b', data: { amount: 2, mode: 'concurrency' }, opts: { durable: true } },
    ]);
    workerB.run();
    await waitFor('worker B to occupy the global concurrency slot', () =>
      startedBy.has(concurrencyJobs[0].id)
    );
    workerB.pause();
    const concurrencyDrained = new Promise<void>((resolve) => workerC.once('drained', resolve));
    workerC.run();
    await withTimeout(
      'worker C observing an exhausted concurrency cap',
      () => concurrencyDrained,
      5_000
    );

    const constrainedStates = await Promise.all(
      concurrencyJobs.map((job) => peer.getJobState(job.id))
    );
    invariant(
      constrainedStates.filter((state) => state === 'active').length === 1 &&
        constrainedStates.filter((state) => state === 'waiting').length === 1,
      'the global concurrency cap did not hold one active and one waiting job'
    );
    invariant(await peer.isMaxed(), 'broker C did not report the exhausted concurrency cap');
    invariant(
      ![...startedBy.values()].includes('c'),
      'worker C started work while the global slot was occupied'
    );

    releaseConcurrency();
    await waitFor('both concurrency jobs to complete', async () => {
      const states = await Promise.all(concurrencyJobs.map((job) => primary.getJobState(job.id)));
      return states.every((state) => state === 'completed');
    });
    invariant(
      startedBy.get(concurrencyJobs[0].id) === 'b' && startedBy.get(concurrencyJobs[1].id) === 'c',
      'the two brokers did not hand off the constrained work'
    );
    await primary.removeGlobalConcurrencyAsync();

    workerC.pause();
    await primary.setGlobalRateLimitAsync(2, 300_000);
    await waitFor(
      'the rate limit to reach broker C',
      async () => (await peer.getGlobalRateLimit())?.max === 2
    );
    workerB.resume();
    const firstRateJob = await primary.add(
      'rate-a',
      { amount: 3, mode: 'rate' },
      { durable: true }
    );
    await waitFor('worker B to consume the first rate token', async () => {
      return (await peer.getJobState(firstRateJob.id)) === 'completed';
    });
    invariant(startedBy.get(firstRateJob.id) === 'b', 'worker B did not consume the first token');
    workerB.pause();

    const remainingRateJobs = await peer.addBulk([
      { name: 'rate-b', data: { amount: 4, mode: 'rate' }, opts: { durable: true } },
      { name: 'rate-c', data: { amount: 5, mode: 'rate' }, opts: { durable: true } },
    ]);
    const rateDrained = new Promise<void>((resolve) => workerC.once('drained', resolve));
    workerC.resume();
    await withTimeout('worker C exhausting the shared rate window', () => rateDrained, 5_000);
    const rateStates = await Promise.all(
      remainingRateJobs.map((job) => primary.getJobState(job.id))
    );
    invariant(
      rateStates.filter((state) => state === 'completed').length === 1 &&
        rateStates.filter((state) => state === 'waiting').length === 1,
      'the fixed-window rate budget did not stop the third claim'
    );
    invariant((await peer.getRateLimitTtl()) > 0, 'the shared rate window has no TTL');

    await primary.removeGlobalRateLimitAsync();
    await waitFor('the final rate-limited job to complete after limit removal', async () => {
      const states = await Promise.all(remainingRateJobs.map((job) => peer.getJobState(job.id)));
      return states.every((state) => state === 'completed');
    });
    invariant((await peer.getWorkersCount()) >= 2, 'both broker workers were not registered');
    workerB.resume();

    await primary.pauseAsync();
    await waitFor('the paused state to reach broker C', () => peer.isPausedAsync());
    const customId = `${queueName}-charge-once`;
    const [left, right] = await Promise.all([
      primary.add('charge', { amount: 49 }, { durable: true, jobId: customId }),
      peer.add('charge', { amount: 49 }, { durable: true, jobId: customId }),
    ]);
    invariant(
      left.id === customId && right.id === customId,
      'custom-ID admission was not idempotent'
    );
    invariant((await peer.getJobCountsAsync()).paused === 1, 'the paused job was not shared');

    await peer.resumeAsync();
    await waitFor('the idempotent payment to complete', async () => {
      return (await primary.getJobState(customId)) === 'completed';
    });
    invariant(attempts.get(customId) === 1, 'the custom-ID job executed more than once');

    const failed = await primary.add(
      'charge-after-review',
      { amount: 125, failUntilRetried: true },
      { attempts: 1, durable: true }
    );
    await waitFor('the payment to reach the shared DLQ', async () => {
      return (await peer.getJobState(failed.id)) === 'failed';
    });
    invariant(
      (await peer.getDlqAsync()).some((entry) => entry.job.id === failed.id),
      'the failed job was not visible from broker C'
    );

    allowRetry = true;
    invariant((await peer.retryDlqAsync(failed.id)) === 1, 'the DLQ retry was not accepted');
    await waitFor('the reviewed payment to complete', async () => {
      return (await primary.getJobState(failed.id)) === 'completed';
    });
    invariant(attempts.get(failed.id) === 2, 'the reviewed payment did not execute twice');
    invariant((await primary.getDlqStatsAsync()).total === 0, 'the DLQ was not cleared');
  } finally {
    releaseConcurrency();
    await settleCleanup(
      [
        () => primary.removeGlobalRateLimitAsync(),
        () => primary.removeGlobalConcurrencyAsync(),
        () => workerB.close(true),
        () => workerC.close(true),
      ],
      [() => primary.obliterateAsync()],
      [() => primary.close(), () => peer.close()]
    );
  }
}
