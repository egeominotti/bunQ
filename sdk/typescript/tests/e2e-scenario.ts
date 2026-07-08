/** E2E: realistic production mix — multi-queue, flaky processors, retries,
 * DLQ, cron, priorities and delays running together. Verifies zero job loss:
 * every enqueued job ends up either completed or in the DLQ. */

import {
  assert,
  assertEq,
  makeWorker,
  namedQueue,
  qname,
  sleep,
  test,
  waitFor,
} from './harness.ts';

const ORDERS = 120;
const EMAILS = 150;

test('scenario: realistic production mix with zero job loss', async () => {
  const ordersName = qname('scn-orders');
  const emailsName = qname('scn-emails');
  const reportsName = qname('scn-reports');
  const orders = namedQueue<{ i: number }>(ordersName);
  const emails = namedQueue<{ i: number }>(emailsName);
  const reports = namedQueue<{ tick: boolean }>(reportsName);

  // Orders: flaky processor — every 10th job fails twice, then succeeds
  // (attempts=3). Every 25th is a poison job that always fails → DLQ.
  const orderAttempts = new Map<number, number>();
  const orderWorker = makeWorker<{ i: number }, string>(
    ordersName,
    async (job) => {
      const i = job.data.i;
      const seen = (orderAttempts.get(i) ?? 0) + 1;
      orderAttempts.set(i, seen);
      await sleep(2);
      if (i % 25 === 0) throw new Error(`poison order ${i}`);
      if (i % 10 === 0 && seen < 3) throw new Error(`transient failure ${i} (attempt ${seen})`);
      return `order-${i}`;
    },
    { concurrency: 8 }
  );
  orderWorker.on('error', () => {});

  // Emails: fast and reliable, higher concurrency.
  const emailWorker = makeWorker<{ i: number }, string>(
    emailsName,
    async (job) => {
      await sleep(1);
      return `mail-${job.data.i}`;
    },
    { concurrency: 10 }
  );
  emailWorker.on('error', () => {});

  // Reports: fed by a cron every 400ms while the load runs.
  const reportTicks: string[] = [];
  const reportWorker = makeWorker<{ tick: boolean }, string>(
    reportsName,
    (job) => {
      reportTicks.push(job.id);
      return 'report';
    },
    { concurrency: 2 }
  );
  reportWorker.on('error', () => {});
  await reports.every(qname('scn-cron'), 400, { tick: true });

  // Producers: mixed priorities and delays, bulk + singles interleaved.
  const orderJobs: string[] = [];
  for (let i = 1; i <= ORDERS; i += 1) {
    const job = await orders.add(
      'order',
      { i },
      {
        attempts: 3,
        backoff: 50,
        priority: i % 5,
        delay: i % 20 === 0 ? 300 : undefined,
      }
    );
    orderJobs.push(job.id);
  }
  const emailJobs = await emails.addBulk(
    Array.from({ length: EMAILS }, (_, i) => ({
      name: 'mail',
      data: { i },
      opts: { priority: i % 3 },
    }))
  );
  assertEq(emailJobs.length, EMAILS, 'bulk accepted');

  // Everything settles: poison orders → DLQ, the rest completed.
  const poison = Math.floor(ORDERS / 25); // i = 25, 50, 75, 100
  await waitFor(async () => {
    const oc = await orders.getJobCounts();
    const ec = await emails.getJobCounts();
    return (
      oc.completed === ORDERS - poison &&
      (await orders.getDlq()).length === poison &&
      ec.completed === EMAILS
    );
  }, 60_000);

  // Zero loss accounting on orders: every id is completed or in the DLQ.
  const dlq = await orders.getDlq();
  const dlqIds = new Set(dlq.map((e) => String(e.jobId ?? e.id)));
  for (const id of orderJobs) {
    const state = await orders.getJobState(id);
    assert(
      state === 'completed' || dlqIds.has(id),
      `order ${id} neither completed nor in DLQ (state=${state})`
    );
  }

  // Transient failures really did retry (attempt counts prove server retry).
  const retried = [...orderAttempts.entries()].filter(([i]) => i % 10 === 0 && i % 25 !== 0);
  assert(retried.length > 0 && retried.every(([, n]) => n === 3), 'flaky orders took 3 attempts');

  // Cron produced ticks while the system was under load.
  await waitFor(() => reportTicks.length >= 2, 20_000);

  // Cleanup.
  for (const scheduler of await reports.getJobSchedulers()) {
    await reports.removeJobScheduler(scheduler.name as string);
  }
  await orderWorker.close();
  await emailWorker.close();
  await reportWorker.close();
  orders.close();
  emails.close();
  reports.close();
});
