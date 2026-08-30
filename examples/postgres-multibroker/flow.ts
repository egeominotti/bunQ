import { FlowProducer, Queue, Worker } from 'bunqueue/client';
import { connection, invariant, settleCleanup, uniqueQueue } from './shared';

type FlowData = { stage?: string; value?: number };

export async function runFlowExample(): Promise<void> {
  const extractQueue = uniqueQueue('extract');
  const transformQueue = uniqueQueue('transform');
  const reportQueue = uniqueQueue('report');
  const effects: string[] = [];
  const executions = new Map<string, number>();
  const mark = (id: string): void => {
    effects.push(id);
    executions.set(id, (executions.get(id) ?? 0) + 1);
  };

  const extractWorker = new Worker<FlowData, number>(
    extractQueue,
    (job) => {
      mark(job.id);
      return Number(job.data.value);
    },
    { concurrency: 4, connection: connection('b') }
  );
  const transformWorker = new Worker<FlowData, number>(
    transformQueue,
    async (job) => {
      mark(job.id);
      const values = await job.getChildrenValues<number>();
      return Object.values(values).reduce((sum, value) => sum + value, 0) * 2;
    },
    { concurrency: 2, connection: connection('c') }
  );
  const reportWorker = new Worker<FlowData, number>(
    reportQueue,
    async (job) => {
      mark(job.id);
      const values = await job.getChildrenValues<number>();
      return Object.values(values).reduce((sum, value) => sum + value, 0);
    },
    { concurrency: 1, connection: connection('c') }
  );
  const flow = new FlowProducer({ connection: connection('a') });
  const observer = new Queue<FlowData>(reportQueue, { connection: connection('b') });

  try {
    await Promise.all([
      extractWorker.waitUntilReady(),
      transformWorker.waitUntilReady(),
      reportWorker.waitUntilReady(),
      flow.waitUntilReady(),
      observer.waitUntilReady(),
    ]);
    const suffix = crypto.randomUUID();
    const root = await flow.add<FlowData>({
      children: [
        {
          children: [
            {
              data: { value: 2 },
              name: 'extract-sales',
              opts: { durable: true, jobId: `extract-sales-${suffix}` },
              queueName: extractQueue,
            },
            {
              data: { value: 3 },
              name: 'extract-costs',
              opts: { durable: true, jobId: `extract-costs-${suffix}` },
              queueName: extractQueue,
            },
          ],
          data: { stage: 'transform' },
          name: 'transform-ledger',
          opts: { durable: true, jobId: `transform-${suffix}` },
          queueName: transformQueue,
        },
        {
          data: { value: 5 },
          name: 'extract-forecast',
          opts: { durable: true, jobId: `extract-forecast-${suffix}` },
          queueName: extractQueue,
        },
      ],
      data: { stage: 'report' },
      name: 'publish-report',
      opts: { durable: true, jobId: `report-${suffix}` },
      queueName: reportQueue,
    });

    invariant((await root.job.waitUntilFinished(null, 20_000)) === 15, 'wrong flow result');
    invariant(executions.size === 5, 'not every flow node executed');
    invariant(
      [...executions.values()].every((count) => count === 1),
      'a flow node executed more than once'
    );

    const transformNode = root.children?.[0];
    invariant(transformNode, 'the transform node is missing');
    const transformId = transformNode.job.id;
    const reportId = root.job.id;
    for (const leaf of transformNode.children ?? []) {
      invariant(effects.indexOf(leaf.job.id) < effects.indexOf(transformId), 'transform ran early');
    }
    invariant(effects.indexOf(transformId) < effects.indexOf(reportId), 'report ran early');
    invariant((await observer.getJobState(reportId)) === 'completed', 'broker B missed the report');

    const tree = await flow.getFlow({ depth: 3, id: reportId, queueName: reportQueue });
    invariant(tree?.children?.length === 2, 'the persisted flow tree is incomplete');
    invariant((await flow.getParentResult<number>(reportId)) === 15, 'the result is not durable');
  } finally {
    const extract = new Queue(extractQueue, { connection: connection('a') });
    const transform = new Queue(transformQueue, { connection: connection('a') });
    const report = new Queue(reportQueue, { connection: connection('a') });
    await settleCleanup(
      [
        () => extractWorker.close(true),
        () => transformWorker.close(true),
        () => reportWorker.close(true),
        () => flow.close(),
      ],
      [
        () => extract.obliterateAsync(),
        () => transform.obliterateAsync(),
        () => report.obliterateAsync(),
      ],
      [() => extract.close(), () => transform.close(), () => report.close(), () => observer.close()]
    );
  }
}
