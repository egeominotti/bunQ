import { FlowProducer, Queue, QueueEvents, type QueueOptions, Worker } from '../../src/client';

type Mode = 'embedded' | 'tcp';

interface ContractResult {
  failed: number;
  passed: number;
}

async function eventually(
  condition: () => boolean | Promise<boolean>,
  label: string,
  timeoutMs = 10_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await condition()) && Date.now() < deadline) await Bun.sleep(20);
  if (!(await condition())) throw new Error(`${label} timed out`);
}

export async function runRuntimeResultsContract(
  mode: Mode,
  tcpPort?: number
): Promise<ContractResult> {
  const queueName = `runtime-results-${mode}-${crypto.randomUUID()}`;
  const connection = { host: '127.0.0.1', port: tcpPort, poolSize: 1 };
  const runtime: QueueOptions =
    mode === 'embedded' ? { embedded: true } : { embedded: false, connection };
  const queue = new Queue<{ delay?: number; result: unknown }>(queueName, runtime);
  const events = new QueueEvents<unknown>(queueName, runtime);
  const flow = new FlowProducer(runtime);
  const worker = new Worker<{ delay?: number; result: unknown }, unknown>(
    queueName,
    async (job) => {
      if (job.data.delay) await Bun.sleep(job.data.delay);
      return job.data.result;
    },
    { ...runtime, concurrency: 1 }
  );
  let passed = 0;
  let failed = 0;

  const check = (condition: boolean, label: string, detail = ''): void => {
    if (condition) {
      console.log(`   PASS ${label}`);
      passed++;
    } else {
      console.log(`   FAIL ${label}${detail ? `: ${detail}` : ''}`);
      failed++;
    }
  };

  try {
    await queue.obliterateAsync();
    await Promise.all([events.waitUntilReady(), worker.waitUntilReady(), flow.waitUntilReady()]);

    const completed = await queue.add(
      'completed-before-wait',
      { result: { answer: 42 } },
      { durable: true }
    );
    await eventually(
      async () => (await queue.getJobState(completed.id)) === 'completed',
      `${mode} completed fixture`
    );
    const completedResult = await queue.waitJobUntilFinished(completed.id, events, 2_000);
    check(
      JSON.stringify(completedResult) === JSON.stringify({ answer: 42 }),
      'waitJobUntilFinished reads an already-persisted result exactly',
      JSON.stringify(completedResult)
    );

    const live = await queue.add(
      'in-flight-wait',
      { delay: 100, result: ['live', 44] },
      { durable: true }
    );
    const liveResult = await queue.waitJobUntilFinished(live.id, events, 2_000);
    check(
      JSON.stringify(liveResult) === JSON.stringify(['live', 44]),
      'waitJobUntilFinished preserves an in-flight event result',
      JSON.stringify(liveResult)
    );

    const expected = [42, 0, false, '', null] as const;
    const resultJobs = await Promise.all(
      expected.map((result, index) =>
        queue.add(`flow-result-${index}`, { result }, { durable: true })
      )
    );
    await Promise.all(
      resultJobs.map((job) =>
        eventually(
          async () => (await queue.getJobState(job.id)) === 'completed',
          `${mode} flow result ${job.id}`
        )
      )
    );

    const single = await flow.getParentResult<number>(resultJobs[0].id);
    check(single === 42, 'getParentResult returns the exact completed value', String(single));

    const missingId = `missing-${crypto.randomUUID()}`;
    const results = await flow.getParentResults<unknown>([
      ...resultJobs.map((job) => job.id),
      missingId,
    ]);
    check(
      resultJobs.every((job, index) => results.get(job.id) === expected[index]),
      'getParentResults preserves order and every falsy/null result',
      JSON.stringify([...results.entries()])
    );
    check(!results.has(missingId), 'getParentResults omits an ID with no persisted result');
    check(
      (await flow.getParentResult(missingId)) === undefined,
      'getParentResult distinguishes a missing result from persisted null'
    );
  } finally {
    events.close();
    await worker.close(true);
    await flow.close();
    await queue.obliterateAsync();
    await queue.close();
  }

  return { failed, passed };
}
