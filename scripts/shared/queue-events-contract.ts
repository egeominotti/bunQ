import { Queue, QueueEvents, Worker } from '../../src/client';

type Mode = 'embedded' | 'tcp';

interface Result {
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

export async function runQueueEventsContract(mode: Mode, tcpPort?: number): Promise<Result> {
  const suffix = crypto.randomUUID();
  const queueName = `queue-events-${mode}-${suffix}`;
  const otherName = `queue-events-other-${mode}-${suffix}`;
  const connection = mode === 'tcp' ? { host: '127.0.0.1', port: tcpPort, poolSize: 1 } : undefined;
  const runtime = mode === 'embedded' ? { embedded: true } : { embedded: false, connection };
  const queue = new Queue<{ action: 'complete' | 'fail'; value: number }>(queueName, runtime);
  const other = new Queue<{ action: 'other'; value: number }>(otherName, runtime);
  const events = new QueueEvents<{ doubled: number }, number>(queueName, runtime);
  let worker: Worker<{ action: 'complete' | 'fail'; value: number }, { doubled: number }> | null =
    null;
  let passed = 0;
  let failed = 0;

  const check = (condition: boolean, label: string): void => {
    if (condition) {
      console.log(`   PASS ${label}`);
      passed++;
    } else {
      console.log(`   FAIL ${label}`);
      failed++;
    }
  };

  try {
    await Promise.all([queue.obliterateAsync(), other.obliterateAsync()]);
    await events.waitUntilReady();

    const waiting = new Set<string>();
    const active = new Set<string>();
    const progress = new Map<string, number>();
    const completed = new Map<string, { doubled: number }>();
    const failures = new Map<string, string>();
    events.on('waiting', ({ jobId }) => waiting.add(jobId));
    events.on('active', ({ jobId }) => active.add(jobId));
    events.on('progress', ({ jobId, data }) => progress.set(jobId, data));
    events.on('completed', ({ jobId, returnvalue }) => completed.set(jobId, returnvalue));
    events.on('failed', ({ jobId, failedReason }) => failures.set(jobId, failedReason));

    worker = new Worker(
      queueName,
      async (job) => {
        if (job.data.action === 'fail') throw new Error('shared event failure');
        await job.updateProgress(50);
        return { doubled: job.data.value * 2 };
      },
      { ...runtime, concurrency: 1 }
    );
    await worker.waitUntilReady();

    const successful = await queue.add(
      'complete',
      { action: 'complete', value: 21 },
      { durable: true }
    );
    await eventually(() => completed.has(successful.id), `${mode} completed event`);
    check(waiting.has(successful.id), 'waiting event has the exact job id');
    check(active.has(successful.id), 'active event has the exact job id');
    check(progress.get(successful.id) === 50, 'progress event preserves its payload');
    check(completed.get(successful.id)?.doubled === 42, 'completed event preserves its result');

    const doomed = await queue.add(
      'fail',
      { action: 'fail', value: 1 },
      { attempts: 1, durable: true }
    );
    await eventually(() => failures.has(doomed.id), `${mode} failed event`);
    check(
      failures.get(doomed.id)?.includes('shared event failure') === true,
      'failed event preserves the broker error'
    );

    const observedBeforeOtherQueue = waiting.size;
    const otherJob = await other.add('other', { action: 'other', value: 1 }, { durable: true });
    await eventually(
      async () => (await other.getJobState(otherJob.id)) === 'waiting',
      `${mode} other queue fixture`
    );
    await Bun.sleep(100);
    check(waiting.size === observedBeforeOtherQueue, 'subscription filters other queues');

    events.close();
    const observedBeforeClose = waiting.size;
    const afterClose = await queue.add(
      'after-close',
      { action: 'complete', value: 2 },
      { durable: true, delay: 60_000 }
    );
    await eventually(
      async () => (await queue.getJobState(afterClose.id)) === 'delayed',
      `${mode} post-close fixture`
    );
    await Bun.sleep(100);
    check(waiting.size === observedBeforeClose, 'close stops remote and embedded delivery');
  } finally {
    await worker?.close(true);
    events.close();
    await Promise.all([queue.obliterateAsync(), other.obliterateAsync()]);
    await Promise.all([queue.close(), other.close()]);
  }

  return { passed, failed };
}
