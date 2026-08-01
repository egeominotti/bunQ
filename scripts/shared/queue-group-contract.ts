import { Queue, QueueGroup, type Worker } from '../../src/client';

type Mode = 'embedded' | 'tcp';
type Payload = { value: number };

interface ContractResult {
  passed: number;
  failed: number;
}

async function eventually<T>(
  read: () => T | Promise<T>,
  accept: (value: T) => boolean,
  timeoutMs = 10_000
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let value = await read();
  while (!accept(value) && Date.now() < deadline) {
    await Bun.sleep(25);
    value = await read();
  }
  return value;
}

export async function runQueueGroupContract(mode: Mode): Promise<ContractResult> {
  const tcpPort = Number.parseInt(process.env.TCP_PORT ?? '16789', 10);
  const namespace = `queue-group-${mode}-${crypto.randomUUID()}`;
  const group = new QueueGroup(namespace);
  const connection = { hostname: '127.0.0.1', port: tcpPort };
  const queueOptions = mode === 'embedded' ? { embedded: true } : { connection };
  const workerOptions =
    mode === 'embedded'
      ? { embedded: true, concurrency: 1, useLocks: true }
      : { connection, concurrency: 1, useLocks: true };
  const alpha = group.getQueue<Payload>('alpha', queueOptions);
  const beta = group.getQueue<Payload>('beta', queueOptions);
  const directAlpha = new Queue<Payload>(`${namespace}:alpha`, queueOptions);
  const workers: Worker<Payload, unknown>[] = [];
  let passed = 0;
  let failed = 0;

  const check = (condition: boolean, label: string, detail = ''): void => {
    if (condition) {
      console.log(`   [PASS] ${label}`);
      passed++;
    } else {
      console.log(`   [FAIL] ${label}${detail ? `: ${detail}` : ''}`);
      failed++;
    }
  };

  const clean = async (): Promise<void> => {
    await group.resumeAllAsync();
    await group.obliterateAllAsync();
  };

  console.log(`=== QueueGroup contract (${mode}) ===\n`);

  try {
    console.log('1. Namespacing, listing, and direct queue visibility...');
    await clean();
    await alpha.add('alpha', { value: 1 });
    await beta.add('beta', { value: 2 });
    const names = await group.listQueuesAsync();
    const alphaCounts = await alpha.getJobCountsAsync();
    const directCounts = await directAlpha.getJobCountsAsync();
    const betaCounts = await beta.getJobCountsAsync();
    check(
      names.join(',') === 'alpha,beta',
      'listQueuesAsync returns the exact sorted group members'
    );
    check(
      alphaCounts.waiting === 1 &&
        betaCounts.waiting === 1 &&
        directCounts.waiting === alphaCounts.waiting,
      'group queues use the expected namespace and preserve queue isolation',
      JSON.stringify({ alphaCounts, betaCounts, directCounts })
    );

    console.log('\n2. getWorker consumes only its namespaced queue...');
    await clean();
    await alpha.addBulk([
      { name: 'one', data: { value: 1 } },
      { name: 'two', data: { value: 2 } },
      { name: 'three', data: { value: 3 } },
    ]);
    await beta.add('untouched', { value: 99 });
    const processed: number[] = [];
    const worker = group.getWorker<Payload>(
      'alpha',
      (job) => {
        processed.push(job.data.value);
        return job.data.value;
      },
      workerOptions
    );
    workers.push(worker);
    await eventually(
      () => processed.length,
      (count) => count === 3
    );
    await worker.close();
    workers.splice(workers.indexOf(worker), 1);
    const untouched = await beta.getJobCountsAsync();
    check(
      processed.sort((a, b) => a - b).join(',') === '1,2,3',
      'getWorker processes every job exactly once'
    );
    check(untouched.waiting === 1, 'a worker in one group queue does not consume another queue');

    console.log('\n3. pauseAllAsync and resumeAllAsync are acknowledged by the broker...');
    await clean();
    await group.pauseAllAsync();
    const paused = await Promise.all([alpha.isPausedAsync(), beta.isPausedAsync()]);
    await alpha.add('paused-alpha', { value: 10 });
    await beta.add('paused-beta', { value: 20 });
    const resumed: number[] = [];
    const alphaWorker = group.getWorker<Payload>(
      'alpha',
      (job) => resumed.push(job.data.value),
      workerOptions
    );
    const betaWorker = group.getWorker<Payload>(
      'beta',
      (job) => resumed.push(job.data.value),
      workerOptions
    );
    workers.push(alphaWorker, betaWorker);
    await Bun.sleep(500);
    const pausedCounts = await Promise.all([alpha.getJobCountsAsync(), beta.getJobCountsAsync()]);
    check(
      paused.every(Boolean) &&
        resumed.length === 0 &&
        pausedCounts.every((counts) => counts.paused === 1),
      'pauseAllAsync prevents delivery on every member queue',
      JSON.stringify({ paused, resumed, pausedCounts })
    );
    await group.resumeAllAsync();
    const delivered = await eventually(
      () => [...resumed],
      (values) => values.length === 2
    );
    const resumedStates = await Promise.all([alpha.isPausedAsync(), beta.isPausedAsync()]);
    await Promise.all([alphaWorker.close(), betaWorker.close()]);
    workers.length = 0;
    check(
      resumedStates.every((value) => value === false) &&
        delivered.sort((a, b) => a - b).join(',') === '10,20',
      'resumeAllAsync releases both queues and each job completes exactly once',
      JSON.stringify({ resumedStates, delivered })
    );

    console.log('\n4. drainAllAsync returns the exact aggregate removal count...');
    await clean();
    await alpha.addBulk([
      { name: 'a1', data: { value: 1 } },
      { name: 'a2', data: { value: 2 } },
    ]);
    await beta.addBulk([
      { name: 'b1', data: { value: 3 } },
      { name: 'b2', data: { value: 4 } },
    ]);
    const drained = await group.drainAllAsync();
    const afterDrain = await Promise.all([alpha.getJobCountsAsync(), beta.getJobCountsAsync()]);
    check(
      drained === 4 && afterDrain.every((counts) => counts.waiting + counts.prioritized === 0),
      'drainAllAsync removes all four waiting jobs and reports four',
      JSON.stringify({ drained, afterDrain })
    );

    console.log('\n5. obliterateAllAsync removes every state bucket...');
    await alpha.add('completed-later', { value: 5 });
    await beta.add('waiting', { value: 6 });
    await group.obliterateAllAsync();
    const afterObliterate = await Promise.all([
      alpha.getJobCountsAsync(),
      beta.getJobCountsAsync(),
    ]);
    const total = (counts: (typeof afterObliterate)[number]) =>
      Object.values(counts).reduce((sum, count) => sum + count, 0);
    check(
      afterObliterate.every((counts) => total(counts) === 0),
      'obliterateAllAsync leaves no jobs in either queue',
      JSON.stringify(afterObliterate)
    );
  } catch (error) {
    console.error('   [FAIL] unexpected contract error:', error);
    failed++;
  } finally {
    for (const worker of workers) {
      await worker.close(true).catch(() => undefined);
    }
    try {
      await clean();
      alpha.close();
      beta.close();
      directAlpha.close();
    } catch (error) {
      console.error('   [FAIL] cleanup error:', error);
      failed++;
    }
  }

  console.log(`\n=== Summary ===\nPassed: ${passed}\nFailed: ${failed}`);
  return { passed, failed };
}
