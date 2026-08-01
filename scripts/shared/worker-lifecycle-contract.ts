import { Queue, Worker } from '../../src/client';

type Mode = 'embedded' | 'tcp';
type Payload = { value: number };

interface ContractResult {
  passed: number;
  failed: number;
}

interface Deferred {
  promise: Promise<void>;
  resolve: () => void;
}

function deferred(): Deferred {
  let resolve = () => undefined;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function eventually(read: () => number, expected: number): Promise<number> {
  const deadline = Date.now() + 10_000;
  let value = read();
  while (value !== expected && Date.now() < deadline) {
    await Bun.sleep(25);
    value = read();
  }
  return value;
}

export async function runWorkerLifecycleContract(mode: Mode): Promise<ContractResult> {
  const tcpPort = Number.parseInt(process.env.TCP_PORT ?? '16789', 10);
  const connection = { hostname: '127.0.0.1', port: tcpPort };
  const transport = mode === 'embedded' ? { embedded: true } : { connection };
  const firstName = `worker-lifecycle-control-${mode}-${crypto.randomUUID()}`;
  const closeName = `worker-lifecycle-close-${mode}-${crypto.randomUUID()}`;
  const firstQueue = new Queue<Payload>(firstName, transport);
  const closeQueue = new Queue<Payload>(closeName, transport);
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

  const workerOptions =
    mode === 'embedded'
      ? { embedded: true, autorun: false, concurrency: 1, batchSize: 1 }
      : { connection, autorun: false, concurrency: 1, batchSize: 1 };

  console.log(`=== Worker lifecycle contract (${mode}) ===\n`);

  try {
    await firstQueue.obliterateAsync();
    await closeQueue.obliterateAsync();

    console.log('1. autorun, run, pause, and resume control delivery exactly...');
    const firstRelease = deferred();
    const started: number[] = [];
    const completed: number[] = [];
    const worker = new Worker<Payload>(
      firstName,
      async (job) => {
        started.push(job.data.value);
        if (job.data.value === 1) await firstRelease.promise;
        completed.push(job.data.value);
        return job.data.value;
      },
      workerOptions
    );
    workers.push(worker);
    await firstQueue.addBulk([1, 2, 3].map((value) => ({ name: 'lifecycle', data: { value } })));
    await Bun.sleep(200);
    check(started.length === 0, 'autorun:false does not pull work');
    worker.run();
    await eventually(() => started.length, 1);
    worker.pause();
    firstRelease.resolve();
    await eventually(() => completed.length, 1);
    await Bun.sleep(200);
    const pausedCounts = await firstQueue.getJobCountsAsync();
    check(
      started.join(',') === '1' && completed.join(',') === '1' && pausedCounts.waiting === 2,
      'pause lets the active job finish without pulling the two waiting jobs',
      JSON.stringify({ started, completed, pausedCounts })
    );
    worker.resume();
    await eventually(() => completed.length, 3);
    await worker.close();
    workers.splice(workers.indexOf(worker), 1);
    check(completed.join(',') === '1,2,3', 'resume completes every remaining job exactly once');

    console.log('\n2. Graceful close waits for the active processor and emits closed once...');
    const closeRelease = deferred();
    let closeStarted = 0;
    let closedEvents = 0;
    const closingWorker = new Worker<Payload>(
      closeName,
      async () => {
        closeStarted++;
        await closeRelease.promise;
        return 'done';
      },
      { ...workerOptions, autorun: true }
    );
    workers.push(closingWorker);
    closingWorker.on('closed', () => closedEvents++);
    const closingJob = await closeQueue.add('close', { value: 1 });
    await eventually(() => closeStarted, 1);
    let closeResolved = false;
    const closePromise = closingWorker.close().then(() => {
      closeResolved = true;
    });
    await Bun.sleep(150);
    check(!closeResolved, 'close() remains pending while a processor is active');
    closeRelease.resolve();
    await closePromise;
    workers.splice(workers.indexOf(closingWorker), 1);
    const finalState = await closeQueue.getJobState(closingJob.id);
    check(
      closeResolved && closedEvents === 1 && finalState === 'completed',
      'graceful close acknowledges the job and emits one closed event',
      JSON.stringify({ closeResolved, closedEvents, finalState })
    );
  } catch (error) {
    console.error('   [FAIL] unexpected contract error:', error);
    failed++;
  } finally {
    for (const worker of workers) await worker.close(true).catch(() => undefined);
    try {
      await firstQueue.obliterateAsync();
      await closeQueue.obliterateAsync();
      await firstQueue.close();
      await closeQueue.close();
    } catch (error) {
      console.error('   [FAIL] cleanup error:', error);
      failed++;
    }
  }

  console.log(`\n=== Summary ===\nPassed: ${passed}\nFailed: ${failed}`);
  return { passed, failed };
}
