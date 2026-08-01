import { Queue, Worker } from '../../src/client';

type Mode = 'embedded' | 'tcp';
type Payload = { value: number };

interface ContractResult {
  passed: number;
  failed: number;
}

async function eventually<T>(
  read: () => T | Promise<T>,
  accept: (value: T) => boolean,
  timeoutMs = 12_000
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let value = await read();
  while (!accept(value) && Date.now() < deadline) {
    await Bun.sleep(50);
    value = await read();
  }
  return value;
}

export async function runTimeoutContract(mode: Mode): Promise<ContractResult> {
  const tcpPort = Number.parseInt(process.env.TCP_PORT ?? '16789', 10);
  const queueName = `timeout-contract-${mode}-${crypto.randomUUID()}`;
  const connection = { hostname: '127.0.0.1', port: tcpPort };
  const queue = new Queue<Payload>(
    queueName,
    mode === 'embedded' ? { embedded: true } : { connection }
  );
  let worker: Worker<Payload> | null = null;
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

  console.log(`=== Processing-timeout contract (${mode}) ===\n`);

  try {
    await queue.obliterateAsync();
    const job = await queue.add('slow', { value: 1 }, { attempts: 1, timeout: 100 });
    let started = 0;
    worker = new Worker<Payload>(
      queueName,
      async () => {
        started++;
        await new Promise<never>(() => undefined);
      },
      mode === 'embedded'
        ? { embedded: true, concurrency: 1, useLocks: false }
        : { connection, concurrency: 1, useLocks: false }
    );
    worker.on('error', () => undefined);

    const activeState = await eventually(
      () => queue.getJobState(job.id),
      (state) => state === 'active'
    );
    const entries = await eventually(
      () => queue.getDlqAsync({ reason: 'timeout' }),
      (current) => current.length === 1
    );
    const finalState = await queue.getJobState(job.id);
    const stats = await queue.getDlqStatsAsync();

    check(started === 1 && activeState === 'active', 'the worker acquires the job exactly once');
    check(finalState === 'failed', 'the timed-out job reaches the failed state', finalState);
    check(entries.length === 1, 'exactly one timeout entry is created', String(entries.length));
    check(
      entries[0]?.reason === 'timeout' &&
        entries[0]?.attempts.length === 1 &&
        entries[0]?.attempts[0]?.reason === 'timeout',
      'the terminal cause and attempt history are both timeout',
      JSON.stringify(entries[0])
    );
    check(
      stats.total === 1 && stats.byReason.timeout === 1,
      'DLQ counters classify the timeout exactly once',
      JSON.stringify(stats)
    );
  } catch (error) {
    console.error('   [FAIL] unexpected contract error:', error);
    failed++;
  } finally {
    try {
      await worker?.close(true);
      await queue.obliterateAsync();
      await queue.close();
    } catch (error) {
      console.error('   [FAIL] cleanup error:', error);
      failed++;
    }
  }

  console.log(`\n=== Summary ===\nPassed: ${passed}\nFailed: ${failed}`);
  return { passed, failed };
}
