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
  timeoutMs = 15_000
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let value = await read();
  while (!accept(value) && Date.now() < deadline) {
    await Bun.sleep(50);
    value = await read();
  }
  return value;
}

export async function runStallDetectionContract(mode: Mode): Promise<ContractResult> {
  const tcpPort = Number.parseInt(process.env.TCP_PORT ?? '16789', 10);
  const queueName = `stall-contract-${mode}-${crypto.randomUUID()}`;
  const connection = { hostname: '127.0.0.1', port: tcpPort };
  const queue = new Queue<Payload>(
    queueName,
    mode === 'embedded' ? { embedded: true } : { connection }
  );
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

  const options = (heartbeatInterval: number) =>
    mode === 'embedded'
      ? { embedded: true, concurrency: 1, heartbeatInterval, useLocks: true }
      : { connection, concurrency: 1, heartbeatInterval, useLocks: true };

  const clean = async (): Promise<void> => {
    await queue.obliterateAsync();
    await queue.setStallConfigAsync({
      enabled: true,
      stallInterval: 30_000,
      maxStalls: 3,
      gracePeriod: 5_000,
    });
  };

  console.log(`=== Stall detection contract (${mode}) ===\n`);

  try {
    console.log('1. Configuration is authoritative...');
    await clean();
    await queue.setStallConfigAsync({
      enabled: false,
      stallInterval: 1_234,
      maxStalls: 7,
      gracePeriod: 456,
    });
    const config = await queue.getStallConfigAsync();
    check(
      config.enabled === false &&
        config.stallInterval === 1_234 &&
        config.maxStalls === 7 &&
        config.gracePeriod === 456,
      'set/get stall config round-trips every field',
      JSON.stringify(config)
    );

    console.log('\n2. A missing heartbeat reaches the DLQ at maxStalls...');
    await clean();
    await queue.setStallConfigAsync({
      enabled: true,
      stallInterval: 200,
      maxStalls: 1,
      gracePeriod: 50,
    });
    const terminalJob = await queue.add('terminal-stall', { value: 1 }, { attempts: 10 });
    let terminalStarted = false;
    const terminalWorker = new Worker<Payload>(
      queueName,
      async () => {
        terminalStarted = true;
        await new Promise<never>(() => undefined);
      },
      options(0)
    );
    terminalWorker.on('error', () => undefined);
    const stalledEvents: Array<{ jobId: string; reason: string }> = [];
    terminalWorker.on('stalled', (jobId, reason) => stalledEvents.push({ jobId, reason }));
    const activeState = await eventually(
      () => queue.getJobState(terminalJob.id),
      (state) => state === 'active'
    );
    const stalledEntries = await eventually(
      () => queue.getDlqAsync({ reason: 'stalled' }),
      (entries) => entries.length === 1
    );
    const terminalState = await queue.getJobState(terminalJob.id);
    const stats = await queue.getDlqStatsAsync();
    check(
      terminalStarted && activeState === 'active',
      'the worker acquired the job before recovery'
    );
    check(
      stalledEntries.length === 1 && terminalState === 'failed',
      'the first stall reaches the DLQ when maxStalls is one',
      `entries=${stalledEntries.length}, state=${terminalState}`
    );
    check(stats.byReason.stalled === 1, 'DLQ statistics classify the stall');
    check(
      stalledEvents.some((event) => event.jobId === terminalJob.id && event.reason === 'active'),
      'Worker emits the broker stall notification with the exact job id'
    );
    await terminalWorker.close(true);

    console.log('\n3. A recoverable stall is redelivered to another worker...');
    await clean();
    await queue.setStallConfigAsync({
      enabled: true,
      stallInterval: 200,
      maxStalls: 2,
      gracePeriod: 50,
    });
    const recoverableJob = await queue.add('recoverable-stall', { value: 2 }, { attempts: 10 });
    const stuckWorker = new Worker<Payload>(
      queueName,
      async () => await new Promise<never>(() => undefined),
      options(0)
    );
    stuckWorker.on('error', () => undefined);
    await eventually(
      () => queue.getJobState(recoverableJob.id),
      (state) => state === 'active'
    );
    const recoveredWaitingState = await eventually(
      () => queue.getJobState(recoverableJob.id),
      (state) => state === 'waiting' || state === 'prioritized'
    );
    await stuckWorker.close(true);

    let recoveredCount = 0;
    const recoveryWorker = new Worker<Payload>(
      queueName,
      (job) => {
        recoveredCount++;
        return { value: job.data.value };
      },
      options(50)
    );
    recoveryWorker.on('error', () => undefined);
    const completedState = await eventually(
      () => queue.getJobState(recoverableJob.id),
      (state) => state === 'completed'
    );
    await recoveryWorker.close();
    check(
      (recoveredWaitingState === 'waiting' || recoveredWaitingState === 'prioritized') &&
        completedState === 'completed' &&
        recoveredCount === 1,
      'the stalled job is requeued and completed exactly once',
      `waiting=${recoveredWaitingState}, completed=${completedState}, count=${recoveredCount}`
    );
    check((await queue.getDlqAsync()).length === 0, 'a recovered stall does not enter the DLQ');

    console.log('\n4. Heartbeats protect work longer than the stall window...');
    await clean();
    await queue.setStallConfigAsync({
      enabled: true,
      stallInterval: 200,
      maxStalls: 1,
      gracePeriod: 50,
    });
    const heartbeatJob = await queue.add('heartbeat-protected', { value: 3 }, { attempts: 1 });
    const heartbeatWorker = new Worker<Payload>(
      queueName,
      async (job) => {
        await Bun.sleep(6_500);
        return { value: job.data.value };
      },
      options(50)
    );
    heartbeatWorker.on('error', () => undefined);
    const heartbeatState = await eventually(
      () => queue.getJobState(heartbeatJob.id),
      (state) => state === 'completed',
      12_000
    );
    await heartbeatWorker.close();
    check(heartbeatState === 'completed', 'a heartbeating long job completes normally');
    check(
      (await queue.getDlqAsync()).length === 0,
      'heartbeating work is never classified stalled'
    );
  } catch (error) {
    console.error('   [FAIL] unexpected contract error:', error);
    failed++;
  } finally {
    try {
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
