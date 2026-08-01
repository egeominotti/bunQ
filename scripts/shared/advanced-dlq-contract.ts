import { Queue, Worker } from '../../src/client';
import type { DlqEntry, FailureReason } from '../../src/client/types';

type Mode = 'embedded' | 'tcp';
type Payload = { value: number };

interface ContractResult {
  passed: number;
  failed: number;
}

const FAILURE_REASON = 'max_attempts_exceeded' as FailureReason;

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

export async function runAdvancedDlqContract(mode: Mode): Promise<ContractResult> {
  const tcpPort = Number.parseInt(process.env.TCP_PORT ?? '16789', 10);
  const queueName = `advanced-dlq-${mode}-${crypto.randomUUID()}`;
  const queue = new Queue<Payload>(queueName, {
    ...(mode === 'embedded'
      ? { embedded: true }
      : { connection: { hostname: '127.0.0.1', port: tcpPort } }),
    defaultJobOptions: { attempts: 1, backoff: 0 },
  });
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
    await queue.obliterateAsync();
    await queue.setDlqConfigAsync({
      autoRetry: false,
      autoRetryInterval: 60_000,
      maxAutoRetries: 3,
      maxAge: null,
      maxEntries: 100,
    });
  };

  const failJobs = async (
    values: number[],
    attempts = 1
  ): Promise<{ processed: number; entries: DlqEntry<Payload>[] }> => {
    for (const value of values) {
      await queue.add('fail', { value }, { attempts, backoff: 0 });
    }

    let processed = 0;
    const worker = new Worker<Payload>(
      queueName,
      () => {
        processed++;
        throw new Error(`failure-${processed}`);
      },
      mode === 'embedded'
        ? { embedded: true, concurrency: 1 }
        : { connection: { hostname: '127.0.0.1', port: tcpPort }, concurrency: 1 }
    );
    worker.on('error', () => undefined);

    await eventually(
      () => processed,
      (count) => count === values.length * attempts
    );
    const entries = await eventually(
      () => queue.getDlqAsync(),
      (current) => current.length === values.length
    );
    await worker.close();
    return { processed, entries };
  };

  console.log(`=== Advanced DLQ contract (${mode}) ===\n`);

  try {
    console.log('1. Configuration is applied by the selected broker...');
    await clean();
    await queue.setDlqConfigAsync({
      autoRetry: true,
      autoRetryInterval: 5_000,
      maxAutoRetries: 4,
      maxAge: 120_000,
      maxEntries: 17,
    });
    const config = await queue.getDlqConfigAsync();
    check(
      config.autoRetry === true &&
        config.autoRetryInterval === 5_000 &&
        config.maxAutoRetries === 4 &&
        config.maxAge === 120_000 &&
        config.maxEntries === 17,
      'set/get DLQ config round-trips every option',
      JSON.stringify(config)
    );

    console.log('\n2. Entries, filters, pagination, stats, and attempt history...');
    await clean();
    const history = await failJobs([11], 3);
    const all = history.entries;
    const filtered = await queue.getDlqAsync({ reason: FAILURE_REASON });
    const paged = await queue.getDlqAsync({ limit: 1, offset: 0 });
    const stats = await queue.getDlqStatsAsync();
    check(history.processed === 3, 'the job consumed all configured attempts');
    check(
      all.length === 1 && all[0].attempts.length === 3,
      'the DLQ entry preserves the complete attempt history',
      JSON.stringify(all[0]?.attempts)
    );
    check(
      filtered.length === 1 && filtered[0].reason === FAILURE_REASON,
      'reason filtering is applied by the broker'
    );
    check(paged.length === 1, 'DLQ pagination returns the requested page');
    check(
      stats.total === 1 && stats.byReason[FAILURE_REASON] === 1,
      'DLQ statistics reflect authoritative broker state',
      JSON.stringify(stats)
    );

    console.log('\n3. Selective and id-based retry remove exactly the selected entries...');
    await clean();
    await failJobs([21, 22]);
    const retriedByFilter = await queue.retryDlqByFilterAsync({ reason: FAILURE_REASON, limit: 1 });
    check(retriedByFilter === 1, 'filtered retry honours its limit', String(retriedByFilter));
    let remaining = await queue.getDlqAsync();
    check(remaining.length === 1, 'filtered retry leaves unselected entries in the DLQ');
    const retriedById = await queue.retryDlqAsync(remaining[0]?.job.id);
    check(retriedById === 1, 'id-based retry returns the exact applied count', String(retriedById));
    remaining = await queue.getDlqAsync();
    check(remaining.length === 0, 'all retried entries are removed from the DLQ');

    console.log('\n4. maxEntries evicts the oldest terminal jobs...');
    await clean();
    await queue.setDlqConfigAsync({ maxEntries: 2 });
    const bounded = await failJobs([31, 32, 33]);
    check(
      bounded.entries.length === 2 &&
        bounded.entries.map((entry) => entry.job.data.value).join(',') === '32,33',
      'maxEntries keeps only the two newest entries',
      bounded.entries.map((entry) => entry.job.data.value).join(',')
    );

    console.log('\n5. maxAge and purge expose exact state and counts...');
    await clean();
    await queue.setDlqConfigAsync({ maxAge: 100 });
    await failJobs([41, 42]);
    const expired = await eventually(
      () => queue.getDlqAsync({ expired: true }),
      (entries) => entries.length === 2
    );
    const expiredStats = await queue.getDlqStatsAsync();
    check(expired.length === 2 && expiredStats.expired === 2, 'maxAge marks both entries expired');
    const purged = await queue.purgeDlqAsync();
    check(purged === 2, 'purge returns the exact number removed', String(purged));
    check((await queue.getDlqAsync()).length === 0, 'purge leaves the DLQ empty');

    console.log('\n6. The background task performs one bounded automatic retry...');
    await clean();
    await queue.setDlqConfigAsync({
      autoRetry: true,
      autoRetryInterval: 1,
      maxAutoRetries: 1,
      maxAge: null,
    });
    await queue.add('auto-retry', { value: 51 }, { attempts: 1, backoff: 0 });
    let processingAttempts = 0;
    const autoRetryWorker = new Worker<Payload>(
      queueName,
      () => {
        processingAttempts++;
        throw new Error(`automatic-failure-${processingAttempts}`);
      },
      mode === 'embedded'
        ? { embedded: true, concurrency: 1 }
        : { connection: { hostname: '127.0.0.1', port: tcpPort }, concurrency: 1 }
    );
    autoRetryWorker.on('error', () => undefined);
    const automatic = await eventually(
      async () => ({ processingAttempts, entries: await queue.getDlqAsync() }),
      (current) =>
        current.processingAttempts === 2 &&
        current.entries.length === 1 &&
        current.entries[0]?.retryCount === 1,
      75_000
    );
    await autoRetryWorker.close();
    check(
      automatic.processingAttempts === 2,
      'the maintenance loop redelivers the terminal job exactly once',
      String(automatic.processingAttempts)
    );
    check(
      automatic.entries.length === 1 &&
        automatic.entries[0]?.retryCount === 1 &&
        automatic.entries[0]?.nextRetryAt === null &&
        automatic.entries[0]?.attempts.map((attempt) => attempt.error).join(',') ===
          'automatic-failure-1,automatic-failure-2',
      'the exhausted automatic retry retains its full history and stops',
      JSON.stringify(automatic.entries[0])
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
