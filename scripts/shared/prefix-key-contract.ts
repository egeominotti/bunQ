import { Queue, Worker } from '../../src/client';

type Mode = 'embedded' | 'tcp';
type Payload = { environment: string; value: number };

interface ContractResult {
  passed: number;
  failed: number;
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

export async function runPrefixKeyContract(mode: Mode): Promise<ContractResult> {
  const tcpPort = Number.parseInt(process.env.TCP_PORT ?? '16789', 10);
  const logicalName = `prefix-parity-${mode}-${crypto.randomUUID()}`;
  const firstPrefix = 'first:';
  const secondPrefix = 'second:';
  const connection = { hostname: '127.0.0.1', port: tcpPort };
  const transport = mode === 'embedded' ? { embedded: true } : { connection };
  const first = new Queue<Payload>(logicalName, { ...transport, prefixKey: firstPrefix });
  const second = new Queue<Payload>(logicalName, { ...transport, prefixKey: secondPrefix });
  const directFirst = new Queue<Payload>(`${firstPrefix}${logicalName}`, transport);
  let worker: Worker<Payload, unknown> | null = null;
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
    await first.resumeAsync();
    await second.resumeAsync();
    await first.obliterateAsync();
    await second.obliterateAsync();
  };

  console.log(`=== prefixKey namespace contract (${mode}) ===\n`);

  try {
    await clean();
    console.log('1. Logical names and physical namespace isolation...');
    await first.addBulk([
      { name: 'one', data: { environment: 'first', value: 1 } },
      { name: 'two', data: { environment: 'first', value: 2 } },
    ]);
    await second.add('other', { environment: 'second', value: 3 });
    const [firstCounts, secondCounts, directCounts] = await Promise.all([
      first.getJobCountsAsync(),
      second.getJobCountsAsync(),
      directFirst.getJobCountsAsync(),
    ]);
    check(
      first.name === logicalName && second.name === logicalName,
      'prefixKey preserves the public logical queue name'
    );
    check(
      firstCounts.waiting === 2 &&
        secondCounts.waiting === 1 &&
        directCounts.waiting === firstCounts.waiting,
      'different prefixes isolate jobs and map to the expected physical queue',
      JSON.stringify({ firstCounts, secondCounts, directCounts })
    );

    console.log('\n2. Workers consume only the matching namespace...');
    const processed: string[] = [];
    worker = new Worker<Payload>(
      logicalName,
      (job) => processed.push(job.data.environment),
      mode === 'embedded'
        ? { embedded: true, prefixKey: firstPrefix, concurrency: 1 }
        : { connection, prefixKey: firstPrefix, concurrency: 1 }
    );
    await eventually(() => processed.length, 2);
    await worker.close();
    worker = null;
    const untouched = await second.getJobCountsAsync();
    check(processed.join(',') === 'first,first', 'the prefixed worker processes both jobs once');
    check(untouched.waiting === 1, 'the other namespace remains untouched');

    console.log('\n3. Queue controls remain namespace-scoped...');
    await first.pauseAsync();
    const paused = await Promise.all([first.isPausedAsync(), second.isPausedAsync()]);
    check(paused[0] === true && paused[1] === false, 'pausing one prefix does not pause another');
    await first.resumeAsync();
  } catch (error) {
    console.error('   [FAIL] unexpected contract error:', error);
    failed++;
  } finally {
    await worker?.close(true).catch(() => undefined);
    try {
      await clean();
      await first.close();
      await second.close();
      await directFirst.close();
    } catch (error) {
      console.error('   [FAIL] cleanup error:', error);
      failed++;
    }
  }

  console.log(`\n=== Summary ===\nPassed: ${passed}\nFailed: ${failed}`);
  return { passed, failed };
}
