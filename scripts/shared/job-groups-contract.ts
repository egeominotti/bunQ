import { Queue, Worker } from '../../src/client';

type Mode = 'embedded' | 'tcp';
type Payload = { label: string };

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
    await Bun.sleep(20);
    value = await read();
  }
  return value;
}

export async function runJobGroupsContract(mode: Mode): Promise<ContractResult> {
  const tcpPort = Number.parseInt(process.env.TCP_PORT ?? '16789', 10);
  const connection = { hostname: '127.0.0.1', port: tcpPort };
  const queueOptions = mode === 'embedded' ? { embedded: true } : { connection };
  const workerOptions = mode === 'embedded' ? { embedded: true } : { connection };
  const prefix = `job-groups-${mode}-${crypto.randomUUID()}`;
  const ordering = new Queue<Payload>(`${prefix}-ordering`, queueOptions);
  const concurrency = new Queue<Payload>(`${prefix}-concurrency`, queueOptions);
  const rate = new Queue<Payload>(`${prefix}-rate`, queueOptions);
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

  const rejects = async (operation: () => Promise<unknown>): Promise<boolean> => {
    try {
      await operation();
      return false;
    } catch {
      return true;
    }
  };

  console.log(`=== Job-groups contract (${mode}) ===\n`);

  try {
    console.log('1. FIFO lanes rotate fairly and plain jobs keep precedence...');
    await ordering.obliterateAsync();
    await ordering.pauseAsync();
    const timestamp = Date.now();
    const orderingJobs = await ordering.addBulk([
      { name: 'plain', data: { label: 'plain' }, opts: { jobId: 'plain', timestamp } },
      {
        name: 'A1',
        data: { label: 'A1' },
        opts: { group: { id: 'A' }, jobId: 'z-first', timestamp },
      },
      {
        name: 'A2',
        data: { label: 'A2' },
        opts: { group: { id: 'A' }, jobId: 'a-second', timestamp },
      },
      {
        name: 'B1',
        data: { label: 'B1' },
        opts: { group: { id: 'B' }, jobId: 'y-first', timestamp },
      },
      {
        name: 'B2',
        data: { label: 'B2' },
        opts: { group: { id: 'B' }, jobId: 'b-second', timestamp },
      },
    ]);
    const aDepth = await ordering.getGroupJobsCount('A');
    const totalDepth = await ordering.getGroupsJobsCount();
    check(aDepth === 2 && totalDepth === 4, 'group depth excludes the ungrouped job');

    const order: string[] = [];
    const orderingWorker = new Worker<Payload, string>(
      ordering.name,
      (job) => {
        order.push(job.data.label);
        return job.data.label;
      },
      { ...workerOptions, batchSize: 1, concurrency: 1 }
    );
    workers.push(orderingWorker);
    await orderingWorker.waitUntilReady();
    await ordering.resumeAsync();
    await eventually(
      () => order.length,
      (count) => count === 5
    );
    check(
      order.join(',') === 'plain,A1,B1,A2,B2',
      'claims are plain-first, round-robin, and insertion-FIFO despite reverse IDs',
      order.join(',')
    );
    await eventually(
      async () => await Promise.all(orderingJobs.map((job) => ordering.getJobState(job.id))),
      (states) => states.every((state) => state === 'completed')
    );
    await orderingWorker.close();
    workers.splice(workers.indexOf(orderingWorker), 1);

    console.log('\n2. Group concurrency is broker-authoritative...');
    await concurrency.obliterateAsync();
    await concurrency.pauseAsync();
    await concurrency.setGroupConcurrency('A', 1);
    check(
      (await concurrency.getGroupConcurrency('A')) === 1,
      'a local concurrency override round-trips'
    );
    const concurrencyJobs = await concurrency.addBulk(
      ['A1', 'A2', 'B1', 'B2'].map((label) => ({
        name: label,
        data: { label },
        opts: { group: { id: label[0] } },
      }))
    );
    const active = new Map<string, number>();
    const peak = new Map<string, number>();
    let concurrencyDone = 0;
    const concurrencyWorker = new Worker<Payload, string>(
      concurrency.name,
      async (job) => {
        const groupId = String(job.opts.group?.id);
        const current = (active.get(groupId) ?? 0) + 1;
        active.set(groupId, current);
        peak.set(groupId, Math.max(peak.get(groupId) ?? 0, current));
        await Bun.sleep(100);
        active.set(groupId, current - 1);
        concurrencyDone++;
        return job.data.label;
      },
      { ...workerOptions, batchSize: 4, concurrency: 4, group: { concurrency: 2 } }
    );
    workers.push(concurrencyWorker);
    await concurrencyWorker.waitUntilReady();
    await concurrency.resumeAsync();
    await eventually(
      () => concurrencyDone,
      (count) => count === 4
    );
    check(
      peak.get('A') === 1 && peak.get('B') === 2,
      'the override caps A at one while the worker default lets B reach two',
      JSON.stringify(Object.fromEntries(peak))
    );
    await eventually(
      async () => await Promise.all(concurrencyJobs.map((job) => concurrency.getJobState(job.id))),
      (states) => states.every((state) => state === 'completed')
    );
    await concurrencyWorker.close();
    workers.splice(workers.indexOf(concurrencyWorker), 1);
    check(
      (await concurrency.removeGroupConcurrency('A')) === 1 &&
        (await concurrency.getGroupConcurrency('A')) === null,
      'removing a concurrency override is observable'
    );

    console.log('\n3. Per-group rate windows expose backpressure TTL...');
    await rate.obliterateAsync();
    await rate.pauseAsync();
    await rate.setGroupRateLimit('A', 1, 500);
    check(
      JSON.stringify(await rate.getGroupRateLimit('A')) ===
        JSON.stringify({ max: 1, duration: 500 }),
      'a local rate override round-trips'
    );
    const rateJobs = await rate.addBulk([
      { name: 'A1', data: { label: 'A1' }, opts: { group: { id: 'A' } } },
      { name: 'A2', data: { label: 'A2' }, opts: { group: { id: 'A' } } },
    ]);
    const starts: number[] = [];
    const rateWorker = new Worker<Payload, string>(
      rate.name,
      (job) => {
        starts.push(Date.now());
        return job.data.label;
      },
      {
        ...workerOptions,
        batchSize: 2,
        concurrency: 2,
        group: { limit: { max: 2, duration: 500 } },
      }
    );
    workers.push(rateWorker);
    await rateWorker.waitUntilReady();
    await rate.resumeAsync();
    await eventually(
      () => starts.length,
      (count) => count === 1
    );
    const ttl = await rate.getGroupRateLimitTtl('A', 1);
    await eventually(
      () => starts.length,
      (count) => count === 2
    );
    check(ttl > 0 && ttl <= 500, 'an exhausted group exposes a positive TTL', String(ttl));
    check(
      starts.length === 2 && starts[1]! - starts[0]! >= 350,
      'the second group claim waits for the fixed window',
      JSON.stringify(starts)
    );
    await eventually(
      async () => await Promise.all(rateJobs.map((job) => rate.getJobState(job.id))),
      (states) => states.every((state) => state === 'completed')
    );
    await rateWorker.close();
    workers.splice(workers.indexOf(rateWorker), 1);
    check(
      (await rate.removeGroupRateLimit('A')) === 1 && (await rate.getGroupRateLimit('A')) === null,
      'removing a rate override is observable'
    );

    console.log('\n4. Invalid group identifiers fail before mutation...');
    const invalid = await Promise.all([
      rejects(() => ordering.add('invalid', { label: 'empty' }, { group: { id: '' } })),
      rejects(() => ordering.add('invalid', { label: 'nul' }, { group: { id: 'A\0B' } })),
      rejects(() => ordering.add('invalid', { label: 'fraction' }, { group: { id: 1.5 } })),
    ]);
    check(invalid.every(Boolean), 'empty, NUL, and non-safe-integer group IDs are rejected');
  } catch (error) {
    console.error('   [FAIL] unexpected contract error:', error);
    failed++;
  } finally {
    for (const worker of workers) await worker.close(true).catch(() => undefined);
    for (const queue of [ordering, concurrency, rate]) {
      await queue.obliterateAsync().catch(() => undefined);
      queue.close();
    }
  }

  console.log(`\n=== Summary ===\nPassed: ${passed}\nFailed: ${failed}`);
  return { passed, failed };
}
