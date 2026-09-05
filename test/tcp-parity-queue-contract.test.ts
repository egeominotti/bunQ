import { expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer } from '../src/infrastructure/server/tcp';
import { Queue } from '../src/client/queue/queue';

// Test the distributable package: source-only equivalence cannot prove that
// build boundaries, exports, and portable transport preserve the contract.
const distribution = await import('../sdk/typescript/dist/index.js');
const NetworkQueue = distribution.Queue as unknown as typeof Queue;

test('packaged Queue shares native TCP query, group, scheduler and DLQ contracts', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'bunqueue-queue-contract-'));
  const manager = new QueueManager({ dataPath: join(directory, 'broker.db') });
  const server = createTcpServer(manager, { hostname: '127.0.0.1', port: 0 });
  const name = `contract-${crypto.randomUUID()}`;
  const prefixKey = 'tenant:';
  const options = {
    embedded: false,
    prefixKey,
    defaultJobOptions: { priority: 3, attempts: 1 },
    autoBatch: { enabled: false },
    connection: { host: '127.0.0.1', port: server.server.port, poolSize: 1 },
  };
  const native = new Queue(name, options);
  const network = new NetworkQueue(name, options);
  try {
    const added = await network.add(
      'first',
      { value: 7 },
      {
        deduplication: { id: 'unique' },
        group: { id: 'customer' },
      }
    );
    expect(added.name).toBe('first');
    expect(added.priority).toBe(3);
    expect(added.opts.attempts).toBe(1);
    expect((await native.getJob(added.id))?.data).toEqual({ value: 7 });
    expect(await network.getDeduplicationJobId('unique')).toBe(added.id);
    expect(await network.getJobCountsAsync()).toEqual(await native.getJobCountsAsync());
    expect(manager.count(name)).toBe(0);
    expect(manager.count(prefixKey + name)).toBe(1);

    const bulk = await network.addBulk([
      { name: 'second', data: { value: 8 }, opts: { priority: 0 } },
      { name: 'third', data: { value: 9 }, opts: { delay: 60_000 } },
    ]);
    const query = { state: ['waiting', 'prioritized', 'delayed'], start: 1, end: 3, asc: true };
    expect((await network.getJobsAsync(query)).map((job) => job.id)).toEqual(
      (await native.getJobsAsync(query)).map((job) => job.id)
    );
    expect(await network.getCountsPerPriorityAsync()).toEqual(
      await native.getCountsPerPriorityAsync()
    );
    expect(await network.getGroupJobsCount('customer')).toBe(1);
    await network.setGroupConcurrency('customer', 2);
    expect(await native.getGroupConcurrency('customer')).toBe(2);
    await network.setGroupRateLimit('customer', 5, 2_000);
    expect(await native.getGroupRateLimit('customer')).toEqual({ max: 5, duration: 2_000 });
    expect(await network.pauseGroup('customer')).toBe(true);
    expect(await native.isGroupPaused('customer')).toBe(true);
    expect(await network.resumeGroup('customer')).toBe(true);
    expect(await network.removeGroupConcurrency('customer')).toBe(1);
    expect(await network.removeGroupRateLimit('customer')).toBe(1);

    await network.setGlobalConcurrencyAsync(2);
    expect(await native.getGlobalConcurrency()).toBe(2);
    await network.setGlobalRateLimitAsync(5, 2_000);
    expect(await native.getGlobalRateLimit()).toEqual({ max: 5, duration: 2_000 });
    expect(await network.getRateLimitTtl()).toBe(await native.getRateLimitTtl());
    await network.removeGlobalConcurrencyAsync();
    await network.removeGlobalRateLimitAsync();

    expect(await network.addJobLog(added.id, 'first log')).toBe(1);
    expect(await network.getJobLogs(added.id)).toEqual(await native.getJobLogs(added.id));
    expect(await network.getJobLogs(added.id)).toEqual({ logs: ['[info] first log'], count: 1 });
    const scheduler = await network.upsertJobScheduler(
      'schedule',
      { every: 3_600_000 },
      {
        name: 'scheduled',
        data: { value: 10 },
      }
    );
    expect(scheduler).toEqual(await native.getJobScheduler('schedule'));
    expect(await network.getJobSchedulers(0, 0, true)).toEqual(
      await native.getJobSchedulers(0, 0, true)
    );
    expect(await network.removeJobScheduler('schedule')).toBe(true);

    for (const job of bulk) await network.removeAsync(job.id);
    expect((await manager.pull(prefixKey + name))?.id).toBe(added.id);
    await manager.fail(added.id, 'contract failure');
    const entries = await network.getDlqAsync({ limit: 1 });
    expect(entries.length).toBe(1);
    expect(entries[0].job.id).toBe(added.id);
    expect(entries[0].error).toBe('contract failure');
    expect(await network.getDlqStatsAsync()).toEqual(await native.getDlqStatsAsync());
    expect(await network.getMetrics('failed', 0, 0)).toEqual(
      await native.getMetrics('failed', 0, 0)
    );
    expect(await network.removeDlqJob(added.id)).toBe(true);
    expect(await native.getDlqStatsAsync()).toMatchObject({ total: 0 });
  } finally {
    network.close();
    native.close();
    server.stop();
    manager.shutdown();
    rmSync(directory, { recursive: true, force: true });
  }
}, 20_000);
