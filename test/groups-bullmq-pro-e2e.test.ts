import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { Queue, shutdownManager, Worker } from '../src/client';
import { TcpClient } from '../src/client/tcp/client';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';

interface Harness {
  manager: QueueManager;
  server: TcpServer;
  port: number;
  queues: Queue<unknown>[];
  workers: Worker<unknown, unknown>[];
  clients: TcpClient[];
}

let harness: Harness | null = null;
const temporaryDirectories: string[] = [];

function start(): Harness {
  const manager = new QueueManager();
  const server = createTcpServer(manager, { hostname: '127.0.0.1', port: 0 });
  harness = { manager, server, port: server.server.port, queues: [], workers: [], clients: [] };
  return harness;
}

function connection(current: Harness) {
  return { host: '127.0.0.1', port: current.port, pingInterval: 0, poolSize: 1 };
}

function queue<T>(current: Harness, name: string): Queue<T> {
  const instance = new Queue<T>(name, {
    autoBatch: { enabled: false },
    connection: connection(current),
    embedded: false,
  });
  current.queues.push(instance as Queue<unknown>);
  return instance;
}

afterEach(async () => {
  if (harness) {
    await Promise.allSettled(harness.workers.map((worker) => worker.close()));
    for (const queue of harness.queues) queue.close();
    for (const client of harness.clients) client.close();
    harness.server.stop();
    harness.manager.shutdown();
    harness = null;
  }
  shutdownManager();
  for (const directory of temporaryDirectories.splice(0)) {
    rmSync(directory, { force: true, recursive: true });
  }
});

describe('BullMQ Pro compatible groups end to end', () => {
  test('validates the intra-group priority range over TCP', async () => {
    const current = start();
    const producer = queue(current, `groups-e2e-priority-${Bun.randomUUIDv7()}`);

    await expect(producer.add('job', {}, { group: { id: 'A', priority: -1 } })).rejects.toThrow(
      'group.priority must be between 0 and 2097151'
    );
    await expect(
      producer.add('job', {}, { group: { id: 'A', priority: 2_097_152 } })
    ).rejects.toThrow('group.priority must be between 0 and 2097151');

    const accepted = await producer.add('job', {}, { group: { id: 'A', priority: 2_097_151 } });
    expect(accepted.priority).toBe(2_097_151);
  });

  test('preserves insertion FIFO over TCP when custom IDs sort in reverse', async () => {
    const current = start();
    const name = `groups-e2e-fifo-${Bun.randomUUIDv7()}`;
    const producer = queue<{ label: string }>(current, name);
    const timestamp = Date.now();

    await producer.add(
      'job',
      { label: 'first' },
      {
        group: { id: 'A' },
        jobId: 'z-first',
        timestamp,
      }
    );
    await producer.add(
      'job',
      { label: 'second' },
      {
        group: { id: 'A' },
        jobId: 'a-second',
        timestamp,
      }
    );

    const pulled = await current.manager.pullBatch(name, 2);
    expect(pulled.map((job) => (job.data as { label: string }).label)).toEqual(['first', 'second']);
  });

  test('serves the public Queue, TCP protocol and Worker group contract', async () => {
    const current = start();
    const name = `groups-e2e-${Bun.randomUUIDv7()}`;
    const producer = queue<{ label: string }>(current, name);
    for (const [label, groupId] of [
      ['plain', null],
      ['A1', 'A'],
      ['A2', 'A'],
      ['B1', 'B'],
      ['B2', 'B'],
    ] as const) {
      await producer.add('job', { label }, groupId ? { group: { id: groupId } } : undefined);
    }

    expect(await producer.getGroupJobsCount('A')).toBe(2);
    expect(await producer.getGroupsJobsCount()).toBe(4);
    expect((await producer.getGroupJobs('A')).map((job) => job.data.label)).toEqual(['A1', 'A2']);
    expect(await producer.getCountsPerPriorityForGroup('A')).toEqual({ 0: 2 });
    await producer.setGroupRateLimit('A', 3, 1_000);
    expect(await producer.getGroupRateLimit('A')).toEqual({ max: 3, duration: 1_000 });
    await producer.setGroupConcurrency('A', 2);
    expect(await producer.getGroupConcurrency('A')).toBe(2);
    expect(await producer.pauseGroup('A')).toBe(true);
    expect(await producer.isGroupPaused('A')).toBe(true);
    expect(await producer.resumeGroup('A')).toBe(true);

    const tcp = new TcpClient({
      autoReconnect: false,
      commandTimeout: 5_000,
      host: '127.0.0.1',
      pingInterval: 0,
      port: current.port,
    });
    current.clients.push(tcp);
    await tcp.connect();
    for (const [command, error] of [
      [
        { cmd: 'SetGroupRateLimit', queue: name, groupId: 'invalid', max: 1.5, duration: 1_000 },
        'max must be a positive safe integer',
      ],
      [
        { cmd: 'SetGroupRateLimit', queue: name, groupId: 'invalid', max: 1, duration: 1.5 },
        'duration must be a positive safe integer',
      ],
      [
        { cmd: 'SetGroupConcurrency', queue: name, groupId: 'invalid', concurrency: 1.5 },
        'concurrency must be a positive safe integer',
      ],
    ] as const) {
      const response = await tcp.send(command);
      expect(response.ok).toBe(false);
      expect(response.error).toBe(error);
    }
    expect(await producer.getGroupRateLimit('invalid')).toBeNull();
    expect(await producer.getGroupConcurrency('invalid')).toBeNull();

    const pulled = await tcp.send({ cmd: 'PULLB', queue: name, count: 5 });
    expect(
      (pulled.jobs as Array<{ data: { label: string } }>).map((job) => job.data.label)
    ).toEqual(['plain', 'A1', 'B1', 'A2', 'B2']);
    expect(
      (
        await tcp.send({
          cmd: 'ACKB',
          ids: (pulled.jobs as Array<{ id: string }>).map((job) => job.id),
        })
      ).ok
    ).toBe(true);
    const invalid = await tcp.send({
      cmd: 'PULL',
      queue: name,
      group: { concurrency: 0 },
    });
    expect(invalid.ok).toBe(false);
    expect(String(invalid.error)).toContain('group.concurrency');
    expect(await producer.removeGroupRateLimit('A')).toBe(1);
    expect(await producer.removeGroupConcurrency('A')).toBe(1);

    const workerQueue = queue<{ label: string }>(current, `${name}-worker`);
    for (const label of ['A1', 'A2', 'B1', 'B2']) {
      await workerQueue.add('job', { label }, { group: { id: label[0] } });
    }
    await workerQueue.setGroupConcurrency('A', 1);
    const active = new Map<string, number>();
    const maximum = new Map<string, number>();
    const completed = Promise.withResolvers<undefined>();
    let completedCount = 0;
    const worker = new Worker<{ label: string }, string>(
      `${name}-worker`,
      async (job) => {
        const groupId = String(job.opts.group?.id);
        const count = (active.get(groupId) ?? 0) + 1;
        active.set(groupId, count);
        maximum.set(groupId, Math.max(maximum.get(groupId) ?? 0, count));
        await Bun.sleep(20);
        active.set(groupId, count - 1);
        if (++completedCount === 4) completed.resolve(undefined);
        return job.data.label;
      },
      {
        batchSize: 4,
        concurrency: 4,
        connection: connection(current),
        embedded: false,
        group: { concurrency: 2 },
      }
    );
    current.workers.push(worker as Worker<unknown, unknown>);
    await worker.waitUntilReady();
    await Promise.race([
      completed.promise,
      Bun.sleep(5_000).then(() => {
        throw new Error('Timed out waiting for grouped worker jobs');
      }),
    ]);
    expect(Object.fromEntries(maximum)).toEqual({ A: 1, B: 2 });
  }, 10_000);

  test('restores SQLite group overrides and removes them on obliterate', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'bunqueue-groups-e2e-'));
    temporaryDirectories.push(directory);
    const dataPath = join(directory, 'groups.db');
    const name = `groups-restart-${Bun.randomUUIDv7()}`;
    const options = { autoBatch: { enabled: false }, dataPath, embedded: true } as const;

    const before = new Queue<{ label: string }>(name, options);
    await before.setGroupRateLimit('A', 3, 60_000);
    await before.setGroupConcurrency('A', 2);
    await before.pauseGroup('A');
    await before.add('job', { label: 'A1' }, { durable: true, group: { id: 'A' } });
    before.close();
    shutdownManager();

    const restored = new Queue<{ label: string }>(name, options);
    expect(await restored.getGroupRateLimit('A')).toEqual({ max: 3, duration: 60_000 });
    expect(await restored.getGroupConcurrency('A')).toBe(2);
    expect(await restored.isGroupPaused('A')).toBe(true);
    expect(await restored.getGroupJobsCount('A')).toBe(1);
    expect(await restored.resumeGroup('A')).toBe(true);
    await restored.obliterateAsync();
    restored.close();
    shutdownManager();

    const cleared = new Queue(name, options);
    expect(await cleared.getGroupRateLimit('A')).toBeNull();
    expect(await cleared.getGroupConcurrency('A')).toBeNull();
    expect(await cleared.getGroupJobsCount('A')).toBe(0);
    cleared.close();
  });

  test('processes one native affinity batch over TCP', async () => {
    const current = start();
    const name = `groups-batch-e2e-${Bun.randomUUIDv7()}`;
    const producer = queue<{ label: string }>(current, name);
    for (const label of ['A1', 'A2', 'B1', 'B2']) {
      await producer.add('job', { label }, { group: { id: label[0] } });
    }

    const batches: string[][] = [];
    const finished = Promise.withResolvers<undefined>();
    let completed = 0;
    const worker = new Worker<{ label: string }, string>(
      name,
      (job) => {
        const labels = (job.getBatch?.() ?? []).map((member) => member.data.label);
        batches.push(labels);
        return labels.join(',');
      },
      {
        batch: { size: 2, minSize: 2, timeout: 100, groupAffinity: true },
        concurrency: 1,
        connection: connection(current),
        embedded: false,
        group: { concurrency: 2 },
      }
    );
    current.workers.push(worker as Worker<unknown, unknown>);
    worker.on('completed', () => {
      if (++completed === 4) finished.resolve(undefined);
    });
    await finished.promise;
    expect(batches).toHaveLength(2);
    expect(batches.every((batch) => batch[0][0] === batch[1][0])).toBe(true);
  });
});
