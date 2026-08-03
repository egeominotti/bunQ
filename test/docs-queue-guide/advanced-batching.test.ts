/**
 * Executable proof for the auto-batching and store-and-forward halves of
 * /guide/queue/advanced/ ("Namespaces, Auto-Batching and Store-and-Forward").
 *
 * Auto-batching is observed on a real broker whose push entry points are
 * counted by a QueueManager subclass, so "one round-trip" is measured rather
 * than assumed.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { closeAllSharedPools, Queue, shutdownManager } from '../../src/client';
import { QueueManager } from '../../src/application/queueManager';
import type { Job, JobId, JobInput } from '../../src/domain/types/job';
import { createTcpServer, type TcpServer } from '../../src/infrastructure/server/tcp';
import { waitUntil } from '../docs-guide-support';

class CountingQueueManager extends QueueManager {
  singlePushes = 0;
  batchPushes = 0;
  batchedJobs = 0;

  override async push(queue: string, input: JobInput): Promise<Job> {
    this.singlePushes++;
    return super.push(queue, input);
  }

  override async pushBatch(queue: string, inputs: JobInput[]): Promise<JobId[]> {
    this.batchPushes++;
    this.batchedJobs += inputs.length;
    return super.pushBatch(queue, inputs);
  }
}

interface Broker {
  manager: CountingQueueManager;
  server: TcpServer;
  port: number;
  dataDir: string;
}

const brokers: Broker[] = [];
const closers: Array<() => Promise<void> | void> = [];
const scratchDirs: string[] = [];

/** Embedded queues share one process-wide manager; give each test a fresh one. */
function edgeDataDir(label: string): string {
  const dir = mkdtempSync(join(tmpdir(), `bunqueue-docs-${label}-`));
  scratchDirs.push(dir);
  return dir;
}

function startBroker(label: string): Broker {
  const dataDir = mkdtempSync(join(tmpdir(), `bunqueue-docs-${label}-`));
  const manager = new CountingQueueManager({ dataPath: join(dataDir, 'queue.db') });
  const server = createTcpServer(manager, { hostname: '127.0.0.1', port: 0 });
  const broker: Broker = { manager, server, port: server.server.port, dataDir };
  brokers.push(broker);
  return broker;
}

function track<T extends { close(): unknown }>(closeable: T): T {
  closers.push(() => {
    void closeable.close();
  });
  return closeable;
}

afterEach(async () => {
  for (const close of closers.splice(0).reverse()) await close();
  shutdownManager();
  closeAllSharedPools();
  for (const broker of brokers.splice(0)) {
    broker.server.stop();
    broker.manager.shutdown();
    rmSync(broker.dataDir, { recursive: true, force: true });
  }
  for (const dir of scratchDirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

describe('queue guide · auto-batching (TCP mode)', () => {
  test('concurrent adds collapse into one bulk round-trip', async () => {
    const broker = startBroker('autobatch');
    const queue = track(
      new Queue(`autobatch-${crypto.randomUUID()}`, {
        embedded: false,
        connection: { host: '127.0.0.1', port: broker.port, poolSize: 1 },
        autoBatch: { enabled: true, maxSize: 50, maxDelayMs: 5 },
      })
    );

    const jobs = await Promise.all([
      queue.add('a', { x: 1 }),
      queue.add('b', { x: 2 }),
      queue.add('c', { x: 3 }),
    ]);

    expect(new Set(jobs.map((job) => job.id)).size).toBe(3);
    expect(broker.manager.batchPushes).toBeGreaterThanOrEqual(1);
    expect(broker.manager.batchedJobs).toBe(3);
    expect(broker.manager.singlePushes).toBe(0);
    await waitUntil(async () => (await queue.countAsync()) === 3, 'all three jobs to persist');
  }, 20_000);

  test('sequential awaits are sent immediately, one job at a time', async () => {
    const broker = startBroker('autobatch-seq');
    const queue = track(
      new Queue(`autobatch-${crypto.randomUUID()}`, {
        embedded: false,
        connection: { host: '127.0.0.1', port: broker.port, poolSize: 1 },
        autoBatch: { enabled: true },
      })
    );

    for (let i = 0; i < 3; i++) await queue.add(`job-${i}`, { i });

    // Each sequential add flushes on its own: three batches of one, never a
    // buffered group.
    expect(broker.manager.batchPushes).toBe(3);
    expect(broker.manager.batchedJobs).toBe(3);
  }, 20_000);

  test('durable jobs bypass the batcher and are pushed individually', async () => {
    const broker = startBroker('autobatch-durable');
    const queue = track(
      new Queue(`autobatch-${crypto.randomUUID()}`, {
        embedded: false,
        connection: { host: '127.0.0.1', port: broker.port, poolSize: 1 },
        autoBatch: { enabled: true },
      })
    );

    await Promise.all([
      queue.add('critical-1', { x: 1 }, { durable: true }),
      queue.add('critical-2', { x: 2 }, { durable: true }),
    ]);

    expect(broker.manager.singlePushes).toBe(2);
    expect(broker.manager.batchPushes).toBe(0);
  }, 20_000);

  test('auto-batching can be disabled', async () => {
    const broker = startBroker('autobatch-off');
    const queue = track(
      new Queue(`autobatch-${crypto.randomUUID()}`, {
        embedded: false,
        connection: { host: '127.0.0.1', port: broker.port, poolSize: 1 },
        autoBatch: { enabled: false },
      })
    );

    await Promise.all([queue.add('a', { x: 1 }), queue.add('b', { x: 2 })]);

    expect(broker.manager.batchPushes).toBe(0);
    expect(broker.manager.singlePushes).toBe(2);
  }, 20_000);
});

describe('queue guide · store-and-forward', () => {
  test('forward drains an embedded queue into a remote server', async () => {
    const central = startBroker('forward-central');
    const edgeDir = edgeDataDir('forward-edge');
    const edge = track(
      new Queue<{ reading: number }>(`edge-${crypto.randomUUID()}`, {
        embedded: true,
        dataPath: join(edgeDir, 'edge.db'),
      })
    );
    const remoteName = `central-${crypto.randomUUID()}`;
    const remote = track(
      new Queue(remoteName, {
        embedded: false,
        connection: { host: '127.0.0.1', port: central.port, poolSize: 1 },
        autoBatch: { enabled: false },
      })
    );

    const forwarded: Array<{ id: string; remoteId: string; name: string }> = [];
    const forwarder = edge.forward({
      to: { host: '127.0.0.1', port: central.port },
      queue: remoteName,
      concurrency: 4,
      durable: true,
    });
    forwarder.on('forwarded', (info) => forwarded.push(info));
    forwarder.on('error', () => undefined);

    try {
      const local = await edge.add('reading', { reading: 42 }, { durable: true });
      await waitUntil(
        async () => (await remote.countAsync()) === 1,
        'the job to reach the central broker',
        15_000
      );

      expect(forwarded).toHaveLength(1);
      expect(forwarded[0].id).toBe(local.id);
      expect(forwarded[0].remoteId).toContain('fwd:');
      expect(forwarded[0].name).toBe('reading');

      const remoteJob = await remote.getJob(forwarded[0].remoteId);
      expect(remoteJob).not.toBeNull();
      expect((remoteJob?.data as { reading?: number }).reading).toBe(42);
    } finally {
      await forwarder.close();
    }
  }, 30_000);

  test('a re-forward of the same local job is deduped remotely', async () => {
    const central = startBroker('forward-dedupe');
    const remoteName = `central-${crypto.randomUUID()}`;
    const remote = track(
      new Queue(remoteName, {
        embedded: false,
        connection: { host: '127.0.0.1', port: central.port, poolSize: 1 },
        autoBatch: { enabled: false },
      })
    );

    const deterministicId = `fwd:edge-queue:${crypto.randomUUID()}`;
    await remote.add('reading', { reading: 1 }, { jobId: deterministicId, durable: true });
    await remote.add('reading', { reading: 1 }, { jobId: deterministicId, durable: true });

    expect(await remote.countAsync()).toBe(1);
  }, 20_000);

  test('a rejected remote push sends the local job to the local DLQ', async () => {
    // A token-protected central broker rejects the unauthenticated forward, the
    // documented "remote failure" path: local retry, then the local DLQ.
    const centralDir = mkdtempSync(join(tmpdir(), 'bunqueue-docs-forward-auth-'));
    const centralManager = new CountingQueueManager({ dataPath: join(centralDir, 'queue.db') });
    const centralServer = createTcpServer(centralManager, {
      hostname: '127.0.0.1',
      port: 0,
      authTokens: ['secret-token'],
    });
    brokers.push({
      manager: centralManager,
      server: centralServer,
      port: centralServer.server.port,
      dataDir: centralDir,
    });
    const edgeDir = edgeDataDir('forward-rejected');
    const edge = track(
      new Queue<{ reading: number }>(`edge-${crypto.randomUUID()}`, {
        embedded: true,
        dataPath: join(edgeDir, 'edge.db'),
      })
    );

    const forwarded: unknown[] = [];
    const forwarder = edge.forward({
      to: { host: '127.0.0.1', port: centralServer.server.port },
      concurrency: 1,
    });
    forwarder.on('error', () => undefined);
    forwarder.on('forwarded', (info) => forwarded.push(info));

    try {
      const local = await edge.add('reading', { reading: 7 }, { attempts: 1, durable: true });
      await waitUntil(
        async () => (await edge.getDlqAsync()).some((entry) => entry.job.id === local.id),
        'the rejected job to land in the local DLQ',
        20_000
      );

      expect(forwarded).toEqual([]);
      const entry = (await edge.getDlqAsync()).find((item) => item.job.id === local.id);
      expect(entry?.job.data).toMatchObject({ reading: 7 });
      expect(centralManager.singlePushes + centralManager.batchPushes).toBe(0);
    } finally {
      await forwarder.close();
    }
  }, 40_000);

  test('an unreachable remote never loses the local job', async () => {
    const edgeDir = edgeDataDir('forward-down');
    const edge = track(
      new Queue<{ reading: number }>(`edge-${crypto.randomUUID()}`, {
        embedded: true,
        dataPath: join(edgeDir, 'edge.db'),
      })
    );

    // Port 1 is not listening: the forward stays pending against the broker's
    // command timeout instead of dropping the job.
    const forwarder = edge.forward({ to: { host: '127.0.0.1', port: 1 }, concurrency: 1 });
    forwarder.on('error', () => undefined);
    const delivered: unknown[] = [];
    forwarder.on('forwarded', (info) => delivered.push(info));

    try {
      const local = await edge.add('reading', { reading: 7 }, { attempts: 1, durable: true });
      for (let i = 0; i < 6; i++) {
        await Bun.sleep(250);
        expect(['waiting', 'active', 'delayed', 'failed']).toContain(
          await edge.getJobState(local.id)
        );
      }
      expect(delivered).toEqual([]);
      expect((await edge.getJob(local.id))?.data).toMatchObject({ reading: 7 });
    } finally {
      await forwarder.close();
    }
  }, 40_000);
});
