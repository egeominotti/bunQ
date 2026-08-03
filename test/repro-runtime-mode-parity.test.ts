import { afterEach, describe, expect, test } from 'bun:test';
import { EventEmitter } from 'node:events';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { FlowProducer, Queue, QueueEvents, Worker } from '../src/client';
import { QueueManager } from '../src/application/queueManager';
import { TcpClient } from '../src/client/tcpClient';
import { createTcpServer } from '../src/infrastructure/server/tcp';
import { CoreE2eHarness } from './core-e2e/support/harness';

let harness: CoreE2eHarness | null = null;

function timeout<T>(label: string, ms = 2_000): Promise<T> {
  return new Promise<T>((_, reject) => {
    setTimeout(() => reject(new Error(`${label} timed out`)), ms);
  });
}

async function waitForCompleted(
  queue: { getJobState(id: string): Promise<string> },
  id: string
): Promise<void> {
  const deadline = Date.now() + 5_000;
  while ((await queue.getJobState(id)) !== 'completed' && Date.now() < deadline) {
    await Bun.sleep(20);
  }
  expect(await queue.getJobState(id)).toBe('completed');
}

async function waitUntilReady(events: QueueEvents, timeoutMs = 5_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      await events.waitUntilReady();
      return;
    } catch (error) {
      lastError = error;
      await Bun.sleep(50);
    }
  }
  throw lastError instanceof Error ? lastError : new Error('QueueEvents did not become ready');
}

function subscriberCount(manager: QueueManager): number {
  return (
    manager as unknown as {
      eventsManager: { subscriberCount: number };
    }
  ).eventsManager.subscriberCount;
}

afterEach(async () => {
  await harness?.close();
  harness = null;
});

describe('embedded and TCP public feature parity', () => {
  test('QueueEvents receives real remote lifecycle events over TCP', async () => {
    harness = await CoreE2eHarness.start('tcp', 'queue-events-parity');
    const queue = harness.queue<{ value: number }>('events');
    const events = new QueueEvents(queue.name, {
      embedded: false,
      connection: harness.connection(),
    });
    harness.addCleanup(() => events.close());

    const waiting = new Promise<string>((resolve) => {
      events.once('waiting', ({ jobId }) => resolve(jobId));
    });
    const completed = new Promise<{ jobId: string; returnvalue: unknown }>((resolve) => {
      events.once('completed', resolve);
    });
    await events.waitUntilReady();

    harness.worker(queue.name, async (job) => ({ doubled: job.data.value * 2 }));
    const job = await queue.add('remote-events', { value: 21 }, { durable: true });

    expect(await Promise.race([waiting, timeout<string>('waiting event')])).toBe(job.id);
    expect(
      await Promise.race([
        completed,
        timeout<{ jobId: string; returnvalue: unknown }>('completed event'),
      ])
    ).toEqual({ jobId: job.id, returnvalue: { doubled: 42 } });
  });

  test('QueueEvents authenticates its dedicated TCP subscription', async () => {
    const dataDir = mkdtempSync(join(tmpdir(), 'bunqueue-events-auth-parity-'));
    const manager = new QueueManager({ dataPath: join(dataDir, 'queue.db') });
    const server = createTcpServer(manager, {
      hostname: '127.0.0.1',
      port: 0,
      authTokens: ['events-secret'],
    });
    const queueName = `events-auth-${crypto.randomUUID()}`;
    const connection = {
      host: '127.0.0.1',
      port: server.server.port,
      poolSize: 1,
      token: 'events-secret',
    };
    const queue = new Queue(queueName, { embedded: false, connection });
    const events = new QueueEvents(queueName, { embedded: false, connection });
    const rejected = new QueueEvents(queueName, {
      embedded: false,
      connection: { ...connection, token: 'wrong-secret' },
    });
    events.on('error', () => undefined);
    rejected.on('error', () => undefined);

    try {
      await events.waitUntilReady();
      await expect(rejected.waitUntilReady()).rejects.toThrow('Authentication failed');
      const waiting = new Promise<string>((resolve) => {
        events.once('waiting', ({ jobId }) => resolve(jobId));
      });
      const job = await queue.add('authenticated-event', { value: 1 }, { durable: true });
      expect(await Promise.race([waiting, timeout<string>('authenticated event')])).toBe(job.id);
    } finally {
      events.close();
      rejected.close();
      await queue.close();
      server.stop();
      manager.shutdown();
      rmSync(dataDir, { recursive: true, force: true });
    }
  });

  test('QueueEvents re-subscribes after a broker restart', async () => {
    const dataDir = mkdtempSync(join(tmpdir(), 'bunqueue-events-reconnect-parity-'));
    const manager = new QueueManager({ dataPath: join(dataDir, 'queue.db') });
    const baselineSubscribers = subscriberCount(manager);
    let server = createTcpServer(manager, { hostname: '127.0.0.1', port: 0 });
    const port = server.server.port;
    const connection = { host: '127.0.0.1', port, poolSize: 1 };
    const queueName = `events-reconnect-${crypto.randomUUID()}`;
    const events = new QueueEvents(queueName, { embedded: false, connection });
    events.on('error', () => undefined);

    try {
      await events.waitUntilReady();
      server.stop();
      await Bun.sleep(150);
      server = createTcpServer(manager, { hostname: '127.0.0.1', port });
      await waitUntilReady(events);
      expect(server.getConnectionCount()).toBeGreaterThanOrEqual(1);
      expect(subscriberCount(manager)).toBe(baselineSubscribers + 1);

      const waiting = new Promise<string>((resolve) => {
        events.once('waiting', ({ jobId }) => resolve(jobId));
      });
      const queue = new Queue(queueName, { embedded: false, connection });
      try {
        const job = await queue.add('after-reconnect', { value: 1 }, { durable: true });
        expect(await Promise.race([waiting, timeout<string>('reconnected event')])).toBe(job.id);
      } finally {
        await queue.close();
      }
    } finally {
      events.close();
      server.stop();
      manager.shutdown();
      rmSync(dataDir, { recursive: true, force: true });
    }
  });

  test('TCP event unsubscribe stops delivery without closing the command channel', async () => {
    const dataDir = mkdtempSync(join(tmpdir(), 'bunqueue-events-unsubscribe-parity-'));
    const manager = new QueueManager({ dataPath: join(dataDir, 'queue.db') });
    const baselineSubscribers = subscriberCount(manager);
    const server = createTcpServer(manager, { hostname: '127.0.0.1', port: 0 });
    const connection = { host: '127.0.0.1', port: server.server.port };
    const queueName = `events-unsubscribe-${crypto.randomUUID()}`;
    const queue = new Queue(queueName, {
      embedded: false,
      connection: { ...connection, poolSize: 1 },
    });
    const client = new TcpClient({ ...connection, commandTimeout: 2_000 });
    client.on('error', () => undefined);
    const received: string[] = [];
    client.on('queueEvent', (event) => received.push(event.jobId));

    try {
      expect(await client.send({ cmd: 'SubscribeEvents', queue: queueName })).toMatchObject({
        ok: true,
      });
      expect(subscriberCount(manager)).toBe(baselineSubscribers + 1);
      const first = await queue.add('subscribed', { value: 1 }, { durable: true });
      const deadline = Date.now() + 2_000;
      while (!received.includes(first.id) && Date.now() < deadline) await Bun.sleep(20);
      expect(received).toContain(first.id);

      expect(await client.send({ cmd: 'UnsubscribeEvents' })).toMatchObject({ ok: true });
      const second = await queue.add('unsubscribed', { value: 2 }, { durable: true });
      await Bun.sleep(100);
      expect(received).not.toContain(second.id);
      expect(await client.send({ cmd: 'Ping' })).toMatchObject({ ok: true });
      expect(subscriberCount(manager)).toBe(baselineSubscribers);
    } finally {
      client.close();
      await queue.close();
      server.stop();
      manager.shutdown();
      rmSync(dataDir, { recursive: true, force: true });
    }
  });

  test('FlowProducer reads one completed result over TCP', async () => {
    harness = await CoreE2eHarness.start('tcp', 'flow-parent-result-parity');
    const queue = harness.queue<{ value: number }>('results');
    const producer = harness.flow();
    harness.worker(queue.name, async (job) => ({ doubled: job.data.value * 2 }));
    const job = await queue.add('result', { value: 21 }, { durable: true });
    await waitForCompleted(queue, job.id);

    expect(await producer.getParentResult<{ doubled: number }>(job.id)).toEqual({ doubled: 42 });
  });

  test('FlowProducer reads multiple completed results over TCP', async () => {
    harness = await CoreE2eHarness.start('tcp', 'flow-parent-results-parity');
    const queue = harness.queue<{ value: number }>('results');
    const producer: FlowProducer = harness.flow();
    harness.worker(queue.name, async (job) => job.data.value * 10);
    const first = await queue.add('first', { value: 1 }, { durable: true });
    const second = await queue.add('second', { value: 2 }, { durable: true });
    await Promise.all([waitForCompleted(queue, first.id), waitForCompleted(queue, second.id)]);

    const results = await producer.getParentResults<number>([first.id, second.id]);
    expect([...results.entries()]).toEqual([
      [first.id, 10],
      [second.id, 20],
    ]);
  });

  test('Queue.waitJobUntilFinished returns the completed TCP result', async () => {
    harness = await CoreE2eHarness.start('tcp', 'queue-wait-result-parity');
    const queue = harness.queue<{ value: number }>('results');
    harness.worker(queue.name, async (job) => ({ doubled: job.data.value * 2 }));
    const job = await queue.add('result', { value: 21 }, { durable: true });
    await waitForCompleted(queue, job.id);

    expect(await queue.waitJobUntilFinished(job.id, new EventEmitter(), 2_000)).toEqual({
      doubled: 42,
    });
  });

  test('Worker receives broker stall notifications over TCP', async () => {
    const dataDir = mkdtempSync(join(tmpdir(), 'bunqueue-worker-stalled-parity-'));
    const manager = new QueueManager({
      dataPath: join(dataDir, 'queue.db'),
      stallCheckMs: 25,
    });
    const server = createTcpServer(manager, { hostname: '127.0.0.1', port: 0 });
    const connection = { host: '127.0.0.1', port: server.server.port, poolSize: 1 };
    const queueName = `worker-stalled-parity-${crypto.randomUUID()}`;
    const queue = new Queue(queueName, {
      embedded: false,
      connection,
      autoBatch: { enabled: false },
    });
    const worker = new Worker(queueName, async () => await new Promise<never>(() => undefined), {
      embedded: false,
      connection,
      heartbeatInterval: 0,
      useLocks: true,
      lockDuration: 50,
    });
    worker.on('error', () => undefined);

    try {
      await queue.setStallConfigAsync({
        enabled: true,
        stallInterval: 20,
        maxStalls: 1,
        gracePeriod: 0,
      });
      const stalled = new Promise<{ jobId: string; reason: string }>((resolve) => {
        worker.once('stalled', (jobId, reason) => resolve({ jobId, reason }));
      });
      const job = await queue.add('stall', { value: 1 }, { durable: true });

      expect(
        await Promise.race([
          stalled,
          timeout<{ jobId: string; reason: string }>('worker stalled event'),
        ])
      ).toEqual({ jobId: job.id, reason: 'active' });
    } finally {
      await worker.close(true);
      await queue.close();
      server.stop();
      manager.shutdown();
      rmSync(dataDir, { recursive: true, force: true });
    }
  });
});
