import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { FlowProducer, Queue, Worker } from '../src/client';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';

interface Broker {
  manager: QueueManager;
  server: TcpServer;
  connection: { host: string; port: number; poolSize: number; pingInterval: number };
  stopped: boolean;
}

const brokers: Broker[] = [];
const producers: FlowProducer[] = [];
const queues: Queue<unknown>[] = [];
const workers: Worker<unknown, unknown>[] = [];
const tempDirs: string[] = [];

function startBroker(dbPath: string): Broker {
  const manager = new QueueManager({
    dataPath: dbPath,
    stallCheckMs: 100,
    writeBufferFlushMs: 5,
  });
  const server = createTcpServer(manager, { hostname: '127.0.0.1', port: 0 });
  const broker: Broker = {
    manager,
    server,
    connection: {
      host: '127.0.0.1',
      port: server.server.port,
      poolSize: 1,
      pingInterval: 0,
    },
    stopped: false,
  };
  brokers.push(broker);
  return broker;
}

function stopBroker(broker: Broker): void {
  if (broker.stopped) return;
  broker.stopped = true;
  broker.server.stop();
  broker.manager.shutdown();
}

function clientQueue<T>(name: string, broker: Broker): Queue<T> {
  const queue = new Queue<T>(name, {
    embedded: false,
    connection: broker.connection,
    autoBatch: { enabled: false },
  });
  queues.push(queue as Queue<unknown>);
  return queue;
}

function clientWorker<T, R>(
  name: string,
  broker: Broker,
  processor: (job: Parameters<ConstructorParameters<typeof Worker<T, R>>[1]>[0]) => Promise<R>
): Worker<T, R> {
  const worker = new Worker<T, R>(name, processor, {
    embedded: false,
    connection: broker.connection,
    concurrency: 4,
  });
  workers.push(worker as Worker<unknown, unknown>);
  return worker;
}

async function waitFor(
  description: string,
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 20_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await Bun.sleep(25);
  }
  throw new Error(`Timed out waiting for ${description}`);
}

afterEach(async () => {
  for (const worker of workers.splice(0).reverse()) {
    try {
      await worker.close(true);
    } catch {
      // Best-effort cleanup after an assertion failure.
    }
  }
  for (const producer of producers.splice(0).reverse()) {
    try {
      await producer.close();
    } catch {
      // Best-effort cleanup after an assertion failure.
    }
  }
  for (const queue of queues.splice(0).reverse()) {
    try {
      await queue.close();
    } catch {
      // Best-effort cleanup after an assertion failure.
    }
  }
  for (const broker of brokers.splice(0).reverse()) {
    try {
      stopBroker(broker);
    } catch {
      // Best-effort cleanup after an assertion failure.
    }
  }
  for (const dir of tempDirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

describe('FlowProducer realistic TCP/SQLite E2E', () => {
  test('executes a durable three-level cross-queue graph exactly once', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'bunqueue-flow-e2e-'));
    tempDirs.push(dir);
    const broker = startBroker(join(dir, 'broker.sqlite'));
    const suffix = crypto.randomUUID();
    const ingestName = `flow-ingest-${suffix}`;
    const transformName = `flow-transform-${suffix}`;
    const aggregateName = `flow-aggregate-${suffix}`;
    const effects: string[] = [];
    const executions = new Map<string, number>();
    const mark = (id: string): void => {
      effects.push(id);
      executions.set(id, (executions.get(id) ?? 0) + 1);
    };

    clientWorker<Record<string, unknown>, number>(ingestName, broker, async (job) => {
      mark(job.id);
      return Number(job.data.value);
    });
    clientWorker<Record<string, unknown>, number>(transformName, broker, async (job) => {
      mark(job.id);
      const values = await job.getChildrenValues<number>();
      return Object.values(values).reduce((sum, value) => sum + value, 0) * 2;
    });
    clientWorker<Record<string, unknown>, number>(aggregateName, broker, async (job) => {
      mark(job.id);
      const values = await job.getChildrenValues<number>();
      return Object.values(values).reduce((sum, value) => sum + value, 0);
    });

    const producer = new FlowProducer({ embedded: false, connection: broker.connection });
    producers.push(producer);
    const root = await producer.add<Record<string, unknown>>({
      name: 'aggregate',
      queueName: aggregateName,
      data: { stage: 'aggregate' },
      opts: { durable: true, jobId: `aggregate-${suffix}` },
      children: [
        {
          name: 'transform',
          queueName: transformName,
          data: { stage: 'transform' },
          opts: { durable: true, jobId: `transform-${suffix}` },
          children: [
            {
              name: 'extract-a',
              queueName: ingestName,
              data: { stage: 'extract', value: 2 },
              opts: { durable: true, jobId: `extract-a-${suffix}` },
            },
            {
              name: 'extract-b',
              queueName: ingestName,
              data: { stage: 'extract', value: 3 },
              opts: { durable: true, jobId: `extract-b-${suffix}` },
            },
          ],
        },
        {
          name: 'audit',
          queueName: ingestName,
          data: { stage: 'audit', value: 5 },
          opts: { durable: true, jobId: `audit-${suffix}` },
        },
      ],
    });

    expect(await root.job.waitUntilFinished(null, 20_000)).toBe(15);
    expect(executions.size).toBe(5);
    expect([...executions.values()]).toEqual([1, 1, 1, 1, 1]);
    const transformId = root.children![0].job.id;
    const aggregateId = root.job.id;
    for (const leaf of [
      root.children![0].children![0].job.id,
      root.children![0].children![1].job.id,
    ]) {
      expect(effects.indexOf(leaf)).toBeLessThan(effects.indexOf(transformId));
    }
    expect(effects.indexOf(transformId)).toBeLessThan(effects.indexOf(aggregateId));
    expect(effects.indexOf(root.children![1].job.id)).toBeLessThan(effects.indexOf(aggregateId));

    const fetched = await producer.getFlow({
      id: root.job.id,
      queueName: aggregateName,
    });
    expect(fetched?.children).toHaveLength(2);
    expect(fetched?.children?.[0].children).toHaveLength(2);
  }, 30_000);

  test('continues a failed-child flow exactly once after a full broker restart', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'bunqueue-flow-restart-'));
    tempDirs.push(dir);
    const dbPath = join(dir, 'broker.sqlite');
    const suffix = crypto.randomUUID();
    const childName = `flow-restart-child-${suffix}`;
    const parentName = `flow-restart-parent-${suffix}`;
    let broker = startBroker(dbPath);
    const childQueue = clientQueue(childName, broker);
    const parentQueue = clientQueue(parentName, broker);
    const childWorker = clientWorker<Record<string, unknown>, never>(
      childName,
      broker,
      async () => {
        throw new Error('realistic child failure');
      }
    );
    const producer = new FlowProducer({ embedded: false, connection: broker.connection });
    producers.push(producer);
    const root = await producer.add<Record<string, unknown>>({
      name: 'recover-parent',
      queueName: parentName,
      data: { stage: 'parent' },
      opts: { durable: true, attempts: 1, jobId: `recover-parent-${suffix}` },
      children: [
        {
          name: 'recover-child',
          queueName: childName,
          data: { stage: 'child' },
          opts: {
            continueParentOnFailure: true,
            durable: true,
            attempts: 1,
            jobId: `recover-child-${suffix}`,
          },
        },
      ],
    });

    await waitFor(
      'child failure and parent promotion',
      async () =>
        (await childQueue.getJobState(root.children![0].job.id)) === 'failed' &&
        (await parentQueue.getJobState(root.job.id)) === 'waiting'
    );
    await childWorker.close(true);
    await producer.close();
    await childQueue.close();
    await parentQueue.close();
    stopBroker(broker);

    broker = startBroker(dbPath);
    const recoveredProducer = new FlowProducer({
      embedded: false,
      connection: broker.connection,
    });
    producers.push(recoveredProducer);
    const recovered = await recoveredProducer.getFlow({
      id: root.job.id,
      queueName: parentName,
    });
    expect(recovered).not.toBeNull();
    expect(recovered?.children).toHaveLength(1);
    const persistedFailures = await recovered!.job.getFailedChildrenValues();
    expect(Object.values(persistedFailures)).toEqual(['realistic child failure']);

    let parentExecutions = 0;
    let observedFailures: Record<string, string> = {};
    clientWorker<Record<string, unknown>, string>(parentName, broker, async (job) => {
      parentExecutions++;
      observedFailures = await job.getFailedChildrenValues();
      return 'recovered';
    });

    expect(await recovered!.job.waitUntilFinished(null, 20_000)).toBe('recovered');
    expect(parentExecutions).toBe(1);
    expect(Object.values(observedFailures)).toEqual(['realistic child failure']);
  }, 35_000);
});
