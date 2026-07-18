import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { Queue, Worker } from '../src/client';
import type { Job } from '../src/client';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';
import { createHttpServer } from '../src/infrastructure/server/http';

type Work = {
  id: string;
  kind: 'priority' | 'normal' | 'transient' | 'dead' | 'delayed' | 'deduplicated';
  queuedAt: number;
};

type Broker = {
  manager: QueueManager;
  tcp: TcpServer;
  http: ReturnType<typeof createHttpServer>;
  connection: { host: string; port: number; poolSize: number; pingInterval: number };
  httpUrl: string;
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function waitFor(
  description: string,
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 20_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await sleep(25);
  }
  throw new Error(`Timed out waiting for ${description}`);
}

function startBroker(dbPath: string): Broker {
  const manager = new QueueManager({
    dataPath: dbPath,
    stallCheckMs: 100,
    writeBufferFlushMs: 10,
  });
  const tcp = createTcpServer(manager, { hostname: '127.0.0.1', port: 0 });
  const http = createHttpServer(manager, { hostname: '127.0.0.1', port: 0 });
  return {
    manager,
    tcp,
    http,
    connection: {
      host: '127.0.0.1',
      port: tcp.server.port,
      poolSize: 2,
      pingInterval: 0,
    },
    httpUrl: http.server.url.toString().replace(/\/$/, ''),
  };
}

function stopBroker(broker: Broker): void {
  broker.http.stop();
  broker.tcp.stop();
  broker.manager.shutdown();
}

describe('production readiness: durable mixed workload over TCP', () => {
  const openBrokers: Broker[] = [];
  const openQueues: Queue<Work>[] = [];
  const openWorkers: Worker<Work>[] = [];
  const tempDirs: string[] = [];

  afterEach(async () => {
    for (const worker of openWorkers.splice(0)) {
      try {
        await worker.close(true);
      } catch {
        // Best-effort cleanup after assertion failures.
      }
    }
    for (const queue of openQueues.splice(0)) queue.close();
    for (const broker of openBrokers.splice(0)) {
      try {
        stopBroker(broker);
      } catch {
        // Best-effort cleanup after assertion failures.
      }
    }
    for (const dir of tempDirs.splice(0)) rmSync(dir, { recursive: true, force: true });
  });

  test('survives rolling restarts without loss, duplicate effects, or limit drift', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'bunqueue-production-'));
    tempDirs.push(dir);
    const dbPath = join(dir, 'broker.sqlite');
    const queueName = `orders-${crypto.randomUUID()}`;

    let broker = startBroker(dbPath);
    openBrokers.push(broker);
    let queue = new Queue<Work>(queueName, {
      embedded: false,
      connection: broker.connection,
      autoBatch: { enabled: false },
    });
    openQueues.push(queue);
    await queue.waitUntilReady();
    await queue.setGlobalConcurrencyAsync(3);

    const queuedAt = Date.now();
    const jobs = await queue.addBulk([
      ...Array.from({ length: 4 }, (_, i) => ({
        name: 'fulfil-order',
        data: { id: `priority-${i}`, kind: 'priority' as const, queuedAt },
        opts: { priority: 50, durable: true },
      })),
      ...Array.from({ length: 12 }, (_, i) => ({
        name: 'fulfil-order',
        data: { id: `normal-${i}`, kind: 'normal' as const, queuedAt },
        opts: { durable: true },
      })),
      ...Array.from({ length: 3 }, (_, i) => ({
        name: 'fulfil-order',
        data: { id: `transient-${i}`, kind: 'transient' as const, queuedAt },
        opts: { attempts: 3, backoff: 25, durable: true },
      })),
      ...Array.from({ length: 2 }, (_, i) => ({
        name: 'fulfil-order',
        data: { id: `dead-${i}`, kind: 'dead' as const, queuedAt },
        opts: { attempts: 2, backoff: 25, durable: true },
      })),
      ...Array.from({ length: 3 }, (_, i) => ({
        name: 'fulfil-order',
        data: { id: `delayed-${i}`, kind: 'delayed' as const, queuedAt },
        opts: { delay: 300, durable: true },
      })),
    ]);
    const dedupA = await queue.add(
      'capture-payment',
      { id: 'payment-42', kind: 'deduplicated', queuedAt },
      { jobId: 'payment-idempotency-key-42', durable: true }
    );
    const dedupB = await queue.add(
      'capture-payment',
      { id: 'payment-42', kind: 'deduplicated', queuedAt },
      { jobId: 'payment-idempotency-key-42', durable: true }
    );
    expect(dedupB.id).toBe(dedupA.id);

    const beforeRestart = await queue.getJobCountsAsync();
    expect(beforeRestart.waiting + beforeRestart.prioritized + beforeRestart.delayed).toBe(25);
    queue.close();
    openQueues.splice(openQueues.indexOf(queue), 1);
    stopBroker(broker);
    openBrokers.splice(openBrokers.indexOf(broker), 1);

    broker = startBroker(dbPath);
    openBrokers.push(broker);
    queue = new Queue<Work>(queueName, {
      embedded: false,
      connection: broker.connection,
      autoBatch: { enabled: false },
    });
    openQueues.push(queue);
    await queue.waitUntilReady();

    const attempts = new Map<string, number>();
    const successfulEffects = new Map<string, number>();
    const starts: Array<{ work: Work; at: number }> = [];
    let active = 0;
    let maxActive = 0;
    const processor = async (job: Job<Work>) => {
      const work = job.data;
      starts.push({ work, at: Date.now() });
      active++;
      maxActive = Math.max(maxActive, active);
      const attempt = (attempts.get(work.id) ?? 0) + 1;
      attempts.set(work.id, attempt);
      try {
        await sleep(15 + (work.id.charCodeAt(work.id.length - 1) % 4) * 5);
        if (work.kind === 'transient' && attempt === 1) throw new Error('upstream timeout');
        if (work.kind === 'dead') throw new Error('invalid customer account');
        successfulEffects.set(work.id, (successfulEffects.get(work.id) ?? 0) + 1);
        return { processedBy: 'production-readiness', attempt };
      } finally {
        active--;
      }
    };

    const workerA = new Worker<Work>(queueName, processor, {
      embedded: false,
      connection: broker.connection,
      concurrency: 6,
      batchSize: 10,
      pollTimeout: 100,
      useLocks: true,
      lockDuration: 2_000,
      heartbeatInterval: 250,
    });
    const workerB = new Worker<Work>(queueName, processor, {
      embedded: false,
      connection: broker.connection,
      concurrency: 6,
      batchSize: 10,
      pollTimeout: 100,
      useLocks: true,
      lockDuration: 2_000,
      heartbeatInterval: 250,
    });
    openWorkers.push(workerA, workerB);
    await Promise.all([workerA.waitUntilReady(), workerB.waitUntilReady()]);

    await waitFor('mixed workload to drain', async () => {
      const counts = await queue.getJobCountsAsync();
      return (
        counts.waiting + counts.prioritized + counts.active + counts.delayed === 0 &&
        counts.completed === 23 &&
        counts.failed === 2
      );
    });

    expect(maxActive).toBeLessThanOrEqual(3);
    expect(starts.slice(0, 4).every(({ work }) => work.kind === 'priority')).toBe(true);
    expect(starts.filter(({ work }) => work.kind === 'deduplicated')).toHaveLength(1);
    expect(
      starts
        .filter(({ work }) => work.kind === 'delayed')
        .every(({ work, at }) => at >= work.queuedAt + 300)
    ).toBe(true);
    expect(attempts.get('transient-0')).toBe(2);
    expect(attempts.get('transient-1')).toBe(2);
    expect(attempts.get('transient-2')).toBe(2);
    expect(attempts.get('dead-0')).toBe(2);
    expect(attempts.get('dead-1')).toBe(2);
    expect(successfulEffects.size).toBe(23);
    expect([...successfulEffects.values()].every((count) => count === 1)).toBe(true);
    expect(await queue.getDlqJobsAsync()).toHaveLength(2);

    const health = await fetch(`${broker.httpUrl}/health`).then((response) => response.json());
    expect(health.ok).toBe(true);
    const metrics = await fetch(`${broker.httpUrl}/prometheus`).then((response) => response.text());
    expect(metrics).toContain('bunqueue_jobs_completed 23');
    expect(metrics).toContain(`bunqueue_queue_jobs_dlq{queue="${queueName}"} 2`);

    await Promise.all([workerA.close(), workerB.close()]);
    openWorkers.length = 0;
    queue.close();
    openQueues.length = 0;
    stopBroker(broker);
    openBrokers.length = 0;

    broker = startBroker(dbPath);
    openBrokers.push(broker);
    const inspector = new Queue<Work>(queueName, {
      embedded: false,
      connection: broker.connection,
      autoBatch: { enabled: false },
    });
    openQueues.push(inspector);
    await inspector.waitUntilReady();
    const recovered = await inspector.getJobCountsAsync();
    expect(recovered).toMatchObject({
      waiting: 0,
      prioritized: 0,
      active: 0,
      delayed: 0,
      completed: 23,
      failed: 2,
    });
    expect(await inspector.getDlqJobsAsync()).toHaveLength(2);
    for (const job of jobs.filter((job) => job.data.kind !== 'dead')) {
      expect(await inspector.getJobState(job.id)).toBe('completed');
    }
  }, 30_000);
});
