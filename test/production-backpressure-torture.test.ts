import { Database } from 'bun:sqlite';
import { afterEach, describe, expect, test } from 'bun:test';
import { existsSync, mkdtempSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Queue, Worker, type Job } from '../src/client';
import { QueueManager } from '../src/application/queueManager';
import { createHttpServer } from '../src/infrastructure/server/http';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';

type Payload = { sequence: number; acceptedAt: number };
type Broker = {
  manager: QueueManager;
  tcp: TcpServer;
  http: ReturnType<typeof createHttpServer>;
  connection: {
    host: string;
    port: number;
    poolSize: number;
    pingInterval: number;
    commandTimeout: number;
    maxInFlight: number;
  };
  httpUrl: string;
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function startBroker(dbPath: string): Broker {
  const manager = new QueueManager({
    dataPath: dbPath,
    stallCheckMs: 250,
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
      commandTimeout: 60_000,
      maxInFlight: 100,
    },
    httpUrl: http.server.url.toString().replace(/\/$/, ''),
  };
}

function stopBroker(broker: Broker): void {
  broker.http.stop();
  broker.tcp.stop();
  broker.manager.shutdown();
}

function percentile(values: number[], ratio: number): number {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * ratio))];
}

async function waitForDrain(queue: Queue<Payload>, completed: Set<string>, total: number) {
  const deadline = Date.now() + Math.max(60_000, total * 5);
  while (Date.now() < deadline) {
    const counts = await queue.getJobCountsAsync();
    const pending =
      counts.waiting +
      counts.prioritized +
      counts.active +
      counts.delayed +
      counts.paused +
      counts['waiting-children'];
    if (pending === 0 && completed.size === total) return;
    await sleep(50);
  }
  throw new Error(`drain timed out at ${completed.size}/${total} completed events`);
}

describe('production torture: durable backpressure over public TCP API', () => {
  const brokers: Broker[] = [];
  const queues: Queue<Payload>[] = [];
  const workers: Worker<Payload>[] = [];
  const tempDirs: string[] = [];

  afterEach(async () => {
    for (const worker of workers.splice(0)) {
      try {
        await worker.close(true);
      } catch {
        // Best-effort cleanup after assertion failures.
      }
    }
    for (const queue of queues.splice(0)) queue.close();
    for (const broker of brokers.splice(0)) {
      try {
        stopBroker(broker);
      } catch {
        // Best-effort cleanup after assertion failures.
      }
    }
    for (const dir of tempDirs.splice(0)) rmSync(dir, { recursive: true, force: true });
  });

  test('massive durable backlog restarts and drains without loss or duplicate effects', async () => {
    const total = Number(process.env.PRODUCTION_BACKPRESSURE_JOBS ?? 5_000);
    const batchSize = 500;
    const dir = mkdtempSync(join(tmpdir(), 'bunqueue-backpressure-'));
    const dbPath = join(dir, 'broker.sqlite');
    const queueName = `backpressure-${crypto.randomUUID()}`;
    tempDirs.push(dir);

    let broker = startBroker(dbPath);
    brokers.push(broker);
    let queue = new Queue<Payload>(queueName, {
      embedded: false,
      connection: broker.connection,
      autoBatch: { enabled: false },
    });
    queues.push(queue);
    await queue.waitUntilReady();

    const ids: string[] = [];
    const enqueueStarted = performance.now();
    for (let offset = 0; offset < total; offset += batchSize) {
      const acceptedAt = Date.now();
      const jobs = await queue.addBulk(
        Array.from({ length: Math.min(batchSize, total - offset) }, (_, index) => ({
          name: 'persist-order',
          data: { sequence: offset + index, acceptedAt },
          opts: { durable: true },
        }))
      );
      ids.push(...jobs.map((job) => job.id));
    }
    const enqueueMs = performance.now() - enqueueStarted;
    expect(new Set(ids).size).toBe(total);
    const full = await queue.getJobCountsAsync();
    expect(full.waiting + full.prioritized + full.delayed).toBe(total);

    queue.close();
    queues.length = 0;
    stopBroker(broker);
    brokers.length = 0;

    broker = startBroker(dbPath);
    brokers.push(broker);
    queue = new Queue<Payload>(queueName, {
      embedded: false,
      connection: broker.connection,
      autoBatch: { enabled: false },
    });
    queues.push(queue);
    await queue.waitUntilReady();
    const recovered = await queue.getJobCountsAsync();
    expect(recovered.waiting + recovered.prioritized + recovered.delayed).toBe(total);

    const effects = new Uint8Array(total);
    const completed = new Set<string>();
    const failures: string[] = [];
    const errors: string[] = [];
    const probeLatencies: number[] = [];
    let active = 0;
    let maxActive = 0;
    let probing = true;
    const probe = (async () => {
      while (probing) {
        const started = performance.now();
        const response = await fetch(`${broker.httpUrl}/health`);
        if (!response.ok) errors.push(`health ${response.status}`);
        probeLatencies.push(performance.now() - started);
        await sleep(50);
      }
    })();

    const processor = async (job: Job<Payload>) => {
      active++;
      maxActive = Math.max(maxActive, active);
      try {
        await sleep(2);
        effects[job.data.sequence]++;
        return job.data.sequence;
      } finally {
        active--;
      }
    };
    for (let index = 0; index < 4; index++) {
      const worker = new Worker<Payload>(queueName, processor, {
        embedded: false,
        connection: broker.connection,
        concurrency: 32,
        batchSize: 250,
        pollTimeout: 100,
        useLocks: true,
        lockDuration: 10_000,
        heartbeatInterval: 1_000,
      });
      worker.on('completed', (job) => completed.add(job.id));
      worker.on('failed', (job, error) => failures.push(`${job.id}:${error.message}`));
      worker.on('error', (error) => errors.push(error.message));
      workers.push(worker);
    }
    await Promise.all(workers.map((worker) => worker.waitUntilReady()));

    const drainStarted = performance.now();
    await waitForDrain(queue, completed, total);
    const drainMs = performance.now() - drainStarted;
    probing = false;
    await probe;

    expect(failures).toEqual([]);
    expect(errors).toEqual([]);
    expect(completed.size).toBe(total);
    expect(effects.every((count) => count === 1)).toBe(true);
    expect(maxActive).toBeGreaterThan(1);
    expect(percentile(probeLatencies, 0.99)).toBeLessThan(2_000);

    await Promise.all(workers.splice(0).map((worker) => worker.close()));
    queue.close();
    queues.length = 0;
    const memory = broker.manager.getMemoryStats();
    stopBroker(broker);
    brokers.length = 0;

    const database = new Database(dbPath, { readonly: true });
    const states = database
      .query<{ state: string; count: number }, []>(
        'SELECT state, COUNT(*) AS count FROM jobs GROUP BY state ORDER BY state'
      )
      .all();
    database.close();
    expect(states).toEqual([{ state: 'completed', count: total }]);

    const dbBytes =
      statSync(dbPath).size +
      (existsSync(`${dbPath}-wal`) ? statSync(`${dbPath}-wal`).size : 0) +
      (existsSync(`${dbPath}-shm`) ? statSync(`${dbPath}-shm`).size : 0);
    console.log(
      `[production-backpressure] jobs=${total} enqueue=${enqueueMs.toFixed(0)}ms ` +
        `drain=${drainMs.toFixed(0)}ms health-p99=${percentile(probeLatencies, 0.99).toFixed(1)}ms ` +
        `rss=${(process.memoryUsage.rss() / 1024 / 1024).toFixed(1)}MiB ` +
        `db=${(dbBytes / 1024 / 1024).toFixed(1)}MiB queued=${memory.queuedTotal}`
    );
  }, 900_000);
});
