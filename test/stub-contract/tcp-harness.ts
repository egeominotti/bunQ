import { Queue } from '../../src/client';
import { QueueManager } from '../../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../../src/infrastructure/server/tcp';

export interface TcpHarness {
  manager: QueueManager;
  server: TcpServer;
  port: number;
  queues: Queue<unknown>[];
}

export function startTcpHarness(): TcpHarness {
  const manager = new QueueManager();
  const server = createTcpServer(manager, { hostname: '127.0.0.1', port: 0 });
  return { manager, server, port: server.server.port, queues: [] };
}

export function makeTcpQueue<T>(harness: TcpHarness, name: string): Queue<T> {
  const queue = new Queue<T>(name, {
    embedded: false,
    connection: {
      host: '127.0.0.1',
      port: harness.port,
      poolSize: 1,
      pingInterval: 0,
    },
    autoBatch: { enabled: false },
  });
  harness.queues.push(queue as Queue<unknown>);
  return queue;
}

export async function failJob<T>(
  harness: TcpHarness,
  queue: Queue<T>,
  name: string,
  data: T
): Promise<string> {
  const added = await queue.add('fail', data, { attempts: 1, backoff: 0 });
  const pulled = await harness.manager.pull(name);
  if (!pulled) throw new Error(`Expected a job in ${name}`);
  await harness.manager.fail(pulled.id, 'stub-contract failure');
  return added.id;
}

export async function completeJob<T>(
  harness: TcpHarness,
  queue: Queue<T>,
  name: string,
  data: T
): Promise<string> {
  const added = await queue.add('complete', data);
  const pulled = await harness.manager.pull(name);
  if (!pulled) throw new Error(`Expected a job in ${name}`);
  await harness.manager.ack(pulled.id, { completed: true });
  return added.id;
}

export async function closeTcpHarness(harness: TcpHarness | null): Promise<void> {
  if (!harness) return;
  for (const queue of harness.queues) {
    try {
      await queue.close();
    } catch {
      // Best-effort cleanup for a regression harness.
    }
  }
  try {
    harness.server.stop();
  } finally {
    harness.manager.shutdown();
  }
  await Bun.sleep(25);
}
