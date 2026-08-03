import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { Queue, Worker, closeAllSharedPools, shutdownManager } from '../src/client';
import { createTcpServer } from '../src/infrastructure/server/tcp';
import { waitForState } from './docs-guide-support';

const directories: string[] = [];

// `getSharedManager()` keeps the first manager it built and ignores a later
// caller's `dataPath`, so ANY earlier test file that leaves an embedded manager
// alive makes this file's queues write into that file's database instead of
// their own — the parent then reads back as `unknown`. Bun discovers test files
// in readdir order, not sorted, so which file runs first differs between macOS
// and Linux. Claiming the leak here keeps this file order-independent rather
// than depending on every other suite cleaning up after itself.
beforeEach(() => {
  shutdownManager();
});

afterEach(async () => {
  closeAllSharedPools();
  shutdownManager();
  for (const directory of directories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

function databasePath(label: string): string {
  const directory = mkdtempSync(join(tmpdir(), `bunqueue-parent-${label}-`));
  directories.push(directory);
  return join(directory, 'queue.db');
}

describe('parent option dependency recovery', () => {
  test('embedded keeps the parent dependency across a broker restart', async () => {
    const dataPath = databasePath('embedded');
    const queueName = `parent-restart-embedded-${crypto.randomUUID()}`;
    const options = { embedded: true, dataPath } as const;
    let queue = new Queue<{ role: string }>(queueName, options);
    let worker: Worker<{ role: string }, string> | null = null;

    try {
      await queue.pauseAsync();
      const parent = await queue.add('parent', { role: 'parent' }, { durable: true });
      const child = await queue.add(
        'child',
        { role: 'child' },
        { parent: { id: parent.id, queue: queueName }, durable: true }
      );

      expect(await queue.getJobState(parent.id)).toBe('waiting-children');
      expect((await queue.getJobDependencies(parent.id)).unprocessed).toEqual([
        `${queueName}:${child.id}`,
      ]);

      await queue.close();
      shutdownManager();
      queue = new Queue<{ role: string }>(queueName, options);

      expect(await queue.getJobState(parent.id)).toBe('waiting-children');
      expect((await queue.getJobDependencies(parent.id)).unprocessed).toEqual([
        `${queueName}:${child.id}`,
      ]);

      const order: string[] = [];
      worker = new Worker<{ role: string }, string>(
        queueName,
        async (job) => {
          order.push(job.data.role);
          return job.data.role;
        },
        { ...options, concurrency: 1, batchSize: 1 }
      );
      await queue.resumeAsync();
      await waitForState(queue, child.id, 'completed', 15_000);
      await waitForState(queue, parent.id, 'completed', 15_000);
      expect(order).toEqual(['child', 'parent']);
    } finally {
      await worker?.close(true).catch(() => undefined);
      try {
        await queue.close();
      } catch {
        // The first broker may already have been closed for the restart.
      }
    }
  }, 45_000);

  test('TCP keeps the parent dependency across a broker restart', async () => {
    const dataPath = databasePath('tcp');
    const queueName = `parent-restart-tcp-${crypto.randomUUID()}`;
    let manager = new QueueManager({ dataPath });
    let server = createTcpServer(manager, { hostname: '127.0.0.1', port: 0 });
    let connection = { host: '127.0.0.1', port: server.server.port, poolSize: 1 };
    let queue = new Queue<{ role: string }>(queueName, {
      connection,
      autoBatch: { enabled: false },
    });
    let worker: Worker<{ role: string }, string> | null = null;

    try {
      await queue.pauseAsync();
      const parent = await queue.add('parent', { role: 'parent' }, { durable: true });
      const child = await queue.add(
        'child',
        { role: 'child' },
        { parent: { id: parent.id, queue: queueName }, durable: true }
      );

      expect(await queue.getJobState(parent.id)).toBe('waiting-children');
      expect((await queue.getJobDependencies(parent.id)).unprocessed).toEqual([
        `${queueName}:${child.id}`,
      ]);

      await queue.close();
      closeAllSharedPools();
      server.stop();
      manager.shutdown();

      manager = new QueueManager({ dataPath });
      server = createTcpServer(manager, { hostname: '127.0.0.1', port: 0 });
      connection = { host: '127.0.0.1', port: server.server.port, poolSize: 1 };
      queue = new Queue<{ role: string }>(queueName, {
        connection,
        autoBatch: { enabled: false },
      });

      expect(await queue.getJobState(parent.id)).toBe('waiting-children');
      expect((await queue.getJobDependencies(parent.id)).unprocessed).toEqual([
        `${queueName}:${child.id}`,
      ]);

      const order: string[] = [];
      worker = new Worker<{ role: string }, string>(
        queueName,
        async (job) => {
          order.push(job.data.role);
          return job.data.role;
        },
        { connection, concurrency: 1, batchSize: 1 }
      );
      await queue.resumeAsync();
      await waitForState(queue, child.id, 'completed', 15_000);
      await waitForState(queue, parent.id, 'completed', 15_000);
      expect(order).toEqual(['child', 'parent']);
    } finally {
      await worker?.close(true).catch(() => undefined);
      try {
        await queue.close();
      } catch {
        // The first broker may already have been closed for the restart.
      }
      closeAllSharedPools();
      server.stop();
      manager.shutdown();
    }
  }, 45_000);
});
