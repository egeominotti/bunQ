/**
 * Executable proof for /guide/queue/ ("Queue API: Add and Manage Jobs in Bun").
 *
 * Runs the page's own snippets: constructor modes, typed queues,
 * defaultJobOptions, and the documented TCP connection options.
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../../src/client';
import { QueueManager } from '../../src/application/queueManager';
import { createTcpServer } from '../../src/infrastructure/server/tcp';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  type CoreE2eHarness,
  MODES,
  closeHarness,
  startHarness,
  waitForState,
  waitUntil,
} from '../docs-guide-support';

let harness: CoreE2eHarness | null = null;

afterEach(async () => {
  await closeHarness(harness);
  harness = null;
});

interface TaskData {
  userId: number;
  action: string;
}

for (const mode of MODES) {
  describe(`queue guide · overview [${mode}]`, () => {
    test('create a queue and add a job', async () => {
      harness = await startHarness('overview', mode);
      const queue = harness.queue('my-queue');

      const job = await queue.add('job-name', { key: 'value' });

      expect(job.id).toBeString();
      expect(job.name).toBe('job-name');
      expect(job.data).toEqual({ key: 'value' });
      expect(await queue.getJobState(job.id)).toBe('waiting');
    });

    test('typed queue round-trips the declared data shape', async () => {
      harness = await startHarness('overview', mode);
      const typedQueue = harness.queue<TaskData>('tasks');

      const job = await typedQueue.add('sync', { userId: 7, action: 'refresh' });
      expect(job.data).toEqual({ userId: 7, action: 'refresh' });

      const fetched = await typedQueue.getJob(job.id);
      expect(fetched?.data.userId).toBe(7);
      expect(fetched?.data.action).toBe('refresh');
    });

    test('getJob() returns exactly the data that was added', async () => {
      harness = await startHarness('overview', mode);
      const typedQueue = harness.queue<TaskData>('tasks');

      const job = await typedQueue.add('sync', { userId: 7, action: 'refresh' });

      expect((await typedQueue.getJob(job.id))?.data).toEqual({
        userId: 7,
        action: 'refresh',
      });
    });

    test('the job name stays separate from a user data name field on every waiting read', async () => {
      harness = await startHarness('overview', mode);
      const typedQueue = harness.queue<{ name: string; userId: number }>('named-tasks');
      const expectedData = { name: 'customer-visible-name', userId: 7 };

      const added = await typedQueue.add('internal-job-name', expectedData, { durable: true });
      const fetched = await typedQueue.getJob(added.id);
      const allAsync = await typedQueue.getJobsAsync({ state: 'waiting', end: -1 });
      const waitingAsync = await typedQueue.getWaitingAsync(0, -1);

      for (const job of [added, fetched, allAsync[0], waitingAsync[0]]) {
        expect(job?.name).toBe('internal-job-name');
        expect(job?.data).toEqual(expectedData);
      }

      if (mode === 'embedded') {
        for (const job of [
          typedQueue.getJobs({ state: 'waiting', end: -1 })[0],
          typedQueue.getWaiting(0, -1)[0],
        ]) {
          expect(job?.name).toBe('internal-job-name');
          expect(job?.data).toEqual(expectedData);
        }
      }
    });

    test('defaultJobOptions apply to every job added to the queue', async () => {
      harness = await startHarness('overview', mode);
      const emailQueue = harness.queue('emails', {
        defaultJobOptions: { attempts: 3, backoff: 1000, removeOnComplete: true },
      });

      const first = await emailQueue.add('send', { to: 'a@example.com' });
      const second = await emailQueue.add('send', { to: 'b@example.com' });

      for (const id of [first.id, second.id]) {
        const job = await emailQueue.getJob(id);
        expect(job?.opts.attempts).toBe(3);
        expect(job?.opts.backoff).toBe(1000);
        expect(job?.opts.removeOnComplete).toBe(true);
      }

      // removeOnComplete from the queue default is honored by the worker path.
      harness.worker(emailQueue.name, async () => ({ sent: true }));
      await waitUntil(
        async () => (await emailQueue.getJob(first.id)) === null,
        'queue-level removeOnComplete to drop the finished job'
      );
    });

    test('a per-job option overrides the queue default', async () => {
      harness = await startHarness('overview', mode);
      const emailQueue = harness.queue('emails', {
        defaultJobOptions: { attempts: 3, removeOnComplete: true },
      });

      const job = await emailQueue.add('send', { to: 'c@example.com' }, { attempts: 9 });

      const fetched = await emailQueue.getJob(job.id);
      expect(fetched?.opts.attempts).toBe(9);
      expect(fetched?.opts.removeOnComplete).toBe(true);
    });
  });
}

describe('queue guide · overview [tcp connection options]', () => {
  test('host, port, token and poolSize are accepted by a token-protected server', async () => {
    const dataDir = mkdtempSync(join(tmpdir(), 'bunqueue-docs-overview-auth-'));
    const manager = new QueueManager({ dataPath: join(dataDir, 'queue.db') });
    const server = createTcpServer(manager, {
      hostname: '127.0.0.1',
      port: 0,
      authTokens: ['secret-token'],
    });
    const name = `docs-overview-auth-${crypto.randomUUID()}`;
    const authed = new Queue(name, {
      embedded: false,
      connection: {
        host: '127.0.0.1',
        port: server.server.port,
        token: 'secret-token',
        poolSize: 4,
      },
      autoBatch: { enabled: false },
    });
    const rejected = new Queue(name, {
      embedded: false,
      connection: { host: '127.0.0.1', port: server.server.port, poolSize: 1 },
      autoBatch: { enabled: false },
    });

    try {
      const job = await authed.add('task', { ok: true });
      expect(await authed.getJobState(job.id)).toBe('waiting');

      // Same server, no token: the documented AUTH_TOKENS gate rejects the add.
      await expect(rejected.add('task', { ok: false })).rejects.toThrow();
    } finally {
      await authed.close();
      await rejected.close();
      server.stop();
      manager.shutdown();
      rmSync(dataDir, { recursive: true, force: true });
    }
  });

  test('a Worker in the other mode does not consume the queue', async () => {
    const serverDir = mkdtempSync(join(tmpdir(), 'bunqueue-docs-overview-mode-srv-'));
    const workerDir = mkdtempSync(join(tmpdir(), 'bunqueue-docs-overview-mode-wrk-'));
    const manager = new QueueManager({ dataPath: join(serverDir, 'queue.db') });
    const server = createTcpServer(manager, { hostname: '127.0.0.1', port: 0 });
    const name = `docs-overview-mode-${crypto.randomUUID()}`;
    const tcpQueue = new Queue(name, {
      embedded: false,
      connection: { host: '127.0.0.1', port: server.server.port, poolSize: 1 },
      autoBatch: { enabled: false },
    });
    let processed = 0;
    const embeddedWorker = new Worker(
      name,
      async () => {
        processed++;
        return true;
      },
      { embedded: true, dataPath: join(workerDir, 'queue.db') }
    );

    try {
      await embeddedWorker.waitUntilReady();
      const job = await tcpQueue.add('task', { value: 1 }, { durable: true });
      await Bun.sleep(300);

      expect(processed).toBe(0);
      expect(await tcpQueue.getJobState(job.id)).toBe('waiting');

      // The same-mode worker does consume it.
      const tcpWorker = new Worker(name, async () => true, {
        embedded: false,
        connection: { host: '127.0.0.1', port: server.server.port, poolSize: 1 },
      });
      try {
        await waitForState(tcpQueue, job.id, 'completed');
      } finally {
        await tcpWorker.close(true);
      }
    } finally {
      await embeddedWorker.close(true);
      await tcpQueue.close();
      server.stop();
      manager.shutdown();
      shutdownManager();
      rmSync(serverDir, { recursive: true, force: true });
      rmSync(workerDir, { recursive: true, force: true });
    }
  });
});
