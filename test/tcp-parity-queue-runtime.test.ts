import { expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer } from '../src/infrastructure/server/tcp';
import { getSharedManager, shutdownManager } from '../src/client/manager';
import { QueueGroup } from '../src/client/queueGroup';

test('TCP DLQ and QueueGroup reads never initialize an embedded client database', async () => {
  const root = resolve(import.meta.dir, '..');
  const directory = mkdtempSync(join(tmpdir(), 'bunqueue-tcp-dlq-runtime-'));
  const manager = new QueueManager({ dataPath: join(directory, 'broker.db') });
  const server = createTcpServer(manager, { hostname: '127.0.0.1', port: 0 });
  const queueName = `dlq-runtime-${crypto.randomUUID()}`;
  let child: ReturnType<typeof Bun.spawn> | undefined;
  try {
    const job = await manager.push(queueName, {
      name: 'failed-job',
      data: { value: 7 },
      maxAttempts: 1,
    });
    await manager.pull(queueName);
    await manager.fail(job.id, 'expected failure');
    const script = `
      import assert from 'node:assert/strict';
      import { existsSync } from 'node:fs';
      import { Queue } from ${JSON.stringify(resolve(root, 'src/client/queue/queue.ts'))};
      import { QueueGroup } from ${JSON.stringify(resolve(root, 'src/client/queueGroup.ts'))};
      import { shutdownManager } from ${JSON.stringify(resolve(root, 'src/client/manager.ts'))};
      const queue = new Queue(${JSON.stringify(queueName)}, {
        embedded: false,
        autoBatch: { enabled: false },
        connection: { host: '127.0.0.1', port: ${server.server.port}, poolSize: 1 },
      });
      const group = new QueueGroup('remote-only');
      const grouped = group.getQueue('registered', {
        embedded: false,
        connection: { host: '127.0.0.1', port: ${server.server.port}, poolSize: 1 },
      });
      try {
        assert.deepEqual(await group.listQueuesAsync(), ['registered']);
        const entries = await queue.getDlqAsync();
        assert.equal(entries.length, 1);
        assert.equal(entries[0].job.id, ${JSON.stringify(String(job.id))});
        assert.equal(entries[0].job.data.value, 7);
        assert.equal(await entries[0].job.getState(), 'failed');
        assert.equal((await queue.getDlqStatsAsync()).total, 1);
        assert.equal(existsSync(process.env.BUNQUEUE_DATA_PATH), false,
          'TCP DLQ access created an embedded database');
      } finally {
        queue.close();
        grouped.close();
        shutdownManager();
      }
    `;
    child = Bun.spawn([process.execPath, '--eval', script], {
      env: {
        PATH: process.env.PATH,
        BUNQUEUE_EMBEDDED: '0',
        BUNQUEUE_DATA_PATH: join(directory, 'forbidden-client.db'),
        LOG_LEVEL: 'error',
      },
      stdout: 'pipe',
      stderr: 'pipe',
    });
    const [exitCode, stdout, stderr] = await Promise.all([
      child.exited,
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
    ]);
    expect({ exitCode, stderr, stdout }).toEqual({ exitCode: 0, stderr: '', stdout: '' });
    expect(manager.getDlqStats(queueName).total).toBe(1);
  } finally {
    if (child?.exitCode === null) {
      child.kill();
      await child.exited;
    }
    server.stop();
    manager.shutdown();
    rmSync(directory, { recursive: true, force: true });
  }
}, 20_000);

test('QueueGroup async listing preserves queues from an existing embedded manager', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'bunqueue-group-discovery-'));
  const group = new QueueGroup(`discovery-${crypto.randomUUID()}`);
  const manager = getSharedManager(join(directory, 'embedded.db'));
  const registered = group.getQueue('registered', { embedded: true });
  try {
    await manager.push(`${group.prefix}external`, { name: 'external', data: {} });
    await manager.push('other:external', { name: 'other', data: {} });
    expect(await group.listQueuesAsync()).toEqual(['external', 'registered']);
  } finally {
    registered.close();
    shutdownManager();
    rmSync(directory, { recursive: true, force: true });
  }
});
