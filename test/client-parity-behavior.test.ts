import { expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import * as native from '../src/client';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer } from '../src/infrastructure/server/tcp';

// Separate builds have nominally distinct private class fields. The declaration
// gate verifies their complete public contracts; this adapter runs one scenario.
const portable = (await import('../sdk/typescript/dist/index.js')) as unknown as typeof native;

async function lifecycle(client: typeof native) {
  const directory = mkdtempSync(join(tmpdir(), 'bunqueue-client-parity-'));
  const manager = new QueueManager({ dataPath: join(directory, 'broker.db') });
  const server = createTcpServer(manager, { hostname: '127.0.0.1', port: 0 });
  const name = `parity-${crypto.randomUUID()}`;
  const connection = { host: '127.0.0.1', port: server.server.port, poolSize: 1, pingInterval: 0 };
  const queue = new client.Queue<{ value: number }>(name, { embedded: false, connection });
  let worker: native.Worker<{ value: number }, { doubled: number }> | undefined;
  try {
    await queue.pauseAsync();
    const job = await queue.add('double', { value: 21 }, { jobId: 'stable-parity-id' });
    const duplicate = await queue.add('double', { value: 100 }, { jobId: 'stable-parity-id' });
    expect(duplicate.id).toBe(job.id);
    const before = await queue.getJobCountsAsync();
    const events: string[] = [];
    let resolveCompletion!: () => void;
    let rejectCompletion!: (error: Error) => void;
    const completed = new Promise<void>((resolve, reject) => {
      resolveCompletion = resolve;
      rejectCompletion = reject;
    });
    worker = new client.Worker(
      name,
      async (active) => {
        events.push('processor');
        await active.updateProgress(50);
        await active.log('halfway');
        return { doubled: active.data.value * 2 };
      },
      { embedded: false, connection, concurrency: 1 }
    );
    worker.on('active', () => events.push('active'));
    worker.on('progress', () => events.push('progress'));
    worker.on('completed', (_job, result) => {
      events.push('completed');
      try {
        expect(result).toEqual({ doubled: 42 });
        resolveCompletion();
      } catch (error) {
        rejectCompletion(error as Error);
      }
    });
    worker.on('error', rejectCompletion);
    await worker.waitUntilReady();
    await queue.resumeAsync();
    const timeout = setTimeout(
      () => rejectCompletion(new Error('Parity worker timed out')),
      10_000
    );
    try {
      await completed;
    } finally {
      clearTimeout(timeout);
    }
    await worker.close();
    worker = undefined;
    const stored = await queue.getJob(job.id);
    expect(stored).not.toBeNull();
    const state = await queue.getJobState(job.id);
    const counts = await queue.getJobCountsAsync();
    expect(state).toBe('completed');
    expect(counts.completed).toBe(1);
    expect(counts.active).toBe(0);
    expect(counts.waiting).toBe(0);
    expect(stored?.data).toEqual({ value: 21 });
    const logs = await queue.getJobLogs(job.id);
    expect(logs.count).toBe(1);
    expect(logs.logs[0]).toContain('halfway');
    expect(stored?.returnvalue).toEqual({ doubled: 42 });
    return {
      before,
      counts,
      state,
      events,
      logs,
      progress: stored?.progress,
      data: stored?.data,
      result: stored?.returnvalue,
    };
  } finally {
    try {
      await worker?.close();
      await queue.close();
    } finally {
      server.stop();
      manager.shutdown();
      rmSync(directory, { recursive: true, force: true });
    }
  }
}

test('published portable and native clients have the same TCP lifecycle and defaults', async () => {
  expect(await lifecycle(portable)).toEqual(await lifecycle(native));
}, 30_000);
