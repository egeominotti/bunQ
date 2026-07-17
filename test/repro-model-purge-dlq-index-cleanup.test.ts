import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { QueueManager } from '../src/application/queueManager';
import { purgeExpiredDlq, type DlqContext } from '../src/application/dlqManager';

let manager: QueueManager | undefined;
let directory: string | undefined;

afterEach(async () => {
  manager?.shutdown();
  if (directory) await rm(directory, { force: true, recursive: true });
  manager = undefined;
  directory = undefined;
});

test('purging DLQ removes discarded jobs from jobIndex and persistence', async () => {
  directory = await mkdtemp(join(tmpdir(), 'bunqueue-purge-dlq-'));
  const dataPath = join(directory, 'queue.db');
  manager = new QueueManager({ dataPath });
  const queue = 'repro-model-purge-dlq';
  const otherQueue = 'repro-model-purge-dlq-other';
  const job = await manager.push(queue, {
    data: { source: 'model-regression' },
    durable: true,
    priority: 2,
  });
  const otherJob = await manager.push(otherQueue, {
    data: { source: 'queue-isolation' },
    durable: true,
  });

  expect(manager.addLog(job.id, 'purged log')).toBe(true);
  expect(manager.addLog(otherJob.id, 'preserved log')).toBe(true);
  expect(await manager.discard(job.id)).toBe(true);
  expect(await manager.discard(otherJob.id)).toBe(true);
  expect(manager.getMemoryStats().jobIndex).toBe(2);
  expect(manager.getMemoryStats().jobLogs).toBe(2);
  expect(manager.getQueueJobCounts(queue).failed).toBe(1);

  expect(manager.purgeDlq(queue)).toBe(1);

  expect(await manager.getJobState(job.id)).toBe('unknown');
  expect(await manager.getJob(job.id)).toBeNull();
  expect(manager.getMemoryStats().jobIndex).toBe(1);
  expect(manager.getMemoryStats().jobLogs).toBe(1);
  expect(manager.getQueueJobCounts(queue).failed).toBe(0);
  expect(await manager.getJobState(otherJob.id)).toBe('failed');
  expect(manager.getQueueJobCounts(otherQueue).failed).toBe(1);

  manager.shutdown();
  manager = new QueueManager({ dataPath });
  expect(await manager.getJobState(job.id)).toBe('unknown');
  expect(await manager.getJob(job.id)).toBeNull();
  expect(await manager.getJobState(otherJob.id)).toBe('failed');
});

test('expired DLQ cleanup removes indexes and durable rows exactly once', async () => {
  directory = await mkdtemp(join(tmpdir(), 'bunqueue-expired-dlq-'));
  const dataPath = join(directory, 'queue.db');
  manager = new QueueManager({ dataPath });
  const queue = 'repro-model-expired-dlq';
  const job = await manager.push(queue, {
    data: { source: 'expired-model-regression' },
    durable: true,
  });

  manager.setDlqConfig(queue, { maxAge: 1 });
  expect(manager.addLog(job.id, 'expired log')).toBe(true);
  expect(await manager.discard(job.id)).toBe(true);
  await Bun.sleep(10);

  const internals = manager as unknown as {
    contextFactory: { getDlqContext(): DlqContext };
  };
  expect(purgeExpiredDlq(queue, internals.contextFactory.getDlqContext())).toBe(1);
  expect(purgeExpiredDlq(queue, internals.contextFactory.getDlqContext())).toBe(0);
  expect(await manager.getJobState(job.id)).toBe('unknown');
  expect(await manager.getJob(job.id)).toBeNull();
  expect(manager.getMemoryStats().jobIndex).toBe(0);
  expect(manager.getMemoryStats().jobLogs).toBe(0);

  manager.shutdown();
  manager = new QueueManager({ dataPath });
  expect(await manager.getJobState(job.id)).toBe('unknown');
  expect(await manager.getJob(job.id)).toBeNull();
});
