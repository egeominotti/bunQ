import { afterEach, expect, test } from 'bun:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { jobId } from '../src/domain/types/job';

let manager: QueueManager | undefined;
let directory: string | undefined;

afterEach(async () => {
  manager?.shutdown();
  if (directory) await rm(directory, { force: true, recursive: true });
  manager = undefined;
  directory = undefined;
});

test('removal racing with discard cannot resurrect the DLQ entry after restart', async () => {
  directory = await mkdtemp(join(tmpdir(), 'bunqueue-remove-dlq-race-'));
  const dataPath = join(directory, 'queue.db');
  const queue = 'remove-dlq-race';
  manager = new QueueManager({ dataPath });
  const job = await manager.push(queue, {
    data: { generation: 1 },
    durable: true,
  });

  const discard = manager.discard(job.id);
  for (let attempt = 0; attempt < 20 && manager.getDlq(queue).length === 0; attempt++) {
    await Promise.resolve();
  }

  expect(manager.getDlq(queue).map((entry) => entry.id)).toEqual([job.id]);
  expect(manager.removeDlqJob(queue, job.id)).toBe(true);
  await discard;
  expect(manager.getDlq(queue)).toEqual([]);

  manager.shutdown();
  manager = new QueueManager({ dataPath });
  expect(manager.getDlq(queue)).toEqual([]);
  expect(await manager.getJobState(job.id)).toBe('unknown');
  expect(await manager.getJob(job.id)).toBeNull();
});

test('one removal clears every recovered DLQ row for the same queue and job ID', async () => {
  directory = await mkdtemp(join(tmpdir(), 'bunqueue-remove-dlq-duplicates-'));
  const dataPath = join(directory, 'queue.db');
  const queue = 'remove-dlq-duplicates';
  manager = new QueueManager({ dataPath });
  const job = await manager.push(queue, {
    data: { source: 'duplicate-recovery' },
    durable: true,
  });
  expect(await manager.discard(job.id)).toBe(true);

  const entry = manager.getDlqEntries(queue)[0];
  expect(entry).toBeDefined();
  const storage = (
    manager as unknown as {
      storage: { saveDlqEntry(value: NonNullable<typeof entry>): void };
    }
  ).storage;
  storage.saveDlqEntry(entry!);

  manager.shutdown();
  manager = new QueueManager({ dataPath });
  expect(manager.getDlq(queue).map((candidate) => candidate.id)).toEqual([job.id, job.id]);

  expect(manager.removeDlqJob(queue, job.id)).toBe(true);
  expect(manager.getDlq(queue)).toEqual([]);
  expect(manager.getQueueJobCounts(queue).failed).toBe(0);
  expect(await manager.getJobState(job.id)).toBe('unknown');
  expect(manager.removeDlqJob(queue, job.id)).toBe(false);

  manager.shutdown();
  manager = new QueueManager({ dataPath });
  expect(manager.getDlq(queue)).toEqual([]);
  expect(await manager.getJobState(job.id)).toBe('unknown');
});

test('permanent removal releases custom-ID and dependency-result ownership', async () => {
  manager = new QueueManager();
  const queue = 'remove-dlq-ownership';
  const dependency = await manager.push(queue, { data: { dependency: true } });
  const pulledDependency = await manager.pull(queue);
  expect(pulledDependency?.id).toBe(dependency.id);
  await manager.ack(dependency.id, { value: 42 });

  const dependent = await manager.push(queue, {
    customId: 'remove-dlq-owned-id',
    data: { dependent: true },
    dependsOn: [dependency.id],
  });
  const internals = manager as unknown as {
    customIdMap: { set(id: string, value: typeof dependent.id): void };
    dependencyResults: {
      hasConsumers(id: typeof dependency.id): boolean;
      registerConsumer(id: typeof dependent.id, dependencies: (typeof dependent.id)[]): void;
    };
  };
  const dependencyResults = (
    manager as unknown as {
      dependencyResults: typeof internals.dependencyResults;
    }
  ).dependencyResults;
  expect(dependencyResults.hasConsumers(dependency.id)).toBe(true);
  expect(manager.getMemoryStats().customIdMap).toBe(1);

  expect(await manager.discard(dependent.id)).toBe(true);
  dependencyResults.registerConsumer(dependent.id, [dependency.id]);
  internals.customIdMap.set('remove-dlq-owned-id', dependent.id);
  expect(dependencyResults.hasConsumers(dependency.id)).toBe(true);
  expect(manager.getMemoryStats().customIdMap).toBe(1);
  expect(manager.removeDlqJob(queue, dependent.id)).toBe(true);

  expect(dependencyResults.hasConsumers(dependency.id)).toBe(false);
  expect(manager.getMemoryStats().customIdMap).toBe(0);
});

test('permanent removal deletes parent-owned flow-failure records', async () => {
  directory = await mkdtemp(join(tmpdir(), 'bunqueue-remove-dlq-flow-failure-'));
  const dataPath = join(directory, 'queue.db');
  const queue = 'remove-dlq-flow-failure';
  manager = new QueueManager({ dataPath });
  const parent = await manager.push(queue, {
    data: { parent: true },
    durable: true,
  });
  expect(await manager.discard(parent.id)).toBe(true);

  const storage = (
    manager as unknown as {
      storage: {
        saveFlowFailure(record: {
          parentId: typeof parent.id;
          childId: typeof parent.id;
          childQueue: string;
          mode: 'fail';
          error: string;
          createdAt: number;
        }): void;
        loadFlowFailures(): Array<{ parentId: typeof parent.id }>;
      };
    }
  ).storage;
  storage.saveFlowFailure({
    parentId: parent.id,
    childId: jobId('removed-flow-child'),
    childQueue: queue,
    mode: 'fail',
    error: 'terminal child',
    createdAt: Date.now(),
  });
  expect(storage.loadFlowFailures()).toHaveLength(1);

  expect(manager.removeDlqJob(queue, parent.id)).toBe(true);
  expect(storage.loadFlowFailures()).toEqual([]);
});

test('a storage failure leaves the DLQ entry and indexes authoritative', async () => {
  directory = await mkdtemp(join(tmpdir(), 'bunqueue-remove-dlq-storage-failure-'));
  const dataPath = join(directory, 'queue.db');
  const queue = 'remove-dlq-storage-failure';
  manager = new QueueManager({ dataPath });
  const job = await manager.push(queue, { data: { durable: true }, durable: true });
  expect(await manager.discard(job.id)).toBe(true);

  const internals = manager as unknown as {
    jobLogs: Map<typeof job.id, unknown>;
    jobResults: Map<typeof job.id, unknown>;
    storage: { purgeDlqEntries(...args: unknown[]): void };
  };
  internals.jobLogs.set(job.id, ['retained log']);
  internals.jobResults.set(job.id, { retained: true });
  const purgeDlqEntries = internals.storage.purgeDlqEntries;
  internals.storage.purgeDlqEntries = () => {
    throw new Error('forced SQLite failure');
  };

  try {
    expect(() => manager!.removeDlqJob(queue, job.id)).toThrow('forced SQLite failure');
  } finally {
    internals.storage.purgeDlqEntries = purgeDlqEntries;
  }
  expect(manager.getDlq(queue).map((entry) => entry.id)).toEqual([job.id]);
  expect(await manager.getJobState(job.id)).toBe('failed');
  expect(internals.jobLogs.has(job.id)).toBe(true);
  expect(internals.jobResults.has(job.id)).toBe(true);

  manager.shutdown();
  manager = new QueueManager({ dataPath });
  expect(manager.getDlq(queue).map((entry) => entry.id)).toEqual([job.id]);
});
