import { afterEach, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { processPendingDependencies } from '../src/application/dependencyProcessor';
import type { JobId } from '../src/domain/types/job';

let manager: QueueManager | null = null;
let directory: string | null = null;

afterEach(() => {
  manager?.shutdown();
  manager = null;
  if (directory) rmSync(directory, { force: true, recursive: true });
  directory = null;
});

function createManager(
  name: string,
  maxCompletedJobs: number = 3
): {
  dataPath: string;
  manager: QueueManager;
} {
  directory = mkdtempSync(join(tmpdir(), `bunqueue-dependency-pinning-${name}-`));
  const dataPath = join(directory, 'queue.db');
  manager = new QueueManager({ dataPath, maxCompletedJobs, dependencyCheckMs: 60_000 });
  return { dataPath, manager };
}

function suppressAutomaticFlush(target: QueueManager): void {
  (
    target as unknown as {
      scheduleDependencyFlush: () => void;
    }
  ).scheduleDependencyFlush = () => {
    // Intentionally defer the flush to simulate a crash boundary.
  };
}

async function flushDependencies(target: QueueManager): Promise<void> {
  const context = (
    target as unknown as {
      contextFactory: {
        getBackgroundContext: () => Parameters<typeof processPendingDependencies>[0];
      };
    }
  ).contextFactory.getBackgroundContext();
  await processPendingDependencies(context);
}

async function completeRemoved(target: QueueManager, queue: string, index: number): Promise<JobId> {
  const job = await target.push(queue, {
    data: { index },
    durable: true,
    removeOnComplete: true,
  });
  expect((await target.pull(queue))?.id).toBe(job.id);
  await target.ack(job.id);
  return job.id;
}

test('ACKB larger than the FIFO cap retains proofs needed by an unresolved parent', async () => {
  const created = createManager('batch');
  const { dataPath } = created;
  manager = created.manager;
  const childQueue = 'dependency-pinning-children';
  const parentQueue = 'dependency-pinning-parent';

  const children = await Promise.all(
    Array.from({ length: 6 }, (_, index) =>
      manager!.push(childQueue, {
        data: { index },
        durable: true,
        removeOnComplete: true,
      })
    )
  );
  const parent = await manager.push(parentQueue, {
    data: { role: 'parent' },
    dependsOn: children.map((child) => child.id),
    durable: true,
    priority: 1,
  });
  suppressAutomaticFlush(manager);

  const pulled = await manager.pullBatch(childQueue, children.length);
  await manager.ackBatch(pulled.map((job) => job.id));
  expect(await manager.getJobState(parent.id)).toBe('waiting-children');

  manager.shutdown();
  manager = new QueueManager({ dataPath, maxCompletedJobs: 3 });
  expect(await manager.getJobState(parent.id)).toBe('prioritized');
  expect((await manager.pull(parentQueue))?.id).toBe(parent.id);
});

test('a late parent pins an already-completed dependency while another remains pending', async () => {
  manager = createManager('late-parent').manager;
  const completed = await completeRemoved(manager, 'late-parent-source', 0);
  const pending = await manager.push('late-parent-source', {
    data: { pending: true },
    durable: true,
    removeOnComplete: true,
  });
  const parent = await manager.push('late-parent-consumer', {
    data: { parent: true },
    dependsOn: [completed, pending.id],
    durable: true,
    priority: 1,
  });
  expect(await manager.getJobState(parent.id)).toBe('waiting-children');

  for (let index = 0; index < 8; index++) {
    await completeRemoved(manager, 'late-parent-fillers', index);
  }
  expect((await manager.pull('late-parent-source'))?.id).toBe(pending.id);
  await manager.ack(pending.id);
  await flushDependencies(manager);

  expect(await manager.getJobState(parent.id)).toBe('prioritized');
});

test('a shared proof stays pinned until the final waiting parent releases it', async () => {
  const { dataPath } = createManager('shared-parent');
  const shared = await manager!.push('shared-source', {
    data: { shared: true },
    durable: true,
    removeOnComplete: true,
  });
  const remainingA = await manager!.push('shared-source-a', {
    data: { branch: 'a' },
    durable: true,
    removeOnComplete: true,
  });
  const remainingB = await manager!.push('shared-source-b', {
    data: { branch: 'b' },
    durable: true,
    removeOnComplete: true,
  });
  const parentA = await manager!.push('shared-parent-a', {
    data: { branch: 'a' },
    dependsOn: [shared.id, remainingA.id],
    durable: true,
    priority: 1,
  });
  const parentB = await manager!.push('shared-parent-b', {
    data: { branch: 'b' },
    dependsOn: [shared.id, remainingB.id],
    durable: true,
    priority: 1,
  });

  expect((await manager!.pull('shared-source'))?.id).toBe(shared.id);
  await manager!.ack(shared.id);
  expect((await manager!.pull('shared-source-a'))?.id).toBe(remainingA.id);
  await manager!.ack(remainingA.id);
  await flushDependencies(manager!);
  expect(await manager!.getJobState(parentA.id)).toBe('prioritized');
  expect(await manager!.getJobState(parentB.id)).toBe('waiting-children');

  for (let index = 0; index < 8; index++) {
    await completeRemoved(manager!, 'shared-fillers', index);
  }
  manager!.shutdown();
  manager = new QueueManager({ dataPath, maxCompletedJobs: 3 });

  expect((await manager.pull('shared-source-b'))?.id).toBe(remainingB.id);
  await manager.ack(remainingB.id);
  await flushDependencies(manager);
  expect(await manager.getJobState(parentB.id)).toBe('prioritized');
});

test('recovery pins legacy evidence before applying a smaller retention cap', async () => {
  const { dataPath } = createManager('reduced-cap', 10);
  const children = await Promise.all(
    Array.from({ length: 6 }, (_, index) =>
      manager!.push('reduced-cap-source', {
        data: { index },
        durable: true,
        removeOnComplete: true,
      })
    )
  );
  const parent = await manager!.push('reduced-cap-parent', {
    data: { parent: true },
    dependsOn: children.map((child) => child.id),
    durable: true,
    priority: 1,
  });
  suppressAutomaticFlush(manager!);
  const pulled = await manager!.pullBatch('reduced-cap-source', children.length);
  await manager!.ackBatch(pulled.map((job) => job.id));
  manager!.shutdown();

  const database = new Database(dataPath);
  database.run('UPDATE dependency_completions SET pinned = 0');
  database.close();
  manager = new QueueManager({ dataPath, maxCompletedJobs: 3 });

  expect(await manager.getJobState(parent.id)).toBe('prioritized');
  const verification = new Database(dataPath, { readonly: true });
  const unpinned = verification
    .query<{ count: number }, []>(
      'SELECT COUNT(*) AS count FROM dependency_completions WHERE pinned = 0'
    )
    .get();
  verification.close();
  expect(unpinned?.count).toBeLessThanOrEqual(3);
});

test('obliterating a waiting parent releases its child completion pin', async () => {
  const { dataPath } = createManager('parent-obliterate');
  const child = await manager!.push('parent-obliterate-source', {
    data: { child: true },
    durable: true,
    removeOnComplete: true,
  });
  await manager!.push('parent-obliterate-consumer', {
    data: { parent: true },
    dependsOn: [child.id],
    durable: true,
    priority: 1,
  });
  suppressAutomaticFlush(manager!);
  expect((await manager!.pull('parent-obliterate-source'))?.id).toBe(child.id);
  await manager!.ack(child.id);

  manager!.obliterate('parent-obliterate-consumer');
  const database = new Database(dataPath, { readonly: true });
  const proof = database
    .query<{ pinned: number }, [string]>(
      'SELECT pinned FROM dependency_completions WHERE job_id = ?'
    )
    .get(String(child.id));
  database.close();
  expect(proof?.pinned).toBe(0);
});
