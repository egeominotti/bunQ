import { afterEach, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { checkExpiredLocks } from '../src/application/lockManager';
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

function openDatabasePath(name: string): string {
  directory = mkdtempSync(join(tmpdir(), `bunqueue-${name}-`));
  return join(directory, 'queue.db');
}

async function completeRemoved(queue: string, index: number): Promise<JobId> {
  const job = await manager!.push(queue, {
    data: { index },
    durable: true,
    removeOnComplete: true,
  });
  expect((await manager!.pull(queue))?.id).toBe(job.id);
  await manager!.ack(job.id);
  return job.id;
}

async function waitForState(id: JobId, expected: string): Promise<void> {
  const deadline = Date.now() + 2_000;
  while (Date.now() < deadline) {
    if ((await manager!.getJobState(id)) === expected) return;
    await Bun.sleep(5);
  }
  expect(await manager!.getJobState(id)).toBe(expected);
}

function completionIds(path: string): string[] {
  const database = new Database(path, { readonly: true });
  try {
    return database
      .query<{ job_id: string }, []>('SELECT job_id FROM dependency_completions ORDER BY sequence')
      .all()
      .map((row) => row.job_id);
  } finally {
    database.close();
  }
}

test('durable completion evidence is bounded and hydrates the newest FIFO window', async () => {
  const dataPath = openDatabasePath('dependency-completion-cap');
  manager = new QueueManager({ dataPath, maxCompletedJobs: 3 });
  const completed: JobId[] = [];
  for (let index = 0; index < 12; index++) {
    completed.push(await completeRemoved('completion-cap-source', index));
  }

  expect(completionIds(dataPath)).toEqual(completed.slice(-3).map(String));
  manager.shutdown();
  manager = new QueueManager({ dataPath, maxCompletedJobs: 3 });

  const retainedDependent = await manager.push('completion-cap-consumers', {
    data: { retained: true },
    dependsOn: [completed.at(-1)!],
    durable: true,
    priority: 1,
  });
  const expiredDependent = await manager.push('completion-cap-consumers', {
    data: { retained: false },
    dependsOn: [completed[0]],
    durable: true,
    priority: 1,
  });
  expect(await manager.getJobState(retainedDependent.id)).toBe('prioritized');
  expect(await manager.getJobState(expiredDependent.id)).toBe('waiting-children');
});

test('a promoted parent never regresses after its completion proof is evicted', async () => {
  const dataPath = openDatabasePath('dependency-completion-promoted');
  manager = new QueueManager({ dataPath, maxCompletedJobs: 3 });
  const child = await manager.push('completion-eviction-children', {
    data: { role: 'child' },
    durable: true,
    removeOnComplete: true,
  });
  const parent = await manager.push('completion-eviction-parent', {
    data: { role: 'parent' },
    dependsOn: [child.id],
    durable: true,
    priority: 1,
  });

  expect((await manager.pull('completion-eviction-children'))?.id).toBe(child.id);
  await manager.ack(child.id);
  await waitForState(parent.id, 'prioritized');
  for (let index = 0; index < 6; index++) {
    await completeRemoved('completion-eviction-fillers', index);
  }
  expect(completionIds(dataPath)).not.toContain(String(child.id));

  manager.shutdown();
  manager = new QueueManager({ dataPath, maxCompletedJobs: 3 });
  expect(await manager.getJobState(parent.id)).toBe('prioritized');
  expect(manager.count('completion-eviction-parent')).toBe(1);
});

test('obliterate removes completion evidence only for the selected queue', async () => {
  const dataPath = openDatabasePath('dependency-completion-obliterate');
  manager = new QueueManager({ dataPath, maxCompletedJobs: 10 });
  const removedA = await completeRemoved('completion-obliterate-a', 1);
  const removedB = await completeRemoved('completion-obliterate-b', 2);

  manager.obliterate('completion-obliterate-a');
  expect(completionIds(dataPath)).toEqual([String(removedB)]);

  const blocked = await manager.push('completion-obliterate-consumers', {
    data: { source: 'a' },
    dependsOn: [removedA],
    durable: true,
    priority: 1,
  });
  const ready = await manager.push('completion-obliterate-consumers', {
    data: { source: 'b' },
    dependsOn: [removedB],
    durable: true,
    priority: 1,
  });
  expect(await manager.getJobState(blocked.id)).toBe('waiting-children');
  expect(await manager.getJobState(ready.id)).toBe('prioritized');
});

test('custom ID reuse cannot erase evidence while an old consumer is unresolved', async () => {
  const dataPath = openDatabasePath('dependency-completion-reuse-guard');
  manager = new QueueManager({ dataPath, dependencyCheckMs: 60_000 });
  const child = await manager.push('completion-reuse-guard', {
    customId: 'completion-reuse-guard-id',
    data: { generation: 1 },
    durable: true,
    removeOnComplete: true,
  });
  const oldConsumer = await manager.push('completion-reuse-guard', {
    data: { generation: 1, role: 'consumer' },
    dependsOn: [child.id],
    durable: true,
    priority: 1,
  });
  (
    manager as unknown as {
      scheduleDependencyFlush: () => void;
    }
  ).scheduleDependencyFlush = () => {
    // Intentionally defer the flush to exercise the durable proof.
  };

  expect((await manager.pull('completion-reuse-guard'))?.id).toBe(child.id);
  await manager.ack(child.id);
  await expect(
    manager.push('completion-reuse-guard', {
      customId: String(child.id),
      data: { generation: 2 },
      durable: true,
    })
  ).rejects.toThrow('still has unresolved dependency consumers');

  const context = (
    manager as unknown as {
      contextFactory: {
        getBackgroundContext: () => Parameters<typeof processPendingDependencies>[0];
      };
    }
  ).contextFactory.getBackgroundContext();
  await processPendingDependencies(context);
  expect(await manager.getJobState(oldConsumer.id)).toBe('prioritized');
  expect(
    await manager.push('completion-reuse-guard', {
      customId: String(child.id),
      data: { generation: 2 },
      durable: true,
    })
  ).toBeDefined();
});

test('late ACK of a stall-requeued removed job persists completion evidence', async () => {
  const dataPath = openDatabasePath('dependency-completion-late-ack');
  manager = new QueueManager({ dataPath, dependencyCheckMs: 60_000 });
  const child = await manager.push('completion-late-ack-child', {
    data: { role: 'child' },
    durable: true,
    removeOnComplete: true,
  });
  const parent = await manager.push('completion-late-ack-parent', {
    data: { role: 'parent' },
    dependsOn: [child.id],
    durable: true,
    priority: 1,
  });
  const lease = await manager.pullWithLock('completion-late-ack-child', 'worker', 0, 5);
  expect(lease.job?.id).toBe(child.id);
  await Bun.sleep(15);
  const lockContext = (
    manager as unknown as {
      contextFactory: { getLockContext: () => Parameters<typeof checkExpiredLocks>[0] };
    }
  ).contextFactory.getLockContext();
  await checkExpiredLocks(lockContext);
  (
    manager as unknown as {
      scheduleDependencyFlush: () => void;
    }
  ).scheduleDependencyFlush = () => {
    // Intentionally defer the flush to exercise restart recovery.
  };

  await manager.ack(child.id, { late: true }, lease.token!);
  expect(await manager.getJobState(parent.id)).toBe('waiting-children');
  manager.shutdown();
  manager = new QueueManager({ dataPath });
  expect(await manager.getJobState(child.id)).toBe('unknown');
  expect(await manager.getJobState(parent.id)).toBe('prioritized');
});

test('failed completion transaction never deletes the job without durable evidence', async () => {
  const dataPath = openDatabasePath('dependency-completion-atomic-failure');
  manager = new QueueManager({ dataPath });
  const child = await manager.push('completion-atomic-child', {
    data: { role: 'child' },
    durable: true,
    removeOnComplete: true,
  });
  const parent = await manager.push('completion-atomic-parent', {
    data: { role: 'parent' },
    dependsOn: [child.id],
    durable: true,
    priority: 1,
  });
  expect((await manager.pull('completion-atomic-child'))?.id).toBe(child.id);

  const fault = new Database(dataPath);
  fault.run(`
    CREATE TRIGGER reject_dependency_completion
    BEFORE INSERT ON dependency_completions
    BEGIN
      SELECT RAISE(ABORT, 'injected completion failure');
    END
  `);
  fault.close();
  const completedBefore = manager.getStats().completed;
  await expect(manager.ack(child.id)).rejects.toThrow('injected completion failure');

  const database = new Database(dataPath);
  try {
    const job = database
      .query<{ state: string }, [string]>('SELECT state FROM jobs WHERE id = ?')
      .get(String(child.id));
    const proof = database
      .query<{ count: number }, [string]>(
        'SELECT COUNT(*) AS count FROM dependency_completions WHERE job_id = ?'
      )
      .get(String(child.id));
    expect(job?.state).toBe('active');
    expect(proof?.count).toBe(0);
    database.run('DROP TRIGGER reject_dependency_completion');
  } finally {
    database.close();
  }
  expect(manager.getStats().completed).toBe(completedBefore);

  manager.shutdown();
  manager = new QueueManager({ dataPath });
  expect(await manager.getJobState(child.id)).not.toBe('completed');
  expect(await manager.getJobState(parent.id)).toBe('waiting-children');
});
