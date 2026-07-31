import { afterEach, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import type { JobId } from '../src/domain/types/job';

let manager: QueueManager | null = null;
let directory: string | null = null;

afterEach(() => {
  manager?.shutdown();
  manager = null;
  if (directory) rmSync(directory, { force: true, recursive: true });
  directory = null;
});

async function untilState(id: JobId, expected: string): Promise<void> {
  const deadline = Date.now() + 2_000;
  while (Date.now() < deadline) {
    if ((await manager!.getJobState(id)) === expected) return;
    await Bun.sleep(5);
  }
  expect(await manager!.getJobState(id)).toBe(expected);
}

test('optimized batch ACK persists dependency-parent promotion before recovery', async () => {
  directory = mkdtempSync(join(tmpdir(), 'bunqueue-model-flow-batch-parent-'));
  const dataPath = join(directory, 'queue.db');
  const queue = 'model-flow-batch-parent';
  manager = new QueueManager({ dataPath });

  const children = await Promise.all(
    Array.from({ length: 5 }, (_, index) =>
      manager!.push(queue, {
        data: { index, role: 'child' },
        durable: true,
        priority: 2,
      })
    )
  );
  const parent = await manager.push(queue, {
    data: { role: 'parent' },
    dependsOn: children.map((child) => child.id),
    durable: true,
    priority: 1,
  });

  const pulled = await manager.pullBatch(queue, children.length);
  expect(new Set(pulled.map((job) => job.id))).toEqual(new Set(children.map((job) => job.id)));
  await manager.ackBatch(pulled.map((job) => job.id));
  await untilState(parent.id, 'prioritized');
  expect(manager.count(queue)).toBe(1);

  const database = new Database(dataPath, { readonly: true });
  try {
    const row = database
      .query<{ state: string }, [string]>('SELECT state FROM jobs WHERE id = ?')
      .get(String(parent.id));
    expect(row?.state).toBe('prioritized');
  } finally {
    database.close();
  }

  manager.shutdown();
  manager = new QueueManager({ dataPath });
  expect(await manager.getJobState(parent.id)).toBe('prioritized');
  expect(manager.count(queue)).toBe(1);
});

test('removeOnComplete dependency survives a crash before the dependency flush', async () => {
  directory = mkdtempSync(join(tmpdir(), 'bunqueue-flow-removed-dependency-crash-'));
  const dataPath = join(directory, 'queue.db');
  const queue = 'flow-removed-dependency-crash';
  manager = new QueueManager({ dataPath, dependencyCheckMs: 60_000 });

  const child = await manager.push(queue, {
    data: { role: 'child' },
    durable: true,
    priority: 2,
    removeOnComplete: true,
  });
  const parent = await manager.push(queue, {
    data: { role: 'parent' },
    dependsOn: [child.id],
    durable: true,
    priority: 1,
  });

  // Model a process crash after the durable ACK but before the event-driven
  // dependency processor runs. The restart path must still have enough durable
  // evidence to prove that the removed child completed.
  (
    manager as unknown as {
      scheduleDependencyFlush: () => void;
    }
  ).scheduleDependencyFlush = () => {
    // Intentionally defer the flush to simulate the crash window.
  };

  expect((await manager.pull(queue))?.id).toBe(child.id);
  await manager.ack(child.id);
  expect(await manager.getJobState(child.id)).toBe('unknown');
  expect(await manager.getJobState(parent.id)).toBe('waiting-children');

  manager.shutdown();
  manager = new QueueManager({ dataPath });

  expect(await manager.getJobState(parent.id)).toBe('prioritized');
  expect(manager.count(queue)).toBe(1);
  expect((await manager.pull(queue))?.id).toBe(parent.id);
});

test('recovery repairs a stale parent state even when its timeline already records promotion', async () => {
  directory = mkdtempSync(join(tmpdir(), 'bunqueue-flow-parent-recovery-heal-'));
  const dataPath = join(directory, 'queue.db');
  const queue = 'flow-parent-recovery-heal';
  manager = new QueueManager({ dataPath });

  const child = await manager.push(queue, {
    data: { role: 'child' },
    durable: true,
    priority: 2,
  });
  const parent = await manager.push(queue, {
    data: { role: 'parent' },
    dependsOn: [child.id],
    durable: true,
    priority: 1,
  });
  expect((await manager.pull(queue))?.id).toBe(child.id);
  await manager.ack(child.id);
  await untilState(parent.id, 'prioritized');

  manager.shutdown();
  manager = null;
  const stale = new Database(dataPath);
  stale.run("UPDATE jobs SET state = 'waiting-children' WHERE id = ?", String(parent.id));
  stale.close();

  manager = new QueueManager({ dataPath });
  expect(await manager.getJobState(parent.id)).toBe('prioritized');
  manager.shutdown();
  manager = null;

  const healed = new Database(dataPath, { readonly: true });
  try {
    const row = healed
      .query<{ state: string }, [string]>('SELECT state FROM jobs WHERE id = ?')
      .get(String(parent.id));
    expect(row?.state).toBe('prioritized');
  } finally {
    healed.close();
  }

  manager = new QueueManager({ dataPath });
  expect(await manager.getJobState(parent.id)).toBe('prioritized');
  expect(manager.count(queue)).toBe(1);
});

test('reusing a removed custom ID invalidates the previous generation completion', async () => {
  directory = mkdtempSync(join(tmpdir(), 'bunqueue-flow-completion-generation-'));
  const dataPath = join(directory, 'queue.db');
  const queue = 'flow-completion-generation';
  manager = new QueueManager({ dataPath });

  const first = await manager.push(queue, {
    customId: 'reused-flow-child',
    data: { generation: 1 },
    durable: true,
    removeOnComplete: true,
  });
  expect((await manager.pull(queue))?.id).toBe(first.id);
  await manager.ack(first.id);

  const second = await manager.push(queue, {
    customId: 'reused-flow-child',
    data: { generation: 2 },
    durable: true,
    priority: 2,
  });
  const dependent = await manager.push(queue, {
    data: { generation: 2, role: 'dependent' },
    dependsOn: [second.id],
    durable: true,
    priority: 1,
  });

  expect(second.id).toBe(first.id);
  expect(await manager.getJobState(second.id)).toBe('prioritized');
  expect(await manager.getJobState(dependent.id)).toBe('waiting-children');

  const database = new Database(dataPath, { readonly: true });
  try {
    const row = database
      .query<{ count: number }, [string]>(
        'SELECT COUNT(*) AS count FROM dependency_completions WHERE job_id = ?'
      )
      .get(String(first.id));
    expect(row?.count).toBe(0);
  } finally {
    database.close();
  }
});

test('an orphan result row cannot release a dependency after active-job recovery', async () => {
  directory = mkdtempSync(join(tmpdir(), 'bunqueue-flow-orphan-result-'));
  const dataPath = join(directory, 'queue.db');
  const queue = 'flow-orphan-result';
  manager = new QueueManager({ dataPath });

  const child = await manager.push(queue, {
    data: { role: 'child' },
    durable: true,
    maxAttempts: 3,
  });
  const parent = await manager.push(queue, {
    data: { role: 'parent' },
    dependsOn: [child.id],
    durable: true,
    priority: 1,
  });
  expect((await manager.pull(queue))?.id).toBe(child.id);

  // Simulate a legacy crash after result persistence but before the jobs row
  // transitions from active to completed.
  (
    manager as unknown as {
      storage: { storeResult: (id: JobId, result: unknown) => void };
    }
  ).storage.storeResult(child.id, { partial: true });
  manager.shutdown();
  manager = new QueueManager({ dataPath });

  expect(await manager.getJobState(parent.id)).toBe('waiting-children');
  expect(await manager.getJobState(child.id)).not.toBe('completed');
  expect((await manager.pull(queue))?.id).not.toBe(parent.id);
});

test('optimized result ACKB persists every removed dependency before restart', async () => {
  directory = mkdtempSync(join(tmpdir(), 'bunqueue-flow-removed-ackb-'));
  const dataPath = join(directory, 'queue.db');
  const queue = 'flow-removed-ackb';
  manager = new QueueManager({ dataPath, dependencyCheckMs: 60_000 });
  const children = await Promise.all(
    Array.from({ length: 6 }, (_, index) =>
      manager!.push(queue, {
        data: { index },
        durable: true,
        priority: 2,
        removeOnComplete: true,
      })
    )
  );
  const parent = await manager.push(queue, {
    data: { role: 'parent' },
    dependsOn: children.map((child) => child.id),
    durable: true,
    priority: 1,
  });
  (
    manager as unknown as {
      scheduleDependencyFlush: () => void;
    }
  ).scheduleDependencyFlush = () => {
    // Intentionally defer the flush to exercise result-batch recovery.
  };

  const pulled = await manager.pullBatch(queue, children.length);
  await manager.ackBatchWithResults(
    pulled.map((job, index) => ({ id: job.id, result: { index } }))
  );
  expect(await manager.getJobState(parent.id)).toBe('waiting-children');

  manager.shutdown();
  manager = new QueueManager({ dataPath });
  expect(await manager.getJobState(parent.id)).toBe('prioritized');
  expect((await manager.pull(queue))?.id).toBe(parent.id);
});
