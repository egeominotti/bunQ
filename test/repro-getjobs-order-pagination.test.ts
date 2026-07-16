import { afterEach, describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { unlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';

const managers: QueueManager[] = [];
const databasePaths: string[] = [];

function createManager(options: ConstructorParameters<typeof QueueManager>[0] = {}): QueueManager {
  const manager = new QueueManager(options);
  managers.push(manager);
  return manager;
}

function createSqliteManager(): QueueManager {
  const path = join(tmpdir(), `bunqueue-getjobs-regression-${randomUUID()}.db`);
  databasePaths.push(path);
  return createManager({ dataPath: path });
}

function indexes(jobs: ReturnType<QueueManager['getJobs']>): number[] {
  return jobs.map((job) => (job.data as { index: number }).index);
}

function kinds(jobs: ReturnType<QueueManager['getJobs']>): string[] {
  return jobs.map((job) => (job.data as { kind: string }).kind);
}

afterEach(async () => {
  for (const manager of managers.splice(0)) manager.shutdown();

  for (const path of databasePaths.splice(0)) {
    for (const file of [path, `${path}-wal`, `${path}-shm`]) {
      try {
        await unlink(file);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
      }
    }
  }
});

describe('getJobs ordering and logical pagination regressions', () => {
  test('in-memory descending pages are selected after global createdAt ordering', async () => {
    const manager = createManager();
    const queue = 'getjobs-memory-descending';
    const baseTimestamp = Date.now() - 100_000;

    for (let index = 0; index < 5; index++) {
      await manager.push(queue, {
        data: { index },
        timestamp: baseTimestamp + index * 100,
      });
    }

    const firstPage = manager.getJobs(queue, {
      state: 'waiting',
      start: 0,
      end: 2,
      asc: false,
    });
    const secondPage = manager.getJobs(queue, {
      state: 'waiting',
      start: 2,
      end: 4,
      asc: false,
    });

    expect([indexes(firstPage), indexes(secondPage)]).toEqual([
      [4, 3],
      [2, 1],
    ]);
  });

  test('SQLite prioritized filtering happens before limiting the logical result', async () => {
    const manager = createSqliteManager();
    const queue = 'getjobs-sqlite-sparse-priority';
    const baseTimestamp = Date.now() - 100_000;

    for (let index = 0; index < 20; index++) {
      await manager.push(queue, {
        data: { index },
        priority: 0,
        timestamp: baseTimestamp + index,
        durable: true,
      });
    }
    for (let index = 0; index < 5; index++) {
      await manager.push(queue, {
        data: { index },
        priority: 10,
        timestamp: baseTimestamp + 20 + index,
        durable: true,
      });
    }

    const prioritized = manager.getJobs(queue, {
      state: 'prioritized',
      start: 0,
      end: 10,
      asc: true,
    });

    expect(indexes(prioritized)).toEqual([0, 1, 2, 3, 4]);
  });

  test('SQLite prioritized offset is applied to prioritized jobs, not raw waiting rows', async () => {
    const manager = createSqliteManager();
    const queue = 'getjobs-sqlite-priority-offset';
    const baseTimestamp = Date.now() - 100_000;

    for (let index = 0; index < 10; index++) {
      await manager.push(queue, {
        data: { index },
        priority: index % 2 === 1 ? 10 : 0,
        timestamp: baseTimestamp + index,
        durable: true,
      });
    }

    const firstPage = manager.getJobs(queue, {
      state: 'prioritized',
      start: 0,
      end: 2,
      asc: true,
    });
    const secondPage = manager.getJobs(queue, {
      state: 'prioritized',
      start: 2,
      end: 4,
      asc: true,
    });

    expect([indexes(firstPage), indexes(secondPage)]).toEqual([
      [1, 3],
      [5, 7],
    ]);
  });

  test('SQLite priority predicates do not remove other requested states', async () => {
    const manager = createSqliteManager();
    const queue = 'getjobs-sqlite-multi-state';
    const baseTimestamp = Date.now() - 100_000;

    await manager.push(queue, {
      data: { kind: 'completed' },
      priority: 0,
      timestamp: baseTimestamp,
      durable: true,
    });
    const active = await manager.pull(queue);
    expect(active).not.toBeNull();
    await manager.ack(active!.id);

    await manager.push(queue, {
      data: { kind: 'prioritized' },
      priority: 10,
      timestamp: baseTimestamp + 100,
      durable: true,
    });

    const jobs = manager.getJobs(queue, {
      state: ['prioritized', 'completed'],
      start: 0,
      end: 10,
      asc: true,
    });

    expect(kinds(jobs)).toEqual(['completed', 'prioritized']);
  });

  test('SQLite classifies matured delayed rows before filtering by logical state', async () => {
    const manager = createSqliteManager();
    const queue = 'getjobs-sqlite-matured-delayed';

    await manager.push(queue, {
      data: { kind: 'matured-waiting' },
      delay: 20,
      priority: 0,
      durable: true,
    });
    await manager.push(queue, {
      data: { kind: 'matured-prioritized' },
      delay: 20,
      priority: 10,
      durable: true,
    });
    await manager.push(queue, {
      data: { kind: 'future-delayed' },
      delay: 60_000,
      priority: 10,
      durable: true,
    });

    await Bun.sleep(40);

    expect(kinds(manager.getJobs(queue, { state: 'waiting', end: 10 }))).toEqual([
      'matured-waiting',
    ]);
    expect(kinds(manager.getJobs(queue, { state: 'prioritized', end: 10 }))).toEqual([
      'matured-prioritized',
    ]);
    expect(kinds(manager.getJobs(queue, { state: 'delayed', end: 10 }))).toEqual([
      'future-delayed',
    ]);
  });

  test('SQLite uses job id as a stable tie-break for equal createdAt values', async () => {
    const manager = createSqliteManager();
    const queue = 'getjobs-sqlite-total-order';
    const timestamp = Date.now() - 100_000;
    const ids: string[] = [];

    for (let index = 0; index < 5; index++) {
      const job = await manager.push(queue, {
        data: { index },
        timestamp,
        durable: true,
      });
      ids.push(String(job.id));
    }

    const asc = manager.getJobs(queue, { state: 'waiting', end: 10, asc: true });
    const desc = manager.getJobs(queue, { state: 'waiting', end: 10, asc: false });
    const sortedIds = [...ids].sort();

    expect(asc.map((job) => String(job.id))).toEqual(sortedIds);
    expect(desc.map((job) => String(job.id))).toEqual([...sortedIds].reverse());
  });

  test('SQLite does not list dependency-blocked jobs as waiting or duplicate them', async () => {
    const manager = createSqliteManager();
    const queue = 'getjobs-sqlite-waiting-children';
    const blocker = await manager.push(queue, {
      data: { kind: 'blocker' },
      durable: true,
    });
    const child = await manager.push(queue, {
      data: { kind: 'child' },
      dependsOn: [blocker.id],
      durable: true,
    });

    const waiting = manager.getJobs(queue, { state: 'waiting', end: 10 });
    const waitingChildren = manager.getJobs(queue, {
      state: 'waiting-children',
      end: 10,
    });
    const combined = manager.getJobs(queue, {
      state: ['waiting', 'waiting-children'],
      end: 10,
    });

    expect(waiting.map((job) => job.id)).toEqual([blocker.id]);
    expect(waitingChildren.map((job) => job.id)).toEqual([child.id]);
    expect(combined.map((job) => job.id).sort()).toEqual([blocker.id, child.id].sort());
  });

  test('SQLite does not list a parent moved to waiting-children as active', async () => {
    const manager = createSqliteManager();
    const queue = 'getjobs-sqlite-moved-waiting-children';
    const parent = await manager.push(queue, {
      data: { kind: 'parent' },
      durable: true,
    });

    expect((await manager.pull(queue))?.id).toBe(parent.id);
    expect(await manager.moveToWaitingChildren(parent.id)).toBe(true);

    const active = manager.getJobs(queue, { state: 'active', end: 10 });
    const combined = manager.getJobs(queue, {
      state: ['active', 'waiting-children'],
      end: 10,
    });

    expect(active).toEqual([]);
    expect(combined.map((job) => job.id)).toEqual([parent.id]);
  });
});
