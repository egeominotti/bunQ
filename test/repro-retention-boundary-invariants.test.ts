import { afterEach, describe, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';

const databasePaths: string[] = [];
const managers: QueueManager[] = [];

function createManager(path: string): QueueManager {
  const manager = new QueueManager({
    dataPath: path,
    maxCompletedJobs: 3,
    maxJobResults: 2,
  });
  managers.push(manager);
  return manager;
}

afterEach(() => {
  for (const manager of managers.splice(0)) manager.shutdown();
  for (const path of databasePaths.splice(0)) {
    for (const suffix of ['', '-wal', '-shm']) {
      const candidate = `${path}${suffix}`;
      if (existsSync(candidate)) rmSync(candidate, { force: true });
    }
  }
});

describe('retention-boundary invariants', () => {
  test('durable state and results remain observable after hot-cache eviction and restart', async () => {
    const path = join(tmpdir(), `bunqueue-retention-${process.pid}-${crypto.randomUUID()}.db`);
    const queue = 'retention-boundary';
    const total = 12;
    databasePaths.push(path);
    let manager = createManager(path);

    for (let index = 0; index < total; index++) {
      const pushed = await manager.push(queue, {
        customId: `retained-${index}`,
        data: { index },
        durable: true,
      });
      const pulled = await manager.pull(queue);
      expect(pulled?.id).toBe(pushed.id);
      await manager.ack(pushed.id, { result: index });
    }

    const beforeRestart = manager.getMemoryStats();
    expect(beforeRestart.completedJobs).toBeLessThanOrEqual(3);
    expect(beforeRestart.jobResults).toBeLessThanOrEqual(2);
    await assertDurableObservability(manager, total);

    manager.shutdown();
    managers.splice(managers.indexOf(manager), 1);
    manager = createManager(path);
    manager.compactMemory();

    const afterRestart = manager.getMemoryStats();
    expect(afterRestart.completedJobs).toBeLessThanOrEqual(3);
    expect(afterRestart.jobResults).toBeLessThanOrEqual(2);
    await assertDurableObservability(manager, total);

    const db = new Database(path, { readonly: true });
    try {
      const jobs = db
        .query<{ count: number }, [string]>(
          "SELECT COUNT(*) AS count FROM jobs WHERE queue = ? AND state = 'completed'"
        )
        .get(queue);
      const results = db
        .query<{ count: number }, []>('SELECT COUNT(*) AS count FROM job_results')
        .get();
      expect(jobs?.count).toBe(total);
      expect(results?.count).toBe(total);
    } finally {
      db.close();
    }
  });
});

async function assertDurableObservability(manager: QueueManager, total: number): Promise<void> {
  for (let index = 0; index < total; index++) {
    const id = `retained-${index}`;
    expect(await manager.getJobState(id), `state after eviction for ${id}`).toBe('completed');
    const job = await manager.getJob(id);
    expect((job?.data as { index?: number })?.index, `payload after eviction for ${id}`).toBe(
      index
    );
    expect(manager.getResult(id), `result after eviction for ${id}`).toEqual({ result: index });
  }
}
