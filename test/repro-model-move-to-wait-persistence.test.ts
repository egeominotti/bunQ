import { afterEach, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { existsSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';

const path = join(tmpdir(), `bunqueue-model-move-wait-${process.pid}-${crypto.randomUUID()}.db`);
let manager: QueueManager | null = null;

afterEach(() => {
  manager?.shutdown();
  manager = null;
  for (const suffix of ['', '-wal', '-shm']) {
    const candidate = `${path}${suffix}`;
    if (existsSync(candidate)) rmSync(candidate, { force: true });
  }
});

test('moveToWait persists the active-to-waiting transition', async () => {
  manager = new QueueManager({ dataPath: path });
  const pushed = await manager.push('move-to-wait-persistence', {
    data: { source: 'model-seed--741116562' },
    durable: true,
  });
  const pulled = await manager.pull('move-to-wait-persistence');
  expect(pulled?.id).toBe(pushed.id);

  expect(await manager.moveActiveToWait(pushed.id)).toBe(true);
  expect(await manager.getJobState(pushed.id)).toBe('waiting');
  expect(manager.getStats()).toMatchObject({
    active: 0,
    waiting: 1,
  });

  const db = new Database(path, { readonly: true });
  const row = db
    .query<{ run_at: number; started_at: number | null; state: string }, [string]>(
      'SELECT state, run_at, started_at FROM jobs WHERE id = ?'
    )
    .get(String(pushed.id));
  db.close();
  expect(row?.state).toBe('waiting');
  expect(row?.started_at).toBeNull();
  expect(row?.run_at).toBeLessThanOrEqual(Date.now());

  manager.shutdown();
  manager = new QueueManager({ dataPath: path });
  expect(await manager.getJobState(pushed.id)).toBe('waiting');
  const redelivered = await manager.pull('move-to-wait-persistence');
  expect(redelivered?.id).toBe(pushed.id);
});
