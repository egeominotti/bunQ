import { afterEach, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { jobId } from '../src/domain/types/job';

let manager: QueueManager | null = null;
let directory: string | null = null;

afterEach(() => {
  manager?.shutdown();
  manager = null;
  if (directory) rmSync(directory, { force: true, recursive: true });
  directory = null;
});

test('prioritized flow children and waiting parents survive SQLite restart', async () => {
  directory = mkdtempSync(join(tmpdir(), 'bunqueue-model-priority-recovery-'));
  const dataPath = join(directory, 'queue.db');
  const queue = 'model-prioritized-flow-recovery';
  const childId = jobId('model-prioritized-child');

  manager = new QueueManager({ dataPath });
  const child = await manager.push(queue, {
    customId: String(childId),
    data: { role: 'child' },
    durable: true,
    priority: 2,
  });
  const parent = await manager.push(queue, {
    childrenIds: [child.id],
    customId: 'model-prioritized-parent',
    data: { role: 'parent' },
    dependsOn: [child.id],
    durable: true,
    priority: 1,
  });

  const database = new Database(dataPath, { readonly: true });
  const rows = database
    .query<{ id: string; state: string }, []>('SELECT id, state FROM jobs ORDER BY id')
    .all();
  database.close();
  expect(new Map(rows.map((row) => [row.id, row.state]))).toEqual(
    new Map([
      [String(child.id), 'prioritized'],
      [String(parent.id), 'waiting-children'],
    ])
  );

  manager.shutdown();
  manager = new QueueManager({ dataPath });

  expect(await manager.getJobState(child.id)).toBe('prioritized');
  expect(await manager.getJobState(parent.id)).toBe('waiting-children');
  expect(manager.getJobs(queue, { state: 'prioritized' }).map((job) => job.id)).toEqual([child.id]);
  expect(manager.getJobs(queue, { state: 'waiting-children' }).map((job) => job.id)).toEqual([
    parent.id,
  ]);
  expect((await manager.pull(queue))?.id).toBe(child.id);
});
