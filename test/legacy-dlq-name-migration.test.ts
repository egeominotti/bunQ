import { expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pack, unpack } from 'msgpackr';
import { QueueManager } from '../src/application/queueManager';

test('a pre-name-column DLQ blob restores and retries without losing its job name', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'bunqueue-legacy-dlq-name-'));
  const dataPath = join(directory, 'queue.db');
  const queue = 'legacy-dlq';
  let manager: QueueManager | null = new QueueManager({ dataPath });
  try {
    const job = await manager.push(queue, {
      name: 'modern-job',
      data: { marker: 'modern' },
      maxAttempts: 1,
      durable: true,
    });
    const pulled = await manager.pull(queue, 0);
    expect(pulled?.id).toBe(job.id);
    await manager.fail(job.id, 'expected failure');
    expect(manager.getDlqEntries(queue)).toHaveLength(1);
    manager.shutdown();
    manager = null;

    const database = new Database(dataPath);
    try {
      const row = database
        .query<{ entry: Uint8Array }, []>('SELECT entry FROM dlq WHERE job_id = ?')
        .get(String(job.id));
      expect(row).not.toBeNull();
      const entry = unpack(row!.entry) as {
        job: { name?: string; data: unknown };
      };
      delete entry.job.name;
      entry.job.data = { name: 'legacy-job', marker: 'legacy' };
      database.run('UPDATE dlq SET entry = ? WHERE job_id = ?', [pack(entry), String(job.id)]);
    } finally {
      database.close();
    }

    manager = new QueueManager({ dataPath });
    const [restored] = manager.getDlqEntries(queue);
    expect(restored.job.name).toBe('legacy-job');
    expect(restored.job.data).toEqual({ marker: 'legacy' });

    expect(manager.retryDlq(queue, job.id)).toBe(1);
    const retried = await manager.pull(queue, 0);
    expect(retried?.name).toBe('legacy-job');
    expect(retried?.data).toEqual({ marker: 'legacy' });
  } finally {
    manager?.shutdown();
    rmSync(directory, { recursive: true, force: true });
  }
});
