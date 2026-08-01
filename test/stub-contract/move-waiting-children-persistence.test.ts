import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../../src/application/queueManager';

const directories: string[] = [];

afterEach(() => {
  for (const directory of directories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

describe('moveToWaitingChildren persistence', () => {
  test('a manually parked active job remains waiting-children after restart', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'bunqueue-waiting-children-'));
    directories.push(directory);
    const database = join(directory, 'queue.db');
    const queue = `waiting-children-${Bun.randomUUIDv7()}`;

    const first = new QueueManager({ dataPath: database });
    const job = await first.push(queue, { data: { role: 'parent' }, durable: true });
    expect(await first.pull(queue)).not.toBeNull();
    expect(await first.moveToWaitingChildren(job.id)).toBe(true);
    expect(await first.getJobState(job.id)).toBe('waiting-children');
    first.shutdown();

    const recovered = new QueueManager({ dataPath: database });
    try {
      expect(await recovered.getJobState(job.id)).toBe('waiting-children');
      expect(await recovered.pull(queue)).toBeNull();
      expect(recovered.getQueueJobCounts(queue)['waiting-children']).toBe(1);
    } finally {
      recovered.shutdown();
    }
  });
});
