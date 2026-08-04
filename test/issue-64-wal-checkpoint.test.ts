/**
 * Test: Issue #64 - WAL checkpoint on close
 *
 * Reproduces the bug: after close(), WAL file retains data,
 * which can cause stale locks and disk I/O errors on rapid restart.
 */
import { beforeEach, describe, expect, it } from 'bun:test';
import { Queue, shutdownManager } from '../src/client';
import { existsSync, unlinkSync } from 'fs';

describe('Issue #64: WAL checkpoint on close', () => {
  const dbPath = '/tmp/test-wal-checkpoint.db';

  beforeEach(() => {
    shutdownManager();
  });

  function cleanup() {
    for (const f of [dbPath, `${dbPath}-wal`, `${dbPath}-shm`]) {
      try {
        unlinkSync(f);
      } catch {}
    }
  }

  it('WAL file should be empty after shutdown (checkpoint needed)', async () => {
    cleanup();

    const queue = new Queue('wal-test', { embedded: true, dataPath: dbPath });
    await Promise.all(Array.from({ length: 10 }, (_, i) => queue.add(`job-${i}`, { i })));

    // shutdownManager closes the QueueManager which calls storage.close()
    shutdownManager();

    // BUG: Without WAL checkpoint, WAL file retains data after close
    const walPath = `${dbPath}-wal`;
    if (existsSync(walPath)) {
      const walSize = Bun.file(walPath).size;
      expect(walSize).toBe(0); // Should be 0 after TRUNCATE checkpoint
    }

    cleanup();
  });

  it('rapid restart after shutdown should not cause errors', async () => {
    cleanup();

    // Simulate rapid restart cycles
    for (let cycle = 0; cycle < 5; cycle++) {
      const queue = new Queue(`wal-reopen-${cycle}`, { embedded: true, dataPath: dbPath });
      await queue.add('job', { cycle });
      shutdownManager();
    }

    // Final open should succeed without disk I/O error
    const finalQueue = new Queue('wal-final', { embedded: true, dataPath: dbPath });
    await expect(finalQueue.add('final-job', { ok: true })).resolves.toBeDefined();
    shutdownManager();

    cleanup();
  });
});
