/**
 * Test: Issue #64 - WAL checkpoint on close
 *
 * Reproduces the bug: after close(), WAL file retains data,
 * which can cause stale locks and disk I/O errors on rapid restart.
 */
import { describe, it, expect } from 'bun:test';
import { Queue, shutdownManager } from '../src/client';
import { existsSync, unlinkSync } from 'fs';

describe('Issue #64: WAL checkpoint on close', () => {
  const dbPath = '/tmp/test-wal-checkpoint.db';

  function cleanup() {
    for (const f of [dbPath, `${dbPath}-wal`, `${dbPath}-shm`]) {
      try { unlinkSync(f); } catch {}
    }
  }

  it('WAL file should be empty after shutdown (checkpoint needed)', () => {
    cleanup();

    const queue = new Queue('wal-test', { embedded: true, dataPath: dbPath });
    for (let i = 0; i < 10; i++) {
      queue.add(`job-${i}`, { i });
    }

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

  it('rapid restart after shutdown should not cause errors', () => {
    cleanup();

    // Simulate rapid restart cycles
    for (let cycle = 0; cycle < 5; cycle++) {
      const queue = new Queue(`wal-reopen-${cycle}`, { embedded: true, dataPath: dbPath });
      queue.add('job', { cycle });
      shutdownManager();
    }

    // Final open should succeed without disk I/O error
    const finalQueue = new Queue('wal-final', { embedded: true, dataPath: dbPath });
    expect(() => finalQueue.add('final-job', { ok: true })).not.toThrow();
    shutdownManager();

    cleanup();
  });
});
