/**
 * Repro: rate-limit `duration` and TTL are dropped end-to-end.
 *
 * Bug 1: setRateLimit's window is hardwired to 1 second. The client's
 * `setGlobalRateLimit(max, duration)` accepts a duration and silently drops
 * it, so "2 per minute" behaves as "2 per second".
 *
 * Bug 2: a temporary rate limit (`queue.rateLimit(ms)`) only expires via a
 * client-side setTimeout in embedded mode; the broker itself never expires
 * it, and a persisted limit resurrects as PERMANENT after a restart.
 */

import { describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { unlinkSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';

function removeDbFiles(dbPath: string): void {
  for (const suffix of ['', '-wal', '-shm']) {
    try {
      unlinkSync(dbPath + suffix);
    } catch {
      // No WAL sidecar.
    }
  }
}

describe('rate limit duration window', () => {
  test('a "2 per minute" limit must not refill within ~1s', async () => {
    const dbPath = join(tmpdir(), `bunqueue-rl-window-${randomUUID()}.db`);
    const manager = new QueueManager({ dataPath: dbPath });
    const queue = 'rl-window';

    try {
      for (let i = 0; i < 3; i++) await manager.push(queue, { data: { i } });
      manager.setRateLimit(queue, 2, 60_000);

      expect(await manager.pull(queue)).not.toBeNull();
      expect(await manager.pull(queue)).not.toBeNull();

      // With the 1s hardwired window the bucket is full again after ~1s and
      // this pull wrongly returns a job. With a real 60s window it must not.
      await Bun.sleep(1_100);
      expect(await manager.pull(queue)).toBeNull();
    } finally {
      manager.shutdown();
      removeDbFiles(dbPath);
    }
  }, 15_000);
});

describe('rate limit TTL (temporary limit expires broker-side)', () => {
  test('an expired TTL limit stops limiting without any client timer', async () => {
    const dbPath = join(tmpdir(), `bunqueue-rl-ttl-${randomUUID()}.db`);
    const manager = new QueueManager({ dataPath: dbPath });
    const queue = 'rl-ttl';

    try {
      for (let i = 0; i < 3; i++) await manager.push(queue, { data: { i } });
      manager.setRateLimit(queue, 1, undefined, 400);

      expect(await manager.pull(queue)).not.toBeNull();
      expect(await manager.pull(queue)).toBeNull();

      // 600ms > 400ms TTL, but < the 1s refill of the buggy fixed window:
      // today this pull stays blocked (limit never expires broker-side).
      await Bun.sleep(600);
      expect(await manager.pull(queue)).not.toBeNull();
      expect(manager.getQueueLimits(queue).rateLimit).toBeNull();
    } finally {
      manager.shutdown();
      removeDbFiles(dbPath);
    }
  }, 15_000);

  test('an expired TTL limit must not resurrect as permanent after restart', async () => {
    const dbPath = join(tmpdir(), `bunqueue-rl-restart-${randomUUID()}.db`);
    const queue = 'rl-restart';
    let m1: QueueManager | null = null;
    let m2: QueueManager | null = null;

    try {
      m1 = new QueueManager({ dataPath: dbPath });
      await m1.push(queue, { data: { i: 0 } });
      m1.setRateLimit(queue, 1, undefined, 300);
      m1.shutdown();
      m1 = null;

      await Bun.sleep(400);
      m2 = new QueueManager({ dataPath: dbPath });
      expect(m2.getQueueLimits(queue).rateLimit).toBeNull();
    } finally {
      m1?.shutdown();
      m2?.shutdown();
      removeDbFiles(dbPath);
    }
  }, 15_000);
});
