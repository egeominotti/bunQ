/**
 * Edge cases for rate-limit `duration` (window) and `ttl` (auto-expiry).
 * Companion to test/repro-ratelimit-duration-ttl.test.ts (happy paths).
 */

import { describe, expect, test } from 'bun:test';
import { randomUUID } from 'node:crypto';
import { unlinkSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { LimiterManager } from '../src/domain/queue/limiterManager';

function removeDbFiles(dbPath: string): void {
  for (const suffix of ['', '-wal', '-shm']) {
    try {
      unlinkSync(dbPath + suffix);
    } catch {
      // No WAL sidecar.
    }
  }
}

describe('LimiterManager duration/ttl guards (domain-level)', () => {
  test.each([
    Number.NaN,
    Number.POSITIVE_INFINITY,
    0,
    -1000,
    undefined,
  ])('invalid duration %p degrades to the default 1s window', (duration) => {
    const lm = new LimiterManager();
    lm.setRateLimit('q', 1, duration as number | undefined);

    expect(lm.getState('q').rateLimitDuration).toBeNull();
    // Default window still enforces the limit within the same instant.
    expect(lm.tryAcquireRateLimit('q')).toBe(true);
    expect(lm.tryAcquireRateLimit('q')).toBe(false);
  });

  test.each([
    Number.NaN,
    Number.POSITIVE_INFINITY,
    0,
    -50,
    undefined,
  ])('invalid ttl %p means permanent (no expiry timestamp)', (ttl) => {
    const lm = new LimiterManager();
    lm.setRateLimit('q', 1, undefined, ttl as number | undefined);

    expect(lm.getState('q').rateLimitExpiresAt).toBeNull();
  });

  test('a valid ttl sets an expiry in the future and lazily clears', async () => {
    const lm = new LimiterManager();
    lm.setRateLimit('q', 1, undefined, 80);

    const expiresAt = lm.getState('q').rateLimitExpiresAt;
    expect(expiresAt).not.toBeNull();
    expect(expiresAt!).toBeGreaterThan(Date.now());

    expect(lm.tryAcquireRateLimit('q')).toBe(true);
    expect(lm.tryAcquireRateLimit('q')).toBe(false);

    await Bun.sleep(120);
    // Lazy expiry: the first touch after the deadline clears everything.
    expect(lm.tryAcquireRateLimit('q')).toBe(true);
    expect(lm.getState('q').rateLimit).toBeNull();
    expect(lm.getState('q').rateLimitDuration).toBeNull();
    expect(lm.getState('q').rateLimitExpiresAt).toBeNull();
  });

  test('re-setting the limit replaces window AND expiry (ttl does not leak)', async () => {
    const lm = new LimiterManager();
    lm.setRateLimit('q', 1, undefined, 60);
    // Replace with a permanent limit before the ttl fires.
    lm.setRateLimit('q', 1, 5_000);

    await Bun.sleep(100);
    expect(lm.getState('q').rateLimitExpiresAt).toBeNull();
    expect(lm.tryAcquireRateLimit('q')).toBe(true);
    // Still limited: the old 60ms ttl must not have cleared the new limit.
    expect(lm.tryAcquireRateLimit('q')).toBe(false);
    expect(lm.getState('q').rateLimit).toBe(1);
  });

  test('clearRateLimit wipes limit, window and expiry together', () => {
    const lm = new LimiterManager();
    lm.setRateLimit('q', 3, 60_000, 60_000);
    lm.clearRateLimit('q');

    const state = lm.getState('q');
    expect(state.rateLimit).toBeNull();
    expect(state.rateLimitDuration).toBeNull();
    expect(state.rateLimitExpiresAt).toBeNull();
    expect(lm.tryAcquireRateLimit('q')).toBe(true);
  });

  test('window math: 2 per 200ms refills after the window, not after 1s', async () => {
    const lm = new LimiterManager();
    lm.setRateLimit('q', 2, 200);

    expect(lm.tryAcquireRateLimit('q')).toBe(true);
    expect(lm.tryAcquireRateLimit('q')).toBe(true);
    expect(lm.tryAcquireRateLimit('q')).toBe(false);

    await Bun.sleep(250);
    expect(lm.tryAcquireRateLimit('q')).toBe(true);
    expect(lm.tryAcquireRateLimit('q')).toBe(true);
  });

  test('expiry on a queue that never had a limit is a no-op', () => {
    const lm = new LimiterManager();
    lm.expireRateLimitIfNeeded('ghost');
    expect(lm.tryAcquireRateLimit('ghost')).toBe(true);
  });
});

describe('QueueManager duration/ttl persistence edges', () => {
  test('a still-live ttl survives restart with its REMAINING time, then expires', async () => {
    const dbPath = join(tmpdir(), `bunqueue-rl-live-ttl-${randomUUID()}.db`);
    const queue = 'rl-live-ttl';
    let m1: QueueManager | null = null;
    let m2: QueueManager | null = null;

    try {
      m1 = new QueueManager({ dataPath: dbPath });
      await m1.push(queue, { data: { i: 0 } });
      await m1.push(queue, { data: { i: 1 } });
      m1.setRateLimit(queue, 1, undefined, 1_500);
      m1.shutdown();
      m1 = null;

      m2 = new QueueManager({ dataPath: dbPath });
      // Restored and still limiting right after restart.
      expect(m2.getQueueLimits(queue).rateLimit).toBe(1);
      expect(await m2.pull(queue)).not.toBeNull();
      expect(await m2.pull(queue)).toBeNull();

      // The ORIGINAL deadline still applies (not restarted from scratch).
      await Bun.sleep(1_700);
      expect(await m2.pull(queue)).not.toBeNull();
      expect(m2.getQueueLimits(queue).rateLimit).toBeNull();
    } finally {
      m1?.shutdown();
      m2?.shutdown();
      removeDbFiles(dbPath);
    }
  }, 20_000);

  test('the duration window survives restart', async () => {
    const dbPath = join(tmpdir(), `bunqueue-rl-window-persist-${randomUUID()}.db`);
    const queue = 'rl-window-persist';
    let m1: QueueManager | null = null;
    let m2: QueueManager | null = null;

    try {
      m1 = new QueueManager({ dataPath: dbPath });
      for (let i = 0; i < 3; i++) await m1.push(queue, { data: { i } });
      m1.setRateLimit(queue, 2, 60_000);
      m1.shutdown();
      m1 = null;

      m2 = new QueueManager({ dataPath: dbPath });
      expect(await m2.pull(queue)).not.toBeNull();
      expect(await m2.pull(queue)).not.toBeNull();
      // A 1s-window regression would refill here; the 60s window must not.
      await Bun.sleep(1_100);
      expect(await m2.pull(queue)).toBeNull();
    } finally {
      m1?.shutdown();
      m2?.shutdown();
      removeDbFiles(dbPath);
    }
  }, 20_000);

  test('getQueueLimits alone (no pull) reports null after the ttl elapses', async () => {
    const dbPath = join(tmpdir(), `bunqueue-rl-lazyread-${randomUUID()}.db`);
    const manager = new QueueManager({ dataPath: dbPath });
    const queue = 'rl-lazyread';

    try {
      manager.setRateLimit(queue, 5, undefined, 100);
      expect(manager.getQueueLimits(queue).rateLimit).toBe(5);

      await Bun.sleep(150);
      expect(manager.getQueueLimits(queue).rateLimit).toBeNull();
    } finally {
      manager.shutdown();
      removeDbFiles(dbPath);
    }
  });

  test('re-setting a fresh limit after expiry works from a clean slate', async () => {
    const dbPath = join(tmpdir(), `bunqueue-rl-reset-${randomUUID()}.db`);
    const manager = new QueueManager({ dataPath: dbPath });
    const queue = 'rl-reset';

    try {
      manager.setRateLimit(queue, 1, undefined, 80);
      await Bun.sleep(120);
      expect(manager.getQueueLimits(queue).rateLimit).toBeNull();

      for (let i = 0; i < 2; i++) await manager.push(queue, { data: { i } });
      manager.setRateLimit(queue, 1, 60_000);
      expect(await manager.pull(queue)).not.toBeNull();
      expect(await manager.pull(queue)).toBeNull();
    } finally {
      manager.shutdown();
      removeDbFiles(dbPath);
    }
  });
});
