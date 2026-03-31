/**
 * Lock Manager & Lock Operations Tests
 * Unit tests for lockManager.ts and lockOperations.ts API surface.
 * Focuses on create/verify/renew/release/getLockInfo operations,
 * batch renewals, token uniqueness, edge cases, and independence of locks.
 *
 * NOTE: Does NOT duplicate tests from lockExpiration.test.ts which covers
 * background expiration processing, requeue on stall, DLQ on max stalls, events.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import type { JobId, JobLock } from '../src/domain/types/job';

describe('Lock Manager & Lock Operations', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  // Helper: push a job and pull it with a lock
  async function pushAndPullWithLock(
    queue: string,
    data: unknown = { msg: 'test' },
    ttlMs: number = 60_000
  ) {
    await qm.push(queue, { data });
    return qm.pullWithLock(queue, 'test-worker', 0, ttlMs);
  }

  describe('createLock', () => {
    test('creates a lock for a processing job and returns a token', async () => {
      await qm.push('lock-create', { data: { v: 1 } });
      const job = await qm.pull('lock-create');
      expect(job).not.toBeNull();

      const token = qm.createLock(job!.id, 'worker-1');
      expect(token).not.toBeNull();
      expect(typeof token).toBe('string');
      expect(token!.length).toBeGreaterThan(0);
    });

    test('generated lock tokens are unique across calls', async () => {
      const tokens = new Set<string>();

      for (let i = 0; i < 10; i++) {
        await qm.push('unique-tokens', { data: { i } });
        const job = await qm.pull('unique-tokens');
        const token = qm.createLock(job!.id, 'worker-1');
        expect(token).not.toBeNull();
        tokens.add(token!);
      }

      // All 10 tokens must be distinct
      expect(tokens.size).toBe(10);
    });

    test('returns null for a job not in processing state', async () => {
      const job = await qm.push('not-processing', { data: { v: 1 } });

      // Job is in waiting state, not processing
      const token = qm.createLock(job.id, 'worker-1');
      expect(token).toBeNull();
    });

    test('returns null if lock already exists for the job', async () => {
      const { job, token } = await pushAndPullWithLock('double-create');
      expect(job).not.toBeNull();
      expect(token).not.toBeNull();

      // Second createLock on same job must fail
      const secondToken = qm.createLock(job!.id, 'worker-2');
      expect(secondToken).toBeNull();
    });

    test('respects custom TTL parameter', async () => {
      await qm.push('custom-ttl', { data: { v: 1 } });
      const job = await qm.pull('custom-ttl');
      const ttl = 5_000;
      const beforeCreate = Date.now();
      const token = qm.createLock(job!.id, 'worker-1', ttl);
      expect(token).not.toBeNull();

      const lockInfo = qm.getLockInfo(job!.id);
      expect(lockInfo).not.toBeNull();
      expect(lockInfo!.ttl).toBe(ttl);
      expect(lockInfo!.expiresAt).toBeGreaterThanOrEqual(beforeCreate + ttl);
      expect(lockInfo!.expiresAt).toBeLessThanOrEqual(Date.now() + ttl);
    });
  });

  describe('verifyLock', () => {
    test('returns true for correct token on active lock', async () => {
      const { job, token } = await pushAndPullWithLock('verify-ok');
      expect(qm.verifyLock(job!.id, token!)).toBe(true);
    });

    test('returns false for wrong token', async () => {
      const { job } = await pushAndPullWithLock('verify-wrong');
      expect(qm.verifyLock(job!.id, 'totally-wrong-token')).toBe(false);
    });

    test('returns false for a job with no lock', async () => {
      await qm.push('verify-nolock', { data: { v: 1 } });
      const job = await qm.pull('verify-nolock');
      expect(job).not.toBeNull();

      // Pulled without lock
      expect(qm.verifyLock(job!.id, 'any-token')).toBe(false);
    });

    test('returns false for non-existent job ID', () => {
      expect(qm.verifyLock('non-existent-id' as JobId, 'any-token')).toBe(false);
    });

    test('returns false after lock TTL expires', async () => {
      const { job, token } = await pushAndPullWithLock('verify-expired', { v: 1 }, 50);
      expect(qm.verifyLock(job!.id, token!)).toBe(true);

      // Wait for the 50ms TTL to expire
      await new Promise((r) => setTimeout(r, 80));

      expect(qm.verifyLock(job!.id, token!)).toBe(false);
    });
  });

  describe('renewJobLock', () => {
    test('renew returns true and extends expiration', async () => {
      // Use a short initial TTL so renewal with longer TTL produces a measurable difference
      const { job, token } = await pushAndPullWithLock('renew-ok', { v: 1 }, 1_000);
      const infoBefore = qm.getLockInfo(job!.id);
      const expiresAtBefore = infoBefore!.expiresAt;

      // Renew with a much longer TTL so the new expiresAt is clearly greater
      const renewed = qm.renewJobLock(job!.id, token!, 120_000);
      expect(renewed).toBe(true);

      const infoAfter = qm.getLockInfo(job!.id);
      // New expiresAt = Date.now() + 120_000 which is at least 119_000 more than
      // old expiresAt = createdAt + 1_000
      expect(infoAfter!.expiresAt).toBeGreaterThan(expiresAtBefore);
    });

    test('renew increments renewal count', async () => {
      const { job, token } = await pushAndPullWithLock('renew-count');

      expect(qm.getLockInfo(job!.id)!.renewalCount).toBe(0);

      qm.renewJobLock(job!.id, token!);
      expect(qm.getLockInfo(job!.id)!.renewalCount).toBe(1);

      qm.renewJobLock(job!.id, token!);
      expect(qm.getLockInfo(job!.id)!.renewalCount).toBe(2);

      qm.renewJobLock(job!.id, token!);
      expect(qm.getLockInfo(job!.id)!.renewalCount).toBe(3);
    });

    test('renew updates lastRenewalAt', async () => {
      const { job, token } = await pushAndPullWithLock('renew-time');
      const t0 = qm.getLockInfo(job!.id)!.lastRenewalAt;

      await new Promise((r) => setTimeout(r, 10));

      qm.renewJobLock(job!.id, token!);
      const t1 = qm.getLockInfo(job!.id)!.lastRenewalAt;
      expect(t1).toBeGreaterThan(t0);
    });

    test('renew with wrong token returns false', async () => {
      const { job } = await pushAndPullWithLock('renew-bad');
      const result = qm.renewJobLock(job!.id, 'wrong-token');
      expect(result).toBe(false);
    });

    test('renew on expired lock returns false and removes lock entry', async () => {
      const { job, token } = await pushAndPullWithLock('renew-exp', { v: 1 }, 30);

      await new Promise((r) => setTimeout(r, 60));

      const result = qm.renewJobLock(job!.id, token!);
      expect(result).toBe(false);

      // The lock entry should be deleted after failed renew on expired lock
      expect(qm.getLockInfo(job!.id)).toBeNull();
    });

    test('renew with custom newTtl changes effective TTL', async () => {
      const { job, token } = await pushAndPullWithLock('renew-ttl', { v: 1 }, 60_000);
      const beforeRenew = Date.now();

      const renewed = qm.renewJobLock(job!.id, token!, 120_000);
      expect(renewed).toBe(true);

      const info = qm.getLockInfo(job!.id);
      // expiresAt should be approximately now + 120s
      expect(info!.expiresAt).toBeGreaterThanOrEqual(beforeRenew + 120_000);
    });

    test('renew on non-existent job returns false', () => {
      expect(qm.renewJobLock('no-such-job' as JobId, 'any-token')).toBe(false);
    });
  });

  describe('renewJobLockBatch', () => {
    test('batch renew returns successfully renewed job IDs', async () => {
      const items: Array<{ id: JobId; token: string }> = [];

      for (let i = 0; i < 3; i++) {
        const { job, token } = await pushAndPullWithLock(`batch-renew-${i}`);
        items.push({ id: job!.id, token: token! });
      }

      const renewed = qm.renewJobLockBatch(items);
      expect(renewed.length).toBe(3);

      for (const item of items) {
        expect(renewed).toContain(String(item.id));
      }
    });

    test('batch renew skips items with wrong tokens', async () => {
      const { job: good, token: goodToken } = await pushAndPullWithLock('batch-good');
      const { job: bad } = await pushAndPullWithLock('batch-bad');

      const renewed = qm.renewJobLockBatch([
        { id: good!.id, token: goodToken! },
        { id: bad!.id, token: 'wrong-token' },
      ]);

      expect(renewed.length).toBe(1);
      expect(renewed[0]).toBe(String(good!.id));
    });

    test('batch renew with empty array returns empty array', () => {
      const renewed = qm.renewJobLockBatch([]);
      expect(renewed).toEqual([]);
    });

    test('batch renew with custom TTL per item', async () => {
      const { job, token } = await pushAndPullWithLock('batch-ttl');
      const beforeRenew = Date.now();

      const renewed = qm.renewJobLockBatch([{ id: job!.id, token: token!, ttl: 90_000 }]);
      expect(renewed.length).toBe(1);

      const info = qm.getLockInfo(job!.id);
      expect(info!.expiresAt).toBeGreaterThanOrEqual(beforeRenew + 90_000);
    });
  });

  describe('releaseLock', () => {
    test('release removes the lock', async () => {
      const { job, token } = await pushAndPullWithLock('release-ok');
      expect(qm.getLockInfo(job!.id)).not.toBeNull();

      const released = qm.releaseLock(job!.id, token!);
      expect(released).toBe(true);
      expect(qm.getLockInfo(job!.id)).toBeNull();
    });

    test('release without token succeeds (force release)', async () => {
      const { job } = await pushAndPullWithLock('release-notoken');
      expect(qm.getLockInfo(job!.id)).not.toBeNull();

      const released = qm.releaseLock(job!.id);
      expect(released).toBe(true);
      expect(qm.getLockInfo(job!.id)).toBeNull();
    });

    test('release with wrong token returns false and keeps lock', async () => {
      const { job, token } = await pushAndPullWithLock('release-wrong');
      expect(qm.getLockInfo(job!.id)).not.toBeNull();

      const released = qm.releaseLock(job!.id, 'wrong-token');
      expect(released).toBe(false);

      // Lock should still exist
      expect(qm.getLockInfo(job!.id)).not.toBeNull();
      expect(qm.verifyLock(job!.id, token!)).toBe(true);
    });

    test('release on non-existent lock returns true (idempotent)', () => {
      const result = qm.releaseLock('no-lock-here' as JobId);
      expect(result).toBe(true);
    });

    test('verify returns false after release', async () => {
      const { job, token } = await pushAndPullWithLock('release-verify');
      expect(qm.verifyLock(job!.id, token!)).toBe(true);

      qm.releaseLock(job!.id, token!);
      expect(qm.verifyLock(job!.id, token!)).toBe(false);
    });
  });

  describe('getLockInfo', () => {
    test('returns full lock details for locked job', async () => {
      const { job, token } = await pushAndPullWithLock('info-full');

      const info = qm.getLockInfo(job!.id);
      expect(info).not.toBeNull();
      expect(info!.jobId).toBe(job!.id);
      expect(info!.token).toBe(token);
      expect(info!.owner).toBe('test-worker');
      expect(info!.renewalCount).toBe(0);
      expect(info!.ttl).toBe(60_000);
      expect(typeof info!.createdAt).toBe('number');
      expect(typeof info!.expiresAt).toBe('number');
      expect(typeof info!.lastRenewalAt).toBe('number');
      expect(info!.expiresAt).toBeGreaterThan(info!.createdAt);
    });

    test('returns null for job without lock', async () => {
      await qm.push('info-nolock', { data: { v: 1 } });
      const job = await qm.pull('info-nolock');
      expect(qm.getLockInfo(job!.id)).toBeNull();
    });

    test('returns null for non-existent job', () => {
      expect(qm.getLockInfo('phantom-job' as JobId)).toBeNull();
    });

    test('lock info reflects owner from pullWithLock', async () => {
      await qm.push('info-owner', { data: { v: 1 } });
      const { job } = await qm.pullWithLock('info-owner', 'my-custom-worker', 0, 60_000);

      const info = qm.getLockInfo(job!.id);
      expect(info!.owner).toBe('my-custom-worker');
    });
  });

  describe('multiple independent locks', () => {
    test('locks on different jobs are fully independent', async () => {
      const { job: jobA, token: tokenA } = await pushAndPullWithLock('indep-a');
      const { job: jobB, token: tokenB } = await pushAndPullWithLock('indep-b');

      // Both should be valid
      expect(qm.verifyLock(jobA!.id, tokenA!)).toBe(true);
      expect(qm.verifyLock(jobB!.id, tokenB!)).toBe(true);

      // Cross-token verification must fail
      expect(qm.verifyLock(jobA!.id, tokenB!)).toBe(false);
      expect(qm.verifyLock(jobB!.id, tokenA!)).toBe(false);

      // Release one, the other stays valid
      qm.releaseLock(jobA!.id, tokenA!);
      expect(qm.getLockInfo(jobA!.id)).toBeNull();
      expect(qm.verifyLock(jobB!.id, tokenB!)).toBe(true);
    });

    test('renewing one lock does not affect another', async () => {
      const { job: jobA, token: tokenA } = await pushAndPullWithLock('renew-indep-a');
      const { job: jobB, token: tokenB } = await pushAndPullWithLock('renew-indep-b');

      const infoBBefore = qm.getLockInfo(jobB!.id);

      // Renew only A
      qm.renewJobLock(jobA!.id, tokenA!, 120_000);

      // B should be unchanged
      const infoBAfter = qm.getLockInfo(jobB!.id);
      expect(infoBAfter!.expiresAt).toBe(infoBBefore!.expiresAt);
      expect(infoBAfter!.renewalCount).toBe(0);
    });

    test('many locks on different queues coexist correctly', async () => {
      const count = 20;
      const entries: Array<{ id: JobId; token: string }> = [];

      for (let i = 0; i < count; i++) {
        const { job, token } = await pushAndPullWithLock(`multi-q-${i}`);
        entries.push({ id: job!.id, token: token! });
      }

      // All should be verifiable
      for (const entry of entries) {
        expect(qm.verifyLock(entry.id, entry.token)).toBe(true);
      }

      // Release half
      for (let i = 0; i < count / 2; i++) {
        qm.releaseLock(entries[i].id, entries[i].token);
      }

      // First half gone, second half still valid
      for (let i = 0; i < count / 2; i++) {
        expect(qm.getLockInfo(entries[i].id)).toBeNull();
      }
      for (let i = count / 2; i < count; i++) {
        expect(qm.verifyLock(entries[i].id, entries[i].token)).toBe(true);
      }
    });
  });

  describe('pullWithLock integration', () => {
    test('pullWithLock returns a job and a valid token', async () => {
      await qm.push('pull-lock', { data: { v: 1 } });
      const { job, token } = await qm.pullWithLock('pull-lock', 'w1', 0, 60_000);

      expect(job).not.toBeNull();
      expect(token).not.toBeNull();
      expect(qm.verifyLock(job!.id, token!)).toBe(true);
    });

    test('pullWithLock from empty queue returns nulls', async () => {
      const { job, token } = await qm.pullWithLock('empty', 'w1', 0, 60_000);
      expect(job).toBeNull();
      expect(token).toBeNull();
    });

    test('pullBatchWithLock creates individual locks for each job', async () => {
      for (let i = 0; i < 5; i++) {
        await qm.push('batch-pull-lock', { data: { i } });
      }

      const { jobs, tokens } = await qm.pullBatchWithLock('batch-pull-lock', 5, 'w1', 0, 60_000);
      expect(jobs.length).toBe(5);
      expect(tokens.length).toBe(5);

      // Each job should have its own valid lock
      for (let i = 0; i < 5; i++) {
        expect(tokens[i]).not.toBe('');
        expect(qm.verifyLock(jobs[i].id, tokens[i])).toBe(true);
      }

      // Tokens should all be unique
      const uniqueTokens = new Set(tokens);
      expect(uniqueTokens.size).toBe(5);
    });
  });

  describe('lock and ack/fail integration', () => {
    test('ack with correct token succeeds and releases lock', async () => {
      const { job, token } = await pushAndPullWithLock('ack-token');

      await qm.ack(job!.id, { result: 'done' }, token!);

      expect(qm.getLockInfo(job!.id)).toBeNull();
      const state = await qm.getJobState(job!.id);
      expect(state).toBe('completed');
    });

    test('fail with correct token succeeds and releases lock', async () => {
      const { job, token } = await pushAndPullWithLock('fail-token');

      await qm.fail(job!.id, 'some error', token!);

      expect(qm.getLockInfo(job!.id)).toBeNull();
    });

    test('ack without token on a locked job still succeeds', async () => {
      const { job } = await pushAndPullWithLock('ack-notoken');

      // Ack without providing a token should not throw
      await qm.ack(job!.id, { result: 'ok' });

      const state = await qm.getJobState(job!.id);
      expect(state).toBe('completed');
    });
  });

  describe('jobHeartbeat with lock', () => {
    test('jobHeartbeat with valid token renews lock', async () => {
      const { job, token } = await pushAndPullWithLock('hb-lock');
      const infoBefore = qm.getLockInfo(job!.id);

      await new Promise((r) => setTimeout(r, 10));

      const result = qm.jobHeartbeat(job!.id, token!);
      expect(result).toBe(true);

      const infoAfter = qm.getLockInfo(job!.id);
      expect(infoAfter!.expiresAt).toBeGreaterThanOrEqual(infoBefore!.expiresAt);
    });

    test('jobHeartbeat with wrong token returns false', async () => {
      const { job } = await pushAndPullWithLock('hb-bad-token');
      const result = qm.jobHeartbeat(job!.id, 'wrong-token');
      expect(result).toBe(false);
    });

    test('jobHeartbeatBatch renews multiple locks', async () => {
      const ids: JobId[] = [];
      const tokens: string[] = [];

      for (let i = 0; i < 3; i++) {
        const { job, token } = await pushAndPullWithLock(`hb-batch-${i}`);
        ids.push(job!.id);
        tokens.push(token!);
      }

      const count = qm.jobHeartbeatBatch(ids, tokens);
      expect(count).toBe(3);
    });
  });

  describe('edge cases', () => {
    test('createLock after job is completed returns null', async () => {
      await qm.push('create-after-ack', { data: { v: 1 } });
      const job = await qm.pull('create-after-ack');
      await qm.ack(job!.id);

      // Job is now completed, not processing
      const token = qm.createLock(job!.id, 'worker-1');
      expect(token).toBeNull();
    });

    test('lock info fields have correct types and values', async () => {
      const beforePull = Date.now();
      const { job, token } = await pushAndPullWithLock('info-types');
      const afterPull = Date.now();

      const info = qm.getLockInfo(job!.id)!;

      // createdAt should be between beforePull and afterPull
      expect(info.createdAt).toBeGreaterThanOrEqual(beforePull);
      expect(info.createdAt).toBeLessThanOrEqual(afterPull);

      // lastRenewalAt should equal createdAt initially
      expect(info.lastRenewalAt).toBe(info.createdAt);

      // expiresAt = createdAt + ttl
      expect(info.expiresAt).toBe(info.createdAt + 60_000);

      // renewalCount starts at 0
      expect(info.renewalCount).toBe(0);
    });

    test('release is idempotent - second release returns true', async () => {
      const { job, token } = await pushAndPullWithLock('release-twice');

      const first = qm.releaseLock(job!.id, token!);
      expect(first).toBe(true);

      // Second release on same job (no lock) returns true
      const second = qm.releaseLock(job!.id, token!);
      expect(second).toBe(true);
    });

    test('verify after release returns false', async () => {
      const { job, token } = await pushAndPullWithLock('verify-after-release');
      qm.releaseLock(job!.id, token!);

      expect(qm.verifyLock(job!.id, token!)).toBe(false);
    });

    test('renew after release returns false', async () => {
      const { job, token } = await pushAndPullWithLock('renew-after-release');
      qm.releaseLock(job!.id, token!);

      expect(qm.renewJobLock(job!.id, token!)).toBe(false);
    });

    test('lock with very short TTL expires quickly', async () => {
      const { job, token } = await pushAndPullWithLock('short-ttl', { v: 1 }, 1);

      // TTL of 1ms - may already be expired
      await new Promise((r) => setTimeout(r, 10));

      expect(qm.verifyLock(job!.id, token!)).toBe(false);
    });

    test('lock with very long TTL stays valid', async () => {
      const { job, token } = await pushAndPullWithLock('long-ttl', { v: 1 }, 3_600_000);

      // 1 hour TTL
      expect(qm.verifyLock(job!.id, token!)).toBe(true);

      const info = qm.getLockInfo(job!.id);
      expect(info!.expiresAt - info!.createdAt).toBe(3_600_000);
    });
  });
});
