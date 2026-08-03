/**
 * Bug #75: Cron job with preventOverlap fires immediately on reconnect
 *
 * Root cause: TWO interacting bugs:
 *
 * 1. processExpiredLockInner (lockManager.ts) re-queues cron jobs with
 *    preventOverlap instead of discarding them. During graceful shutdown,
 *    _doClose() clears the heartbeat timer, so locks are not renewed.
 *    If the job takes longer than the lock TTL (30s default), the lock
 *    expires and the cron job is re-queued — sitting in the queue waiting
 *    for the next worker to pull it.
 *
 * 2. ackBatchWithResults / ackBatch (queueManager.ts) silently skip jobs
 *    whose lock verification fails, without calling completeStallRetriedJob.
 *    The single ack() method has this recovery, but the batch paths don't.
 *    Since workers use the ackBatcher (ACKB), the re-queued cron job is
 *    never cleaned up.
 *
 * Reproduction:
 *   Run 1: cron fires → worker pulls → ^C → lock expires → job re-queued
 *   → ACK via ACKB skips recovery → job stays in queue
 *   Run 2: worker connects → immediately pulls the stale cron job
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { checkExpiredLocks } from '../src/application/lockManager';
import { jobId } from '../src/domain/types/job';
import { shardIndex } from '../src/shared/hash';

function getInternalLockContext(qm: QueueManager): Parameters<typeof checkExpiredLocks>[0] {
  return (
    qm as unknown as {
      contextFactory: {
        getLockContext(): Parameters<typeof checkExpiredLocks>[0];
      };
    }
  ).contextFactory.getLockContext();
}

describe('Bug #75: Cron lock expiry re-queues job causing immediate fire on reconnect', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('lock expiration should discard cron jobs with preventOverlap instead of re-queuing', async () => {
    // 1. Register cron with preventOverlap (adds cron to scheduler)
    qm.addCron({
      name: 'test-cron',
      queue: 'testing',
      data: {},
      schedule: '* * * * *',
      preventOverlap: true,
    });

    // 2. Push a cron job with the uniqueKey (simulating what the cron scheduler does)
    const cronJob = await qm.push('testing', {
      data: { type: 'cron' },
      uniqueKey: 'cron:test-cron',
    });
    expect(cronJob).not.toBeNull();

    // 3. Pull with lock (short TTL for testing)
    const { job, token } = await qm.pullWithLock('testing', 'worker-1', 0, 50); // 50ms TTL
    expect(job).not.toBeNull();
    expect(token).not.toBeNull();
    expect(job!.uniqueKey).toBe('cron:test-cron');

    // 4. Wait for lock to expire (simulating heartbeat stopped during shutdown)
    await Bun.sleep(100);

    // 5. Run lock expiration check
    await checkExpiredLocks(getInternalLockContext(qm));

    expect(await qm.getJobState(job!.id)).toBe('unknown');
    expect(await qm.pull('testing', 0)).toBeNull();
    expect(
      qm.getShards()[shardIndex('testing')].getUniqueKeyEntry('testing', 'cron:test-cron')
    ).toBeNull();
  });

  test('late batch ACK ignores only the exact retired cron lease', async () => {
    // 1. Push a cron job with uniqueKey
    await qm.push('testing', {
      data: { type: 'cron' },
      uniqueKey: 'cron:test-cron',
    });

    // 2. Pull with lock (short TTL)
    const { job, token } = await qm.pullWithLock('testing', 'worker-1', 0, 50);
    expect(job).not.toBeNull();

    // 3. Wait for lock to expire
    await Bun.sleep(100);

    // 4. Lock expiration retires the cron generation
    await checkExpiredLocks(getInternalLockContext(qm));

    expect(await qm.getJobState(job!.id)).toBe('unknown');

    await expect(
      qm.ackBatchWithResults([{ id: job!.id, result: 'wrong', token: 'wrong-token' }])
    ).rejects.toThrow('Job not found');
    await expect(
      qm.ackBatchWithResults([{ id: job!.id, result: 'done', token: token! }])
    ).resolves.toEqual({ ignoredIds: [job!.id], ignoredIndices: [0] });

    expect(await qm.getJobState(job!.id)).toBe('unknown');
    expect(await qm.pull('testing', 0)).toBeNull();

    // The uniqueKey should be released so the next cron fire can push
    const idx = shardIndex('testing');
    const shard = qm.getShards()[idx];
    const keyEntry = shard.getUniqueKeyEntry('testing', 'cron:test-cron');
    expect(keyEntry).toBeNull();
  });

  test('late single ACK cannot resurrect a retired cron generation', async () => {
    // 1. Push cron job
    await qm.push('testing', {
      data: { type: 'cron-job' },
      uniqueKey: 'cron:start-new-test-job',
    });

    // 2. Worker pulls with lock
    const { job, token } = await qm.pullWithLock('testing', 'worker-1', 0, 50);
    expect(job).not.toBeNull();

    // 3. Lock expires (heartbeat stopped during shutdown)
    await Bun.sleep(100);
    await checkExpiredLocks(getInternalLockContext(qm));

    // 4. An embedded worker returns after its generation was retired.
    await expect(qm.ack(job!.id, { done: true }, token!)).resolves.toEqual({
      applied: false,
      reason: 'already-finalized',
    });
    await expect(qm.fail(job!.id, 'late failure', token!)).resolves.toEqual({
      applied: false,
      reason: 'already-finalized',
    });

    expect(await qm.getJobState(job!.id)).toBe('unknown');
    const nextJob = await qm.pull('testing', 0);
    expect(nextJob).toBeNull();

    // 6. UniqueKey should be released
    const idx = shardIndex('testing');
    const shard = qm.getShards()[idx];
    expect(shard.getUniqueKeyEntry('testing', 'cron:start-new-test-job')).toBeNull();
  });

  test('ACK still rejects arbitrary missing and already-completed jobs', async () => {
    const missingId = jobId(Bun.randomUUIDv7());
    await expect(qm.ack(missingId, 'missing', 'unknown-token')).rejects.toThrow('Job not found');
    await expect(
      qm.ackBatchWithResults([{ id: missingId, result: 'missing', token: 'unknown-token' }])
    ).rejects.toThrow('Job not found');

    const accepted = await qm.push('testing', { data: { type: 'ordinary' } });
    const pulled = await qm.pullWithLock('testing', 'worker-1');
    expect(pulled.job?.id).toBe(accepted.id);
    await qm.ack(accepted.id, 'first', pulled.token!);

    await expect(qm.ack(accepted.id, 'duplicate', pulled.token!)).rejects.toThrow('Job not found');
    await expect(
      qm.ackBatchWithResults([{ id: accepted.id, result: 'duplicate', token: pulled.token! }])
    ).rejects.toThrow('Job not found');
    expect(await qm.getJobState(accepted.id)).toBe('completed');
  });

  test('custom-ID reuse cannot inherit a retired cron lease', async () => {
    const customId = `cron-generation-${Bun.randomUUIDv7()}`;
    const original = await qm.push('testing', {
      data: { generation: 'retired' },
      customId,
      uniqueKey: 'cron:generation-safe',
    });
    const retired = await qm.pullWithLock('testing', 'worker-old', 0, 50);
    expect(retired.job?.id).toBe(original.id);
    await Bun.sleep(100);
    await checkExpiredLocks(getInternalLockContext(qm));

    const replacement = await qm.push('testing', {
      data: { generation: 'current' },
      customId,
    });
    expect(replacement.id).toBe(original.id);
    const current = await qm.pullWithLock('testing', 'worker-new');
    expect(current.job?.id).toBe(replacement.id);
    expect(current.token).not.toBe(retired.token);

    await expect(qm.ack(replacement.id, 'stale', retired.token!)).rejects.toThrow(
      'Invalid or expired lock token'
    );
    await expect(qm.ack(replacement.id, 'current', current.token!)).resolves.toBeUndefined();
    expect(await qm.getJobState(replacement.id)).toBe('completed');
  });

  test('discarded cron generation cannot recover from SQLite after restart', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'bunqueue-cron-retired-'));
    const dataPath = join(directory, 'queue.db');
    let persistent: QueueManager | null = new QueueManager({ dataPath });
    try {
      const accepted = await persistent.push('persistent-cron', {
        data: { generation: 'retired' },
        uniqueKey: 'cron:persistent-retirement',
        durable: true,
      });
      const active = await persistent.pullWithLock('persistent-cron', 'worker-old', 0, 50);
      expect(active.job?.id).toBe(accepted.id);
      await Bun.sleep(100);
      await checkExpiredLocks(getInternalLockContext(persistent));
      expect(await persistent.getJobState(accepted.id)).toBe('unknown');

      persistent.shutdown();
      persistent = null;
      persistent = new QueueManager({ dataPath });
      expect(await persistent.getJobState(accepted.id)).toBe('unknown');
      expect(await persistent.pull('persistent-cron', 0)).toBeNull();
    } finally {
      persistent?.shutdown();
      rmSync(directory, { recursive: true, force: true });
    }
  });
});
