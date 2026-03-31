/**
 * DLQ Selective Retry and RetryCompleted Tests
 * Tests DLQ failure routing, retry, purge, maxAge, maxEntries,
 * auto-retry config, retryCompleted, and data preservation.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { shardIndex } from '../src/shared/hash';
import { FailureReason } from '../src/domain/types/dlq';
import type { JobId } from '../src/domain/types/job';

describe('DLQ Selective Retry', () => {
  let qm: QueueManager;

  beforeEach(() => { qm = new QueueManager(); });
  afterEach(() => { qm.shutdown(); });

  /** Push a job, pull it, and fail it so it lands in the DLQ. */
  async function pushPullFail(
    queue: string, data: unknown, error = 'test error', maxAttempts = 1
  ): Promise<JobId> {
    const job = await qm.push(queue, { data, maxAttempts, backoff: 0 });
    const pulled = await qm.pull(queue, 0);
    expect(pulled).not.toBeNull();
    await qm.fail(pulled!.id, error);
    return job.id;
  }

  test('1. failed job after max attempts goes to DLQ', async () => {
    const id = await pushPullFail('dlq-max1', { val: 1 });
    const dlq = qm.getDlq('dlq-max1');
    expect(dlq.length).toBe(1);
    expect(dlq[0].id).toBe(id);
  });

  test('job retries before going to DLQ', async () => {
    const job = await qm.push('dlq-retry-b4', { data: { v: 1 }, maxAttempts: 3, backoff: 0 });
    // Attempt 1
    const p1 = await qm.pull('dlq-retry-b4', 0);
    await qm.fail(p1!.id, 'a1');
    expect(qm.getDlq('dlq-retry-b4').length).toBe(0);
    await Bun.sleep(10);
    // Attempt 2
    const p2 = await qm.pull('dlq-retry-b4', 0);
    await qm.fail(p2!.id, 'a2');
    expect(qm.getDlq('dlq-retry-b4').length).toBe(0);
    await Bun.sleep(10);
    // Attempt 3 -> DLQ
    const p3 = await qm.pull('dlq-retry-b4', 0);
    await qm.fail(p3!.id, 'a3');
    expect(qm.getDlq('dlq-retry-b4').length).toBe(1);
    expect(qm.getDlq('dlq-retry-b4')[0].id).toBe(job.id);
  });

  test('2. retryDlq retries all DLQ entries', async () => {
    for (let i = 0; i < 3; i++) await pushPullFail('dlq-ra', { i });
    expect(qm.getDlq('dlq-ra').length).toBe(3);

    const retried = qm.retryDlq('dlq-ra');
    expect(retried).toBe(3);
    expect(qm.getDlq('dlq-ra').length).toBe(0);
    expect(qm.getStats().waiting).toBe(3);
  });

  test('retry single job from DLQ by jobId', async () => {
    const id1 = await pushPullFail('dlq-rs', { i: 1 });
    const id2 = await pushPullFail('dlq-rs', { i: 2 });
    expect(qm.getDlq('dlq-rs').length).toBe(2);

    expect(qm.retryDlq('dlq-rs', id1)).toBe(1);
    const remaining = qm.getDlq('dlq-rs');
    expect(remaining.length).toBe(1);
    expect(remaining[0].id).toBe(id2);

    const pulled = await qm.pull('dlq-rs', 0);
    expect(pulled!.id).toBe(id1);
    expect(pulled!.attempts).toBe(0);
  });

  test('3. DLQ entries have correct reason (max_attempts)', async () => {
    await pushPullFail('dlq-reason', { v: 1 });
    const idx = shardIndex('dlq-reason');
    const entries = qm.getShards()[idx].getDlqEntries('dlq-reason');
    expect(entries.length).toBe(1);
    expect(entries[0].reason).toBe(FailureReason.MaxAttemptsExceeded);
  });

  test('DLQ entry preserves error message', async () => {
    await pushPullFail('dlq-err', { v: 1 }, 'Connection refused');
    const idx = shardIndex('dlq-err');
    const entries = qm.getShards()[idx].getDlqEntries('dlq-err');
    expect(entries[0].error).toBe('Connection refused');
  });

  test('DLQ entry has attempt record', async () => {
    await pushPullFail('dlq-att', { v: 1 }, 'failed');
    const idx = shardIndex('dlq-att');
    const entries = qm.getShards()[idx].getDlqEntries('dlq-att');
    expect(entries[0].attempts.length).toBeGreaterThanOrEqual(1);
    expect(entries[0].attempts[0].reason).toBe(FailureReason.MaxAttemptsExceeded);
    expect(entries[0].enteredAt).toBeGreaterThan(0);
  });

  test('4. purgeDlq clears all entries', async () => {
    for (let i = 0; i < 5; i++) await pushPullFail('dlq-pa', { i });
    expect(qm.getDlq('dlq-pa').length).toBe(5);
    expect(qm.purgeDlq('dlq-pa')).toBe(5);
    expect(qm.getDlq('dlq-pa').length).toBe(0);
  });

  test('purge returns 0 for empty DLQ', () => {
    expect(qm.purgeDlq('empty-q')).toBe(0);
  });

  test('purge one queue does not affect another', async () => {
    await pushPullFail('dlq-pq1', { q: 1 });
    await pushPullFail('dlq-pq2', { q: 2 });
    qm.purgeDlq('dlq-pq1');
    expect(qm.getDlq('dlq-pq1').length).toBe(0);
    expect(qm.getDlq('dlq-pq2').length).toBe(1);
  });

  test('5. DLQ maxAge - old entries expire', async () => {
    const q = 'dlq-ma';
    const idx = shardIndex(q);
    const shard = qm.getShards()[idx];
    shard.setDlqConfig(q, { maxAge: 1 });
    await pushPullFail(q, { v: 1 });
    await Bun.sleep(10);
    expect(shard.purgeExpired(q, Date.now())).toBeGreaterThanOrEqual(1);
    expect(qm.getDlq(q).length).toBe(0);
  });

  test('entries with null maxAge never expire', async () => {
    const q = 'dlq-ne';
    const idx = shardIndex(q);
    const shard = qm.getShards()[idx];
    shard.setDlqConfig(q, { maxAge: null });
    await pushPullFail(q, { v: 1 });
    await Bun.sleep(10);
    expect(shard.purgeExpired(q, Date.now())).toBe(0);
    expect(qm.getDlq(q).length).toBe(1);
  });

  test('6. DLQ maxEntries - oldest entries evicted when full', async () => {
    const q = 'dlq-me';
    const idx = shardIndex(q);
    qm.getShards()[idx].setDlqConfig(q, { maxEntries: 3 });
    const ids: JobId[] = [];
    for (let i = 0; i < 5; i++) ids.push(await pushPullFail(q, { i }));
    const dlq = qm.getDlq(q);
    expect(dlq.length).toBe(3);
    const dlqIds = dlq.map((j) => j.id);
    expect(dlqIds).toContain(ids[2]);
    expect(dlqIds).toContain(ids[3]);
    expect(dlqIds).toContain(ids[4]);
  });

  test('maxEntries of 1 keeps only the latest entry', async () => {
    const q = 'dlq-me1';
    const idx = shardIndex(q);
    qm.getShards()[idx].setDlqConfig(q, { maxEntries: 1 });
    await pushPullFail(q, { s: 0 });
    await pushPullFail(q, { s: 1 });
    const lastId = await pushPullFail(q, { s: 2 });
    const dlq = qm.getDlq(q);
    expect(dlq.length).toBe(1);
    expect(dlq[0].id).toBe(lastId);
  });

  test('7. auto-retry config sets nextRetryAt on DLQ entries', async () => {
    const q = 'dlq-arc';
    const idx = shardIndex(q);
    const shard = qm.getShards()[idx];
    shard.setDlqConfig(q, { autoRetry: true, autoRetryInterval: 60000, maxAutoRetries: 3 });
    await pushPullFail(q, { v: 1 });
    const entries = shard.getDlqEntries(q);
    expect(entries.length).toBe(1);
    expect(entries[0].nextRetryAt).not.toBeNull();
  });

  test('auto-retry disabled means no nextRetryAt', async () => {
    const q = 'dlq-ard';
    const idx = shardIndex(q);
    const shard = qm.getShards()[idx];
    shard.setDlqConfig(q, { autoRetry: false });
    await pushPullFail(q, { v: 1 });
    const entries = shard.getDlqEntries(q);
    expect(entries[0].nextRetryAt).toBeNull();
  });

  test('auto-retry eligible entries can be fetched', async () => {
    const q = 'dlq-are';
    const idx = shardIndex(q);
    const shard = qm.getShards()[idx];
    shard.setDlqConfig(q, { autoRetry: true, autoRetryInterval: 1, maxAutoRetries: 3 });
    await pushPullFail(q, { v: 1 });
    await pushPullFail(q, { v: 2 });
    await Bun.sleep(10);
    expect(shard.getAutoRetryEntries(q, Date.now()).length).toBe(2);
  });

  test('8. retryCompleted returns 0 in memory-only mode', async () => {
    const job = await qm.push('rc-mem', { data: { v: 1 }, removeOnComplete: false });
    const pulled = await qm.pull('rc-mem', 0);
    await qm.ack(pulled!.id, { done: true });
    expect(qm.retryCompleted('rc-mem', job.id)).toBe(0);
  });

  test('retryCompleted returns 0 for non-existent job', () => {
    const { jobId } = require('../src/domain/types/job');
    expect(qm.retryCompleted('any', jobId('non-existent'))).toBe(0);
  });

  test('retryCompleted with SQLite storage works', async () => {
    const tmpPath = `/tmp/bunqueue-test-retry-${Date.now()}.db`;
    const qmS = new QueueManager({ dataPath: tmpPath });
    try {
      const job = await qmS.push('rc-sql', { data: { v: 42 }, removeOnComplete: false, durable: true });
      const pulled = await qmS.pull('rc-sql', 0);
      await qmS.ack(pulled!.id, { result: 'ok' });
      expect(await qmS.getJobState(job.id)).toBe('completed');
      expect(qmS.retryCompleted('rc-sql', job.id)).toBe(1);
      const retried = await qmS.pull('rc-sql', 0);
      expect(retried).not.toBeNull();
      expect(retried!.id).toBe(job.id);
      expect(retried!.attempts).toBe(0);
    } finally {
      qmS.shutdown();
      try {
        const { unlinkSync } = require('fs');
        for (const ext of ['', '-wal', '-shm']) {
          try { unlinkSync(tmpPath + ext); } catch {}
        }
      } catch {}
    }
  });

  test('9. DLQ entries preserve original job data', async () => {
    const data = { user: 'alice', action: 'transfer', amount: 9999, meta: { src: 'api' } };
    await pushPullFail('dlq-dp', data);
    const dlq = qm.getDlq('dlq-dp');
    expect(dlq.length).toBe(1);
    const d = dlq[0].data as typeof data;
    expect(d.user).toBe('alice');
    expect(d.amount).toBe(9999);
    expect(d.meta.src).toBe('api');
  });

  test('retried job from DLQ preserves data', async () => {
    const orig = { key: 'important', count: 42 };
    const id = await pushPullFail('dlq-rd', orig);
    qm.retryDlq('dlq-rd', id);
    const retried = await qm.pull('dlq-rd', 0);
    expect(retried).not.toBeNull();
    const d = retried!.data as typeof orig;
    expect(d.key).toBe('important');
    expect(d.count).toBe(42);
  });

  test('multiple DLQ entries each preserve their own data', async () => {
    const datasets = [
      { type: 'email', to: 'a@test.com' },
      { type: 'sms', phone: '+123' },
      { type: 'push', token: 'abc' },
    ];
    const ids: JobId[] = [];
    for (const data of datasets) ids.push(await pushPullFail('dlq-md', data));
    const dlq = qm.getDlq('dlq-md');
    expect(dlq.length).toBe(3);
    for (let i = 0; i < datasets.length; i++) {
      const entry = dlq.find((j) => j.id === ids[i]);
      expect(entry).toBeDefined();
      expect(entry!.data).toEqual(datasets[i]);
    }
  });
});
