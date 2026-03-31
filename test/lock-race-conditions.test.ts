/**
 * Lock Race Conditions Tests
 * Edge cases: token validation, expiration recovery, concurrent pulls,
 * extend semantics, stall interaction, and no-lock mode.
 * Uses QueueManager directly in embedded mode.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { shardIndex } from '../src/shared/hash';
import type { JobId } from '../src/domain/types/job';

describe('Lock Race Conditions', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  // 1. Pull with lock returns a lock token
  test('pull with lock returns a valid lock token', async () => {
    await qm.push('lock-pull', { data: { v: 1 } });
    const { job, token } = await qm.pullWithLock('lock-pull', 'w1', 0, 60_000);
    expect(job).not.toBeNull();
    expect(token).not.toBeNull();
    expect(typeof token).toBe('string');
    expect(token!.length).toBeGreaterThan(0);
    expect(qm.verifyLock(job!.id, token!)).toBe(true);
  });

  // 2. ACK with correct lock token succeeds
  test('ack with correct lock token succeeds', async () => {
    await qm.push('ack-correct', { data: { v: 1 } });
    const { job, token } = await qm.pullWithLock('ack-correct', 'w1', 0, 60_000);
    await qm.ack(job!.id, { done: true }, token!);
    expect(qm.getLockInfo(job!.id)).toBeNull();
    expect(await qm.getJobState(job!.id)).toBe('completed');
  });

  // 3. ACK with wrong lock token fails
  test('ack with wrong lock token throws ownership error', async () => {
    await qm.push('ack-wrong', { data: { v: 1 } });
    const { job } = await qm.pullWithLock('ack-wrong', 'w1', 0, 60_000);
    await expect(qm.ack(job!.id, { done: true }, 'wrong-token')).rejects.toThrow(
      /Invalid or expired lock token/
    );
    expect(await qm.getJobState(job!.id)).toBe('active');
  });

  // 4. Lock expiration makes job pullable again
  test('expired lock makes job pullable again after background check', async () => {
    await qm.push('expire-repull', { data: { v: 1 }, maxAttempts: 5 });
    const { job } = await qm.pullWithLock('expire-repull', 'w1', 0, 50);
    const jobId = job!.id;

    await Bun.sleep(6_500);

    expect(qm.getLockInfo(jobId)).toBeNull();
    const state = await qm.getJobState(jobId);
    expect(['waiting', 'delayed']).toContain(state);

    await Bun.sleep(2_000);
    let job2 = await qm.pull('expire-repull', 0);
    if (!job2) {
      await Bun.sleep(2_000);
      job2 = await qm.pull('expire-repull', 0);
    }
    if (job2) {
      expect(job2.id).toBe(jobId);
      expect(job2.attempts).toBeGreaterThanOrEqual(1);
    } else {
      expect(await qm.getJobState(jobId)).toBe('delayed');
    }
  }, 15_000);

  // 5. ExtendLock extends an active lock
  test('extendLock extends an active lock expiration', async () => {
    await qm.push('extend-active', { data: { v: 1 } });
    const { job, token } = await qm.pullWithLock('extend-active', 'w1', 0, 60_000);
    const infoBefore = qm.getLockInfo(job!.id)!;

    const extended = await qm.extendLock(job!.id, token!, 120_000);
    expect(extended).toBe(true);

    const infoAfter = qm.getLockInfo(job!.id)!;
    expect(infoAfter.expiresAt).toBeGreaterThanOrEqual(infoBefore.expiresAt);
    expect(infoAfter.renewalCount).toBe(1);
    expect(infoAfter.expiresAt).toBeGreaterThanOrEqual(Date.now() + 100_000);
  });

  // 6. ExtendLock on expired lock fails
  test('extendLock on expired lock returns false', async () => {
    await qm.push('extend-expired', { data: { v: 1 } });
    const { job, token } = await qm.pullWithLock('extend-expired', 'w1', 0, 30);
    await Bun.sleep(60);
    expect(qm.verifyLock(job!.id, token!)).toBe(false);
    expect(await qm.extendLock(job!.id, token!, 60_000)).toBe(false);
  });

  // 7. Concurrent pulls with locks get different jobs
  test('concurrent pulls with locks get different jobs and tokens', async () => {
    const queue = 'concurrent-pull';
    for (let i = 0; i < 10; i++) await qm.push(queue, { data: { i } });

    const results = await Promise.all(
      Array.from({ length: 10 }, (_, i) =>
        qm.pullWithLock(queue, `worker-${i}`, 0, 60_000)
      )
    );

    const jobs = results.filter((r) => r.job !== null);
    const jobIds = new Set(jobs.map((r) => r.job!.id));
    const tokens = new Set(jobs.filter((r) => r.token).map((r) => r.token!));

    expect(jobIds.size).toBe(jobs.length);
    expect(tokens.size).toBe(jobs.length);
    for (const r of jobs) expect(qm.verifyLock(r.job!.id, r.token!)).toBe(true);
  });

  // 8. Lock token validation on fail operation
  test('fail with correct lock token succeeds', async () => {
    await qm.push('fail-correct', { data: { v: 1 }, maxAttempts: 3 });
    const { job, token } = await qm.pullWithLock('fail-correct', 'w1', 0, 60_000);
    await qm.fail(job!.id, 'intentional error', token!);
    expect(qm.getLockInfo(job!.id)).toBeNull();
  });

  test('fail with wrong lock token throws ownership error', async () => {
    await qm.push('fail-wrong', { data: { v: 1 }, maxAttempts: 3 });
    const { job } = await qm.pullWithLock('fail-wrong', 'w1', 0, 60_000);
    await expect(qm.fail(job!.id, 'error', 'wrong-token')).rejects.toThrow(
      /Invalid or expired lock token/
    );
    expect(await qm.getJobState(job!.id)).toBe('active');
  });

  // 9. Multiple lock extensions keep job active
  test('multiple lock extensions keep job active', async () => {
    await qm.push('multi-extend', { data: { v: 1 } });
    const { job, token } = await qm.pullWithLock('multi-extend', 'w1', 0, 500);

    for (let i = 0; i < 5; i++) {
      await Bun.sleep(80);
      expect(await qm.extendLock(job!.id, token!, 500)).toBe(true);
    }

    expect(qm.verifyLock(job!.id, token!)).toBe(true);
    expect(qm.getLockInfo(job!.id)!.renewalCount).toBe(5);
    expect(await qm.getJobState(job!.id)).toBe('active');
  });

  // 10. Lock expiration with stall detection interaction
  test('lock expiration sends to DLQ when maxStalls reached', async () => {
    const queue = 'stall-lock-dlq';
    const idx = shardIndex(queue);
    qm.getShards()[idx].setStallConfig(queue, { maxStalls: 1 });

    await qm.push(queue, { data: { v: 1 }, maxAttempts: 10 });
    const { job } = await qm.pullWithLock(queue, 'w1', 0, 50);
    const jobId = job!.id;

    await Bun.sleep(6_500);

    expect(await qm.getJobState(jobId)).toBe('failed');
    const dlq = qm.getDlq(queue);
    expect(dlq.find((e) => e.id === jobId)).toBeDefined();
    expect(qm.getLockInfo(jobId)).toBeNull();
  }, 15_000);

  test('lock expiration requeues when under maxStalls threshold', async () => {
    const queue = 'stall-under';
    const idx = shardIndex(queue);
    qm.getShards()[idx].setStallConfig(queue, { maxStalls: 5 });

    await qm.push(queue, { data: { v: 1 }, maxAttempts: 10 });
    const { job } = await qm.pullWithLock(queue, 'w1', 0, 50);

    await Bun.sleep(6_500);

    expect(['waiting', 'delayed']).toContain(await qm.getJobState(job!.id));
    expect(qm.getDlq(queue).length).toBe(0);
    const requeued = await qm.getJob(job!.id);
    expect(requeued!.stallCount).toBeGreaterThanOrEqual(1);
  }, 15_000);

  // 11. Pull without useLocks skips lock validation
  test('pull without lock allows ack without token', async () => {
    await qm.push('no-lock-ack', { data: { v: 1 } });
    const job = await qm.pull('no-lock-ack');
    expect(qm.getLockInfo(job!.id)).toBeNull();
    await qm.ack(job!.id, { result: 'done' });
    expect(await qm.getJobState(job!.id)).toBe('completed');
  });

  test('pull without lock allows fail without token', async () => {
    await qm.push('no-lock-fail', { data: { v: 1 }, maxAttempts: 3 });
    const job = await qm.pull('no-lock-fail');
    expect(qm.getLockInfo(job!.id)).toBeNull();
    await qm.fail(job!.id, 'some error');
    expect(await qm.getJobState(job!.id)).not.toBe('active');
  });

  // Additional edge cases

  test('extendLock with wrong token returns false, lock intact', async () => {
    await qm.push('extend-wrong', { data: { v: 1 } });
    const { job } = await qm.pullWithLock('extend-wrong', 'w1', 0, 60_000);
    expect(await qm.extendLock(job!.id, 'wrong-token', 60_000)).toBe(false);
    expect(qm.getLockInfo(job!.id)).not.toBeNull();
  });

  test('extendLock without token falls back to internal lookup', async () => {
    await qm.push('extend-null', { data: { v: 1 } });
    const { job } = await qm.pullWithLock('extend-null', 'w1', 0, 60_000);
    const extended = await qm.extendLock(job!.id, null, 120_000);
    expect(extended).toBe(true);
    expect(qm.getLockInfo(job!.id)!.renewalCount).toBe(1);
  });

  test('cross-token ack between two locked jobs fails', async () => {
    await qm.push('cross-a', { data: { v: 1 } });
    await qm.push('cross-b', { data: { v: 2 } });
    const { job: jobA, token: tokenA } = await qm.pullWithLock('cross-a', 'w1', 0, 60_000);
    const { job: jobB, token: tokenB } = await qm.pullWithLock('cross-b', 'w2', 0, 60_000);

    await expect(qm.ack(jobA!.id, {}, tokenB!)).rejects.toThrow(/Invalid or expired lock token/);
    expect(await qm.getJobState(jobA!.id)).toBe('active');
    expect(await qm.getJobState(jobB!.id)).toBe('active');

    await qm.ack(jobA!.id, {}, tokenA!);
    await qm.ack(jobB!.id, {}, tokenB!);
    expect(await qm.getJobState(jobA!.id)).toBe('completed');
    expect(await qm.getJobState(jobB!.id)).toBe('completed');
  });

  test('batch pull with locks assigns unique tokens per job', async () => {
    const queue = 'batch-unique';
    for (let i = 0; i < 5; i++) await qm.push(queue, { data: { i } });

    const { jobs, tokens } = await qm.pullBatchWithLock(queue, 5, 'w1', 0, 60_000);
    expect(jobs.length).toBe(5);
    expect(new Set(tokens).size).toBe(5);

    for (let i = 0; i < 5; i++) {
      expect(qm.verifyLock(jobs[i].id, tokens[i])).toBe(true);
      for (let j = 0; j < 5; j++) {
        if (i !== j) expect(qm.verifyLock(jobs[i].id, tokens[j])).toBe(false);
      }
    }
  });
});
