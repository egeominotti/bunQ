/**
 * Durable Jobs Tests
 * Tests for durable: true option which bypasses the write buffer
 * and writes directly to SQLite for immediate persistence.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { Database } from 'bun:sqlite';
import { existsSync, unlinkSync } from 'fs';

const DB_PATH = '/tmp/test-durable-jobs.db';

function cleanupDb() {
  for (const ext of ['', '-wal', '-shm']) {
    const path = DB_PATH + ext;
    if (existsSync(path)) unlinkSync(path);
  }
}

function jobExistsInSqlite(jobId: string): boolean {
  const db = new Database(DB_PATH, { readonly: true });
  try {
    return db.query('SELECT id FROM jobs WHERE id = ?').get(jobId) !== null;
  } finally {
    db.close();
  }
}

function countJobsInSqlite(queue: string): number {
  const db = new Database(DB_PATH, { readonly: true });
  try {
    const row = db.query<{ cnt: number }, [string]>(
      'SELECT COUNT(*) as cnt FROM jobs WHERE queue = ?'
    ).get(queue);
    return row?.cnt ?? 0;
  } finally {
    db.close();
  }
}

describe('Durable Jobs', () => {
  let qm: QueueManager;

  afterEach(() => {
    qm?.shutdown();
    cleanupDb();
  });

  // ============ Core Durable Behavior ============

  describe('immediate persistence', () => {
    test('durable job is written to SQLite immediately', async () => {
      qm = new QueueManager({ dataPath: DB_PATH });
      const job = await qm.push('durable-q', { data: { critical: true }, durable: true });
      expect(jobExistsInSqlite(job.id)).toBe(true);
    });

    test('non-durable job goes through write buffer', async () => {
      qm = new QueueManager({ dataPath: DB_PATH });
      const job = await qm.push('buffered-q', { data: { normal: true }, durable: false });
      // Job is in memory and retrievable even if not yet flushed to disk
      const retrieved = await qm.getJob(job.id);
      expect(retrieved).not.toBeNull();
      expect(retrieved!.data).toEqual({ normal: true });
    });

    test('durable job is retrievable immediately from both memory and disk', async () => {
      qm = new QueueManager({ dataPath: DB_PATH });
      const job = await qm.push('durable-q', { data: { msg: 'instant' }, durable: true });

      const retrieved = await qm.getJob(job.id);
      expect(retrieved).not.toBeNull();
      expect(retrieved!.data).toEqual({ msg: 'instant' });
      expect(jobExistsInSqlite(job.id)).toBe(true);
    });
  });

  // ============ Durable with Various Options ============

  describe('durable with job options', () => {
    test('durable job with priority', async () => {
      qm = new QueueManager({ dataPath: DB_PATH });
      const job = await qm.push('durable-q', {
        data: { task: 'priority' }, durable: true, priority: 10,
      });
      expect(job.priority).toBe(10);
      expect(jobExistsInSqlite(job.id)).toBe(true);
    });

    test('durable job with delay is persisted and not pullable', async () => {
      qm = new QueueManager({ dataPath: DB_PATH });
      const before = Date.now();
      const job = await qm.push('durable-q', {
        data: { task: 'delayed' }, durable: true, delay: 5000,
      });
      expect(job.runAt).toBeGreaterThanOrEqual(before + 5000);
      expect(jobExistsInSqlite(job.id)).toBe(true);
      expect(await qm.pull('durable-q', 0)).toBeNull();
    });

    test('durable job with custom ID', async () => {
      qm = new QueueManager({ dataPath: DB_PATH });
      const job = await qm.push('durable-q', {
        data: {}, durable: true, customId: 'durable-custom-123',
      });
      expect(job.id).toBe('durable-custom-123');
      expect(jobExistsInSqlite('durable-custom-123')).toBe(true);
    });

    test('durable job with combined options persists correctly', async () => {
      qm = new QueueManager({ dataPath: DB_PATH });
      const job = await qm.push('durable-q', {
        data: { task: 'combined' },
        durable: true,
        maxAttempts: 5,
        backoff: { type: 'exponential', delay: 2000 },
        tags: ['urgent', 'billing'],
        groupId: 'finance',
        timeout: 30000,
        stallTimeout: 60000,
        removeOnComplete: true,
        removeOnFail: true,
      });

      expect(job.maxAttempts).toBe(5);
      expect(job.backoffConfig).toEqual({ type: 'exponential', delay: 2000 });
      expect(job.tags).toEqual(['urgent', 'billing']);
      expect(job.groupId).toBe('finance');
      expect(job.timeout).toBe(30000);
      expect(job.stallTimeout).toBe(60000);
      expect(job.removeOnComplete).toBe(true);
      expect(job.removeOnFail).toBe(true);
      expect(jobExistsInSqlite(job.id)).toBe(true);
    });
  });

  // ============ Mixing Durable and Non-Durable ============

  describe('mixing durable and non-durable jobs', () => {
    test('both coexist in same queue and are pullable', async () => {
      qm = new QueueManager({ dataPath: DB_PATH });
      const durableJob = await qm.push('mixed-q', { data: { type: 'durable' }, durable: true });
      await qm.push('mixed-q', { data: { type: 'normal' } });

      expect(qm.count('mixed-q')).toBe(2);
      expect(jobExistsInSqlite(durableJob.id)).toBe(true);

      const pulled1 = await qm.pull('mixed-q', 0);
      const pulled2 = await qm.pull('mixed-q', 0);
      expect(pulled1).not.toBeNull();
      expect(pulled2).not.toBeNull();
    });

    test('durable jobs respect priority ordering with non-durable jobs', async () => {
      qm = new QueueManager({ dataPath: DB_PATH });
      await qm.push('prio-q', { data: { id: 'low' }, priority: 1 });
      await qm.push('prio-q', { data: { id: 'high-durable' }, priority: 10, durable: true });
      await qm.push('prio-q', { data: { id: 'medium' }, priority: 5 });

      const first = await qm.pull('prio-q');
      const second = await qm.pull('prio-q');
      const third = await qm.pull('prio-q');

      expect((first?.data as { id: string }).id).toBe('high-durable');
      expect((second?.data as { id: string }).id).toBe('medium');
      expect((third?.data as { id: string }).id).toBe('low');
    });
  });

  // ============ Persistence Across Restart ============

  describe('persistence across restart', () => {
    test('durable job survives server restart with all properties', async () => {
      qm = new QueueManager({ dataPath: DB_PATH });
      const job = await qm.push('persist-q', {
        data: { important: 'data', value: 42 }, durable: true, priority: 7,
      });
      const jobId = job.id;
      qm.shutdown();

      qm = new QueueManager({ dataPath: DB_PATH });
      const recovered = await qm.getJob(jobId);
      expect(recovered).not.toBeNull();
      expect(recovered!.data).toEqual({ important: 'data', value: 42 });
      expect(recovered!.priority).toBe(7);
      expect(recovered!.queue).toBe('persist-q');
    });

    test('multiple durable jobs survive restart', async () => {
      qm = new QueueManager({ dataPath: DB_PATH });
      const job1 = await qm.push('persist-q', { data: { idx: 1 }, durable: true });
      const job2 = await qm.push('persist-q', { data: { idx: 2 }, durable: true, priority: 5 });
      const job3 = await qm.push('persist-q', { data: { idx: 3 }, durable: true, delay: 60000 });
      qm.shutdown();

      qm = new QueueManager({ dataPath: DB_PATH });
      const r1 = await qm.getJob(job1.id);
      const r2 = await qm.getJob(job2.id);
      const r3 = await qm.getJob(job3.id);

      expect(r1).not.toBeNull();
      expect(r2).not.toBeNull();
      expect(r3).not.toBeNull();
      expect(r1!.data).toEqual({ idx: 1 });
      expect(r2!.priority).toBe(5);
      expect(r3!.runAt).toBeGreaterThan(r3!.createdAt);
    });
  });

  // ============ Custom ID Idempotency ============

  describe('durable with custom ID idempotency', () => {
    test('duplicate durable custom ID returns existing job and only one on disk', async () => {
      qm = new QueueManager({ dataPath: DB_PATH });
      const job1 = await qm.push('dedup-q', {
        data: { msg: 'first' }, durable: true, customId: 'durable-dedup-id',
      });
      const job2 = await qm.push('dedup-q', {
        data: { msg: 'second' }, durable: true, customId: 'durable-dedup-id',
      });

      expect(job2.id).toBe(job1.id);
      expect(job2.data).toEqual({ msg: 'first' });
      expect(countJobsInSqlite('dedup-q')).toBe(1);
    });
  });

  // ============ No Storage Configured ============

  describe('durable without storage', () => {
    test('durable flag is a no-op when no storage is configured', async () => {
      qm = new QueueManager({ dependencyCheckMs: 50 });
      const job = await qm.push('no-storage-q', { data: { works: true }, durable: true });

      expect(job).toBeDefined();
      expect(job.data).toEqual({ works: true });
      const retrieved = await qm.getJob(job.id);
      expect(retrieved).not.toBeNull();
    });
  });

  // ============ Job State ============

  describe('durable job state', () => {
    test('waiting state when no delay, delayed state with delay', async () => {
      qm = new QueueManager({ dataPath: DB_PATH });
      const waiting = await qm.push('state-q', { data: {}, durable: true });
      const delayed = await qm.push('state-q', { data: {}, durable: true, delay: 60000 });

      expect(await qm.getJobState(waiting.id)).toBe('waiting');
      expect(await qm.getJobState(delayed.id)).toBe('delayed');
    });

    test('durable job can be pulled and acked normally', async () => {
      qm = new QueueManager({ dataPath: DB_PATH });
      await qm.push('ack-q', { data: { task: 'process-me' }, durable: true });

      const pulled = await qm.pull('ack-q', 0);
      expect(pulled).not.toBeNull();
      expect(pulled!.data).toEqual({ task: 'process-me' });

      await qm.ack(pulled!.id, { result: 'done' });
      expect(await qm.getJobState(pulled!.id)).toBe('completed');
    });
  });

  // ============ Stats ============

  describe('durable jobs stats', () => {
    test('durable and non-durable jobs are both counted correctly', async () => {
      qm = new QueueManager({ dataPath: DB_PATH });
      await qm.push('stats-q', { data: { a: 1 }, durable: true });
      await qm.push('stats-q', { data: { a: 2 }, durable: true });
      await qm.push('stats-q', { data: { a: 3 } });

      expect(Number(qm.getStats().totalPushed)).toBe(3);
      expect(qm.count('stats-q')).toBe(3);
    });
  });
});
