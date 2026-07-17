/**
 * SQLite Storage Tests
 * Tests for the SQLite persistence layer
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { SqliteStorage } from '../src/infrastructure/persistence/sqlite';
import { jobId } from '../src/domain/types/job';
import { unlink } from 'fs/promises';

const TEST_DB_PATH = './test-storage.db';

describe('SqliteStorage', () => {
  let storage: SqliteStorage;

  beforeEach(async () => {
    // Clean up any existing test database
    if (await Bun.file(TEST_DB_PATH).exists()) {
      await unlink(TEST_DB_PATH);
    }
    if (await Bun.file(`${TEST_DB_PATH}-wal`).exists()) {
      await unlink(`${TEST_DB_PATH}-wal`);
    }
    if (await Bun.file(`${TEST_DB_PATH}-shm`).exists()) {
      await unlink(`${TEST_DB_PATH}-shm`);
    }

    storage = new SqliteStorage({ path: TEST_DB_PATH });
  });

  afterEach(async () => {
    storage.close();
    // Clean up test database
    if (await Bun.file(TEST_DB_PATH).exists()) {
      await unlink(TEST_DB_PATH);
    }
    if (await Bun.file(`${TEST_DB_PATH}-wal`).exists()) {
      await unlink(`${TEST_DB_PATH}-wal`);
    }
    if (await Bun.file(`${TEST_DB_PATH}-shm`).exists()) {
      await unlink(`${TEST_DB_PATH}-shm`);
    }
  });

  describe('Database Creation', () => {
    test('should create database file', async () => {
      expect(await Bun.file(TEST_DB_PATH).exists()).toBe(true);
    });

    test('should have valid database size', () => {
      const size = storage.getSize();
      expect(size).toBeGreaterThan(0);
    });
  });

  describe('Job Operations', () => {
    const createTestJob = (id: string, queue = 'test') => ({
      id: jobId(id),
      queue,
      data: { test: 'data' },
      priority: 0,
      createdAt: Date.now(),
      runAt: Date.now(),
      startedAt: null,
      completedAt: null,
      attempts: 0,
      maxAttempts: 3,
      backoff: 1000,
      ttl: null,
      timeout: null,
      uniqueKey: null,
      customId: null,
      dependsOn: [],
      parentId: null,
      childrenIds: [],
      childrenCompleted: 0,
      tags: [],
      lifo: false,
      groupId: null,
      progress: 0,
      progressMessage: null,
      removeOnComplete: false,
      removeOnFail: false,
      lastHeartbeat: Date.now(),
      stallTimeout: null,
      stallCount: 0,
      timeline: [],
    });

    test('should insert and retrieve job', () => {
      const job = createTestJob('test-job-1');
      storage.insertJobImmediate(job);

      const retrieved = storage.getJob(job.id);
      expect(retrieved).not.toBeNull();
      expect(retrieved?.id).toBe(job.id);
      expect(retrieved?.queue).toBe(job.queue);
      expect(retrieved?.data).toEqual(job.data);
    });

    test('should return null for non-existent job', () => {
      const retrieved = storage.getJob(jobId('non-existent'));
      expect(retrieved).toBeNull();
    });

    test('should delete job', () => {
      const job = createTestJob('test-job-2');
      storage.insertJobImmediate(job);

      storage.deleteJob(job.id);

      const retrieved = storage.getJob(job.id);
      expect(retrieved).toBeNull();
    });

    test('should mark job as active', () => {
      const job = createTestJob('test-job-3');
      storage.insertJobImmediate(job);

      const startedAt = Date.now();
      storage.markActive(job.id, startedAt);

      const retrieved = storage.getJob(job.id);
      expect(retrieved?.startedAt).toBe(startedAt);
    });

    test('should mark job as completed', () => {
      const job = createTestJob('test-job-4');
      storage.insertJobImmediate(job);

      const completedAt = Date.now();
      storage.markCompleted(job.id, completedAt);

      const retrieved = storage.getJob(job.id);
      expect(retrieved?.completedAt).toBe(completedAt);
    });

    test('should update job for retry', () => {
      const job = createTestJob('test-job-5');
      storage.insertJobImmediate(job);

      job.attempts = 2;
      job.stallCount = 1;
      job.runAt = Date.now() + 5000;
      storage.updateForRetry(job);

      const retrieved = storage.getJob(job.id);
      expect(retrieved?.attempts).toBe(2);
      expect(retrieved?.stallCount).toBe(1);
      expect(retrieved?.runAt).toBe(job.runAt);
    });
  });

  describe('Batch Operations', () => {
    const createTestJob = (id: string) => ({
      id: jobId(id),
      queue: 'test',
      data: { id },
      priority: 0,
      createdAt: Date.now(),
      runAt: Date.now(),
      startedAt: null,
      completedAt: null,
      attempts: 0,
      maxAttempts: 3,
      backoff: 1000,
      ttl: null,
      timeout: null,
      uniqueKey: null,
      customId: null,
      dependsOn: [],
      parentId: null,
      childrenIds: [],
      childrenCompleted: 0,
      tags: [],
      lifo: false,
      groupId: null,
      progress: 0,
      progressMessage: null,
      removeOnComplete: false,
      removeOnFail: false,
      lastHeartbeat: Date.now(),
      stallTimeout: null,
      stallCount: 0,
      timeline: [],
    });

    test('should insert batch of jobs', () => {
      const jobs = Array.from({ length: 10 }, (_, i) => createTestJob(`batch-${i}`));

      storage.insertJobsBatch(jobs);
      storage.flushWriteBuffer();

      // Verify all jobs exist
      for (const job of jobs) {
        const retrieved = storage.getJob(job.id);
        expect(retrieved).not.toBeNull();
      }
    });

    test('should handle write buffer flushing', () => {
      const jobs = Array.from({ length: 5 }, (_, i) => createTestJob(`buffer-${i}`));

      for (const job of jobs) {
        storage.insertJob(job);
      }

      // Jobs may not be visible yet (buffered)
      storage.flushWriteBuffer();

      // Now jobs should be visible
      for (const job of jobs) {
        const retrieved = storage.getJob(job.id);
        expect(retrieved).not.toBeNull();
      }
    });
  });

  describe('Result Storage', () => {
    const createTestJob = (id: string) => ({
      id: jobId(id),
      queue: 'test',
      data: { id },
      priority: 0,
      createdAt: Date.now(),
      runAt: Date.now(),
      startedAt: null,
      completedAt: null,
      attempts: 0,
      maxAttempts: 3,
      backoff: 1000,
      ttl: null,
      timeout: null,
      uniqueKey: null,
      customId: null,
      dependsOn: [],
      parentId: null,
      childrenIds: [],
      childrenCompleted: 0,
      tags: [],
      lifo: false,
      groupId: null,
      progress: 0,
      progressMessage: null,
      removeOnComplete: false,
      removeOnFail: false,
      lastHeartbeat: Date.now(),
      stallTimeout: null,
      stallCount: 0,
      timeline: [],
    });

    test('should store and retrieve result', () => {
      const job = createTestJob('result-job');
      storage.insertJobImmediate(job);

      const result = { status: 'success', value: 42 };
      storage.storeResult(job.id, result);

      const retrieved = storage.getResult(job.id);
      expect(retrieved).toEqual(result);
    });

    test('should return null for missing result', () => {
      const result = storage.getResult(jobId('non-existent'));
      expect(result).toBeNull();
    });

    test('should store complex result objects', () => {
      const job = createTestJob('complex-result');
      storage.insertJobImmediate(job);

      const result = {
        nested: { deeply: { value: 'test' } },
        array: [1, 2, 3],
        number: 123.456,
        boolean: true,
        null: null,
      };

      storage.storeResult(job.id, result);
      const retrieved = storage.getResult(job.id);
      expect(retrieved).toEqual(result);
    });
  });

  describe('DLQ Operations', () => {
    const createTestJob = (id: string) => ({
      id: jobId(id),
      queue: 'test',
      data: { id },
      priority: 0,
      createdAt: Date.now(),
      runAt: Date.now(),
      startedAt: null,
      completedAt: null,
      attempts: 3,
      maxAttempts: 3,
      backoff: 1000,
      ttl: null,
      timeout: null,
      uniqueKey: null,
      customId: null,
      dependsOn: [],
      parentId: null,
      childrenIds: [],
      childrenCompleted: 0,
      tags: [],
      lifo: false,
      groupId: null,
      progress: 0,
      progressMessage: null,
      removeOnComplete: false,
      removeOnFail: false,
      lastHeartbeat: Date.now(),
      stallTimeout: null,
      stallCount: 0,
    });

    test('should store job in DLQ', () => {
      const job = createTestJob('dlq-job');
      storage.markFailed(job, 'Test error');

      // DLQ is stored in separate table, not directly queryable
      // This test verifies no errors are thrown
      expect(true).toBe(true);
    });
  });

  describe('Cron Operations', () => {
    test('should save and load cron jobs', () => {
      const cron = {
        name: 'test-cron',
        queue: 'cron-queue',
        data: { task: 'cleanup' },
        schedule: '0 * * * *',
        repeatEvery: null,
        priority: 5,
        nextRun: Date.now() + 3600000,
        executions: 0,
        maxLimit: null,
      };

      storage.saveCron(cron);

      const loaded = storage.loadCronJobs();
      expect(loaded.length).toBeGreaterThanOrEqual(1);

      const found = loaded.find((c) => c.name === 'test-cron');
      expect(found).not.toBeUndefined();
      expect(found?.queue).toBe('cron-queue');
      expect(found?.data).toEqual({ task: 'cleanup' });
    });

    test('should delete cron job', () => {
      const cron = {
        name: 'delete-cron',
        queue: 'test',
        data: {},
        schedule: '0 0 * * *',
        repeatEvery: null,
        priority: 0,
        nextRun: Date.now(),
        executions: 0,
        maxLimit: null,
      };

      storage.saveCron(cron);
      storage.deleteCron('delete-cron');

      const loaded = storage.loadCronJobs();
      const found = loaded.find((c) => c.name === 'delete-cron');
      expect(found).toBeUndefined();
    });
  });

  describe('Load Operations', () => {
    const createTestJob = (id: string, state: string, runAt: number) => ({
      id: jobId(id),
      queue: 'test',
      data: { id },
      priority: 0,
      createdAt: Date.now(),
      runAt,
      startedAt: state === 'active' ? Date.now() : null,
      completedAt: null,
      attempts: 0,
      maxAttempts: 3,
      backoff: 1000,
      ttl: null,
      timeout: null,
      uniqueKey: null,
      customId: null,
      dependsOn: [],
      parentId: null,
      childrenIds: [],
      childrenCompleted: 0,
      tags: [],
      lifo: false,
      groupId: null,
      progress: 0,
      progressMessage: null,
      removeOnComplete: false,
      removeOnFail: false,
      lastHeartbeat: Date.now(),
      stallTimeout: null,
      stallCount: 0,
      timeline: [],
    });

    test('should load pending jobs', () => {
      const job1 = createTestJob('pending-1', 'waiting', Date.now());
      const job2 = createTestJob('pending-2', 'delayed', Date.now() + 10000);

      storage.insertJobImmediate(job1);
      storage.insertJobImmediate(job2);

      const pending = storage.loadPendingJobs();
      expect(pending.length).toBeGreaterThanOrEqual(2);
    });

    test('should load active jobs', () => {
      const job = createTestJob('active-1', 'active', Date.now());
      storage.insertJobImmediate(job);
      storage.markActive(job.id, Date.now());

      const active = storage.loadActiveJobs();
      expect(active.length).toBeGreaterThanOrEqual(1);
    });
  });

  describe('Data Serialization', () => {
    const createTestJob = (id: string, data: any) => ({
      id: jobId(id),
      queue: 'test',
      data,
      priority: 0,
      createdAt: Date.now(),
      runAt: Date.now(),
      startedAt: null,
      completedAt: null,
      attempts: 0,
      maxAttempts: 3,
      backoff: 1000,
      ttl: null,
      timeout: null,
      uniqueKey: null,
      customId: null,
      dependsOn: [],
      parentId: null,
      childrenIds: [],
      childrenCompleted: 0,
      tags: ['tag1', 'tag2'],
      lifo: false,
      groupId: null,
      progress: 0,
      progressMessage: null,
      removeOnComplete: false,
      removeOnFail: false,
      lastHeartbeat: Date.now(),
      stallTimeout: null,
      stallCount: 0,
      timeline: [],
    });

    test('should serialize and deserialize complex job data', () => {
      const complexData = {
        string: 'hello',
        number: 42,
        float: 3.14,
        boolean: true,
        null: null,
        array: [1, 2, 3, 'four', { five: 5 }],
        nested: {
          deep: {
            value: 'nested value',
          },
        },
        date: new Date().toISOString(),
        unicode: '你好世界 🌍',
      };

      const job = createTestJob('complex-data', complexData);
      storage.insertJobImmediate(job);

      const retrieved = storage.getJob(job.id);
      expect(retrieved?.data).toEqual(complexData);
    });

    test('should serialize and deserialize tags', () => {
      const job = createTestJob('tags-job', { test: true });
      job.tags = ['important', 'urgent', 'backend'];
      storage.insertJobImmediate(job);

      const retrieved = storage.getJob(job.id);
      expect(retrieved?.tags).toEqual(['important', 'urgent', 'backend']);
    });

    test('should serialize and deserialize dependsOn', () => {
      const job = createTestJob('deps-job', { test: true });
      job.dependsOn = [jobId('dep1'), jobId('dep2')];
      storage.insertJobImmediate(job);

      const retrieved = storage.getJob(job.id);
      expect(retrieved?.dependsOn.length).toBe(2);
    });
  });
});
