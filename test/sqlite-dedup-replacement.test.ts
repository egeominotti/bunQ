import { afterEach, beforeEach, describe, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createJob, jobId, type Job } from '../src/domain/types/job';
import { SqliteStorage } from '../src/infrastructure/persistence/sqlite';

function makeJob(id: string, data: unknown = { id }): Job {
  return createJob(jobId(id), 'dedup-replacement', { data });
}

function makeDedupJob(id: string, uniqueKey: string): Job {
  return createJob(jobId(id), 'dedup-replacement', { data: { id }, uniqueKey });
}

describe('SqliteStorage dedup replacement', () => {
  let directory: string;
  let databasePath: string;
  let storage: SqliteStorage;

  beforeEach(async () => {
    directory = await mkdtemp(join(tmpdir(), 'bunqueue-sqlite-dedup-replacement-'));
    databasePath = join(directory, 'queue.db');
    storage = new SqliteStorage({
      path: databasePath,
      writeBufferSize: 1_000,
      writeBufferFlushMs: 60_000,
    });
  });

  afterEach(async () => {
    storage.close();
    await rm(directory, { recursive: true, force: true });
  });

  test('durably replaces a job and retires its result and parent failure rows', () => {
    const oldJob = makeJob('old-durable');
    const newJob = makeJob('new-durable');
    storage.insertJobImmediate(oldJob);
    storage.storeResult(oldJob.id, { status: 'old' });
    storage.saveFlowFailure({
      parentId: oldJob.id,
      childId: jobId('failed-child'),
      childQueue: 'children',
      mode: 'fail',
      error: 'child failed',
      createdAt: Date.now(),
    });

    storage.replaceJob(oldJob.id, newJob, true);

    expect(storage.getJob(oldJob.id)).toBeNull();
    expect(storage.getResult(oldJob.id)).toBeNull();
    expect(storage.loadFlowFailures()).toEqual([]);
    expect(storage.getJob(newJob.id)?.data).toEqual(newJob.data);
  });

  test('rolls back the durable retirement when the replacement insert fails', () => {
    const oldJob = makeJob('old-rollback');
    const invalidReplacement = {
      ...makeJob('new-invalid'),
      queue: null,
    } as unknown as Job;
    storage.insertJobImmediate(oldJob);
    storage.storeResult(oldJob.id, { status: 'preserved' });
    storage.saveFlowFailure({
      parentId: oldJob.id,
      childId: jobId('rollback-child'),
      childQueue: 'children',
      mode: 'fail',
      error: 'preserved failure',
      createdAt: Date.now(),
    });

    expect(() => storage.replaceJob(oldJob.id, invalidReplacement, true)).toThrow(
      /NOT NULL constraint failed: jobs\.queue/
    );

    expect(storage.getJob(oldJob.id)?.id).toBe(oldJob.id);
    expect(storage.getResult(oldJob.id)).toEqual({ status: 'preserved' });
    expect(storage.loadFlowFailures()).toHaveLength(1);
    expect(storage.getJob(invalidReplacement.id)).toBeNull();
  });

  test('preserves a buffered old job when a durable replacement fails', () => {
    const oldJob = makeJob('old-buffered-rollback');
    const invalidReplacement = {
      ...makeJob('new-buffered-invalid'),
      queue: null,
    } as unknown as Job;
    storage.insertJob(oldJob);

    expect(() => storage.replaceJob(oldJob.id, invalidReplacement, true)).toThrow(
      /NOT NULL constraint failed: jobs\.queue/
    );

    expect(storage.flushWriteBuffer()).toBe(1);
    expect(storage.getJob(oldJob.id)?.id).toBe(oldJob.id);
    expect(storage.getJob(invalidReplacement.id)).toBeNull();
  });

  test('retires buffered and persisted copies before buffering a non-durable replacement', () => {
    const oldJob = makeJob('old-buffered', { version: 'persisted' });
    const pendingOldJob = makeJob('old-buffered', { version: 'pending' });
    const newJob = makeJob('new-buffered');
    storage.insertJobImmediate(oldJob);
    storage.insertJob(pendingOldJob);
    storage.insertJob(pendingOldJob);

    storage.replaceJob(oldJob.id, newJob);

    expect(storage.getJob(oldJob.id)).toBeNull();
    expect(storage.getJob(newJob.id)).toBeNull();
    expect(storage.flushWriteBuffer()).toBe(1);
    expect(storage.getJob(oldJob.id)).toBeNull();
    expect(storage.getJob(newJob.id)?.id).toBe(newJob.id);
  });

  test('preserves buffered copies when non-durable retirement fails', () => {
    const oldJob = makeJob('old-delete-failure', { version: 'persisted' });
    const pendingOldJob = makeJob('old-delete-failure', { version: 'pending' });
    const newJob = makeJob('new-after-delete-failure');
    storage.insertJobImmediate(oldJob);
    storage.insertJob(pendingOldJob);
    storage.insertJob(pendingOldJob);

    const database = new Database(databasePath);
    database.run(`
      CREATE TRIGGER fail_old_job_delete
      BEFORE DELETE ON jobs
      WHEN OLD.id = 'old-delete-failure'
      BEGIN
        SELECT RAISE(ABORT, 'injected delete failure');
      END
    `);
    database.close();

    expect(() => storage.replaceJob(oldJob.id, newJob)).toThrow(/injected delete failure/);

    expect(storage.getJob(oldJob.id)?.data).toEqual({ version: 'persisted' });
    expect(storage.getJob(newJob.id)).toBeNull();
    expect(storage.flushWriteBuffer()).toBe(2);
    expect(storage.getJob(oldJob.id)?.data).toEqual({ version: 'pending' });
  });

  test('durably transfers an active dedup key without retiring the active row', () => {
    const key = 'active-transfer';
    const oldJob = makeDedupJob('old-active', key);
    const newJob = makeDedupJob('new-after-active', key);
    const startedAt = Date.now();
    storage.insertJobImmediate(oldJob);
    storage.markActive(oldJob.id, startedAt);

    storage.transferActiveDedupJob(oldJob.id, newJob, true);

    const persistedOld = storage.getJob(oldJob.id);
    expect(persistedOld?.id).toBe(oldJob.id);
    expect(persistedOld?.startedAt).toBe(startedAt);
    expect(persistedOld?.uniqueKey).toBeNull();
    expect(storage.getJob(newJob.id)?.uniqueKey).toBe(key);
  });

  test('rolls back an active key transfer without disturbing buffered jobs', () => {
    const key = 'active-transfer-rollback';
    const oldJob = makeDedupJob('old-active-rollback', key);
    const bufferedA = makeJob('buffered-a');
    const bufferedB = makeJob('buffered-b');
    const invalidReplacement = {
      ...makeDedupJob('new-active-invalid', key),
      queue: null,
    } as unknown as Job;
    storage.insertJobImmediate(oldJob);
    storage.markActive(oldJob.id, Date.now());
    storage.insertJob(bufferedA);
    storage.insertJob(bufferedB);

    expect(() => storage.transferActiveDedupJob(oldJob.id, invalidReplacement, true)).toThrow(
      /NOT NULL constraint failed: jobs\.queue/
    );

    expect(storage.getJob(oldJob.id)?.uniqueKey).toBe(key);
    expect(storage.getJob(invalidReplacement.id)).toBeNull();
    expect(storage.flushWriteBuffer()).toBe(2);
    expect(storage.getJob(bufferedA.id)?.id).toBe(bufferedA.id);
    expect(storage.getJob(bufferedB.id)?.id).toBe(bufferedB.id);
  });

  test('clears the active key before buffering a non-durable successor', () => {
    const key = 'active-transfer-buffered';
    const oldJob = makeDedupJob('old-active-buffered', key);
    const newJob = makeDedupJob('new-active-buffered', key);
    storage.insertJobImmediate(oldJob);
    storage.markActive(oldJob.id, Date.now());

    storage.transferActiveDedupJob(oldJob.id, newJob);

    expect(storage.getJob(oldJob.id)?.uniqueKey).toBeNull();
    expect(storage.getJob(newJob.id)).toBeNull();
    expect(storage.flushWriteBuffer()).toBe(1);
    expect(storage.getJob(newJob.id)?.uniqueKey).toBe(key);
  });
});
