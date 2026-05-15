/**
 * Regression: SqliteStorage must wire a critical-loss callback into its
 * WriteBuffer so jobs that hit max retries are surfaced for ops recovery.
 *
 * Bug history: src/infrastructure/persistence/sqlite.ts previously constructed
 *   new WriteBuffer(batchManager, size, flushMs, onError /*no 5th arg*\/)
 * without onCriticalError. When max retries (10) were exhausted in
 * sqliteBatch.ts:209-223, the dropped jobs were wiped from activeBuffer and
 * silently discarded — no logs with full job data, no programmatic access.
 *
 * Fix: SqliteStorage now passes its own handler that
 *   1. logs each lost job with id/queue/customId/priority/data preview;
 *   2. retains the last N loss records for inspection via getCriticalLosses();
 *   3. forwards to a user-supplied onCriticalLoss callback if provided.
 */

import { describe, test, expect } from 'bun:test';
import {
  WriteBuffer,
  type BatchInsertManager,
  type WriteBufferErrorCallback,
} from '../src/infrastructure/persistence/sqliteBatch';
import { SqliteStorage } from '../src/infrastructure/persistence/sqlite';
import type { Job, JobId } from '../src/domain/types/job';
import { tmpdir } from 'os';
import { join } from 'path';
import { randomUUID } from 'crypto';
import { unlinkSync } from 'fs';

function mockJob(id: number): Job {
  return {
    id: BigInt(id) as JobId,
    queue: 'q',
    data: { i: id },
    priority: 0,
    createdAt: Date.now(),
    runAt: Date.now(),
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
    tags: [],
    lifo: false,
    groupId: null,
    removeOnComplete: false,
    removeOnFail: false,
    stallTimeout: null,
    startedAt: null,
    completedAt: null,
    failedReason: null,
    progress: null,
    lastHeartbeat: null,
    lockToken: null,
    lockExpiresAt: null,
    timeline: [],
  } as unknown as Job;
}

describe('WriteBuffer critical-loss handling at SqliteStorage', () => {
  test('WriteBuffer.onCriticalError is honored when supplied (sanity)', () => {
    let capturedJobs: Job[] = [];

    const alwaysFailingBatch = {
      insertJobsBatch: () => {
        throw new Error('Persistent DB failure');
      },
    } as unknown as BatchInsertManager;

    const onError: WriteBufferErrorCallback = () => {};

    const buffer = new WriteBuffer(
      alwaysFailingBatch,
      100,
      10000,
      onError,
      (jobs) => {
        capturedJobs = jobs;
      }
    );

    buffer.add(mockJob(1));
    buffer.add(mockJob(2));
    buffer.add(mockJob(3));

    for (let i = 0; i < 11; i++) {
      try {
        buffer.flush();
      } catch {
        // expected
      }
    }
    buffer.stop();

    expect(capturedJobs.length).toBe(3);
    expect(capturedJobs.map((j) => Number(j.id)).sort()).toEqual([1, 2, 3]);
  });

  test('SqliteStorage exposes critical-loss records when WriteBuffer drops jobs', () => {
    // Spin up a real SqliteStorage, then inject a failing BatchInsertManager
    // into its WriteBuffer to force the loss path.
    const dbPath = join(tmpdir(), `bunq-fix-${randomUUID()}.db`);
    const storage = new SqliteStorage({
      path: dbPath,
      writeBufferSize: 1000,
      writeBufferFlushMs: 60_000,
    });

    // Swap the internal batch manager for a failing one
    const buffer = (storage as unknown as { writeBuffer: WriteBuffer }).writeBuffer;
    const failingMgr = {
      insertJobsBatch: () => {
        throw new Error('Simulated disk-full');
      },
    } as unknown as BatchInsertManager;
    (buffer as unknown as { batchManager: BatchInsertManager }).batchManager = failingMgr;

    storage.insertJob(mockJob(1));
    storage.insertJob(mockJob(2));

    for (let i = 0; i < 11; i++) {
      try {
        buffer.flush();
      } catch {
        // expected
      }
    }

    const losses = storage.getCriticalLosses();
    expect(losses.length).toBe(1);
    expect(losses[0].jobs.length).toBe(2);
    expect(losses[0].error).toContain('Simulated disk-full');
    expect(losses[0].attempts).toBeGreaterThanOrEqual(10);

    // Cleanup
    (storage as unknown as { db: { close: (b: boolean) => void } }).db.close(false);
    try {
      unlinkSync(dbPath);
    } catch {
      // ignore
    }
  });

  test('user-supplied onCriticalLoss callback receives the dropped jobs', () => {
    const captured: Array<{ jobs: Job[]; error: Error; attempts: number }> = [];

    const dbPath = join(tmpdir(), `bunq-fix-${randomUUID()}.db`);
    const storage = new SqliteStorage({
      path: dbPath,
      writeBufferSize: 1000,
      writeBufferFlushMs: 60_000,
      onCriticalLoss: (jobs, error, attempts) => {
        captured.push({ jobs, error, attempts });
      },
    });

    const buffer = (storage as unknown as { writeBuffer: WriteBuffer }).writeBuffer;
    const failingMgr = {
      insertJobsBatch: () => {
        throw new Error('Boom');
      },
    } as unknown as BatchInsertManager;
    (buffer as unknown as { batchManager: BatchInsertManager }).batchManager = failingMgr;

    storage.insertJob(mockJob(42));

    for (let i = 0; i < 11; i++) {
      try {
        buffer.flush();
      } catch {
        // expected
      }
    }

    expect(captured.length).toBe(1);
    expect(captured[0].jobs.length).toBe(1);
    expect(captured[0].jobs[0].id).toBe(BigInt(42) as JobId);
    expect(captured[0].error.message).toBe('Boom');

    (storage as unknown as { db: { close: (b: boolean) => void } }).db.close(false);
    try {
      unlinkSync(dbPath);
    } catch {
      // ignore
    }
  });
});
