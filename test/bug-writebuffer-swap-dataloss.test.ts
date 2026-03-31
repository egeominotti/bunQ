/**
 * WriteBuffer Data Loss Bug Tests
 *
 * Bug: The double-buffer swap design in flush() creates data loss scenarios:
 *
 * 1. stopGracefully() timeout: When flush takes longer than timeout, jobs
 *    are silently lost. resolve(-1) is called but no callback reports which
 *    jobs were lost. The buffer is marked stopped, jobs are gone forever.
 *
 * 2. stop() with persistent errors: When flush() throws during stop(),
 *    the error is caught and swallowed (line 294). Jobs remain in buffers
 *    but buffer is marked stopped. No callback reports the lost jobs.
 *
 * 3. Double-buffer swap window: Between buffer swap (line 182-183) and
 *    successful write (line 188), if the process exits, jobs in flushBuffer
 *    are lost. While crash data loss is expected for buffered mode, the
 *    swap design makes error recovery harder than necessary.
 *
 * These tests FAIL before the fix, PASS after.
 */

import { describe, test, expect } from 'bun:test';
import {
  WriteBuffer,
  BatchInsertManager,
  type WriteBufferErrorCallback,
  type CriticalErrorCallback,
} from '../src/infrastructure/persistence/sqliteBatch';
import type { Job, JobId } from '../src/domain/types/job';

/** Helper to create a mock job */
function createMockJob(id: number): Job {
  return {
    id: BigInt(id) as JobId,
    queue: 'test-queue',
    data: { test: id },
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
  };
}

describe('WriteBuffer Data Loss Bugs', () => {
  /**
   * BUG 1: stopGracefully() timeout silently loses jobs.
   *
   * When flush takes longer than the timeout, stopGracefully() resolves
   * with -1 and sets stopped=true. The buffered jobs are silently lost
   * with no callback or way to recover them.
   *
   * Expected behavior: When timeout occurs, lost jobs should be reported
   * via a callback so the caller can take recovery action (e.g., re-queue,
   * log to DLQ, etc.)
   */
  test('stopGracefully should report lost jobs when flush fails', async () => {
    const mockBatchManager = {
      insertJobsBatch: () => {
        throw new Error('Simulated DB error during graceful shutdown');
      },
    } as unknown as BatchInsertManager;

    const lostJobs: Job[] = [];
    const onError: WriteBufferErrorCallback = () => {};
    const onCriticalError: CriticalErrorCallback = (jobs) => {
      lostJobs.push(...jobs);
    };

    const buffer = new WriteBuffer(mockBatchManager, 100, 60000, onError, onCriticalError);

    // Add 5 jobs
    for (let i = 0; i < 5; i++) {
      buffer.add(createMockJob(i));
    }

    expect(buffer.pendingCount).toBe(5);

    // stopGracefully will try to flush, which will fail
    const result = await buffer.stopGracefully(5000);

    // FIX: When flush fails during graceful shutdown, lost jobs should be
    // reported via onCriticalError so the caller can take recovery action.
    expect(result).toBe(0); // flush failed, returns 0
    expect(lostJobs.length).toBe(5);
  });

  /**
   * BUG 2: stop() silently drops jobs when flush fails.
   *
   * When stop() is called and flush() throws, the catch block at line 294
   * swallows the error. The jobs remain in the internal buffer but the
   * buffer is marked as stopped, so they can never be flushed.
   *
   * Expected behavior: When stop() can't flush jobs, they should be
   * reported via onCriticalError callback.
   */
  test('stop() should report lost jobs when flush persistently fails', () => {
    const mockBatchManager = {
      insertJobsBatch: () => {
        throw new Error('Simulated persistent DB error');
      },
    } as unknown as BatchInsertManager;

    const lostJobs: Job[] = [];
    const errorMessages: string[] = [];

    const onError: WriteBufferErrorCallback = (err) => {
      errorMessages.push(err.message);
    };

    const onCriticalError: CriticalErrorCallback = (jobs, lastError) => {
      lostJobs.push(...jobs);
    };

    const buffer = new WriteBuffer(mockBatchManager, 100, 60000, onError, onCriticalError);

    // Add jobs
    for (let i = 0; i < 5; i++) {
      buffer.add(createMockJob(i));
    }

    expect(buffer.pendingCount).toBe(5);

    // stop() will try to flush, which will fail
    buffer.stop();

    // BUG: After stop(), the 5 jobs are silently lost.
    // The catch at line 294 swallows the error.
    // No callback is fired to report the lost jobs.
    // FIX: stop() should call onCriticalError when flush fails during shutdown
    expect(lostJobs.length).toBe(5);
  });

  /**
   * BUG 3: After flush error recovery, pendingCount should include
   * ALL recovered jobs (both failed flush jobs + newly added jobs).
   * This verifies the error recovery path at line 204 works correctly.
   */
  test('flush error recovery preserves all jobs in correct order', () => {
    let flushAttempt = 0;
    const flushedJobIds: number[] = [];

    const mockBatchManager = {
      insertJobsBatch: (jobs: Job[]) => {
        flushAttempt++;
        if (flushAttempt === 1) {
          throw new Error('First flush fails');
        }
        // Second flush succeeds
        for (const job of jobs) {
          flushedJobIds.push(Number(job.id));
        }
      },
    } as unknown as BatchInsertManager;

    const buffer = new WriteBuffer(mockBatchManager, 100, 60000, () => {});

    try {
      // Add initial jobs
      buffer.add(createMockJob(1));
      buffer.add(createMockJob(2));
      buffer.add(createMockJob(3));

      // First flush fails - jobs should be recovered
      try {
        buffer.flush();
      } catch {
        // Expected
      }

      // After failed flush, all 3 jobs should still be pending
      expect(buffer.pendingCount).toBe(3);

      // Add more jobs after failure
      buffer.add(createMockJob(4));
      buffer.add(createMockJob(5));

      // Now we have 5 pending: [1,2,3] (recovered) + [4,5] (new)
      expect(buffer.pendingCount).toBe(5);

      // Second flush succeeds
      const flushed = buffer.flush();
      expect(flushed).toBe(5);

      // Order should be: failed jobs first (1,2,3), then new jobs (4,5)
      expect(flushedJobIds).toEqual([1, 2, 3, 4, 5]);

      // Nothing pending
      expect(buffer.pendingCount).toBe(0);
    } finally {
      buffer.stop();
    }
  });

  /**
   * BUG 4: Verify that the number of jobs reported as lost by
   * onCriticalError after max retries is accurate.
   *
   * After max retries are exhausted, ALL buffered jobs (including any
   * newly added during retry window) should be reported.
   */
  test('max retries reports ALL buffered jobs as lost, including those added during retries', () => {
    let flushCount = 0;
    const mockBatchManager = {
      insertJobsBatch: () => {
        flushCount++;
        throw new Error('Persistent error');
      },
    } as unknown as BatchInsertManager;

    let reportedLostJobs: Job[] = [];
    const onCriticalError: CriticalErrorCallback = (jobs) => {
      reportedLostJobs = [...jobs];
    };

    const buffer = new WriteBuffer(mockBatchManager, 100, 60000, () => {}, onCriticalError);

    // Add initial jobs
    buffer.add(createMockJob(1));
    buffer.add(createMockJob(2));

    // Run 9 failed flushes (max is 10)
    for (let i = 0; i < 9; i++) {
      try {
        buffer.flush();
      } catch {
        // Expected
      }
    }

    // Add another job during retry window
    buffer.add(createMockJob(3));

    // This should be the 10th attempt, triggering critical error
    try {
      buffer.flush();
    } catch {
      // Expected
    }

    buffer.stop();

    // ALL 3 jobs should be reported as lost (2 original + 1 added during retries)
    expect(reportedLostJobs.length).toBe(3);
    const lostIds = reportedLostJobs.map((j) => Number(j.id)).sort();
    expect(lostIds).toEqual([1, 2, 3]);
  });

  /**
   * BUG 5: stopGracefully should report exact lost jobs via onCriticalError
   * when flush fails, so the caller knows precisely which jobs were lost.
   */
  test('stopGracefully reports exact lost jobs via onCriticalError when flush fails', async () => {
    const mockBatchManager = {
      insertJobsBatch: () => {
        throw new Error('DB write failure');
      },
    } as unknown as BatchInsertManager;

    const lostJobs: Job[] = [];
    const onCriticalError: CriticalErrorCallback = (jobs) => {
      lostJobs.push(...jobs);
    };

    const buffer = new WriteBuffer(mockBatchManager, 100, 60000, () => {}, onCriticalError);

    buffer.add(createMockJob(1));
    buffer.add(createMockJob(2));
    buffer.add(createMockJob(3));

    const result = await buffer.stopGracefully(5000);

    // Flush failed, returns 0
    expect(result).toBe(0);
    // onCriticalError should report the exact 3 lost jobs
    expect(lostJobs.length).toBe(3);
    const lostIds = lostJobs.map((j) => Number(j.id)).sort();
    expect(lostIds).toEqual([1, 2, 3]);
  });
});
