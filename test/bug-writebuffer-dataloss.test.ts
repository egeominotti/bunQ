/**
 * WriteBuffer Data Loss Tests
 *
 * These tests verify the fixes for data loss bugs in sqliteBatch.ts WriteBuffer.
 *
 * Fixed bugs:
 * 1. Persistent errors: Now has exponential backoff and max retry limit
 * 2. Shutdown without flush: stop() now flushes pending jobs
 * 3. Error callback: Now includes retry count and backoff information
 */

import { describe, test, expect } from 'bun:test';
import {
  WriteBuffer,
  BatchInsertManager,
  type WriteBufferErrorCallback,
  type CriticalErrorCallback,
} from '../src/infrastructure/persistence/sqliteBatch';
import type { Job, JobId } from '../src/domain/types/job';

// Helper to create a mock job
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

describe('WriteBuffer Fixes', () => {
  describe('Fix 1: Exponential Backoff and Retry Limit', () => {
    test('backoff increases exponentially on each failure', async () => {
      const flushTimes: number[] = [];

      const mockBatchManager = {
        insertJobsBatch: () => {
          flushTimes.push(Date.now());
          throw new Error('Simulated error');
        },
      } as unknown as BatchInsertManager;

      // Use a long base interval so we only get backoff-triggered flushes
      const buffer = new WriteBuffer(mockBatchManager, 100, 10000, () => {});

      try {
        buffer.add(createMockJob(1));

        // Trigger first flush manually
        try {
          buffer.flush();
        } catch {
          // Expected
        }

        // Wait for backoff retries (200ms, 400ms, 800ms = 1400ms total for 3 retries)
        await Bun.sleep(1600);

        console.log('\n--- Exponential Backoff Test ---');
        console.log(`Flush attempts: ${flushTimes.length}`);

        // Calculate intervals
        const intervals: number[] = [];
        for (let i = 1; i < flushTimes.length; i++) {
          intervals.push(flushTimes[i] - flushTimes[i - 1]);
        }

        console.log(`Intervals: ${intervals.join(', ')}ms`);

        // FIX VERIFIED: Intervals should increase (exponential backoff)
        if (intervals.length >= 2) {
          // Each interval should be roughly double the previous (with some tolerance)
          expect(intervals[1]).toBeGreaterThan(intervals[0] * 1.5);
          console.log('FIX VERIFIED: Exponential backoff is working');
        }
      } finally {
        buffer.stop();
      }
    });

    test('max retries limit is enforced', async () => {
      let errorCount = 0;
      let criticalErrorCalled = false;
      let lostJobCount = 0;

      const mockBatchManager = {
        insertJobsBatch: () => {
          errorCount++;
          throw new Error('Simulated persistent DB error');
        },
      } as unknown as BatchInsertManager;

      const onError: WriteBufferErrorCallback = (err, jobCount, retryInfo) => {
        console.log(
          `Error callback: jobs=${jobCount}, retry=${retryInfo?.retryCount}/${retryInfo?.maxRetries}`
        );
      };

      const onCriticalError: CriticalErrorCallback = (jobs, lastError, totalAttempts) => {
        criticalErrorCalled = true;
        lostJobCount = jobs.length;
        console.log(`CRITICAL: ${jobs.length} jobs lost after ${totalAttempts} attempts`);
      };

      // Short backoff for faster test
      const buffer = new WriteBuffer(mockBatchManager, 100, 10000, onError, onCriticalError);

      // Manually trigger flushes to reach max retries faster
      buffer.add(createMockJob(1));
      buffer.add(createMockJob(2));

      console.log('\n--- Max Retry Limit Test ---');

      // Manually flush until we hit the limit (default max is 10)
      for (let i = 0; i < 15; i++) {
        try {
          buffer.flush();
        } catch {
          // Expected
        }
        if (criticalErrorCalled) break;
      }

      buffer.stop();

      console.log(`Error count: ${errorCount}`);
      console.log(`Critical error called: ${criticalErrorCalled}`);
      console.log(`Lost job count: ${lostJobCount}`);

      // FIX VERIFIED: Max retry limit is enforced
      expect(criticalErrorCalled).toBe(true);
      expect(lostJobCount).toBe(2);
      expect(errorCount).toBe(10); // Default max retries
      console.log('FIX VERIFIED: Max retry limit prevents infinite retries');
    });

    test('retry state resets on successful flush', async () => {
      let shouldFail = true;

      const mockBatchManager = {
        insertJobsBatch: () => {
          if (shouldFail) {
            throw new Error('Simulated error');
          }
          // Success
        },
      } as unknown as BatchInsertManager;

      const buffer = new WriteBuffer(mockBatchManager, 100, 10000, () => {});

      try {
        buffer.add(createMockJob(1));

        // Fail a few times
        for (let i = 0; i < 3; i++) {
          try {
            buffer.flush();
          } catch {
            // Expected
          }
        }

        const stateAfterFailures = buffer.getRetryState();
        console.log('\n--- Retry State Reset Test ---');
        console.log(`After failures: retryCount=${stateAfterFailures.retryCount}`);

        expect(stateAfterFailures.retryCount).toBe(3);

        // Now succeed
        shouldFail = false;
        buffer.add(createMockJob(2));
        buffer.flush();

        const stateAfterSuccess = buffer.getRetryState();
        console.log(`After success: retryCount=${stateAfterSuccess.retryCount}`);

        // FIX VERIFIED: Retry state resets on success
        expect(stateAfterSuccess.retryCount).toBe(0);
        expect(stateAfterSuccess.lastError).toBeNull();
        console.log('FIX VERIFIED: Retry state resets after successful flush');
      } finally {
        buffer.stop();
      }
    });
  });

  describe('Fix 2: Graceful Shutdown', () => {
    test('stop() flushes pending jobs', () => {
      let flushedJobs = 0;

      const mockBatchManager = {
        insertJobsBatch: (jobs: Job[]) => {
          flushedJobs += jobs.length;
        },
      } as unknown as BatchInsertManager;

      const buffer = new WriteBuffer(mockBatchManager, 100, 10000, () => {});

      // Add jobs but don't trigger auto-flush
      buffer.add(createMockJob(1));
      buffer.add(createMockJob(2));
      buffer.add(createMockJob(3));

      console.log('\n--- Graceful Shutdown Test ---');
      console.log(`Jobs in buffer before stop: ${buffer.pendingCount}`);

      // FIX: stop() now flushes pending jobs
      buffer.stop();

      console.log(`Jobs in buffer after stop: ${buffer.pendingCount}`);
      console.log(`Jobs flushed to DB: ${flushedJobs}`);

      // FIX VERIFIED: Jobs are flushed on stop
      expect(buffer.pendingCount).toBe(0);
      expect(flushedJobs).toBe(3);
      console.log('FIX VERIFIED: stop() flushes pending jobs');
    });

    test('stopGracefully() returns number of flushed jobs', async () => {
      let flushedJobs = 0;

      const mockBatchManager = {
        insertJobsBatch: (jobs: Job[]) => {
          flushedJobs += jobs.length;
        },
      } as unknown as BatchInsertManager;

      const buffer = new WriteBuffer(mockBatchManager, 100, 10000, () => {});

      // Add jobs
      for (let i = 0; i < 10; i++) {
        buffer.add(createMockJob(i));
      }

      console.log('\n--- Async Graceful Shutdown Test ---');
      console.log(`Jobs added: 10`);

      const result = await buffer.stopGracefully(5000);

      console.log(`stopGracefully returned: ${result}`);
      console.log(`Jobs actually flushed: ${flushedJobs}`);

      // FIX VERIFIED: stopGracefully returns correct count
      expect(result).toBe(10);
      expect(flushedJobs).toBe(10);
      console.log('FIX VERIFIED: stopGracefully() returns flushed count');
    });

    test('stopGracefully() returns 0 when buffer is empty', async () => {
      const mockBatchManager = {
        insertJobsBatch: () => {},
      } as unknown as BatchInsertManager;

      const buffer = new WriteBuffer(mockBatchManager, 100, 10000, () => {});

      const result = await buffer.stopGracefully();

      expect(result).toBe(0);
    });
  });

  describe('Fix 3: Enhanced Error Callback', () => {
    test('error callback receives retry information', async () => {
      const errorInfos: { retryCount: number; nextBackoffMs: number; maxRetries: number }[] = [];

      const mockBatchManager = {
        insertJobsBatch: () => {
          throw new Error('Simulated error');
        },
      } as unknown as BatchInsertManager;

      const onError: WriteBufferErrorCallback = (err, jobCount, retryInfo) => {
        if (retryInfo) {
          errorInfos.push(retryInfo);
        }
      };

      const buffer = new WriteBuffer(mockBatchManager, 100, 10000, onError);

      try {
        buffer.add(createMockJob(1));

        // Trigger a few failures
        for (let i = 0; i < 3; i++) {
          try {
            buffer.flush();
          } catch {
            // Expected
          }
        }

        console.log('\n--- Enhanced Error Callback Test ---');
        console.log(`Error callbacks with retry info: ${errorInfos.length}`);

        // FIX VERIFIED: Error callback now has retry information
        expect(errorInfos.length).toBe(3);

        // Check retry counts increment
        expect(errorInfos[0].retryCount).toBe(1);
        expect(errorInfos[1].retryCount).toBe(2);
        expect(errorInfos[2].retryCount).toBe(3);

        // Check backoff increases
        expect(errorInfos[1].nextBackoffMs).toBeGreaterThan(errorInfos[0].nextBackoffMs);
        expect(errorInfos[2].nextBackoffMs).toBeGreaterThan(errorInfos[1].nextBackoffMs);

        // Check maxRetries is provided
        expect(errorInfos[0].maxRetries).toBe(10);

        console.log('Retry counts:', errorInfos.map((i) => i.retryCount).join(', '));
        console.log('Backoff values:', errorInfos.map((i) => i.nextBackoffMs).join(', '));
        console.log('FIX VERIFIED: Error callback includes retry information');
      } finally {
        buffer.stop();
      }
    });

    test('getRetryState() exposes current retry state for monitoring', () => {
      const mockBatchManager = {
        insertJobsBatch: () => {
          throw new Error('Test error');
        },
      } as unknown as BatchInsertManager;

      const buffer = new WriteBuffer(mockBatchManager, 100, 10000, () => {});

      try {
        buffer.add(createMockJob(1));

        // Initial state
        let state = buffer.getRetryState();
        expect(state.retryCount).toBe(0);
        expect(state.lastError).toBeNull();

        // After failure
        try {
          buffer.flush();
        } catch {
          // Expected
        }

        state = buffer.getRetryState();
        expect(state.retryCount).toBe(1);
        expect(state.lastError).not.toBeNull();
        expect(state.lastError?.message).toBe('Test error');

        console.log('\n--- Retry State Monitoring Test ---');
        console.log(`Retry count: ${state.retryCount}`);
        console.log(`Current backoff: ${state.currentBackoffMs}ms`);
        console.log(`Last error: ${state.lastError?.message}`);
        console.log('FIX VERIFIED: getRetryState() provides monitoring data');
      } finally {
        buffer.stop();
      }
    });
  });

  describe('Backward Compatibility', () => {
    test('old onError signature still works', () => {
      let errorCalled = false;

      const mockBatchManager = {
        insertJobsBatch: () => {
          throw new Error('Test error');
        },
      } as unknown as BatchInsertManager;

      // Using the old signature without retryInfo
      const onError = (err: Error, jobCount: number) => {
        errorCalled = true;
        expect(err.message).toBe('Test error');
        expect(jobCount).toBe(1);
      };

      const buffer = new WriteBuffer(mockBatchManager, 100, 10000, onError);

      try {
        buffer.add(createMockJob(1));

        try {
          buffer.flush();
        } catch {
          // Expected
        }

        expect(errorCalled).toBe(true);
        console.log('\n--- Backward Compatibility Test ---');
        console.log('FIX VERIFIED: Old onError signature still works');
      } finally {
        buffer.stop();
      }
    });
  });

  describe('Concurrent Operations', () => {
    test('automatic flush triggers respect an outstanding retry backoff', () => {
      let flushCalls = 0;
      let criticalLosses = 0;
      const mockBatchManager = {
        insertJobsBatch: (jobs: Job[]) => {
          flushCalls++;
          return {
            transient: [...jobs],
            conflicts: [],
            error: new Error('temporary outage'),
          };
        },
      } as unknown as BatchInsertManager;
      const buffer = new WriteBuffer(
        mockBatchManager,
        1,
        60_000,
        () => {},
        () => {
          criticalLosses++;
        }
      );

      try {
        buffer.add(createMockJob(1));
        expect(flushCalls).toBe(1);
        expect(buffer.getRetryState().retryCount).toBe(1);

        buffer.addBatch(Array.from({ length: 12 }, (_, index) => createMockJob(index + 2)));
        buffer.withDeferredAutoFlush(() => {
          buffer.add(createMockJob(20));
        });

        expect(flushCalls).toBe(1);
        expect(buffer.getRetryState().retryCount).toBe(1);
        expect(buffer.pendingCount).toBe(14);
        expect(criticalLosses).toBe(0);
      } finally {
        buffer.stop();
      }
    });

    test('an explicit successful flush clears the obsolete retry backoff', () => {
      let shouldFail = true;
      let flushCalls = 0;
      const mockBatchManager = {
        insertJobsBatch: (jobs: Job[]) => {
          flushCalls++;
          return shouldFail
            ? { transient: [...jobs], conflicts: [], error: new Error('temporary outage') }
            : { transient: [], conflicts: [] };
        },
      } as unknown as BatchInsertManager;
      const buffer = new WriteBuffer(mockBatchManager, 1, 60_000, () => {});

      try {
        buffer.add(createMockJob(1));
        expect(flushCalls).toBe(1);
        shouldFail = false;
        expect(buffer.flush()).toBe(1);
        buffer.add(createMockJob(2));
        expect(flushCalls).toBe(3);
        expect(buffer.pendingCount).toBe(0);
      } finally {
        buffer.stop();
      }
    });

    test('removing the last failed job clears its obsolete retry backoff', () => {
      let shouldFail = true;
      let flushCalls = 0;
      const mockBatchManager = {
        insertJobsBatch: (jobs: Job[]) => {
          flushCalls++;
          return shouldFail
            ? { transient: [...jobs], conflicts: [], error: new Error('temporary outage') }
            : { transient: [], conflicts: [] };
        },
      } as unknown as BatchInsertManager;
      const buffer = new WriteBuffer(mockBatchManager, 1, 60_000, () => {});

      try {
        const failed = createMockJob(1);
        buffer.add(failed);
        buffer.removePending(failed.id);
        expect(buffer.pendingCount).toBe(0);
        expect(buffer.getRetryState().retryCount).toBe(0);

        shouldFail = false;
        buffer.add(createMockJob(2));
        expect(flushCalls).toBe(2);
        expect(buffer.pendingCount).toBe(0);
      } finally {
        buffer.stop();
      }
    });

    test('removing the last failed queue clears its obsolete retry backoff', () => {
      let shouldFail = true;
      let flushCalls = 0;
      const mockBatchManager = {
        insertJobsBatch: (jobs: Job[]) => {
          flushCalls++;
          return shouldFail
            ? { transient: [...jobs], conflicts: [], error: new Error('temporary outage') }
            : { transient: [], conflicts: [] };
        },
      } as unknown as BatchInsertManager;
      const buffer = new WriteBuffer(mockBatchManager, 1, 60_000, () => {});

      try {
        const failed = createMockJob(1);
        buffer.add(failed);
        expect(buffer.removePendingForQueue(failed.queue)).toEqual([failed.id]);
        expect(buffer.pendingCount).toBe(0);
        expect(buffer.getRetryState().retryCount).toBe(0);

        shouldFail = false;
        buffer.add(createMockJob(2));
        expect(flushCalls).toBe(2);
        expect(buffer.pendingCount).toBe(0);
      } finally {
        buffer.stop();
      }
    });

    test('nested auto-flush deferrals coalesce one oversized admission batch', () => {
      const flushSizes: number[] = [];
      const mockBatchManager = {
        insertJobsBatch: (jobs: Job[]) => {
          flushSizes.push(jobs.length);
        },
      } as unknown as BatchInsertManager;
      const buffer = new WriteBuffer(mockBatchManager, 3, 60_000, () => {});

      try {
        buffer.withDeferredAutoFlush(() => {
          buffer.add(createMockJob(1));
          buffer.withDeferredAutoFlush(() => {
            buffer.add(createMockJob(2));
            buffer.add(createMockJob(3));
          });
          expect(flushSizes).toEqual([]);
          buffer.add(createMockJob(4));
        });

        expect(flushSizes).toEqual([4]);
        expect(buffer.pendingCount).toBe(0);
      } finally {
        buffer.stop();
      }
    });

    test('a deferred batch below the threshold remains buffered and queryable', () => {
      const mockBatchManager = {
        insertJobsBatch: () => undefined,
      } as unknown as BatchInsertManager;
      const buffer = new WriteBuffer(mockBatchManager, 5, 60_000, () => {});

      try {
        buffer.withDeferredAutoFlush(() => {
          buffer.add(createMockJob(1));
          buffer.add({ ...createMockJob(2), queue: 'other-queue' });
        });

        expect(buffer.pendingCount).toBe(2);
        expect(buffer.getPendingJobs('test-queue').map((job) => job.id)).toEqual([1n]);
        expect(buffer.getPendingJobs('other-queue').map((job) => job.id)).toEqual([2n]);
      } finally {
        buffer.stop();
      }
    });

    test('deferred execution preserves an explicitly thrown undefined value', () => {
      const mockBatchManager = {
        insertJobsBatch: () => undefined,
      } as unknown as BatchInsertManager;
      const buffer = new WriteBuffer(mockBatchManager, 5, 60_000, () => {});
      let escaped = false;

      try {
        try {
          buffer.withDeferredAutoFlush(() => {
            throw undefined;
          });
        } catch (error) {
          escaped = true;
          expect(error).toBeUndefined();
        }
        expect(escaped).toBe(true);
      } finally {
        buffer.stop();
      }
    });

    test('concurrent add during flush still works correctly', async () => {
      let flushCount = 0;
      const flushSizes: number[] = [];

      const mockBatchManager = {
        insertJobsBatch: (jobs: Job[]) => {
          flushCount++;
          flushSizes.push(jobs.length);
        },
      } as unknown as BatchInsertManager;

      const buffer = new WriteBuffer(mockBatchManager, 5, 1000, () => {});

      try {
        // Add jobs to trigger flush
        for (let i = 0; i < 5; i++) {
          buffer.add(createMockJob(i));
        }

        // Add more while flush might be happening
        buffer.add(createMockJob(100));

        console.log('\n--- Concurrent Add Test ---');
        console.log(`Flush count: ${flushCount}`);
        console.log(`Flush sizes: ${flushSizes.join(', ')}`);
        console.log(`Pending count: ${buffer.pendingCount}`);

        // Double-buffer design still works
        expect(flushCount).toBe(1);
        expect(flushSizes[0]).toBe(5);
        expect(buffer.pendingCount).toBe(1);
      } finally {
        buffer.stop();
      }
    });
  });
});
