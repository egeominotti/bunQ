/**
 * Domain Types Unit Tests
 *
 * Tests for business logic in domain type files:
 * - job.ts: Job creation, backoff calculation, state checks, lock management
 * - response.ts: Response builder functions
 * - queue.ts: Queue state, RateLimiter, ConcurrencyLimiter
 * - cron.ts: Cron job creation, limit/due checks
 *
 * Note: command.ts contains only type definitions with no runtime logic.
 */

import { describe, test, expect } from 'bun:test';

// ============ job.ts imports ============
import {
  jobId,
  generateJobId,
  createJob,
  isDelayed,
  isReady,
  isExpired,
  isTimedOut,
  calculateBackoff,
  canRetry,
  lockToken,
  generateLockToken,
  createJobLock,
  isLockExpired,
  renewLock,
  JOB_DEFAULTS,
  DEFAULT_LOCK_TTL,
} from '../src/domain/types/job';
import type { JobId, Job, JobInput } from '../src/domain/types/job';

// ============ response.ts imports ============
import {
  ok,
  batch,
  job as jobResponse,
  nullableJob,
  pulledJob,
  pulledJobs,
  jobs as jobsResponse,
  error,
  hello,
  data,
  counts,
  stats,
  metrics,
} from '../src/domain/types/response';

// ============ queue.ts imports ============
import { createQueueState, RateLimiter, ConcurrencyLimiter } from '../src/domain/types/queue';

// ============ cron.ts imports ============
import { createCronJob, isAtLimit, isDue } from '../src/domain/types/cron';

// ============ Helpers ============

/** Create a minimal JobInput for testing */
function minimalInput(overrides: Partial<JobInput> = {}): JobInput {
  return { data: { test: true }, ...overrides };
}

/** Create a full Job for testing state-check functions */
function makeJob(overrides: Partial<Job> = {}): Job {
  const id = jobId('test-id');
  const now = 1000000;
  const base = createJob(id, 'test-queue', minimalInput(), now);
  return { ...base, ...overrides };
}

// =====================================================================
//  job.ts
// =====================================================================

describe('job.ts', () => {
  // ---- Branded types & ID generation ----

  describe('jobId', () => {
    test('should wrap a string as a branded JobId', () => {
      const id = jobId('abc-123');
      expect(id).toBe('abc-123');
      // It's still a string at runtime
      expect(typeof id).toBe('string');
    });
  });

  describe('generateJobId', () => {
    test('should return a UUIDv7 string', () => {
      const id = generateJobId();
      // UUIDv7 has version nibble 7 at position 14
      expect(id[14]).toBe('7');
      expect(id.length).toBe(36);
    });

    test('should produce unique IDs', () => {
      const ids = new Set<string>();
      for (let i = 0; i < 100; i++) {
        ids.add(generateJobId());
      }
      expect(ids.size).toBe(100);
    });
  });

  // ---- JOB_DEFAULTS ----

  describe('JOB_DEFAULTS', () => {
    test('should have expected default values', () => {
      expect(JOB_DEFAULTS.priority).toBe(0);
      expect(JOB_DEFAULTS.maxAttempts).toBe(3);
      expect(JOB_DEFAULTS.backoff).toBe(1000);
      expect(JOB_DEFAULTS.lifo).toBe(false);
      expect(JOB_DEFAULTS.removeOnComplete).toBe(false);
      expect(JOB_DEFAULTS.removeOnFail).toBe(false);
      expect(JOB_DEFAULTS.stackTraceLimit).toBe(10);
    });
  });

  // ---- createJob ----

  describe('createJob', () => {
    test('should create a job with minimal input and all defaults', () => {
      const id = jobId('j1');
      const now = 1700000000000;
      const j = createJob(id, 'emails', minimalInput(), now);

      expect(j.id).toBe('j1');
      expect(j.queue).toBe('emails');
      expect(j.data).toEqual({ test: true });
      expect(j.priority).toBe(JOB_DEFAULTS.priority);
      expect(j.maxAttempts).toBe(JOB_DEFAULTS.maxAttempts);
      expect(j.backoff).toBe(JOB_DEFAULTS.backoff);
      expect(j.lifo).toBe(JOB_DEFAULTS.lifo);
      expect(j.removeOnComplete).toBe(JOB_DEFAULTS.removeOnComplete);
      expect(j.removeOnFail).toBe(JOB_DEFAULTS.removeOnFail);
      expect(j.createdAt).toBe(now);
      expect(j.runAt).toBe(now); // no delay
      expect(j.startedAt).toBeNull();
      expect(j.completedAt).toBeNull();
      expect(j.attempts).toBe(0);
      expect(j.backoffConfig).toBeNull();
      expect(j.ttl).toBeNull();
      expect(j.timeout).toBeNull();
      expect(j.uniqueKey).toBeNull();
      expect(j.customId).toBeNull();
      expect(j.parentId).toBeNull();
      expect(j.groupId).toBeNull();
      expect(j.stallTimeout).toBeNull();
      expect(j.dependsOn).toEqual([]);
      expect(j.childrenIds).toEqual([]);
      expect(j.childrenCompleted).toBe(0);
      expect(j.tags).toEqual([]);
      expect(j.progress).toBe(0);
      expect(j.progressMessage).toBeNull();
      expect(j.repeat).toBeNull();
      expect(j.lastHeartbeat).toBe(now);
      expect(j.stallCount).toBe(0);
    });

    test('should apply delay to runAt', () => {
      const now = 1000;
      const j = createJob(jobId('j2'), 'q', minimalInput({ delay: 5000 }), now);
      expect(j.runAt).toBe(6000);
    });

    test('should use timestamp from input as createdAt when provided', () => {
      const now = 2000;
      const j = createJob(jobId('j3'), 'q', minimalInput({ timestamp: 1500 }), now);
      expect(j.createdAt).toBe(1500);
      // runAt should be based on createdAt (timestamp), not `now`
      expect(j.runAt).toBe(1500);
    });

    test('should apply timestamp + delay together', () => {
      const j = createJob(jobId('j4'), 'q', minimalInput({ timestamp: 1000, delay: 500 }), 9999);
      expect(j.createdAt).toBe(1000);
      expect(j.runAt).toBe(1500);
    });

    test('should set priority from input', () => {
      const j = createJob(jobId('j5'), 'q', minimalInput({ priority: 10 }), 0);
      expect(j.priority).toBe(10);
    });

    test('should set maxAttempts from input', () => {
      const j = createJob(jobId('j6'), 'q', minimalInput({ maxAttempts: 5 }), 0);
      expect(j.maxAttempts).toBe(5);
    });

    test('should set lifo from input', () => {
      const j = createJob(jobId('j7'), 'q', minimalInput({ lifo: true }), 0);
      expect(j.lifo).toBe(true);
    });

    test('should set removeOnComplete and removeOnFail', () => {
      const j = createJob(
        jobId('j8'),
        'q',
        minimalInput({ removeOnComplete: true, removeOnFail: true }),
        0
      );
      expect(j.removeOnComplete).toBe(true);
      expect(j.removeOnFail).toBe(true);
    });

    test('should set optional nullable fields', () => {
      const j = createJob(
        jobId('j9'),
        'q',
        minimalInput({
          ttl: 60000,
          timeout: 30000,
          uniqueKey: 'unique-1',
          customId: 'custom-1',
          parentId: jobId('parent-1'),
          groupId: 'group-a',
          stallTimeout: 15000,
        }),
        0
      );
      expect(j.ttl).toBe(60000);
      expect(j.timeout).toBe(30000);
      expect(j.uniqueKey).toBe('unique-1');
      expect(j.customId).toBe('custom-1');
      expect(j.parentId).toBe('parent-1');
      expect(j.groupId).toBe('group-a');
      expect(j.stallTimeout).toBe(15000);
    });

    test('should set tags and dependsOn', () => {
      const deps = [jobId('dep-1'), jobId('dep-2')];
      const j = createJob(
        jobId('j10'),
        'q',
        minimalInput({ tags: ['urgent', 'email'], dependsOn: deps }),
        0
      );
      expect(j.tags).toEqual(['urgent', 'email']);
      expect(j.dependsOn).toEqual(['dep-1', 'dep-2']);
    });

    // ---- Backoff parsing ----

    test('should parse numeric backoff (no config)', () => {
      const j = createJob(jobId('j11'), 'q', minimalInput({ backoff: 2000 }), 0);
      expect(j.backoff).toBe(2000);
      expect(j.backoffConfig).toBeNull();
    });

    test('should parse object backoff with fixed type', () => {
      const j = createJob(
        jobId('j12'),
        'q',
        minimalInput({ backoff: { type: 'fixed', delay: 3000 } }),
        0
      );
      expect(j.backoff).toBe(3000);
      expect(j.backoffConfig).toEqual({ type: 'fixed', delay: 3000 });
    });

    test('should parse object backoff with exponential type', () => {
      const j = createJob(
        jobId('j13'),
        'q',
        minimalInput({ backoff: { type: 'exponential', delay: 500 } }),
        0
      );
      expect(j.backoff).toBe(500);
      expect(j.backoffConfig).toEqual({ type: 'exponential', delay: 500 });
    });

    // ---- Repeat config parsing ----

    test('should parse repeat config with every', () => {
      const j = createJob(
        jobId('j14'),
        'q',
        minimalInput({ repeat: { every: 5000, limit: 10 } }),
        0
      );
      expect(j.repeat).not.toBeNull();
      expect(j.repeat!.every).toBe(5000);
      expect(j.repeat!.limit).toBe(10);
      expect(j.repeat!.count).toBe(0);
    });

    test('should parse repeat config with pattern and timezone', () => {
      const j = createJob(
        jobId('j15'),
        'q',
        minimalInput({
          repeat: { pattern: '*/5 * * * *', tz: 'Europe/Rome', immediately: true },
        }),
        0
      );
      expect(j.repeat!.pattern).toBe('*/5 * * * *');
      expect(j.repeat!.tz).toBe('Europe/Rome');
      expect(j.repeat!.immediately).toBe(true);
    });

    test('should default repeat count to 0 when not provided', () => {
      const j = createJob(jobId('j16'), 'q', minimalInput({ repeat: { every: 1000 } }), 0);
      expect(j.repeat!.count).toBe(0);
    });

    test('should preserve repeat count when provided', () => {
      const j = createJob(
        jobId('j17'),
        'q',
        minimalInput({ repeat: { every: 1000, count: 5 } }),
        0
      );
      expect(j.repeat!.count).toBe(5);
    });

    test('should parse full repeat config fields', () => {
      const j = createJob(
        jobId('j18'),
        'q',
        minimalInput({
          repeat: {
            every: 1000,
            limit: 100,
            pattern: '0 * * * *',
            count: 3,
            startDate: 1000,
            endDate: 9000,
            tz: 'UTC',
            immediately: false,
            prevMillis: 500,
            offset: 200,
            jobId: 'repeat-job-1',
          },
        }),
        0
      );
      const r = j.repeat!;
      expect(r.every).toBe(1000);
      expect(r.limit).toBe(100);
      expect(r.pattern).toBe('0 * * * *');
      expect(r.count).toBe(3);
      expect(r.startDate).toBe(1000);
      expect(r.endDate).toBe(9000);
      expect(r.tz).toBe('UTC');
      expect(r.immediately).toBe(false);
      expect(r.prevMillis).toBe(500);
      expect(r.offset).toBe(200);
      expect(r.jobId).toBe('repeat-job-1');
    });

    // ---- BullMQ v5 options ----

    test('should set BullMQ v5 options with defaults', () => {
      const j = createJob(jobId('j19'), 'q', minimalInput(), 0);
      expect(j.stackTraceLimit).toBe(JOB_DEFAULTS.stackTraceLimit);
      expect(j.keepLogs).toBeNull();
      expect(j.sizeLimit).toBeNull();
      expect(j.failParentOnFailure).toBe(false);
      expect(j.removeDependencyOnFailure).toBe(false);
      expect(j.continueParentOnFailure).toBe(false);
      expect(j.ignoreDependencyOnFailure).toBe(false);
      expect(j.deduplicationTtl).toBeNull();
      expect(j.deduplicationExtend).toBe(false);
      expect(j.deduplicationReplace).toBe(false);
      expect(j.debounceId).toBeNull();
      expect(j.debounceTtl).toBeNull();
    });

    test('should set BullMQ v5 options from input', () => {
      const j = createJob(
        jobId('j20'),
        'q',
        minimalInput({
          stackTraceLimit: 20,
          keepLogs: 50,
          sizeLimit: 1024,
          failParentOnFailure: true,
          removeDependencyOnFailure: true,
          continueParentOnFailure: true,
          ignoreDependencyOnFailure: true,
          dedup: { ttl: 5000, extend: true, replace: true },
          debounceId: 'debounce-1',
          debounceTtl: 3000,
        }),
        0
      );
      expect(j.stackTraceLimit).toBe(20);
      expect(j.keepLogs).toBe(50);
      expect(j.sizeLimit).toBe(1024);
      expect(j.failParentOnFailure).toBe(true);
      expect(j.removeDependencyOnFailure).toBe(true);
      expect(j.continueParentOnFailure).toBe(true);
      expect(j.ignoreDependencyOnFailure).toBe(true);
      expect(j.deduplicationTtl).toBe(5000);
      expect(j.deduplicationExtend).toBe(true);
      expect(j.deduplicationReplace).toBe(true);
      expect(j.debounceId).toBe('debounce-1');
      expect(j.debounceTtl).toBe(3000);
    });

    test('should use Date.now() when no now parameter supplied', () => {
      const before = Date.now();
      const j = createJob(jobId('j-now'), 'q', minimalInput());
      const after = Date.now();
      expect(j.createdAt).toBeGreaterThanOrEqual(before);
      expect(j.createdAt).toBeLessThanOrEqual(after);
    });
  });

  // ---- State check functions ----

  describe('isDelayed', () => {
    test('should return true when runAt is in the future', () => {
      const j = makeJob({ runAt: 2000 });
      expect(isDelayed(j, 1000)).toBe(true);
    });

    test('should return false when runAt equals now', () => {
      const j = makeJob({ runAt: 1000 });
      expect(isDelayed(j, 1000)).toBe(false);
    });

    test('should return false when runAt is in the past', () => {
      const j = makeJob({ runAt: 500 });
      expect(isDelayed(j, 1000)).toBe(false);
    });
  });

  describe('isReady', () => {
    test('should return true when runAt is in the past', () => {
      const j = makeJob({ runAt: 500 });
      expect(isReady(j, 1000)).toBe(true);
    });

    test('should return true when runAt equals now', () => {
      const j = makeJob({ runAt: 1000 });
      expect(isReady(j, 1000)).toBe(true);
    });

    test('should return false when runAt is in the future', () => {
      const j = makeJob({ runAt: 2000 });
      expect(isReady(j, 1000)).toBe(false);
    });

    test('isReady and isDelayed are mutually exclusive', () => {
      const j = makeJob({ runAt: 1500 });
      const now = 1000;
      // When delayed, not ready; when ready, not delayed
      expect(isDelayed(j, now)).toBe(true);
      expect(isReady(j, now)).toBe(false);

      expect(isDelayed(j, 2000)).toBe(false);
      expect(isReady(j, 2000)).toBe(true);
    });
  });

  describe('isExpired', () => {
    test('should return false when ttl is null', () => {
      const j = makeJob({ ttl: null, createdAt: 1000 });
      expect(isExpired(j, 999999)).toBe(false);
    });

    test('should return false when within TTL', () => {
      const j = makeJob({ ttl: 5000, createdAt: 1000 });
      expect(isExpired(j, 5999)).toBe(false);
    });

    test('should return false exactly at TTL boundary', () => {
      const j = makeJob({ ttl: 5000, createdAt: 1000 });
      // Boundary: createdAt + ttl = 6000. now=6000 => now > 6000 is false
      expect(isExpired(j, 6000)).toBe(false);
    });

    test('should return true when past TTL', () => {
      const j = makeJob({ ttl: 5000, createdAt: 1000 });
      expect(isExpired(j, 6001)).toBe(true);
    });
  });

  describe('isTimedOut', () => {
    test('should return false when timeout is null', () => {
      const j = makeJob({ timeout: null, startedAt: 1000 });
      expect(isTimedOut(j, 999999)).toBe(false);
    });

    test('should return false when startedAt is null', () => {
      const j = makeJob({ timeout: 5000, startedAt: null });
      expect(isTimedOut(j, 999999)).toBe(false);
    });

    test('should return false within timeout window', () => {
      const j = makeJob({ timeout: 5000, startedAt: 1000 });
      expect(isTimedOut(j, 5999)).toBe(false);
    });

    test('should return false exactly at timeout boundary', () => {
      const j = makeJob({ timeout: 5000, startedAt: 1000 });
      expect(isTimedOut(j, 6000)).toBe(false);
    });

    test('should return true when past timeout', () => {
      const j = makeJob({ timeout: 5000, startedAt: 1000 });
      expect(isTimedOut(j, 6001)).toBe(true);
    });
  });

  // ---- Backoff calculation ----

  describe('calculateBackoff', () => {
    test('should use default exponential backoff when no config', () => {
      // Default: backoff * 2^attempts with ±50% jitter
      const j = makeJob({ backoff: 1000, backoffConfig: null, attempts: 0 });
      // Base = 1000 * 2^0 = 1000, jittered range: [500, 1500]
      let val = calculateBackoff(j);
      expect(val).toBeGreaterThanOrEqual(500);
      expect(val).toBeLessThanOrEqual(1500);

      j.attempts = 1;
      // Base = 2000, range: [1000, 3000]
      val = calculateBackoff(j);
      expect(val).toBeGreaterThanOrEqual(1000);
      expect(val).toBeLessThanOrEqual(3000);

      j.attempts = 2;
      // Base = 4000, range: [2000, 6000]
      val = calculateBackoff(j);
      expect(val).toBeGreaterThanOrEqual(2000);
      expect(val).toBeLessThanOrEqual(6000);

      j.attempts = 3;
      // Base = 8000, range: [4000, 12000]
      val = calculateBackoff(j);
      expect(val).toBeGreaterThanOrEqual(4000);
      expect(val).toBeLessThanOrEqual(12000);
    });

    test('should use fixed backoff config', () => {
      const j = makeJob({
        backoff: 3000,
        backoffConfig: { type: 'fixed', delay: 3000 },
        attempts: 0,
      });
      // Fixed 3000 with ±20% jitter: [2400, 3600]
      let val = calculateBackoff(j);
      expect(val).toBeGreaterThanOrEqual(2400);
      expect(val).toBeLessThanOrEqual(3600);

      j.attempts = 5;
      // Still near 3000 regardless of attempts (fixed type)
      val = calculateBackoff(j);
      expect(val).toBeGreaterThanOrEqual(2400);
      expect(val).toBeLessThanOrEqual(3600);
    });

    test('should use exponential backoff config', () => {
      const j = makeJob({
        backoff: 500,
        backoffConfig: { type: 'exponential', delay: 500 },
        attempts: 0,
      });
      // Base = 500 * 2^0 = 500, jittered range: [250, 750]
      let val = calculateBackoff(j);
      expect(val).toBeGreaterThanOrEqual(250);
      expect(val).toBeLessThanOrEqual(750);

      j.attempts = 1;
      // Base = 1000, range: [500, 1500]
      val = calculateBackoff(j);
      expect(val).toBeGreaterThanOrEqual(500);
      expect(val).toBeLessThanOrEqual(1500);

      j.attempts = 2;
      // Base = 2000, range: [1000, 3000]
      val = calculateBackoff(j);
      expect(val).toBeGreaterThanOrEqual(1000);
      expect(val).toBeLessThanOrEqual(3000);

      j.attempts = 4;
      // Base = 8000, range: [4000, 12000]
      val = calculateBackoff(j);
      expect(val).toBeGreaterThanOrEqual(4000);
      expect(val).toBeLessThanOrEqual(12000);
    });

    test('should grow exponentially for high attempt counts', () => {
      const j = makeJob({
        backoff: 100,
        backoffConfig: { type: 'exponential', delay: 100 },
        attempts: 10,
      });
      // Base = 100 * 2^10 = 102400, jittered range: [51200, 153600]
      const val = calculateBackoff(j);
      expect(val).toBeGreaterThanOrEqual(51200);
      expect(val).toBeLessThanOrEqual(153600);
    });
  });

  // ---- canRetry ----

  describe('canRetry', () => {
    test('should return true when attempts < maxAttempts', () => {
      const j = makeJob({ attempts: 0, maxAttempts: 3 });
      expect(canRetry(j)).toBe(true);
    });

    test('should return true when attempts is one less than max', () => {
      const j = makeJob({ attempts: 2, maxAttempts: 3 });
      expect(canRetry(j)).toBe(true);
    });

    test('should return false when attempts equals maxAttempts', () => {
      const j = makeJob({ attempts: 3, maxAttempts: 3 });
      expect(canRetry(j)).toBe(false);
    });

    test('should return false when attempts exceeds maxAttempts', () => {
      const j = makeJob({ attempts: 5, maxAttempts: 3 });
      expect(canRetry(j)).toBe(false);
    });

    test('should return false when maxAttempts is 0', () => {
      const j = makeJob({ attempts: 0, maxAttempts: 0 });
      expect(canRetry(j)).toBe(false);
    });

    test('should return true with single attempt allowed', () => {
      const j = makeJob({ attempts: 0, maxAttempts: 1 });
      expect(canRetry(j)).toBe(true);
    });
  });

  // ---- Lock management ----

  describe('lockToken', () => {
    test('should wrap a string as a branded LockToken', () => {
      const t = lockToken('token-abc');
      expect(t).toBe('token-abc');
      expect(typeof t).toBe('string');
    });
  });

  describe('generateLockToken', () => {
    test('should return a UUIDv7 string', () => {
      const t = generateLockToken();
      expect(t[14]).toBe('7');
      expect(t.length).toBe(36);
    });

    test('should produce unique tokens', () => {
      const tokens = new Set<string>();
      for (let i = 0; i < 50; i++) {
        tokens.add(generateLockToken());
      }
      expect(tokens.size).toBe(50);
    });
  });

  describe('DEFAULT_LOCK_TTL', () => {
    test('should be 30 seconds', () => {
      expect(DEFAULT_LOCK_TTL).toBe(30_000);
    });
  });

  describe('createJobLock', () => {
    test('should create a lock with correct fields', () => {
      const id = jobId('lock-job-1');
      const now = 5000;
      const lock = createJobLock(id, 'worker-1', 10000, now);

      expect(lock.jobId).toBe('lock-job-1');
      expect(lock.owner).toBe('worker-1');
      expect(lock.createdAt).toBe(5000);
      expect(lock.expiresAt).toBe(15000); // now + ttl
      expect(lock.lastRenewalAt).toBe(5000);
      expect(lock.renewalCount).toBe(0);
      expect(lock.ttl).toBe(10000);
      expect(typeof lock.token).toBe('string');
      expect(lock.token.length).toBe(36); // UUID
    });

    test('should use DEFAULT_LOCK_TTL when ttl not specified', () => {
      const lock = createJobLock(jobId('j'), 'w', undefined, 1000);
      expect(lock.ttl).toBe(DEFAULT_LOCK_TTL);
      expect(lock.expiresAt).toBe(1000 + DEFAULT_LOCK_TTL);
    });

    test('should generate unique tokens for different locks', () => {
      const lock1 = createJobLock(jobId('j1'), 'w1', 5000, 0);
      const lock2 = createJobLock(jobId('j2'), 'w2', 5000, 0);
      expect(lock1.token).not.toBe(lock2.token);
    });
  });

  describe('isLockExpired', () => {
    test('should return false before expiration', () => {
      const lock = createJobLock(jobId('j'), 'w', 10000, 1000);
      expect(isLockExpired(lock, 5000)).toBe(false);
    });

    test('should return true at exact expiration', () => {
      const lock = createJobLock(jobId('j'), 'w', 10000, 1000);
      // expiresAt = 11000
      expect(isLockExpired(lock, 11000)).toBe(true);
    });

    test('should return true after expiration', () => {
      const lock = createJobLock(jobId('j'), 'w', 10000, 1000);
      expect(isLockExpired(lock, 20000)).toBe(true);
    });
  });

  describe('renewLock', () => {
    test('should extend expiration with original TTL', () => {
      const lock = createJobLock(jobId('j'), 'w', 10000, 1000);
      renewLock(lock, undefined, 8000);
      expect(lock.expiresAt).toBe(18000); // 8000 + 10000
      expect(lock.lastRenewalAt).toBe(8000);
      expect(lock.renewalCount).toBe(1);
    });

    test('should use new TTL when provided', () => {
      const lock = createJobLock(jobId('j'), 'w', 10000, 1000);
      renewLock(lock, 20000, 5000);
      expect(lock.expiresAt).toBe(25000); // 5000 + 20000
      expect(lock.lastRenewalAt).toBe(5000);
      expect(lock.renewalCount).toBe(1);
    });

    test('should increment renewalCount on each renewal', () => {
      const lock = createJobLock(jobId('j'), 'w', 10000, 0);
      renewLock(lock, undefined, 1000);
      expect(lock.renewalCount).toBe(1);
      renewLock(lock, undefined, 2000);
      expect(lock.renewalCount).toBe(2);
      renewLock(lock, undefined, 3000);
      expect(lock.renewalCount).toBe(3);
    });

    test('should allow renewal even after expiration', () => {
      const lock = createJobLock(jobId('j'), 'w', 10000, 1000);
      // Lock expired at 11000; renew at 20000
      expect(isLockExpired(lock, 20000)).toBe(true);
      renewLock(lock, undefined, 20000);
      expect(lock.expiresAt).toBe(30000);
      expect(isLockExpired(lock, 25000)).toBe(false);
    });
  });
});

// =====================================================================
//  response.ts
// =====================================================================

describe('response.ts', () => {
  describe('ok', () => {
    test('should create OkResponse without id or reqId', () => {
      const r = ok();
      expect(r.ok).toBe(true);
      expect(r.id).toBeUndefined();
      expect(r.reqId).toBeUndefined();
    });

    test('should create OkResponse with id', () => {
      const r = ok('job-123');
      expect(r.ok).toBe(true);
      expect(r.id).toBe('job-123');
    });

    test('should create OkResponse with id and reqId', () => {
      const r = ok('job-123', 'req-1');
      expect(r.ok).toBe(true);
      expect(r.id).toBe('job-123');
      expect(r.reqId).toBe('req-1');
    });
  });

  describe('batch', () => {
    test('should create BatchResponse with ids', () => {
      const r = batch(['id-1', 'id-2', 'id-3']);
      expect(r.ok).toBe(true);
      expect(r.ids).toEqual(['id-1', 'id-2', 'id-3']);
    });

    test('should create BatchResponse with empty ids', () => {
      const r = batch([]);
      expect(r.ok).toBe(true);
      expect(r.ids).toEqual([]);
    });

    test('should include reqId when provided', () => {
      const r = batch(['id-1'], 'req-2');
      expect(r.reqId).toBe('req-2');
    });
  });

  describe('job (response builder)', () => {
    test('should wrap a Job in a JobResponse', () => {
      const j = makeJob();
      const r = jobResponse(j);
      expect(r.ok).toBe(true);
      expect(r.job).toBe(j);
    });

    test('should include reqId when provided', () => {
      const j = makeJob();
      const r = jobResponse(j, 'req-3');
      expect(r.reqId).toBe('req-3');
    });
  });

  describe('nullableJob', () => {
    test('should wrap a job', () => {
      const j = makeJob();
      const r = nullableJob(j);
      expect(r.ok).toBe(true);
      expect(r.job).toBe(j);
    });

    test('should wrap null', () => {
      const r = nullableJob(null);
      expect(r.ok).toBe(true);
      expect(r.job).toBeNull();
    });

    test('should include reqId', () => {
      const r = nullableJob(null, 'req-4');
      expect(r.reqId).toBe('req-4');
    });
  });

  describe('pulledJob', () => {
    test('should include job and token', () => {
      const j = makeJob();
      const r = pulledJob(j, 'token-abc');
      expect(r.ok).toBe(true);
      expect(r.job).toBe(j);
      expect(r.token).toBe('token-abc');
    });

    test('should allow null job and null token', () => {
      const r = pulledJob(null, null);
      expect(r.ok).toBe(true);
      expect(r.job).toBeNull();
      expect(r.token).toBeNull();
    });

    test('should include reqId', () => {
      const r = pulledJob(null, null, 'req-5');
      expect(r.reqId).toBe('req-5');
    });
  });

  describe('pulledJobs', () => {
    test('should include jobs list and tokens list', () => {
      const j1 = makeJob();
      const j2 = makeJob();
      const r = pulledJobs([j1, j2], ['t1', 't2']);
      expect(r.ok).toBe(true);
      expect(r.jobs).toEqual([j1, j2]);
      expect(r.tokens).toEqual(['t1', 't2']);
    });

    test('should handle empty lists', () => {
      const r = pulledJobs([], []);
      expect(r.ok).toBe(true);
      expect(r.jobs).toEqual([]);
      expect(r.tokens).toEqual([]);
    });

    test('should include reqId', () => {
      const r = pulledJobs([], [], 'req-6');
      expect(r.reqId).toBe('req-6');
    });
  });

  describe('jobs (response builder)', () => {
    test('should wrap job list', () => {
      const list = [makeJob(), makeJob()];
      const r = jobsResponse(list);
      expect(r.ok).toBe(true);
      expect(r.jobs).toBe(list);
    });

    test('should handle empty list', () => {
      const r = jobsResponse([]);
      expect(r.ok).toBe(true);
      expect(r.jobs).toEqual([]);
    });
  });

  describe('error', () => {
    test('should create ErrorResponse with message', () => {
      const r = error('Something went wrong');
      expect(r.ok).toBe(false);
      expect(r.error).toBe('Something went wrong');
    });

    test('should include reqId', () => {
      const r = error('fail', 'req-7');
      expect(r.ok).toBe(false);
      expect(r.error).toBe('fail');
      expect(r.reqId).toBe('req-7');
    });

    test('should handle empty error message', () => {
      const r = error('');
      expect(r.ok).toBe(false);
      expect(r.error).toBe('');
    });
  });

  describe('hello', () => {
    test('should create HelloResponse', () => {
      const r = hello(3, ['pipelining', 'separate-job-name'], 'bunqueue', '2.8.56');
      expect(r.ok).toBe(true);
      expect(r.protocolVersion).toBe(3);
      expect(r.capabilities).toEqual(['pipelining', 'separate-job-name']);
      expect(r.server).toBe('bunqueue');
      expect(r.version).toBe('2.8.56');
    });

    test('should handle empty capabilities', () => {
      const r = hello(2, [], 'bunqueue', '1.0.0');
      expect(r.capabilities).toEqual([]);
    });

    test('should include reqId', () => {
      const r = hello(1, [], 'bunqueue', '1.0.0', 'req-8');
      expect(r.reqId).toBe('req-8');
    });
  });

  describe('data', () => {
    test('should wrap any payload', () => {
      const r = data({ foo: 'bar', count: 42 });
      expect(r.ok).toBe(true);
      expect(r.data).toEqual({ foo: 'bar', count: 42 });
    });

    test('should wrap null payload', () => {
      const r = data(null);
      expect(r.ok).toBe(true);
      expect(r.data).toBeNull();
    });

    test('should wrap array payload', () => {
      const r = data([1, 2, 3]);
      expect(r.ok).toBe(true);
      expect(r.data).toEqual([1, 2, 3]);
    });

    test('should wrap primitive payload', () => {
      const r = data(42);
      expect(r.ok).toBe(true);
      expect(r.data).toBe(42);
    });

    test('should include reqId', () => {
      const r = data('value', 'req-9');
      expect(r.reqId).toBe('req-9');
    });
  });

  describe('counts', () => {
    test('should create JobCountsResponse', () => {
      const c = {
        waiting: 10,
        delayed: 5,
        active: 3,
        completed: 100,
        failed: 2,
      };
      const r = counts(c);
      expect(r.ok).toBe(true);
      expect(r.counts).toEqual(c);
    });

    test('should include reqId', () => {
      const c = { waiting: 0, delayed: 0, active: 0, completed: 0, failed: 0 };
      const r = counts(c, 'req-10');
      expect(r.reqId).toBe('req-10');
    });
  });

  describe('stats', () => {
    test('should create StatsResponse', () => {
      const s = {
        queued: 50,
        processing: 10,
        delayed: 5,
        dlq: 2,
        completed: 200,
        uptime: 3600,
        pushPerSec: 100,
        pullPerSec: 95,
      };
      const r = stats(s);
      expect(r.ok).toBe(true);
      expect(r.stats).toEqual(s);
    });

    test('should include reqId', () => {
      const s = {
        queued: 0,
        processing: 0,
        delayed: 0,
        dlq: 0,
        completed: 0,
        uptime: 0,
        pushPerSec: 0,
        pullPerSec: 0,
      };
      const r = stats(s, 'req-11');
      expect(r.reqId).toBe('req-11');
    });
  });

  describe('metrics', () => {
    test('should create MetricsResponse', () => {
      const m = {
        totalPushed: 1000,
        totalPulled: 900,
        totalCompleted: 850,
        totalFailed: 50,
        avgLatencyMs: 12.5,
        avgProcessingMs: 45.3,
        memoryUsageMb: 128,
        sqliteSizeMb: 50,
        activeConnections: 10,
      };
      const r = metrics(m);
      expect(r.ok).toBe(true);
      expect(r.metrics).toEqual(m);
    });

    test('should include reqId', () => {
      const m = {
        totalPushed: 0,
        totalPulled: 0,
        totalCompleted: 0,
        totalFailed: 0,
        avgLatencyMs: 0,
        avgProcessingMs: 0,
        memoryUsageMb: 0,
        sqliteSizeMb: 0,
        activeConnections: 0,
      };
      const r = metrics(m, 'req-12');
      expect(r.reqId).toBe('req-12');
    });
  });
});

// =====================================================================
//  queue.ts
// =====================================================================

describe('queue.ts', () => {
  describe('createQueueState', () => {
    test('should create default queue state', () => {
      const qs = createQueueState('emails');
      expect(qs.name).toBe('emails');
      expect(qs.paused).toBe(false);
      expect(qs.rateLimit).toBeNull();
      expect(qs.concurrencyLimit).toBeNull();
      expect(qs.activeCount).toBe(0);
    });

    test('should handle empty string name', () => {
      const qs = createQueueState('');
      expect(qs.name).toBe('');
    });

    test('should handle special characters in name', () => {
      const qs = createQueueState('my-queue/v2:production');
      expect(qs.name).toBe('my-queue/v2:production');
    });
  });

  describe('RateLimiter', () => {
    test('should acquire tokens up to capacity', () => {
      const rl = new RateLimiter(3);
      expect(rl.tryAcquire()).toBe(true);
      expect(rl.tryAcquire()).toBe(true);
      expect(rl.tryAcquire()).toBe(true);
      expect(rl.tryAcquire()).toBe(false);
    });

    test('should start with full capacity', () => {
      const rl = new RateLimiter(5);
      const tokens = rl.getTokens();
      // Should be approximately 5 (might have tiny refill from time elapsed)
      expect(tokens).toBeGreaterThanOrEqual(4.9);
      expect(tokens).toBeLessThanOrEqual(5.1);
    });

    test('should not exceed capacity on refill', () => {
      const rl = new RateLimiter(3);
      // Even after time passes, tokens should cap at capacity
      const tokens = rl.getTokens();
      expect(tokens).toBeLessThanOrEqual(3.1);
    });

    test('should deny acquisition when depleted', () => {
      const rl = new RateLimiter(1);
      expect(rl.tryAcquire()).toBe(true);
      expect(rl.tryAcquire()).toBe(false);
      expect(rl.tryAcquire()).toBe(false);
    });

    test('should create with custom refill rate', () => {
      // Capacity 10, refill 10 per second (default)
      const rl = new RateLimiter(10, 10);
      // Deplete all
      for (let i = 0; i < 10; i++) {
        expect(rl.tryAcquire()).toBe(true);
      }
      expect(rl.tryAcquire()).toBe(false);
    });
  });

  describe('ConcurrencyLimiter', () => {
    test('should acquire slots up to limit', () => {
      const cl = new ConcurrencyLimiter(3);
      expect(cl.tryAcquire()).toBe(true);
      expect(cl.tryAcquire()).toBe(true);
      expect(cl.tryAcquire()).toBe(true);
      expect(cl.tryAcquire()).toBe(false);
    });

    test('should release slots', () => {
      const cl = new ConcurrencyLimiter(2);
      expect(cl.tryAcquire()).toBe(true);
      expect(cl.tryAcquire()).toBe(true);
      expect(cl.tryAcquire()).toBe(false);

      cl.release();
      expect(cl.tryAcquire()).toBe(true);
      expect(cl.tryAcquire()).toBe(false);
    });

    test('should not go below zero on release', () => {
      const cl = new ConcurrencyLimiter(2);
      cl.release(); // release when active=0
      expect(cl.getActive()).toBe(0);
    });

    test('should track active count', () => {
      const cl = new ConcurrencyLimiter(5);
      expect(cl.getActive()).toBe(0);
      cl.tryAcquire();
      expect(cl.getActive()).toBe(1);
      cl.tryAcquire();
      expect(cl.getActive()).toBe(2);
      cl.release();
      expect(cl.getActive()).toBe(1);
    });

    test('should return limit', () => {
      const cl = new ConcurrencyLimiter(7);
      expect(cl.getLimit()).toBe(7);
    });

    test('should update limit', () => {
      const cl = new ConcurrencyLimiter(3);
      cl.setLimit(10);
      expect(cl.getLimit()).toBe(10);
    });

    test('should allow more acquisitions after limit increase', () => {
      const cl = new ConcurrencyLimiter(1);
      expect(cl.tryAcquire()).toBe(true);
      expect(cl.tryAcquire()).toBe(false);

      cl.setLimit(3);
      expect(cl.tryAcquire()).toBe(true);
      expect(cl.tryAcquire()).toBe(true);
      expect(cl.tryAcquire()).toBe(false);
    });

    test('should handle limit decrease with active slots', () => {
      const cl = new ConcurrencyLimiter(5);
      cl.tryAcquire(); // active = 1
      cl.tryAcquire(); // active = 2
      cl.tryAcquire(); // active = 3

      cl.setLimit(2);
      // Active (3) > limit (2), so no new acquisitions
      expect(cl.tryAcquire()).toBe(false);
      expect(cl.getActive()).toBe(3);

      // Release one, still at 2 which equals limit
      cl.release();
      expect(cl.getActive()).toBe(2);
      expect(cl.tryAcquire()).toBe(false);

      // Release one more, now at 1 which is below limit
      cl.release();
      expect(cl.getActive()).toBe(1);
      expect(cl.tryAcquire()).toBe(true);
    });
  });
});

// =====================================================================
//  cron.ts
// =====================================================================

describe('cron.ts', () => {
  describe('createCronJob', () => {
    test('should create a cron job with schedule', () => {
      const cron = createCronJob(
        {
          name: 'daily-report',
          queue: 'reports',
          data: { type: 'daily' },
          schedule: '0 0 * * *',
        },
        1700000000000
      );
      expect(cron.name).toBe('daily-report');
      expect(cron.queue).toBe('reports');
      expect(cron.data).toEqual({ type: 'daily' });
      expect(cron.schedule).toBe('0 0 * * *');
      expect(cron.repeatEvery).toBeNull();
      expect(cron.priority).toBe(0);
      expect(cron.timezone).toBeNull();
      expect(cron.nextRun).toBe(1700000000000);
      expect(cron.executions).toBe(0);
      expect(cron.maxLimit).toBeNull();
    });

    test('should create a cron job with repeatEvery', () => {
      const cron = createCronJob(
        {
          name: 'heartbeat',
          queue: 'monitor',
          data: null,
          repeatEvery: 5000,
        },
        2000
      );
      expect(cron.schedule).toBeNull();
      expect(cron.repeatEvery).toBe(5000);
    });

    test('should set optional fields', () => {
      const cron = createCronJob(
        {
          name: 'cleanup',
          queue: 'maintenance',
          data: {},
          schedule: '*/5 * * * *',
          priority: 5,
          maxLimit: 100,
          timezone: 'America/New_York',
        },
        3000
      );
      expect(cron.priority).toBe(5);
      expect(cron.maxLimit).toBe(100);
      expect(cron.timezone).toBe('America/New_York');
    });

    test('should throw when neither schedule nor repeatEvery is provided', () => {
      expect(() => createCronJob({ name: 'bad', queue: 'q', data: null }, 0)).toThrow(
        'Cron job must have either schedule or repeatEvery'
      );
    });
  });

  describe('isAtLimit', () => {
    test('should return false when maxLimit is null', () => {
      const cron = createCronJob({ name: 'c', queue: 'q', data: null, schedule: '* * * * *' }, 0);
      cron.executions = 999999;
      expect(isAtLimit(cron)).toBe(false);
    });

    test('should return false when executions below limit', () => {
      const cron = createCronJob(
        { name: 'c', queue: 'q', data: null, schedule: '* * * * *', maxLimit: 10 },
        0
      );
      cron.executions = 5;
      expect(isAtLimit(cron)).toBe(false);
    });

    test('should return true when executions equals limit', () => {
      const cron = createCronJob(
        { name: 'c', queue: 'q', data: null, schedule: '* * * * *', maxLimit: 10 },
        0
      );
      cron.executions = 10;
      expect(isAtLimit(cron)).toBe(true);
    });

    test('should return true when executions exceeds limit', () => {
      const cron = createCronJob(
        { name: 'c', queue: 'q', data: null, schedule: '* * * * *', maxLimit: 10 },
        0
      );
      cron.executions = 15;
      expect(isAtLimit(cron)).toBe(true);
    });
  });

  describe('isDue', () => {
    test('should return true when nextRun is in the past and not at limit', () => {
      const cron = createCronJob(
        { name: 'c', queue: 'q', data: null, schedule: '* * * * *' },
        1000
      );
      expect(isDue(cron, 2000)).toBe(true);
    });

    test('should return true when nextRun equals now', () => {
      const cron = createCronJob(
        { name: 'c', queue: 'q', data: null, schedule: '* * * * *' },
        1000
      );
      expect(isDue(cron, 1000)).toBe(true);
    });

    test('should return false when nextRun is in the future', () => {
      const cron = createCronJob(
        { name: 'c', queue: 'q', data: null, schedule: '* * * * *' },
        5000
      );
      expect(isDue(cron, 3000)).toBe(false);
    });

    test('should return false when at execution limit even if due', () => {
      const cron = createCronJob(
        { name: 'c', queue: 'q', data: null, schedule: '* * * * *', maxLimit: 5 },
        1000
      );
      cron.executions = 5;
      expect(isDue(cron, 2000)).toBe(false);
    });

    test('should return true when under limit and due', () => {
      const cron = createCronJob(
        { name: 'c', queue: 'q', data: null, schedule: '* * * * *', maxLimit: 5 },
        1000
      );
      cron.executions = 4;
      expect(isDue(cron, 2000)).toBe(true);
    });
  });
});
