/**
 * Advanced DLQ Tests
 * Tests for DLQ metadata, auto-retry, and lifecycle management
 */

import { describe, test, expect } from 'bun:test';
import { createJob, jobId, type Job } from '../src/domain/types/job';
import {
  createDlqEntry,
  addAttemptRecord,
  isDlqEntryExpired,
  canAutoRetry,
  scheduleNextRetry,
  FailureReason,
  DEFAULT_DLQ_CONFIG,
  type DlqConfig,
  type DlqEntry,
} from '../src/domain/types/dlq';
import { Shard } from '../src/domain/queue/shard';

describe('Advanced DLQ', () => {
  function makeJob(id: number, queue = 'test', overrides: Partial<Job> = {}): Job {
    const job = createJob(jobId(`test-job-${id}`), queue, { data: { id } }, Date.now());
    return { ...job, ...overrides };
  }

  describe('DlqEntry creation', () => {
    test('should create DLQ entry with metadata', () => {
      const job = makeJob(1);
      job.attempts = 3;
      job.startedAt = Date.now() - 5000;

      const entry = createDlqEntry(job, FailureReason.MaxAttemptsExceeded, 'Max retries exceeded');

      expect(entry.job).toBe(job);
      expect(entry.reason).toBe(FailureReason.MaxAttemptsExceeded);
      expect(entry.error).toBe('Max retries exceeded');
      expect(entry.attempts.length).toBe(1);
      expect(entry.retryCount).toBe(0);
      expect(entry.lastRetryAt).toBeNull();
    });

    test('should set expiration based on config', () => {
      const job = makeJob(1);
      const now = Date.now();

      const config: DlqConfig = { ...DEFAULT_DLQ_CONFIG, maxAge: 3600000 }; // 1 hour
      const entry = createDlqEntry(job, FailureReason.Timeout, null, config);

      expect(entry.expiresAt).toBeGreaterThan(now);
      expect(entry.expiresAt! - now).toBeCloseTo(3600000, -2);
    });

    test('should set next retry if auto-retry enabled', () => {
      const job = makeJob(1);
      const now = Date.now();

      const config: DlqConfig = { ...DEFAULT_DLQ_CONFIG, autoRetry: true, autoRetryInterval: 60000 };
      const entry = createDlqEntry(job, FailureReason.Stalled, null, config);

      expect(entry.nextRetryAt).toBeGreaterThan(now);
      expect(entry.nextRetryAt! - now).toBeCloseTo(60000, -2);
    });

    test('should not set next retry if auto-retry disabled', () => {
      const job = makeJob(1);

      const config: DlqConfig = { ...DEFAULT_DLQ_CONFIG, autoRetry: false };
      const entry = createDlqEntry(job, FailureReason.ExplicitFail, null, config);

      expect(entry.nextRetryAt).toBeNull();
    });
  });

  describe('Attempt records', () => {
    test('should add attempt record to entry', () => {
      const job = makeJob(1);
      job.attempts = 1;
      job.startedAt = Date.now() - 1000;

      const entry = createDlqEntry(job, FailureReason.Timeout, 'Timeout');

      job.attempts = 2;
      job.startedAt = Date.now();
      addAttemptRecord(entry, FailureReason.Stalled, 'Job stalled');

      expect(entry.attempts.length).toBe(2);
      expect(entry.attempts[1].reason).toBe(FailureReason.Stalled);
      expect(entry.attempts[1].error).toBe('Job stalled');
    });

    test('should calculate duration for attempt', () => {
      const job = makeJob(1);
      const startTime = Date.now() - 5000;
      job.startedAt = startTime;
      job.attempts = 1;

      const entry = createDlqEntry(job, FailureReason.Timeout, null);

      expect(entry.attempts[0].duration).toBeGreaterThan(4000);
      expect(entry.attempts[0].duration).toBeLessThan(6000);
    });
  });

  describe('Expiration checks', () => {
    test('should detect expired entry', () => {
      const job = makeJob(1);
      const pastTime = Date.now() - 10000;

      const entry: DlqEntry = {
        job,
        enteredAt: pastTime - 10000,
        reason: FailureReason.Unknown,
        error: null,
        attempts: [],
        retryCount: 0,
        lastRetryAt: null,
        nextRetryAt: null,
        expiresAt: pastTime, // Expired 10 seconds ago
      };

      expect(isDlqEntryExpired(entry)).toBe(true);
    });

    test('should not detect non-expired entry', () => {
      const job = makeJob(1);
      const futureTime = Date.now() + 10000;

      const entry: DlqEntry = {
        job,
        enteredAt: Date.now(),
        reason: FailureReason.Unknown,
        error: null,
        attempts: [],
        retryCount: 0,
        lastRetryAt: null,
        nextRetryAt: null,
        expiresAt: futureTime,
      };

      expect(isDlqEntryExpired(entry)).toBe(false);
    });

    test('should not expire entry with null expiresAt', () => {
      const job = makeJob(1);

      const entry: DlqEntry = {
        job,
        enteredAt: Date.now() - 100000,
        reason: FailureReason.Unknown,
        error: null,
        attempts: [],
        retryCount: 0,
        lastRetryAt: null,
        nextRetryAt: null,
        expiresAt: null,
      };

      expect(isDlqEntryExpired(entry)).toBe(false);
    });
  });

  describe('Auto-retry eligibility', () => {
    test('should be eligible for auto-retry', () => {
      const job = makeJob(1);
      const now = Date.now();

      const entry: DlqEntry = {
        job,
        enteredAt: now - 10000,
        reason: FailureReason.Stalled,
        error: null,
        attempts: [],
        retryCount: 0,
        lastRetryAt: null,
        nextRetryAt: now - 1000, // Past due
        expiresAt: null,
      };

      const config: DlqConfig = { ...DEFAULT_DLQ_CONFIG, autoRetry: true, maxAutoRetries: 3 };

      expect(canAutoRetry(entry, config, now)).toBe(true);
    });

    test('should not be eligible if auto-retry disabled', () => {
      const job = makeJob(1);
      const now = Date.now();

      const entry: DlqEntry = {
        job,
        enteredAt: now - 10000,
        reason: FailureReason.Stalled,
        error: null,
        attempts: [],
        retryCount: 0,
        lastRetryAt: null,
        nextRetryAt: now - 1000,
        expiresAt: null,
      };

      const config: DlqConfig = { ...DEFAULT_DLQ_CONFIG, autoRetry: false };

      expect(canAutoRetry(entry, config, now)).toBe(false);
    });

    test('should not be eligible if max retries reached', () => {
      const job = makeJob(1);
      const now = Date.now();

      const entry: DlqEntry = {
        job,
        enteredAt: now - 10000,
        reason: FailureReason.Stalled,
        error: null,
        attempts: [],
        retryCount: 3,
        lastRetryAt: now - 5000,
        nextRetryAt: now - 1000,
        expiresAt: null,
      };

      const config: DlqConfig = { ...DEFAULT_DLQ_CONFIG, autoRetry: true, maxAutoRetries: 3 };

      expect(canAutoRetry(entry, config, now)).toBe(false);
    });

    test('should not be eligible if next retry is in future', () => {
      const job = makeJob(1);
      const now = Date.now();

      const entry: DlqEntry = {
        job,
        enteredAt: now - 10000,
        reason: FailureReason.Stalled,
        error: null,
        attempts: [],
        retryCount: 0,
        lastRetryAt: null,
        nextRetryAt: now + 60000, // 1 minute in future
        expiresAt: null,
      };

      const config: DlqConfig = { ...DEFAULT_DLQ_CONFIG, autoRetry: true };

      expect(canAutoRetry(entry, config, now)).toBe(false);
    });
  });

  describe('Retry scheduling', () => {
    test('should schedule next retry with backoff', () => {
      const job = makeJob(1);
      const now = Date.now();

      const entry: DlqEntry = {
        job,
        enteredAt: now - 10000,
        reason: FailureReason.Stalled,
        error: null,
        attempts: [],
        retryCount: 0,
        lastRetryAt: null,
        nextRetryAt: null,
        expiresAt: null,
      };

      const config: DlqConfig = { ...DEFAULT_DLQ_CONFIG, autoRetryInterval: 60000, maxAutoRetries: 3 };

      scheduleNextRetry(entry, config);

      expect(entry.retryCount).toBe(1);
      expect(entry.lastRetryAt).toBeGreaterThan(0);
      expect(entry.nextRetryAt).toBeGreaterThan(now);
    });

    test('should apply exponential backoff', () => {
      const job = makeJob(1);
      const now = Date.now();

      const entry: DlqEntry = {
        job,
        enteredAt: now - 10000,
        reason: FailureReason.Stalled,
        error: null,
        attempts: [],
        retryCount: 1, // Already retried once
        lastRetryAt: now - 60000,
        nextRetryAt: null,
        expiresAt: null,
      };

      const config: DlqConfig = { ...DEFAULT_DLQ_CONFIG, autoRetryInterval: 60000, maxAutoRetries: 3 };

      scheduleNextRetry(entry, config);

      // Second retry should be 2x interval
      expect(entry.retryCount).toBe(2);
      expect(entry.nextRetryAt! - now).toBeGreaterThanOrEqual(120000 - 1000);
    });

    test('should set nextRetryAt to null after max retries', () => {
      const job = makeJob(1);
      const now = Date.now();

      const entry: DlqEntry = {
        job,
        enteredAt: now - 10000,
        reason: FailureReason.Stalled,
        error: null,
        attempts: [],
        retryCount: 2, // About to hit max
        lastRetryAt: now - 60000,
        nextRetryAt: now - 1000,
        expiresAt: null,
      };

      const config: DlqConfig = { ...DEFAULT_DLQ_CONFIG, autoRetryInterval: 60000, maxAutoRetries: 3 };

      scheduleNextRetry(entry, config);

      expect(entry.retryCount).toBe(3);
      expect(entry.nextRetryAt).toBeNull();
    });
  });

  describe('Shard DLQ integration', () => {
    test('should add job to DLQ with reason', () => {
      const shard = new Shard();
      const job = makeJob(1, 'emails');

      const entry = shard.addToDlq(job, FailureReason.Timeout, 'Job timeout');

      expect(entry.reason).toBe(FailureReason.Timeout);
      expect(entry.error).toBe('Job timeout');
      expect(shard.getDlqCount('emails')).toBe(1);
    });

    test('should get DLQ entries with metadata', () => {
      const shard = new Shard();
      shard.addToDlq(makeJob(1, 'emails'), FailureReason.Stalled, 'Stalled');
      shard.addToDlq(makeJob(2, 'emails'), FailureReason.Timeout, 'Timeout');

      const entries = shard.getDlqEntries('emails');

      expect(entries.length).toBe(2);
      expect(entries[0].reason).toBe(FailureReason.Stalled);
      expect(entries[1].reason).toBe(FailureReason.Timeout);
    });

    test('should filter DLQ by reason', () => {
      const shard = new Shard();
      shard.addToDlq(makeJob(1, 'emails'), FailureReason.Stalled, 'Stalled');
      shard.addToDlq(makeJob(2, 'emails'), FailureReason.Timeout, 'Timeout');
      shard.addToDlq(makeJob(3, 'emails'), FailureReason.Stalled, 'Stalled again');

      const filtered = shard.getDlqFiltered('emails', { reason: FailureReason.Stalled });

      expect(filtered.length).toBe(2);
    });

    test('should enforce max entries', () => {
      const shard = new Shard();
      shard.setDlqConfig('emails', { maxEntries: 3 });

      for (let i = 0; i < 5; i++) {
        shard.addToDlq(makeJob(i, 'emails'));
      }

      expect(shard.getDlqCount('emails')).toBe(3);
    });

    test('should purge expired entries', () => {
      const shard = new Shard();
      const now = Date.now();

      // Add entries with custom config for immediate expiration
      shard.setDlqConfig('emails', { maxAge: 1 }); // 1ms expiration

      shard.addToDlq(makeJob(1, 'emails'));

      // Wait a bit for expiration
      const purged = shard.purgeExpired('emails', now + 100);

      expect(purged).toBe(1);
      expect(shard.getDlqCount('emails')).toBe(0);
    });

    test('should get auto-retry entries', () => {
      const shard = new Shard();
      const now = Date.now();

      shard.setDlqConfig('emails', { autoRetry: true, autoRetryInterval: 1 }); // 1ms interval

      shard.addToDlq(makeJob(1, 'emails'), FailureReason.Stalled, null);
      shard.addToDlq(makeJob(2, 'emails'), FailureReason.Timeout, null);

      // Wait for retry eligibility
      const eligible = shard.getAutoRetryEntries('emails', now + 100);

      expect(eligible.length).toBe(2);
    });
  });

  describe('Failure reasons', () => {
    test('should have all failure reasons', () => {
      expect(FailureReason.ExplicitFail).toBe('explicit_fail');
      expect(FailureReason.MaxAttemptsExceeded).toBe('max_attempts_exceeded');
      expect(FailureReason.Timeout).toBe('timeout');
      expect(FailureReason.Stalled).toBe('stalled');
      expect(FailureReason.TtlExpired).toBe('ttl_expired');
      expect(FailureReason.WorkerLost).toBe('worker_lost');
      expect(FailureReason.Unknown).toBe('unknown');
    });
  });
});
