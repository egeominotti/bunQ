/**
 * Extended Domain Types Unit Tests
 *
 * Tests for: webhook.ts, deduplication.ts, worker.ts
 */
import { describe, test, expect } from 'bun:test';
import { createWebhook } from '../src/domain/types/webhook';
import type { WebhookEvent } from '../src/domain/types/webhook';
import {
  getDeduplicationStrategy,
  isUniqueKeyExpired,
  calculateExpiration,
} from '../src/domain/types/deduplication';
import type { DeduplicationOptions, UniqueKeyEntry } from '../src/domain/types/deduplication';
import { createWorker, createLogEntry } from '../src/domain/types/worker';

/** Helper to create a UniqueKeyEntry for testing */
function makeEntry(expiresAt: number | null, registeredAt = 500): UniqueKeyEntry {
  return { jobId: 'job-1' as any, expiresAt, registeredAt };
}

// ── webhook.ts ──────────────────────────────────────────────────────

describe('webhook.ts', () => {
  describe('createWebhook', () => {
    test('should create a webhook with all parameters', () => {
      const before = Date.now();
      const wh = createWebhook('https://example.com/hook', ['job.completed', 'job.failed'], 'emails', 'secret');
      const after = Date.now();

      expect(typeof wh.id).toBe('string');
      expect(wh.id.length).toBe(36);
      expect(wh.url).toBe('https://example.com/hook');
      expect(wh.events).toEqual(['job.completed', 'job.failed']);
      expect(wh.queue).toBe('emails');
      expect(wh.secret).toBe('secret');
      expect(wh.createdAt).toBeGreaterThanOrEqual(before);
      expect(wh.createdAt).toBeLessThanOrEqual(after);
      expect(wh.lastTriggered).toBeNull();
      expect(wh.successCount).toBe(0);
      expect(wh.failureCount).toBe(0);
      expect(wh.enabled).toBe(true);
    });

    test('should default queue and secret to null when omitted', () => {
      const wh = createWebhook('https://example.com/hook', ['job.pushed']);
      expect(wh.queue).toBeNull();
      expect(wh.secret).toBeNull();
    });

    test('should generate unique IDs for different webhooks', () => {
      const wh1 = createWebhook('https://a.com', ['job.pushed']);
      const wh2 = createWebhook('https://b.com', ['job.pushed']);
      expect(wh1.id).not.toBe(wh2.id);
    });

    test('should accept all valid event types', () => {
      const allEvents: WebhookEvent[] = [
        'job.pushed', 'job.started', 'job.completed', 'job.failed', 'job.progress', 'job.stalled',
      ];
      const wh = createWebhook('https://example.com', allEvents);
      expect(wh.events).toEqual(allEvents);
    });

    test('should accept an empty events array', () => {
      const wh = createWebhook('https://example.com', []);
      expect(wh.events).toEqual([]);
    });

    test('should accept queue with special characters', () => {
      const wh = createWebhook('https://example.com', ['job.pushed'], 'my-queue/v2:prod');
      expect(wh.queue).toBe('my-queue/v2:prod');
    });

    test('should preserve URL as-is without validation', () => {
      const wh = createWebhook('not-a-valid-url', ['job.pushed']);
      expect(wh.url).toBe('not-a-valid-url');
    });

    test('should handle empty string URL', () => {
      const wh = createWebhook('', ['job.pushed']);
      expect(wh.url).toBe('');
    });

    test('should handle URL with query parameters', () => {
      const wh = createWebhook('https://example.com/hook?token=abc&v=2', ['job.completed']);
      expect(wh.url).toBe('https://example.com/hook?token=abc&v=2');
    });
  });
});

// ── deduplication.ts ────────────────────────────────────────────────

describe('deduplication.ts', () => {
  describe('getDeduplicationStrategy', () => {
    test('should return "reject" when no flags set', () => {
      expect(getDeduplicationStrategy({ id: 'k' })).toBe('reject');
    });

    test('should return "extend" when extend is true', () => {
      expect(getDeduplicationStrategy({ id: 'k', extend: true })).toBe('extend');
    });

    test('should return "replace" when replace is true', () => {
      expect(getDeduplicationStrategy({ id: 'k', replace: true })).toBe('replace');
    });

    test('should prioritize "replace" over "extend" when both true', () => {
      expect(getDeduplicationStrategy({ id: 'k', replace: true, extend: true })).toBe('replace');
    });

    test('should return "reject" when flags are explicitly false', () => {
      expect(getDeduplicationStrategy({ id: 'k', extend: false })).toBe('reject');
      expect(getDeduplicationStrategy({ id: 'k', replace: false })).toBe('reject');
      expect(getDeduplicationStrategy({ id: 'k', replace: false, extend: false })).toBe('reject');
    });

    test('should ignore ttl for strategy determination', () => {
      expect(getDeduplicationStrategy({ id: 'k', ttl: 5000 })).toBe('reject');
    });
  });

  describe('isUniqueKeyExpired', () => {
    test('should return true when entry has expired', () => {
      expect(isUniqueKeyExpired(makeEntry(1000), 1001)).toBe(true);
    });

    test('should return true when now equals expiresAt', () => {
      expect(isUniqueKeyExpired(makeEntry(1000), 1000)).toBe(true);
    });

    test('should return false when entry has not expired', () => {
      expect(isUniqueKeyExpired(makeEntry(2000), 1000)).toBe(false);
    });

    test('should return false when expiresAt is null (no TTL)', () => {
      expect(isUniqueKeyExpired(makeEntry(null), 999999999)).toBe(false);
    });

    test('should use Date.now() when now is not provided', () => {
      expect(isUniqueKeyExpired(makeEntry(Date.now() + 100000))).toBe(false);
    });

    test('should return true for already-expired entry with default now', () => {
      expect(isUniqueKeyExpired(makeEntry(1))).toBe(true);
    });
  });

  describe('calculateExpiration', () => {
    test('should return now + ttl when ttl is provided', () => {
      expect(calculateExpiration(5000, 1000)).toBe(6000);
    });

    test('should return null when ttl is undefined', () => {
      expect(calculateExpiration(undefined, 1000)).toBeNull();
    });

    test('should handle zero TTL', () => {
      expect(calculateExpiration(0, 1000)).toBe(1000);
    });

    test('should handle negative TTL', () => {
      expect(calculateExpiration(-500, 1000)).toBe(500);
    });

    test('should handle large TTL values', () => {
      expect(calculateExpiration(86400000, 1000)).toBe(86401000);
    });

    test('should use Date.now() when now is not provided', () => {
      const before = Date.now();
      const result = calculateExpiration(5000);
      const after = Date.now();
      expect(result).toBeGreaterThanOrEqual(before + 5000);
      expect(result).toBeLessThanOrEqual(after + 5000);
    });

    test('should return null for undefined ttl regardless of now', () => {
      expect(calculateExpiration(undefined, 999999)).toBeNull();
    });
  });
});

// ── worker.ts ───────────────────────────────────────────────────────

describe('worker.ts', () => {
  describe('createWorker', () => {
    test('should create a worker with name and queues', () => {
      const before = Date.now();
      const w = createWorker('worker-1', ['emails', 'notifications']);
      const after = Date.now();

      expect(typeof w.id).toBe('string');
      expect(w.id.length).toBe(36);
      expect(w.name).toBe('worker-1');
      expect(w.queues).toEqual(['emails', 'notifications']);
      expect(w.registeredAt).toBeGreaterThanOrEqual(before);
      expect(w.registeredAt).toBeLessThanOrEqual(after);
      expect(w.lastSeen).toBe(w.registeredAt);
      expect(w.activeJobs).toBe(0);
      expect(w.processedJobs).toBe(0);
      expect(w.failedJobs).toBe(0);
    });

    test('should create a worker with an empty queues list', () => {
      const w = createWorker('idle-worker', []);
      expect(w.queues).toEqual([]);
    });

    test('should generate unique IDs for different workers', () => {
      const w1 = createWorker('w1', ['q']);
      const w2 = createWorker('w2', ['q']);
      expect(w1.id).not.toBe(w2.id);
    });

    test('should set registeredAt and lastSeen to the same timestamp', () => {
      const w = createWorker('w', ['q']);
      expect(w.registeredAt).toBe(w.lastSeen);
    });

    test('should initialize all counters to zero', () => {
      const w = createWorker('w', ['q']);
      expect(w.activeJobs + w.processedJobs + w.failedJobs).toBe(0);
    });

    test('should handle empty and special character names', () => {
      expect(createWorker('', ['q']).name).toBe('');
      expect(createWorker('worker/v2:prod-01', ['q']).name).toBe('worker/v2:prod-01');
    });
  });

  describe('createLogEntry', () => {
    test('should create an info log entry by default', () => {
      const before = Date.now();
      const entry = createLogEntry('Job started processing');
      const after = Date.now();

      expect(entry.level).toBe('info');
      expect(entry.message).toBe('Job started processing');
      expect(entry.timestamp).toBeGreaterThanOrEqual(before);
      expect(entry.timestamp).toBeLessThanOrEqual(after);
    });

    test('should create a warn log entry', () => {
      const entry = createLogEntry('Retrying job', 'warn');
      expect(entry.level).toBe('warn');
      expect(entry.message).toBe('Retrying job');
    });

    test('should create an error log entry', () => {
      const entry = createLogEntry('Job processing failed', 'error');
      expect(entry.level).toBe('error');
      expect(entry.message).toBe('Job processing failed');
    });

    test('should create an info log entry when explicitly specified', () => {
      expect(createLogEntry('All good', 'info').level).toBe('info');
    });

    test('should handle empty message', () => {
      const entry = createLogEntry('');
      expect(entry.message).toBe('');
      expect(entry.level).toBe('info');
    });

    test('should handle message with special characters', () => {
      const msg = 'Error: connection refused (ECONNREFUSED) at 127.0.0.1:6789';
      expect(createLogEntry(msg, 'error').message).toBe(msg);
    });
  });
});
