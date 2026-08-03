/**
 * Executable proof for /guide/dlq/reference/
 * ("DLQ Reference: Reasons, Entry Shape, Methods").
 */

import { afterEach, describe, expect, test } from 'bun:test';
import {
  type CoreE2eHarness,
  MODES,
  closeHarness,
  startHarness,
  waitForState,
  waitUntil,
} from '../docs-guide-support';

let harness: CoreE2eHarness | null = null;

afterEach(async () => {
  await closeHarness(harness);
  harness = null;
});

const ENTRY_FIELDS = [
  'attempts',
  'enteredAt',
  'error',
  'expiresAt',
  'job',
  'lastRetryAt',
  'nextRetryAt',
  'reason',
  'retryCount',
];

const ATTEMPT_FIELDS = ['attempt', 'duration', 'error', 'failedAt', 'reason', 'startedAt'];

for (const mode of MODES) {
  describe(`dlq guide · reference [${mode}]`, () => {
    test('a DLQ entry carries exactly the documented fields', async () => {
      harness = await startHarness('dlq-reference', mode);
      const queue = harness.queue('shape');

      harness.worker(queue.name, async () => {
        throw new Error('boom');
      });
      const job = await queue.add('send', { key: 'value' }, { attempts: 1, durable: true });
      await waitForState(queue, job.id, 'failed', 20_000);

      const entry = (await queue.getDlqAsync())[0];
      expect(Object.keys(entry).sort()).toEqual(ENTRY_FIELDS);
      expect(entry.job.id).toBe(job.id);
      expect(entry.enteredAt).toBeNumber();
      expect(entry.error).toBeString();
      expect(entry.retryCount).toBe(0);
      expect(entry.lastRetryAt).toBeNull();
      expect(entry.nextRetryAt).toBeNull();
      expect(entry.expiresAt).toBeNumber();

      const attempt = entry.attempts[0];
      expect(Object.keys(attempt).sort()).toEqual(ATTEMPT_FIELDS);
      expect(attempt.attempt).toBe(1);
      expect(attempt.startedAt).toBeNumber();
      expect(attempt.failedAt).toBeGreaterThanOrEqual(attempt.startedAt);
      expect(attempt.duration).toBeGreaterThanOrEqual(0);
      expect(attempt.reason).toBeString();
      expect(attempt.error).toContain('boom');
    }, 40_000);

    test('max_attempts_exceeded is the terminal reason for a processor failure', async () => {
      harness = await startHarness('dlq-reference', mode);
      const queue = harness.queue('attempts');

      harness.worker(queue.name, async () => {
        throw new Error('boom');
      });
      const job = await queue.add('send', {}, { attempts: 2, backoff: 10, durable: true });
      await waitForState(queue, job.id, 'failed', 30_000);

      const entry = (await queue.getDlqAsync())[0];
      expect(entry.reason).toBe('max_attempts_exceeded');
      expect(entry.attempts).toHaveLength(2);
      // The non-terminal attempt is recorded as a retryable processor failure.
      expect(entry.attempts[0].reason).toBe('explicit_fail');
      expect(entry.attempts[1].reason).toBe('max_attempts_exceeded');
    }, 60_000);

    test('stalled is the terminal reason for a job that stops heartbeating', async () => {
      harness = await startHarness('dlq-reference', mode);
      const queue = harness.queue('stalled');

      await queue.setStallConfigAsync({
        enabled: true,
        stallInterval: 20,
        maxStalls: 1,
        gracePeriod: 0,
      });
      const worker = harness.worker(
        queue.name,
        async () => await new Promise<never>(() => undefined),
        { heartbeatInterval: 0, useLocks: true }
      );
      worker.on('error', () => undefined);

      const job = await queue.add('silent', {}, { attempts: 10, durable: true });
      await waitForState(queue, job.id, 'failed', 40_000);

      expect((await queue.getDlqAsync())[0].reason).toBe('stalled');
    }, 60_000);

    test('two consecutive timeouts stay recorded as [timeout, timeout]', async () => {
      harness = await startHarness('dlq-reference', mode);
      const queue = harness.queue('timeouts');

      // Never returns: the broker's timeout sweep owns both outcomes. Spare
      // concurrency lets the second delivery start while the first handler is
      // still hung.
      harness.worker(queue.name, async () => await new Promise<never>(() => undefined), {
        concurrency: 4,
      });
      const job = await queue.add(
        'slow',
        {},
        { timeout: 100, attempts: 2, backoff: 10, durable: true }
      );

      await waitForState(queue, job.id, 'failed', 40_000);
      const entry = (await queue.getDlqAsync())[0];
      expect(entry.attempts.map((record) => record.reason)).toEqual(['timeout', 'timeout']);
      expect(entry.reason).toBe('timeout');
    }, 60_000);

    test('a timeout followed by a processor failure is [timeout, max_attempts_exceeded]', async () => {
      harness = await startHarness('dlq-reference', mode);
      const queue = harness.queue('mixed');

      // Worker A is saturated by a handler that never returns, so the retry
      // after the timeout can only be delivered to worker B.
      const hanging = harness.worker(
        queue.name,
        async () => await new Promise<never>(() => undefined),
        { concurrency: 1 }
      );
      hanging.on('error', () => undefined);
      const job = await queue.add(
        'slow',
        {},
        { timeout: 100, attempts: 2, backoff: 10, durable: true }
      );
      await waitForState(queue, job.id, 'active', 10_000);
      await waitUntil(
        async () => (await queue.getJobState(job.id)) !== 'active',
        'the first delivery to time out',
        30_000
      );

      const failing = harness.worker(queue.name, async () => {
        throw new Error('second attempt failed');
      });
      failing.on('error', () => undefined);

      await waitForState(queue, job.id, 'failed', 40_000);
      const entry = (await queue.getDlqAsync())[0];
      expect(entry.attempts.map((record) => record.reason)).toEqual([
        'timeout',
        'max_attempts_exceeded',
      ]);
      expect(entry.reason).toBe('max_attempts_exceeded');
    }, 90_000);

    test('ttl_expired and worker_lost stay reserved categories', async () => {
      harness = await startHarness('dlq-reference', mode);
      const queue = harness.queue('reserved');

      // A disconnected worker is classified as stalled, not worker_lost.
      await queue.setStallConfigAsync({
        enabled: true,
        stallInterval: 20,
        maxStalls: 1,
        gracePeriod: 0,
      });
      const worker = harness.worker(
        queue.name,
        async () => await new Promise<never>(() => undefined),
        { heartbeatInterval: 0, useLocks: true }
      );
      worker.on('error', () => undefined);
      const job = await queue.add('silent', {}, { attempts: 10, durable: true });
      await waitForState(queue, job.id, 'failed', 40_000);

      const stats = await queue.getDlqStatsAsync();
      expect(stats.byReason.worker_lost).toBe(0);
      expect(stats.byReason.ttl_expired).toBe(0);
      expect(stats.byReason.stalled).toBe(1);
    }, 60_000);

    test('the stats object exposes every documented counter', async () => {
      harness = await startHarness('dlq-reference', mode);
      const queue = harness.queue('stats-shape');

      harness.worker(queue.name, async () => {
        throw new Error('boom');
      });
      await queue.add('send', {}, { attempts: 1, durable: true });
      await waitUntil(async () => (await queue.getDlqAsync()).length === 1, 'one entry', 20_000);

      const stats = await queue.getDlqStatsAsync();
      expect(stats.total).toBe(1);
      expect(stats.expired).toBe(0);
      expect(stats.pendingRetry).toBe(0);
      expect(stats.oldestEntry).toBeNumber();
      expect(stats.newestEntry).toBeNumber();
      expect(Object.keys(stats.byReason).sort()).toEqual([
        'explicit_fail',
        'max_attempts_exceeded',
        'stalled',
        'timeout',
        'ttl_expired',
        'unknown',
        'worker_lost',
      ]);
    }, 40_000);

    // DlqStats declares a required `byQueue: Record<string, number>`; the
    // embedded client mapping drops it, so the field is undefined at runtime.
    test('DlqStats carries the declared byQueue map', async () => {
      harness = await startHarness('dlq-reference', mode);
      const queue = harness.queue('stats-by-queue');

      harness.worker(queue.name, async () => {
        throw new Error('boom');
      });
      await queue.add('send', {}, { attempts: 1, durable: true });
      await waitUntil(async () => (await queue.getDlqAsync()).length === 1, 'one entry', 20_000);

      const stats = await queue.getDlqStatsAsync();
      expect(stats.byQueue).toBeDefined();
      expect(stats.byQueue[queue.name]).toBe(1);
    }, 40_000);

    test('DlqStats byQueue is isolated to the requested queue', async () => {
      harness = await startHarness('dlq-reference', mode);
      const first = harness.queue('stats-first');
      const second = harness.queue('stats-second');

      for (const queue of [first, second]) {
        const worker = harness.worker(queue.name, async () => {
          throw new Error(`failed ${queue.name}`);
        });
        worker.on('failed', () => undefined);
        await queue.add('send', {}, { attempts: 1, durable: true });
        await waitUntil(async () => (await queue.getDlqAsync()).length === 1, 'one entry', 20_000);
      }

      const firstStats = await first.getDlqStatsAsync();
      const secondStats = await second.getDlqStatsAsync();
      expect(firstStats.byQueue).toEqual({ [first.name]: 1 });
      expect(secondStats.byQueue).toEqual({ [second.name]: 1 });
    }, 50_000);

    test('a DLQ job keeps its live Job methods across the TCP round trip', async () => {
      harness = await startHarness('dlq-reference', mode);
      const queue = harness.queue('live-methods');

      harness.worker(queue.name, async () => {
        throw new Error('boom');
      });
      const job = await queue.add('send', { key: 'value' }, { attempts: 1, durable: true });
      await waitForState(queue, job.id, 'failed', 20_000);

      const entry = (await queue.getDlqAsync())[0];
      expect(entry.job.getState).toBeFunction();
      expect(await entry.job.getState()).toBe('failed');
      expect(entry.job.log).toBeFunction();
      expect(entry.job.remove).toBeFunction();
      expect(entry.job.retry).toBeFunction();
    }, 40_000);
  });
}
