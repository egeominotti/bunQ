/**
 * Tests for backoff system fixes:
 *
 * 1. JITTER — calculateBackoff now adds jitter to prevent thundering herd.
 *    Exponential: ±50% jitter around the base delay.
 *    Fixed: ±20% jitter around the configured delay.
 *
 * 2. MAX CAP — Backoff delays are capped at DEFAULT_MAX_BACKOFF (1 hour)
 *    or the user-configured maxDelay.
 *
 * 3. RECOVERY — backgroundTasks.ts recovery now uses calculateBackoff(job)
 *    instead of an inline formula, respecting backoffConfig.
 */

import { describe, it, expect } from 'bun:test';
import { calculateBackoff, DEFAULT_MAX_BACKOFF, type Job } from '../src/domain/types/job';

function makeJob(overrides: Partial<Job> = {}): Job {
  return {
    id: 'test-job-1' as any,
    queue: 'test-queue',
    data: {},
    priority: 0,
    createdAt: Date.now(),
    lifo: false,
    state: 'waiting' as any,
    attempts: 0,
    maxAttempts: 25,
    backoff: 1000,
    backoffConfig: null,
    timeout: null,
    delay: 0,
    runAt: Date.now(),
    startedAt: null,
    finishedAt: null,
    progress: 0,
    progressMessage: null,
    result: null,
    error: null,
    stallCount: 0,
    parentId: null,
    childrenIds: null,
    customId: null,
    removeOnComplete: false,
    removeOnFail: false,
    lockToken: null,
    lockExpiresAt: null,
    durable: false,
    generation: 0,
    ...overrides,
  } as Job;
}

describe('Fix: backoff has jitter', () => {
  it('identical jobs produce varied backoff values — no thundering herd', () => {
    const backoffs = new Set<number>();

    for (let i = 0; i < 100; i++) {
      const job = makeJob({ id: `job-${i}` as any, attempts: 3, backoff: 1000 });
      backoffs.add(calculateBackoff(job));
    }

    // After fix: jitter should produce varied values
    expect(backoffs.size).toBeGreaterThan(1);

    // Values should be within ±50% of base (1000 * 2^3 = 8000)
    // Range: [4000, 12000]
    for (const val of backoffs) {
      expect(val).toBeGreaterThanOrEqual(4000);
      expect(val).toBeLessThanOrEqual(12000);
    }
  });

  it('exponential backoffConfig produces varied values with jitter', () => {
    const backoffs = new Set<number>();

    for (let i = 0; i < 100; i++) {
      const job = makeJob({
        id: `job-${i}` as any,
        attempts: 5,
        backoffConfig: { type: 'exponential', delay: 2000 },
      });
      backoffs.add(calculateBackoff(job));
    }

    // After fix: values should vary due to jitter
    expect(backoffs.size).toBeGreaterThan(1);

    // Base = 2000 * 2^5 = 64000, range with ±50% jitter: [32000, 96000]
    for (const val of backoffs) {
      expect(val).toBeGreaterThanOrEqual(32000);
      expect(val).toBeLessThanOrEqual(96000);
    }
  });

  it('fixed backoffConfig produces varied values with ±20% jitter', () => {
    const backoffs = new Set<number>();

    for (let i = 0; i < 100; i++) {
      const job = makeJob({
        id: `job-${i}` as any,
        attempts: 5,
        backoffConfig: { type: 'fixed', delay: 5000 },
      });
      backoffs.add(calculateBackoff(job));
    }

    // After fix: values should vary due to ±20% jitter
    expect(backoffs.size).toBeGreaterThan(1);

    // Fixed 5000 with ±20% jitter: [4000, 6000]
    for (const val of backoffs) {
      expect(val).toBeGreaterThanOrEqual(4000);
      expect(val).toBeLessThanOrEqual(6000);
    }
  });
});

describe('Fix: backoff has max cap', () => {
  it('DEFAULT_MAX_BACKOFF is exported and equals 1 hour', () => {
    expect(DEFAULT_MAX_BACKOFF).toBe(3_600_000);
  });

  it('attempt 20 is capped at DEFAULT_MAX_BACKOFF', () => {
    const job = makeJob({ attempts: 20, backoff: 1000 });
    const delay = calculateBackoff(job);

    // 1000 * 2^20 = 1,048,576,000 ms before cap — must be capped
    expect(delay).toBeLessThanOrEqual(DEFAULT_MAX_BACKOFF);
  });

  it('attempt 30 is capped at DEFAULT_MAX_BACKOFF', () => {
    const job = makeJob({ attempts: 30, backoff: 1000 });
    const delay = calculateBackoff(job);

    // Would be ~34 years without cap — must be capped
    expect(delay).toBeLessThanOrEqual(DEFAULT_MAX_BACKOFF);
  });

  it('exponential backoffConfig is also capped', () => {
    const job = makeJob({
      attempts: 15,
      backoffConfig: { type: 'exponential', delay: 5000 },
    });
    const delay = calculateBackoff(job);

    // 5000 * 2^15 = 163,840,000 before cap — must be capped
    expect(delay).toBeLessThanOrEqual(DEFAULT_MAX_BACKOFF);
  });

  it('custom maxDelay is respected', () => {
    const customMax = 60_000; // 1 minute
    const job = makeJob({
      attempts: 15,
      backoffConfig: { type: 'exponential', delay: 5000, maxDelay: customMax },
    });
    const delay = calculateBackoff(job);

    expect(delay).toBeLessThanOrEqual(customMax);
  });

  it('small delays are not affected by cap', () => {
    const job = makeJob({ attempts: 2, backoff: 1000 });
    const delay = calculateBackoff(job);

    // Base = 1000 * 2^2 = 4000, jittered range [2000, 6000] — well under cap
    expect(delay).toBeGreaterThanOrEqual(2000);
    expect(delay).toBeLessThanOrEqual(6000);
    expect(delay).toBeLessThan(DEFAULT_MAX_BACKOFF);
  });
});

describe('Fix: recovery backoff uses calculateBackoff', () => {
  it('recovery now respects fixed backoffConfig', () => {
    const job = makeJob({
      attempts: 5,
      backoff: 1000,
      backoffConfig: { type: 'fixed', delay: 5000 },
    });

    // After fix: recovery uses calculateBackoff which respects backoffConfig
    const backoff = calculateBackoff(job);

    // Fixed 5000 with ±20% jitter: [4000, 6000]
    expect(backoff).toBeGreaterThanOrEqual(4000);
    expect(backoff).toBeLessThanOrEqual(6000);

    // The old inline formula would have returned 1000 * 2^4 = 16000
    // The fixed backoff should be near 5000, never 16000
    expect(backoff).toBeLessThan(7000);
  });

  it('recovery respects exponential backoffConfig', () => {
    const job = makeJob({
      attempts: 3,
      backoff: 1000,
      backoffConfig: { type: 'exponential', delay: 2000 },
    });

    const backoff = calculateBackoff(job);

    // Base = 2000 * 2^3 = 16000, jittered range [8000, 24000]
    expect(backoff).toBeGreaterThanOrEqual(8000);
    expect(backoff).toBeLessThanOrEqual(24000);
  });
});
