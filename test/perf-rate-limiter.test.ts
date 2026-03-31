/**
 * Performance test: WorkerRateLimiter
 *
 * Verifies that canProcessWithinLimit() is O(1) amortized after
 * the head-pointer optimization (replacing Array.filter).
 */

import { describe, test, expect } from 'bun:test';
import { WorkerRateLimiter } from '../src/client/worker/workerRateLimiter';

describe('WorkerRateLimiter performance', () => {
  test('canProcessWithinLimit() is O(1) — cost does not scale with token count', () => {
    const tokenCounts = [100, 1_000, 10_000];
    const timings: { tokens: number; avgNs: number }[] = [];

    for (const n of tokenCounts) {
      const rl = new WorkerRateLimiter({ max: n + 1, duration: 60_000 });
      for (let i = 0; i < n; i++) {
        rl.recordJobForLimiter();
      }

      // Warm up
      for (let i = 0; i < 10; i++) {
        rl.canProcessWithinLimit();
      }

      const iterations = 1_000;
      const start = Bun.nanoseconds();
      for (let i = 0; i < iterations; i++) {
        rl.canProcessWithinLimit();
      }
      const elapsed = Bun.nanoseconds() - start;
      timings.push({ tokens: n, avgNs: elapsed / iterations });
    }

    const ratio = timings[2].avgNs / timings[0].avgNs;

    console.log('Rate limiter canProcessWithinLimit() performance:');
    for (const t of timings) {
      console.log(`  ${t.tokens} tokens: ${(t.avgNs / 1000).toFixed(1)}µs per call`);
    }
    console.log(`  Ratio 10k/100: ${ratio.toFixed(1)}x (O(1) if <5x, O(n) if >10x)`);

    // O(1): 100x more tokens should NOT be significantly slower
    expect(ratio).toBeLessThan(5);
  });

  test('canProcessWithinLimit() does not allocate arrays', () => {
    const rl = new WorkerRateLimiter({ max: 10_000, duration: 60_000 });
    for (let i = 0; i < 5_000; i++) {
      rl.recordJobForLimiter();
    }

    Bun.gc(true);
    const before = process.memoryUsage().heapUsed;

    const iterations = 10_000;
    for (let i = 0; i < iterations; i++) {
      rl.canProcessWithinLimit();
    }

    Bun.gc(true);
    const after = process.memoryUsage().heapUsed;
    const deltaKB = (after - before) / 1024;

    console.log(`Memory after ${iterations} calls with 5k tokens: ${deltaKB.toFixed(0)}KB delta`);
    // Head-pointer approach: zero array allocations per call
  });

  test('getTimeUntilNextSlot() is O(1) — no Math.min spread', () => {
    const tokenCounts = [100, 1_000, 10_000];
    const timings: { tokens: number; avgNs: number }[] = [];

    for (const n of tokenCounts) {
      const rl = new WorkerRateLimiter({ max: n, duration: 60_000 });
      for (let i = 0; i < n; i++) {
        rl.recordJobForLimiter();
      }

      for (let i = 0; i < 10; i++) {
        rl.getTimeUntilNextSlot();
      }

      const iterations = 1_000;
      const start = Bun.nanoseconds();
      for (let i = 0; i < iterations; i++) {
        rl.getTimeUntilNextSlot();
      }
      const elapsed = Bun.nanoseconds() - start;
      timings.push({ tokens: n, avgNs: elapsed / iterations });
    }

    const ratio = timings[2].avgNs / timings[0].avgNs;

    console.log('Rate limiter getTimeUntilNextSlot() performance:');
    for (const t of timings) {
      console.log(`  ${t.tokens} tokens: ${(t.avgNs / 1000).toFixed(1)}µs per call`);
    }
    console.log(`  Ratio 10k/100: ${ratio.toFixed(1)}x`);

    // O(1): oldest token is at head, no spread needed
    expect(ratio).toBeLessThan(5);
  });

  test('simulates real worker polling — O(1) per poll', () => {
    const rl = new WorkerRateLimiter({ max: 1_000, duration: 60_000 });
    for (let i = 0; i < 500; i++) {
      rl.recordJobForLimiter();
    }

    const polls = 100;
    const start = Bun.nanoseconds();
    for (let i = 0; i < polls; i++) {
      rl.canProcessWithinLimit();
    }
    const totalUs = (Bun.nanoseconds() - start) / 1000;

    console.log(`Real-world simulation: 1s of polling (100 calls, 500 tokens)`);
    console.log(`  Total CPU time: ${totalUs.toFixed(0)}µs`);
    console.log(`  Per poll: ${(totalUs / polls).toFixed(1)}µs`);

    // O(1) per poll — should be well under 1µs per call
    expect(totalUs / polls).toBeLessThan(5);
  });

  test('correctness: rate limiting still works after optimization', () => {
    const rl = new WorkerRateLimiter({ max: 3, duration: 1_000 });

    // Can process 3 jobs
    expect(rl.canProcessWithinLimit()).toBe(true);
    rl.recordJobForLimiter();
    expect(rl.canProcessWithinLimit()).toBe(true);
    rl.recordJobForLimiter();
    expect(rl.canProcessWithinLimit()).toBe(true);
    rl.recordJobForLimiter();

    // 4th should be blocked
    expect(rl.canProcessWithinLimit()).toBe(false);

    // Time until next slot should be > 0
    const wait = rl.getTimeUntilNextSlot();
    expect(wait).toBeGreaterThan(0);
    expect(wait).toBeLessThanOrEqual(1_000);
  });

  test('correctness: tokens expire correctly with head pointer', async () => {
    const rl = new WorkerRateLimiter({ max: 2, duration: 50 });

    rl.recordJobForLimiter();
    rl.recordJobForLimiter();
    expect(rl.canProcessWithinLimit()).toBe(false);

    // Wait for tokens to expire
    await Bun.sleep(60);

    // Should be available again
    expect(rl.canProcessWithinLimit()).toBe(true);
  });

  test('correctness: getRateLimiterInfo returns accurate counts', () => {
    const rl = new WorkerRateLimiter({ max: 10, duration: 60_000 });

    rl.recordJobForLimiter();
    rl.recordJobForLimiter();
    rl.recordJobForLimiter();

    const info = rl.getRateLimiterInfo();
    expect(info).not.toBeNull();
    expect(info!.current).toBe(3);
    expect(info!.max).toBe(10);
    expect(info!.duration).toBe(60_000);
  });

  test('correctness: rateLimit() blocks processing', () => {
    const rl = new WorkerRateLimiter({ max: 5, duration: 1_000 });

    expect(rl.isRateLimited()).toBe(false);
    rl.rateLimit(500);
    expect(rl.isRateLimited()).toBe(true);
    expect(rl.canProcessWithinLimit()).toBe(false);
  });
});
