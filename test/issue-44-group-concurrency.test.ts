/**
 * Issue #44 - Per-group concurrency limiting via groupKey in Worker limiter option
 *
 * Tests for the groupKey feature that allows limiting concurrent jobs per group
 * (e.g., max 3 concurrent jobs per domain when scraping websites).
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Queue, Worker, shutdownManager } from '../src/client';

describe('Worker - Group Concurrency (Issue #44)', () => {
  let queue: Queue<{ domain: string; value: number }>;
  let worker: Worker<{ domain: string; value: number }, string> | null = null;

  beforeEach(() => {
    queue = new Queue('group-concurrency-test', { embedded: true });
    queue.obliterate();
  });

  afterEach(async () => {
    if (worker) {
      await worker.close(true);
      worker = null;
    }
    queue.close();
    shutdownManager();
  });

  test('should limit concurrent jobs per group', async () => {
    // Track how many jobs are concurrently active per group
    const activeCounts = new Map<string, number>();
    const peakCounts = new Map<string, number>();

    worker = new Worker(
      'group-concurrency-test',
      async (job) => {
        const domain = job.data.domain;
        const current = (activeCounts.get(domain) ?? 0) + 1;
        activeCounts.set(domain, current);

        // Track peak concurrency per group
        const peak = peakCounts.get(domain) ?? 0;
        if (current > peak) peakCounts.set(domain, current);

        // Simulate work
        await Bun.sleep(100);

        activeCounts.set(domain, (activeCounts.get(domain) ?? 1) - 1);
        return `done-${domain}`;
      },
      {
        embedded: true,
        concurrency: 20,
        limiter: {
          groupKey: 'domain',
          max: 2,
          duration: 0, // duration is ignored when groupKey is set
        },
      }
    );

    // Add 6 jobs for group "example.com"
    for (let i = 0; i < 6; i++) {
      await queue.add('scrape', { domain: 'example.com', value: i });
    }

    // Wait for all jobs to complete
    let completedCount = 0;
    worker.on('completed', () => {
      completedCount++;
    });

    // Wait until all jobs processed
    const deadline = Date.now() + 5000;
    while (completedCount < 6 && Date.now() < deadline) {
      await Bun.sleep(50);
    }

    expect(completedCount).toBe(6);
    // Peak concurrency for example.com should never exceed 2
    expect(peakCounts.get('example.com')!).toBeLessThanOrEqual(2);
  });

  test('should allow multiple groups to run concurrently up to their limits', async () => {
    const activeCounts = new Map<string, number>();
    const peakCounts = new Map<string, number>();
    let peakTotal = 0;
    let activeTotal = 0;

    worker = new Worker(
      'group-concurrency-test',
      async (job) => {
        const domain = job.data.domain;
        const current = (activeCounts.get(domain) ?? 0) + 1;
        activeCounts.set(domain, current);
        activeTotal++;

        const peak = peakCounts.get(domain) ?? 0;
        if (current > peak) peakCounts.set(domain, current);
        if (activeTotal > peakTotal) peakTotal = activeTotal;

        await Bun.sleep(100);

        activeCounts.set(domain, (activeCounts.get(domain) ?? 1) - 1);
        activeTotal--;
        return `done-${domain}`;
      },
      {
        embedded: true,
        concurrency: 20,
        limiter: {
          groupKey: 'domain',
          max: 3,
          duration: 0,
        },
      }
    );

    // Add jobs for two different groups
    for (let i = 0; i < 6; i++) {
      await queue.add('scrape', { domain: 'site-a.com', value: i });
    }
    for (let i = 0; i < 6; i++) {
      await queue.add('scrape', { domain: 'site-b.com', value: i });
    }

    let completedCount = 0;
    worker.on('completed', () => {
      completedCount++;
    });

    const deadline = Date.now() + 5000;
    while (completedCount < 12 && Date.now() < deadline) {
      await Bun.sleep(50);
    }

    expect(completedCount).toBe(12);
    // Each group should never exceed 3 concurrent
    expect(peakCounts.get('site-a.com')!).toBeLessThanOrEqual(3);
    expect(peakCounts.get('site-b.com')!).toBeLessThanOrEqual(3);
    // Both groups should have had some parallelism (peak total > 3)
    expect(peakTotal).toBeGreaterThan(3);
  });

  test('should not apply group limits when groupKey is not set', async () => {
    let peakActive = 0;
    let activeCount = 0;

    worker = new Worker(
      'group-concurrency-test',
      async (job) => {
        activeCount++;
        if (activeCount > peakActive) peakActive = activeCount;

        await Bun.sleep(100);

        activeCount--;
        return `done-${job.data.value}`;
      },
      {
        embedded: true,
        concurrency: 10,
        // No limiter = no group concurrency limits
      }
    );

    // Add 10 jobs all with same domain
    for (let i = 0; i < 10; i++) {
      await queue.add('scrape', { domain: 'example.com', value: i });
    }

    let completedCount = 0;
    worker.on('completed', () => {
      completedCount++;
    });

    const deadline = Date.now() + 5000;
    while (completedCount < 10 && Date.now() < deadline) {
      await Bun.sleep(50);
    }

    expect(completedCount).toBe(10);
    // Without group limits, more than 3 jobs should run concurrently
    // (concurrency is 10, so peak should be well above a typical group limit)
    expect(peakActive).toBeGreaterThan(3);
  });

  test('should allow jobs without the groupKey field to run without limits', async () => {
    const activeCounts = new Map<string, number>();
    const peakCounts = new Map<string, number>();
    let noGroupPeak = 0;
    let noGroupActive = 0;

    const queueAny = queue as Queue<Record<string, unknown>>;

    worker = new Worker(
      'group-concurrency-test',
      async (job) => {
        const domain = (job.data as { domain?: string }).domain;
        const key = domain ?? '__none__';

        if (!domain) {
          noGroupActive++;
          if (noGroupActive > noGroupPeak) noGroupPeak = noGroupActive;
        } else {
          const current = (activeCounts.get(key) ?? 0) + 1;
          activeCounts.set(key, current);
          const peak = peakCounts.get(key) ?? 0;
          if (current > peak) peakCounts.set(key, current);
        }

        await Bun.sleep(80);

        if (!domain) {
          noGroupActive--;
        } else {
          activeCounts.set(key, (activeCounts.get(key) ?? 1) - 1);
        }
        return 'done';
      },
      {
        embedded: true,
        concurrency: 10,
        limiter: {
          groupKey: 'domain',
          max: 2,
          duration: 0,
        },
      }
    ) as Worker<Record<string, unknown>, string>;

    // Add jobs WITH domain (group limited)
    for (let i = 0; i < 4; i++) {
      await queueAny.add('scrape', { domain: 'limited.com', value: i });
    }
    // Add jobs WITHOUT domain field (should not be group limited)
    for (let i = 0; i < 5; i++) {
      await queueAny.add('other', { value: i });
    }

    let completedCount = 0;
    worker.on('completed', () => {
      completedCount++;
    });

    const deadline = Date.now() + 5000;
    while (completedCount < 9 && Date.now() < deadline) {
      await Bun.sleep(50);
    }

    expect(completedCount).toBe(9);
    // Jobs with domain should be limited to max 2
    expect(peakCounts.get('limited.com')!).toBeLessThanOrEqual(2);
    // Jobs without domain should run without group limits
    // (they compete for the 10 concurrency slots normally)
    expect(noGroupPeak).toBeGreaterThanOrEqual(1);
  });

  test('should ignore duration when groupKey is set (concurrency, not rate limiting)', async () => {
    // When groupKey is set, duration should be ignored.
    // This means jobs should NOT be rate-limited by time windows.
    const completionTimes: number[] = [];
    const startTime = Date.now();

    worker = new Worker(
      'group-concurrency-test',
      async () => {
        // Very fast job - no sleep
        completionTimes.push(Date.now() - startTime);
        return 'done';
      },
      {
        embedded: true,
        concurrency: 10,
        limiter: {
          groupKey: 'domain',
          max: 5,
          duration: 60000, // 60s window - if this were rate limiting, jobs would be slow
        },
      }
    );

    // Add 5 jobs for same group - all should be allowed since max=5
    for (let i = 0; i < 5; i++) {
      await queue.add('scrape', { domain: 'fast.com', value: i });
    }

    let completedCount = 0;
    worker.on('completed', () => {
      completedCount++;
    });

    const deadline = Date.now() + 3000;
    while (completedCount < 5 && Date.now() < deadline) {
      await Bun.sleep(50);
    }

    expect(completedCount).toBe(5);
    // All 5 jobs should complete quickly (within 2s), not rate-limited by the 60s duration
    const totalTime = completionTimes[completionTimes.length - 1];
    expect(totalTime).toBeLessThan(2000);
  });
});

describe('GroupConcurrencyLimiter - Unit Tests', () => {
  test('should track active counts per group', async () => {
    const { GroupConcurrencyLimiter } = await import('../src/client/worker/groupConcurrency');

    const limiter = new GroupConcurrencyLimiter('domain', 3);

    const jobA = { id: '1', data: { domain: 'a.com' } } as any;
    const jobB = { id: '2', data: { domain: 'b.com' } } as any;

    expect(limiter.canProcess(jobA)).toBe(true);
    limiter.increment(jobA);
    expect(limiter.getGroupCount('a.com')).toBe(1);

    limiter.increment(jobA);
    expect(limiter.getGroupCount('a.com')).toBe(2);

    limiter.increment(jobA);
    expect(limiter.getGroupCount('a.com')).toBe(3);

    // At capacity
    expect(limiter.canProcess(jobA)).toBe(false);

    // Different group still has capacity
    expect(limiter.canProcess(jobB)).toBe(true);

    // Decrement frees a slot
    limiter.decrement(jobA);
    expect(limiter.getGroupCount('a.com')).toBe(2);
    expect(limiter.canProcess(jobA)).toBe(true);
  });

  test('should return null group for jobs without groupKey field', async () => {
    const { GroupConcurrencyLimiter } = await import('../src/client/worker/groupConcurrency');

    const limiter = new GroupConcurrencyLimiter('domain', 2);

    const jobNoField = { id: '1', data: { value: 42 } } as any;
    const jobNullData = { id: '2', data: null } as any;

    // Jobs without the field should always pass
    expect(limiter.canProcess(jobNoField)).toBe(true);
    expect(limiter.canProcess(jobNullData)).toBe(true);

    // Increment/decrement should be no-ops for null group
    limiter.increment(jobNoField);
    expect(limiter.getGroupCount('undefined')).toBe(0);
  });

  test('should create from options or return null', async () => {
    const { GroupConcurrencyLimiter } = await import('../src/client/worker/groupConcurrency');

    expect(GroupConcurrencyLimiter.fromOptions(null)).toBeNull();
    expect(GroupConcurrencyLimiter.fromOptions(undefined)).toBeNull();
    expect(GroupConcurrencyLimiter.fromOptions({ max: 5, duration: 1000 })).toBeNull();

    const limiter = GroupConcurrencyLimiter.fromOptions({
      max: 3,
      duration: 0,
      groupKey: 'domain',
    });
    expect(limiter).not.toBeNull();
    expect(limiter!.getMax()).toBe(3);
    expect(limiter!.getGroupKey()).toBe('domain');
  });

  test('should clean up group entry when count reaches zero', async () => {
    const { GroupConcurrencyLimiter } = await import('../src/client/worker/groupConcurrency');

    const limiter = new GroupConcurrencyLimiter('domain', 5);
    const job = { id: '1', data: { domain: 'cleanup.com' } } as any;

    limiter.increment(job);
    expect(limiter.getGroupCount('cleanup.com')).toBe(1);

    limiter.decrement(job);
    expect(limiter.getGroupCount('cleanup.com')).toBe(0);
  });
});
