import { afterEach, describe, expect, test } from 'bun:test';
import fc from 'fast-check';
import { Queue, shutdownManager } from '../../src/client';
import { getSharedManager } from '../../src/client/manager';
import { jobId } from '../../src/domain/types/job';

const runs = 12;

function queueName(prefix: string): string {
  return `${prefix}-${Bun.randomUUIDv7()}`;
}

afterEach(() => {
  shutdownManager();
});

describe('Queue limit getters expose live broker state', () => {
  test('getGlobalConcurrency round-trips every configured positive limit', async () => {
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 64 }), async (limit) => {
        const queue = new Queue(queueName('global-concurrency'), { embedded: true });
        try {
          await queue.setGlobalConcurrencyAsync(limit);
          expect(await queue.getGlobalConcurrency()).toBe(limit);
        } finally {
          await queue.close();
        }
      }),
      { numRuns: runs }
    );
  });

  test('getGlobalRateLimit round-trips max and duration', async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.integer({ min: 1, max: 100 }),
        fc.integer({ min: 50, max: 120_000 }),
        async (max, duration) => {
          const queue = new Queue(queueName('global-rate'), { embedded: true });
          try {
            await queue.setGlobalRateLimitAsync(max, duration);
            expect(await queue.getGlobalRateLimit()).toEqual({ max, duration });
          } finally {
            await queue.close();
          }
        }
      ),
      { numRuns: runs }
    );
  });

  test('getRateLimitTtl reports a positive remaining broker-side TTL', async () => {
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 500, max: 10_000 }), async (ttl) => {
        const queue = new Queue(queueName('rate-ttl'), { embedded: true });
        try {
          await queue.rateLimit(ttl);
          const remaining = await queue.getRateLimitTtl();
          expect(remaining).toBeGreaterThan(0);
          expect(remaining).toBeLessThanOrEqual(ttl);
        } finally {
          await queue.close();
        }
      }),
      { numRuns: runs }
    );
  });

  test('isMaxed becomes true exactly when global concurrency is exhausted', async () => {
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 4 }), async (limit) => {
        const name = queueName('is-maxed');
        const queue = new Queue(name, { embedded: true });
        const manager = getSharedManager();
        try {
          await queue.setGlobalConcurrencyAsync(limit);
          for (let index = 0; index < limit; index++) {
            await queue.add('task', { index });
            expect(await manager.pull(name)).not.toBeNull();
          }
          expect(await queue.isMaxed()).toBe(true);
        } finally {
          await queue.close();
        }
      }),
      { numRuns: runs }
    );
  });
});

describe('Queue dependency methods are not sentinels', () => {
  test('getDependencies returns every unprocessed child of the parent', async () => {
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 8 }), async (childCount) => {
        const name = queueName('dependencies');
        const queue = new Queue(name, { embedded: true });
        const manager = getSharedManager();
        try {
          const children = [];
          for (let index = 0; index < childCount; index++) {
            children.push(await manager.push(name, { data: { index } }));
          }
          const parent = await manager.push(name, {
            data: { name: 'parent' },
            childrenIds: children.map((child) => child.id),
          });

          const result = await queue.getDependencies(String(parent.id), 'unprocessed');
          expect(result.unprocessed).toEqual(
            children.map((child) => `${name}:${String(child.id)}`).sort()
          );
          expect(result.processed).toEqual({});
        } finally {
          await queue.close();
        }
      }),
      { numRuns: runs }
    );
  });
});

describe('intentionally unsupported compatibility methods are explicit', () => {
  test('trimEvents remains a stable no-op while bunqueue has no retained event stream', async () => {
    await fc.assert(
      fc.asyncProperty(fc.integer({ min: 1, max: 6 }), async (jobCount) => {
        const queue = new Queue(queueName('trim-events'), { embedded: true });
        try {
          for (let index = 0; index < jobCount; index++) {
            await queue.add('task', { index });
          }
          expect(await queue.trimEvents(0)).toBe(0);
        } finally {
          await queue.close();
        }
      }),
      { numRuns: runs }
    );
  });
});

describe('deduplication methods address unique keys, not custom job IDs', () => {
  test('getDeduplicationJobId resolves arbitrary deduplication IDs', async () => {
    await fc.assert(
      fc.asyncProperty(fc.uuid(), async (deduplicationId) => {
        const queue = new Queue(queueName('dedup-get'), { embedded: true });
        try {
          const job = await queue.add(
            'task',
            { value: 1 },
            { deduplication: { id: deduplicationId } }
          );
          expect(await queue.getDeduplicationJobId(deduplicationId)).toBe(job.id);
        } finally {
          await queue.close();
        }
      }),
      { numRuns: runs }
    );
  });

  test('Queue.removeDeduplicationKey releases the key and admits a new job', async () => {
    await fc.assert(
      fc.asyncProperty(fc.uuid(), async (deduplicationId) => {
        const queue = new Queue(queueName('dedup-remove-queue'), { embedded: true });
        try {
          const first = await queue.add(
            'task',
            { value: 1 },
            { deduplication: { id: deduplicationId } }
          );
          expect(await queue.removeDeduplicationKey(deduplicationId)).toBe(1);
          const second = await queue.add(
            'task',
            { value: 2 },
            { deduplication: { id: deduplicationId } }
          );
          expect(second.id).not.toBe(first.id);
        } finally {
          await queue.close();
        }
      }),
      { numRuns: runs }
    );
  });

  test('Job.removeDeduplicationKey only removes the key owned by that job', async () => {
    await fc.assert(
      fc.asyncProperty(fc.uuid(), async (deduplicationId) => {
        const queue = new Queue(queueName('dedup-remove-job'), { embedded: true });
        try {
          const first = await queue.add(
            'task',
            { value: 1 },
            { deduplication: { id: deduplicationId } }
          );
          expect(await first.removeDeduplicationKey()).toBe(true);
          expect(await queue.getDeduplicationJobId(deduplicationId)).toBeNull();
          expect(getSharedManager().getJobIndex().has(jobId(first.id))).toBe(true);
        } finally {
          await queue.close();
        }
      }),
      { numRuns: runs }
    );
  });
});
