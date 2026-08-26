import { afterAll, describe, expect, test } from 'bun:test';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { cleanupPostgresNamespace } from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-query-metrics-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function manager(value: string, brokerId: string): PostgresQueueManager {
  return new PostgresQueueManager({
    postgres: { url: postgresUrl!, namespace: value, brokerId, pollIntervalMs: 25 },
  });
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupPostgresNamespace(postgresUrl, value);
});

describe('PostgreSQL query, metric, and lease regressions', () => {
  test.skipIf(!postgresUrl)(
    'loads lifetime terminal totals after removal, clean, purge, and broker restart',
    async () => {
      const value = namespace('lifetime-totals');
      const queue = 'lifetime-totals';
      const first = manager(value, 'totals-first');
      let second: PostgresQueueManager | null = null;
      try {
        await first.waitUntilReady();

        const removed = await first.push(queue, {
          customId: 'removed-completion',
          data: {},
          removeOnComplete: true,
        });
        const removedClaim = await first.pullWithLock(queue, 'removed-worker');
        await first.ack(removed.id, undefined, removedClaim.token!);

        const cleaned = await first.push(queue, {
          customId: 'cleaned-completion',
          data: {},
        });
        const cleanedClaim = await first.pullWithLock(queue, 'cleaned-worker');
        await first.ack(cleaned.id, undefined, cleanedClaim.token!);
        expect(await first.cleanDurable(queue, 0, 'completed')).toContain(cleaned.id);

        const failed = await first.push(queue, {
          customId: 'purged-failure',
          data: {},
          maxAttempts: 1,
        });
        const failedClaim = await first.pullWithLock(queue, 'failed-worker');
        await first.fail(failed.id, 'terminal', failedClaim.token!);
        expect(await first.purgeDlqDurable(queue)).toBe(1);

        await first.shutdownPostgres();
        second = manager(value, 'totals-second');
        await second.waitUntilReady();

        expect(second.getStats()).toMatchObject({ totalCompleted: 2n, totalFailed: 1n });
        expect(second.getQueueJobCounts(queue)).toMatchObject({
          totalCompleted: 2,
          totalFailed: 1,
        });
      } finally {
        await Promise.allSettled([first.shutdownPostgres(), second?.shutdownPostgres()]);
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'matches SQLite binary ID ordering and treats an empty state filter as all states',
    async () => {
      const value = namespace('query-parity');
      const queueManager = manager(value, 'query-parity');
      try {
        await queueManager.waitUntilReady();
        await queueManager.pushBatch('query-parity', [
          { customId: 'a-lower', data: {} },
          { customId: 'Z-upper', data: {} },
        ]);

        expect(
          queueManager.getJobs('query-parity', { state: 'waiting' }).map((job) => String(job.id))
        ).toEqual(['Z-upper', 'a-lower']);
        expect(
          queueManager.getJobs('query-parity', { state: [] }).map((job) => String(job.id))
        ).toEqual(['Z-upper', 'a-lower']);
      } finally {
        await queueManager.shutdownPostgres();
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'refreshes lock expiry, heartbeat time, TTL, and renewal count after every renewal',
    async () => {
      const value = namespace('lease-metadata');
      const queueManager = manager(value, 'lease-metadata');
      try {
        await queueManager.waitUntilReady();
        const job = await queueManager.push('lease-metadata', { data: {} });
        const claim = await queueManager.pullWithLock('lease-metadata', 'lease-worker', 0, 2_000);
        const initial = queueManager.getLockInfo(job.id)!;
        await Bun.sleep(5);

        expect(await queueManager.extendLock(job.id, claim.token, 5_000)).toBe(true);
        const first = queueManager.getLockInfo(job.id)!;
        expect(first.expiresAt).toBeGreaterThan(initial.expiresAt);
        expect(first.lastRenewalAt).toBeGreaterThan(initial.lastRenewalAt);
        expect(first.renewalCount).toBe(1);
        expect(first.ttl).toBe(5_000);

        await Bun.sleep(5);
        expect(await queueManager.extendLock(job.id, claim.token, 7_000)).toBe(true);
        const second = queueManager.getLockInfo(job.id)!;
        expect(second.expiresAt).toBeGreaterThan(first.expiresAt);
        expect(second.lastRenewalAt).toBeGreaterThan(first.lastRenewalAt);
        expect(second.renewalCount).toBe(2);
        expect(second.ttl).toBe(7_000);
      } finally {
        await queueManager.shutdownPostgres();
      }
    }
  );
});
