import { afterAll, describe, expect, test } from 'bun:test';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import type { JobId } from '../src/domain/types/job';
import {
  cleanupPostgresNamespace,
  eventually,
  postgresManagerStore,
} from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-dedup-cache-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function manager(value: string, brokerId: string): PostgresQueueManager {
  return new PostgresQueueManager({
    postgres: { url: postgresUrl!, namespace: value, brokerId, pollIntervalMs: 25 },
  });
}

function queueIds(manager: PostgresQueueManager, queue: string): string[] {
  return manager
    .getJobs(queue)
    .map((job) => String(job.id))
    .sort();
}

function exactIds(manager: PostgresQueueManager, queue: string, ids: readonly JobId[]): boolean {
  return queueIds(manager, queue).join() === ids.map(String).sort().join();
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupPostgresNamespace(postgresUrl, value);
});

describe('PostgreSQL deduplication cache convergence', () => {
  test.skipIf(!postgresUrl)('removes a pending generation replaced on another broker', async () => {
    const value = namespace('pending-replace');
    const queue = 'pending-replace';
    const first = manager(value, 'pending-replace-a');
    const second = manager(value, 'pending-replace-b');

    try {
      await Promise.all([first.waitUntilReady(), second.waitUntilReady()]);
      const old = await first.push(queue, { data: { generation: 1 }, uniqueKey: 'shared-key' });
      expect(await eventually(() => exactIds(second, queue, [old.id]))).toBe(true);

      const replacement = await first.push(queue, {
        data: { generation: 2 },
        uniqueKey: 'shared-key',
        dedup: { replace: true },
      });
      expect(await postgresManagerStore(first).getJob(old.id)).toBeNull();
      expect(
        await eventually(
          () =>
            exactIds(first, queue, [replacement.id]) &&
            exactIds(second, queue, [replacement.id]) &&
            first.getDeduplicationJobId(queue, 'shared-key') === replacement.id &&
            second.getDeduplicationJobId(queue, 'shared-key') === replacement.id
        )
      ).toBe(true);
    } finally {
      await Promise.allSettled([first.shutdownPostgres(), second.shutdownPostgres()]);
    }
  });

  test.skipIf(!postgresUrl)(
    'clears the deduplication key from an active replaced generation',
    async () => {
      const value = namespace('active-replace');
      const queue = 'active-replace';
      const first = manager(value, 'active-replace-a');
      const second = manager(value, 'active-replace-b');

      try {
        await Promise.all([first.waitUntilReady(), second.waitUntilReady()]);
        const old = await first.push(queue, { data: { generation: 1 }, uniqueKey: 'shared-key' });
        const claim = await first.pullWithLock(queue, 'active-replace-worker');
        expect(claim.job?.id).toBe(old.id);
        expect(
          await eventually(() =>
            second.getJobs(queue, { state: 'active' }).some((job) => job.id === old.id)
          )
        ).toBe(true);

        const replacement = await second.push(queue, {
          data: { generation: 2 },
          uniqueKey: 'shared-key',
          dedup: { replace: true },
        });
        expect(
          await eventually(() =>
            [first, second].every((broker) => {
              const oldGeneration = broker.getJobs(queue).find((job) => job.id === old.id);
              return (
                oldGeneration?.uniqueKey === null &&
                broker.getDeduplicationJobId(queue, 'shared-key') === replacement.id
              );
            })
          )
        ).toBe(true);
      } finally {
        await Promise.allSettled([first.shutdownPostgres(), second.shutdownPostgres()]);
      }
    }
  );

  test.skipIf(!postgresUrl)('clears an expired key before admitting its successor', async () => {
    const value = namespace('ttl-expiry');
    const queue = 'ttl-expiry';
    const first = manager(value, 'ttl-expiry-a');
    const second = manager(value, 'ttl-expiry-b');

    try {
      await Promise.all([first.waitUntilReady(), second.waitUntilReady()]);
      const old = await first.push(queue, {
        data: { generation: 1 },
        uniqueKey: 'ttl-key',
        dedup: { ttl: 60 },
      });
      expect(await eventually(() => exactIds(second, queue, [old.id]))).toBe(true);
      await Bun.sleep(100);

      const successor = await second.push(queue, {
        data: { generation: 2 },
        uniqueKey: 'ttl-key',
      });
      expect(successor.id).not.toBe(old.id);
      expect(
        await eventually(() =>
          [first, second].every((broker) => {
            const oldGeneration = broker.getJobs(queue).find((job) => job.id === old.id);
            return (
              exactIds(broker, queue, [old.id, successor.id]) &&
              oldGeneration?.uniqueKey === null &&
              broker.getDeduplicationJobId(queue, 'ttl-key') === successor.id
            );
          })
        )
      ).toBe(true);
    } finally {
      await Promise.allSettled([first.shutdownPostgres(), second.shutdownPostgres()]);
    }
  });

  test.skipIf(!postgresUrl)('preserves an extended TTL across brokers', async () => {
    const value = namespace('ttl-extend');
    const queue = 'ttl-extend';
    const first = manager(value, 'ttl-extend-a');
    const second = manager(value, 'ttl-extend-b');

    try {
      await Promise.all([first.waitUntilReady(), second.waitUntilReady()]);
      const old = await first.push(queue, {
        data: { generation: 1 },
        uniqueKey: 'extend-key',
        dedup: { ttl: 100 },
      });
      await Bun.sleep(60);
      const extended = await second.push(queue, {
        data: { generation: 2 },
        uniqueKey: 'extend-key',
        dedup: { ttl: 180, extend: true },
      });
      expect(extended.id).toBe(old.id);
      expect(extended.deduplicationTtl).toBe(180);
      expect(
        await eventually(
          () =>
            first.getJobs(queue)[0]?.deduplicationTtl === 180 &&
            second.getJobs(queue)[0]?.deduplicationTtl === 180
        )
      ).toBe(true);
      await Bun.sleep(70);

      const contender = await first.push(queue, {
        data: { generation: 3 },
        uniqueKey: 'extend-key',
      });
      expect(contender.id).toBe(old.id);
      expect(await eventually(() => exactIds(second, queue, [old.id]))).toBe(true);
    } finally {
      await Promise.allSettled([first.shutdownPostgres(), second.shutdownPostgres()]);
    }
  });
});
