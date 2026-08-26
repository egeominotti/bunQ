import { describe, expect, test } from 'bun:test';
import type { SQL } from 'bun';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';
import type { PostgresContext } from '../src/infrastructure/persistence/postgres/context';
import {
  PostgresEventStream,
  postgresEventDrainBatchSize,
} from '../src/infrastructure/persistence/postgres/events';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;

interface DrainableEventStream {
  drain(): Promise<void>;
  reportDrainStatus(error: Error | null): void;
}

interface MaintainableStore {
  runMaintenance(operation: () => Promise<unknown>, subsystem?: string): Promise<void>;
}

function fakeContext(execute: () => Promise<unknown[]>): PostgresContext {
  return {
    sql: execute as unknown as SQL,
    config: {
      url: 'postgresql://unused',
      namespace: 'event-health-unit',
      brokerId: 'event-health-unit',
      poolSize: 2,
      leaseDurationMs: 30_000,
      pollIntervalMs: 25,
      maxQueueEvents: 100,
      maxMetricDataPoints: 100,
    },
  };
}

describe('PostgreSQL event stream health', () => {
  test('scales catch-up reads to the retained window within a bounded memory budget', () => {
    expect(postgresEventDrainBatchSize(1)).toBe(1_000);
    expect(postgresEventDrainBatchSize(4_096)).toBe(4_096);
    expect(postgresEventDrainBatchSize(10_000)).toBe(4_096);
    expect(postgresEventDrainBatchSize(1_000_000)).toBe(4_096);
  });

  test('reports a failed durable drain and recovery after a complete retry', async () => {
    const failure = new Error('synthetic journal query failure');
    let nextFailure: Error | null = failure;
    const stream = new PostgresEventStream(
      fakeContext(async () => {
        if (nextFailure) throw nextFailure;
        return [];
      })
    );
    const statuses: Array<Error | null> = [];
    const unsubscribe = stream.subscribeDrainStatus((error) => statuses.push(error));

    try {
      await (stream as unknown as DrainableEventStream).drain();
      expect(statuses).toEqual([failure]);

      nextFailure = null;
      await (stream as unknown as DrainableEventStream).drain();
      expect(statuses).toEqual([failure, null]);
    } finally {
      unsubscribe();
      await stream.close();
    }
  });

  test.skipIf(!postgresUrl)(
    'keeps a journal failure unhealthy until the event stream itself recovers',
    async () => {
      const store = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: `test-event-health-${Date.now()}-${crypto.randomUUID()}`,
        brokerId: 'event-health-runtime',
      });
      try {
        await store.initialize();
        const stream = store.events as unknown as DrainableEventStream;
        stream.reportDrainStatus(new Error('synthetic persistent journal failure'));
        expect(store.health()).toMatchObject({
          ok: false,
          error: 'synthetic persistent journal failure',
        });

        await (store as unknown as MaintainableStore).runMaintenance(async () => undefined);
        expect(store.health().ok).toBe(false);

        stream.reportDrainStatus(null);
        expect(store.health()).toMatchObject({ ok: true, error: null, since: null });
      } finally {
        await store.close();
      }
    }
  );

  test.skipIf(!postgresUrl)(
    'keeps one failed maintenance subsystem unhealthy across unrelated successes',
    async () => {
      const store = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: `test-maintenance-health-${Date.now()}-${crypto.randomUUID()}`,
        brokerId: 'maintenance-health-runtime',
      });
      try {
        await store.initialize();
        const runtime = store as unknown as MaintainableStore;
        await runtime.runMaintenance(async () => {
          throw new Error('synthetic persistent DLQ failure');
        }, 'dlq');
        expect(store.health()).toMatchObject({
          ok: false,
          error: 'synthetic persistent DLQ failure',
        });

        await runtime.runMaintenance(async () => undefined, 'heartbeat');
        expect(store.health()).toMatchObject({
          ok: false,
          error: 'synthetic persistent DLQ failure',
        });

        await runtime.runMaintenance(async () => undefined, 'dlq');
        expect(store.health()).toMatchObject({ ok: true, error: null, since: null });
      } finally {
        await store.close();
      }
    }
  );
});
