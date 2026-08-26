import { afterAll, describe, expect, test } from 'bun:test';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { jobId, type JobId } from '../src/domain/types/job';
import type {
  PostgresQueueStore,
  PostgresStoredJob,
} from '../src/infrastructure/persistence/postgres';
import {
  cleanupPostgresNamespace,
  deferred,
  eventually,
  postgresManagerStore,
} from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

interface DelayedGetJob {
  readonly captured: Promise<PostgresStoredJob | null>;
  release(): void;
  restore(): void;
}

function namespace(label: string): string {
  const value = `test-single-refresh-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function delayNextGetJob(store: PostgresQueueStore): DelayedGetJob {
  const original = store.loadJobProjections;
  const captured = deferred<PostgresStoredJob | null>();
  const release = deferred<undefined>();
  let armed = true;
  store.loadJobProjections = async (...args) => {
    const projections = await original(...args);
    const row = projections.get(args[0][0].id)?.row ?? null;
    if (armed) {
      armed = false;
      captured.resolve(row);
      await release.promise;
    }
    return projections;
  };
  return {
    captured: captured.promise,
    release: () => release.resolve(undefined),
    restore: () => {
      store.loadJobProjections = original;
    },
  };
}

function hasState(manager: PostgresQueueManager, queue: string, id: JobId, state: string): boolean {
  return manager.getJobs(queue, { state }).some((job) => job.id === id);
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

describe('PostgreSQL single-job refresh fencing', () => {
  test.skipIf(!postgresUrl)(
    'does not let refreshJob overwrite a newer completion event',
    async () => {
      const value = namespace('refresh-helper');
      const queue = 'refresh-helper';
      const first = manager(value, 'refresh-helper-a');
      const second = manager(value, 'refresh-helper-b');
      let delayed: DelayedGetJob | null = null;

      try {
        await Promise.all([first.waitUntilReady(), second.waitUntilReady()]);
        const job = await first.push(queue, { data: { marker: 'refresh-helper' } });
        const claim = await first.pullWithLock(queue, 'refresh-worker');
        expect(claim.job?.id).toBe(job.id);
        delayed = delayNextGetJob(postgresManagerStore(first));

        const updating = first.updateProgress(job.id, 10, 'stale-progress');
        expect((await delayed.captured)?.state).toBe('active');
        await second.ack(job.id, { done: true }, claim.token ?? undefined);
        expect(await eventually(() => hasState(first, queue, job.id, 'completed'))).toBe(true);

        delayed.release();
        expect(await updating).toBe(true);
        delayed.restore();
        expect(await postgresManagerStore(first).getJob(job.id)).toMatchObject({
          state: 'completed',
        });
        expect(hasState(first, queue, job.id, 'active')).toBe(false);
        expect(hasState(first, queue, job.id, 'completed')).toBe(true);
        expect(first.getProgress(job.id)).toBeNull();
      } finally {
        delayed?.release();
        delayed?.restore();
        await Promise.allSettled([first.shutdownPostgres(), second.shutdownPostgres()]);
      }
    }
  );

  test.skipIf(!postgresUrl)('does not let getJob downgrade a newer completion event', async () => {
    const value = namespace('get-job');
    const queue = 'get-job';
    const first = manager(value, 'get-job-a');
    const second = manager(value, 'get-job-b');
    let delayed: DelayedGetJob | null = null;

    try {
      await Promise.all([first.waitUntilReady(), second.waitUntilReady()]);
      const job = await first.push(queue, { data: { marker: 'get-job' } });
      const claim = await first.pullWithLock(queue, 'get-job-worker');
      delayed = delayNextGetJob(postgresManagerStore(first));

      const querying = first.getJob(job.id);
      expect((await delayed.captured)?.state).toBe('active');
      await second.ack(job.id, { done: true }, claim.token ?? undefined);
      expect(await eventually(() => hasState(first, queue, job.id, 'completed'))).toBe(true);

      delayed.release();
      await querying;
      delayed.restore();
      expect(hasState(first, queue, job.id, 'active')).toBe(false);
      expect(hasState(first, queue, job.id, 'completed')).toBe(true);
    } finally {
      delayed?.release();
      delayed?.restore();
      await Promise.allSettled([first.shutdownPostgres(), second.shutdownPostgres()]);
    }
  });

  test.skipIf(!postgresUrl)(
    'does not let a stale null getJobState erase a pushed event',
    async () => {
      const value = namespace('get-state-null');
      const queue = 'get-state-null';
      const first = manager(value, 'get-state-null-a');
      const second = manager(value, 'get-state-null-b');
      let delayed: DelayedGetJob | null = null;

      try {
        await Promise.all([first.waitUntilReady(), second.waitUntilReady()]);
        const customId = `late-${crypto.randomUUID()}`;
        const id = jobId(customId);
        delayed = delayNextGetJob(postgresManagerStore(first));

        const querying = first.getJobState(id);
        expect(await delayed.captured).toBeNull();
        const pushed = await second.push(queue, { data: { marker: 'late' }, customId });
        expect(pushed.id).toBe(id);
        expect(await eventually(() => hasState(first, queue, id, 'waiting'))).toBe(true);

        delayed.release();
        await querying;
        delayed.restore();
        expect(hasState(first, queue, id, 'waiting')).toBe(true);
      } finally {
        delayed?.release();
        delayed?.restore();
        await Promise.allSettled([first.shutdownPostgres(), second.shutdownPostgres()]);
      }
    }
  );
});
