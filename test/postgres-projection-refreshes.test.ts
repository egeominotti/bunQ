import { afterEach, describe, expect, test } from 'bun:test';
import { jobId, type JobId } from '../src/domain/types/job';
import {
  PostgresProjectionRefreshes,
  type PostgresJobProjection,
} from '../src/application/postgres-queue-manager/projectionRefreshes';
import { deferred, eventually } from './support/postgres-event-race';

const emptyProjection: PostgresJobProjection = { row: null, completion: null };
const active: PostgresProjectionRefreshes[] = [];

afterEach(async () => {
  for (const refreshes of active.splice(0)) {
    refreshes.close();
    await refreshes.drain();
  }
});

describe('PostgreSQL projection refresh lifecycle', () => {
  test('coalesces one event-loop wave into a single batch load', async () => {
    const batches: JobId[][] = [];
    const applied: JobId[] = [];
    const refreshes = new PostgresProjectionRefreshes(
      async (requests) => {
        batches.push(requests.map(({ id }) => id));
        return new Map(requests.map(({ id }) => [id, emptyProjection]));
      },
      (id) => applied.push(id),
      () => undefined,
      1
    );
    active.push(refreshes);
    const first = jobId('batch-first');
    const second = jobId('batch-second');

    refreshes.start();
    refreshes.request(first, 'queue');
    refreshes.request(second, 'queue');

    expect(await eventually(() => applied.length === 2)).toBe(true);
    expect(batches).toEqual([[first, second]]);
  });

  test('discards an in-flight projection superseded by a direct mutation', async () => {
    const entered = deferred<undefined>();
    const release = deferred<PostgresJobProjection>();
    const applied: PostgresJobProjection[] = [];
    const refreshes = new PostgresProjectionRefreshes(
      async (requests) => {
        entered.resolve(undefined);
        const projection = await release.promise;
        return new Map(requests.map(({ id }) => [id, projection]));
      },
      (_id, projection) => applied.push(projection),
      () => undefined,
      1
    );
    active.push(refreshes);
    const id = jobId('superseded');

    refreshes.start();
    refreshes.request(id, 'queue');
    await entered.promise;
    refreshes.supersede(id);
    release.resolve(emptyProjection);
    await refreshes.drain();

    expect(applied).toEqual([]);
  });

  test('discards in-flight projections superseded by an authoritative queue refresh', async () => {
    const entered = deferred<undefined>();
    const release = deferred<PostgresJobProjection>();
    const applied: JobId[] = [];
    const refreshes = new PostgresProjectionRefreshes(
      async (requests) => {
        entered.resolve(undefined);
        const projection = await release.promise;
        return new Map(requests.map(({ id }) => [id, projection]));
      },
      (id) => applied.push(id),
      () => undefined,
      1
    );
    active.push(refreshes);
    const stale = jobId('stale-queue-projection');
    const retained = jobId('retained-queue-projection');

    refreshes.start();
    refreshes.request(stale, 'obliterated');
    refreshes.request(retained, 'retained');
    await entered.promise;
    try {
      const supersedeQueue = Reflect.get(refreshes, 'supersedeQueue') as unknown;
      expect(supersedeQueue).toBeFunction();
      (supersedeQueue as (queue: string) => void).call(refreshes, 'obliterated');
    } finally {
      release.resolve(emptyProjection);
    }
    await refreshes.drain();

    expect(applied).toEqual([retained]);

    refreshes.request(stale, 'obliterated');
    expect(await eventually(() => applied.length === 2)).toBe(true);
    expect(applied).toEqual([retained, stale]);
  });

  test('retains a failed projection until a bounded retry succeeds', async () => {
    const reports: unknown[] = [];
    const applied: PostgresJobProjection[] = [];
    let attempts = 0;
    const refreshes = new PostgresProjectionRefreshes(
      async (requests) => {
        attempts++;
        if (attempts === 1) throw new Error('transient projection failure');
        return new Map(requests.map(({ id }) => [id, emptyProjection]));
      },
      (_id, projection) => applied.push(projection),
      (_queue, _id, error) => reports.push(error),
      1
    );
    active.push(refreshes);

    refreshes.start();
    refreshes.request(jobId('retry'), 'queue');

    expect(await eventually(() => applied.length === 1)).toBe(true);
    expect(attempts).toBe(2);
    expect(reports[0]).toBeInstanceOf(Error);
    expect(reports.at(-1)).toBeNull();
  });

  test('stops admission and drains an already-running loader on close', async () => {
    const entered = deferred<undefined>();
    const release = deferred<PostgresJobProjection>();
    let attempts = 0;
    const refreshes = new PostgresProjectionRefreshes(
      async (requests) => {
        attempts++;
        entered.resolve(undefined);
        const projection = await release.promise;
        return new Map(requests.map(({ id }) => [id, projection]));
      },
      () => undefined,
      () => undefined,
      1
    );
    active.push(refreshes);

    refreshes.start();
    refreshes.request(jobId('running'), 'queue');
    await entered.promise;
    refreshes.close();
    let drained = false;
    const drain = refreshes.drain().then(() => {
      drained = true;
    });
    await Bun.sleep(10);
    expect(drained).toBe(false);
    refreshes.request(jobId('late'), 'queue');
    release.resolve(emptyProjection);
    await drain;

    expect(drained).toBe(true);
    expect(attempts).toBe(1);
  });

  test('releases generation fences after unique refreshes and local mutations settle', async () => {
    let applied = 0;
    const batchSizes: number[] = [];
    const refreshes = new PostgresProjectionRefreshes(
      async (requests) => {
        batchSizes.push(requests.length);
        return new Map(requests.map(({ id }) => [id, emptyProjection]));
      },
      () => applied++,
      () => undefined,
      1
    );
    active.push(refreshes);
    refreshes.start();

    for (let index = 0; index < 10_000; index++) {
      refreshes.request(jobId(`refreshed-${index}`), 'queue');
    }

    expect(await eventually(() => applied === 10_000)).toBe(true);
    expect(batchSizes).toHaveLength(10);
    expect(Math.max(...batchSizes)).toBe(1_000);
    const generations = Reflect.get(refreshes, 'generations') as Map<JobId, symbol>;
    const generationQueues = Reflect.get(refreshes, 'generationQueues') as Map<JobId, string>;
    expect(generations.size).toBe(0);
    expect(generationQueues.size).toBe(0);

    for (let index = 0; index < 10_000; index++) {
      refreshes.supersede(jobId(`settled-${index}`));
    }

    expect(generations.size).toBe(0);
    expect(generationQueues.size).toBe(0);
  });
});
