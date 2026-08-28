import { afterAll, describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import type { JobId } from '../src/domain/types/job';
import {
  cleanupPostgresNamespace,
  eventually,
  pausePostgresEventStream,
  postgresEventStream,
  postgresManagerStore,
} from './support/postgres-event-race';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

/** Drain the journal without scanning prune watermarks first. */
interface PartialDrainStream {
  drain(scanPruneWatermarks?: boolean): Promise<void>;
}

function namespace(label: string): string {
  const value = `test-event-partial-commit-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

function manager(value: string, brokerId: string, maxQueueEvents: number): PostgresQueueManager {
  return new PostgresQueueManager({
    maxQueueEvents,
    postgres: { url: postgresUrl!, namespace: value, brokerId, pollIntervalMs: 25 },
  });
}

function stateIds(value: PostgresQueueManager, queue: string, state: string): string[] {
  return value
    .getJobs(queue, { state })
    .map((job) => String(job.id))
    .sort();
}

function sortedIds(ids: readonly JobId[]): string[] {
  return ids.map(String).sort();
}

/** Count watermarks whose own commit pruned part of that same commit. */
async function selfPruningWatermarks(value: string, queue: string): Promise<number> {
  const sql = new SQL(postgresUrl!, { max: 1 });
  try {
    const rows = await sql<{ count: number | string | bigint }[]>`
      SELECT COUNT(*) AS count
      FROM bunqueue_event_prune_watermarks
      WHERE namespace = ${value} AND queue = ${queue}
        AND commit_seq IS NOT NULL
        AND pruned_commit_seq >= commit_seq
    `;
    return Number(rows[0]?.count ?? 0);
  } finally {
    await sql.close({ timeout: 5 });
  }
}

/** Read the durable self-pruned commit frontier carried per queue. */
async function carriedSelfPrunedFrontier(value: string, queue: string): Promise<number> {
  const sql = new SQL(postgresUrl!, { max: 1 });
  try {
    const rows = await sql<{ frontier: number | string | bigint | null }[]>`
      SELECT MAX(self_pruned_commit_seq) AS frontier
      FROM bunqueue_event_prune_watermarks
      WHERE namespace = ${value} AND queue = ${queue} AND commit_seq IS NOT NULL
    `;
    return Number(rows[0]?.frontier ?? 0);
  } finally {
    await sql.close({ timeout: 5 });
  }
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupPostgresNamespace(postgresUrl, value);
}, 30_000);

describe('PostgreSQL partially retained commits', () => {
  test.skipIf(!postgresUrl)(
    'invalidates a queue when retention prunes part of the commit the reader just applied',
    async () => {
      const value = namespace('completion');
      const queue = 'partial-commit';
      const batchSize = 14;
      const retention = 5;
      const active = manager(value, 'partial-active', retention);
      const remote = manager(value, 'partial-remote', retention);
      const invalidated: string[] = [];
      try {
        await Promise.all([active.waitUntilReady(), remote.waitUntilReady()]);
        const stream = await pausePostgresEventStream(remote);
        const partial = postgresEventStream(remote) as unknown as PartialDrainStream;
        postgresManagerStore(remote).onInvalidation((name) => {
          if (name === queue) invalidated.push(name);
        });

        const ids = await active.pushBatch(
          queue,
          Array.from({ length: batchSize }, (_, index) => ({ data: { index } }))
        );
        await stream.drain();
        expect(
          await eventually(() => stateIds(remote, queue, 'waiting').length === batchSize)
        ).toBe(true);

        const claims = await active.pullBatchWithLock(
          queue,
          batchSize,
          'partial-worker',
          0,
          60_000
        );
        await stream.drain();
        expect(await eventually(() => stateIds(remote, queue, 'active').length === batchSize)).toBe(
          true
        );

        await active.ackBatchWithResults(
          claims.jobs.map((job, index) => ({
            id: job.id,
            token: claims.tokens[index],
            result: { index },
          }))
        );
        // Precondition: the completion commit pruned its own older events, so
        // the retained window can never expose the whole commit to a reader.
        expect(await selfPruningWatermarks(value, queue)).toBeGreaterThan(0);

        // Apply the retained tail of that commit before any watermark scan.
        await partial.drain(false);
        expect(stateIds(remote, queue, 'completed').length).toBeLessThan(batchSize);

        // A later scan must still detect the partially consumed commit.
        await stream.drain();
        expect(
          await eventually(
            () => stateIds(remote, queue, 'completed').join() === sortedIds(ids).join(),
            5_000
          )
        ).toBe(true);
        expect(ids.map((id) => remote.getResult(id))).toEqual(ids.map((_, index) => ({ index })));

        // The watermark is accounted for once: repeated scans of an unchanged
        // frontier must not reload the queue read model on every poll.
        const settled = invalidated.length;
        for (let scan = 0; scan < 10; scan++) await stream.drain();
        expect(invalidated.length).toBe(settled);
      } finally {
        await Promise.allSettled([active.shutdownPostgres(), remote.shutdownPostgres()]);
      }
    },
    60_000
  );

  test.skipIf(!postgresUrl)(
    'repairs a self-pruning commit hidden by a later superseding watermark',
    async () => {
      const value = namespace('superseded');
      const queue = 'superseded-commit';
      const batchSize = 14;
      const retention = 5;
      const active = manager(value, 'superseded-active', retention);
      const remote = manager(value, 'superseded-remote', retention);
      try {
        await Promise.all([active.waitUntilReady(), remote.waitUntilReady()]);
        const stream = await pausePostgresEventStream(remote);
        const partial = postgresEventStream(remote) as unknown as PartialDrainStream;

        const ids = await active.pushBatch(
          queue,
          Array.from({ length: batchSize }, (_, index) => ({ data: { index } }))
        );
        await stream.drain();
        expect(
          await eventually(() => stateIds(remote, queue, 'waiting').length === batchSize)
        ).toBe(true);

        const claims = await active.pullBatchWithLock(
          queue,
          batchSize,
          'superseded-worker',
          0,
          60_000
        );
        await stream.drain();
        expect(await eventually(() => stateIds(remote, queue, 'active').length === batchSize)).toBe(
          true
        );

        // Commit C prunes its own completion events; commit D then prunes only
        // what C left behind and supersedes C's watermark row.
        await active.ackBatchWithResults(
          claims.jobs.map((job, index) => ({
            id: job.id,
            token: claims.tokens[index],
            result: { index },
          }))
        );
        await active.push(queue, { data: { followUp: true } });
        expect(await selfPruningWatermarks(value, queue)).toBe(0);
        expect(await carriedSelfPrunedFrontier(value, queue)).toBeGreaterThan(0);

        // The reader applies both commits before any watermark scan runs.
        await partial.drain(false);
        expect(stateIds(remote, queue, 'completed').length).toBeLessThan(batchSize);

        await stream.drain();
        expect(
          await eventually(
            () => stateIds(remote, queue, 'completed').join() === sortedIds(ids).join(),
            5_000
          )
        ).toBe(true);
      } finally {
        await Promise.allSettled([active.shutdownPostgres(), remote.shutdownPostgres()]);
      }
    },
    60_000
  );

  test.skipIf(!postgresUrl)(
    'repairs history pruned by a later commit before the reader consumed it',
    async () => {
      const value = namespace('cross-commit');
      const queue = 'cross-commit';
      const retention = 5;
      const pushes = 6;
      const active = manager(value, 'cross-active', retention);
      const remote = manager(value, 'cross-remote', retention);
      try {
        await Promise.all([active.waitUntilReady(), remote.waitUntilReady()]);
        const stream = await pausePostgresEventStream(remote);
        const partial = postgresEventStream(remote) as unknown as PartialDrainStream;

        // Every push is its own commit, so the prune removes an earlier commit's
        // event rather than part of the pruning commit itself.
        const ids: JobId[] = [];
        for (let index = 0; index < pushes; index++) {
          ids.push((await active.push(queue, { data: { index } })).id);
        }
        expect(await carriedSelfPrunedFrontier(value, queue)).toBe(0);

        await partial.drain(false);
        await stream.drain();
        expect(
          await eventually(
            () => stateIds(remote, queue, 'waiting').join() === sortedIds(ids).join(),
            5_000
          )
        ).toBe(true);
      } finally {
        await Promise.allSettled([active.shutdownPostgres(), remote.shutdownPostgres()]);
      }
    },
    60_000
  );
});
