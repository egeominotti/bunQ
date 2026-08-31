import { afterAll, describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import { createJob, generateJobId, jobId } from '../src/domain/types/job';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(): string {
  const value = `test-groups-pro-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

async function cleanup(url: string, value: string): Promise<void> {
  const sql = new SQL(url, { max: 2 });
  try {
    for (const table of [
      'bunqueue_job_logs',
      'bunqueue_flow_failures',
      'bunqueue_dependencies',
      'bunqueue_completions',
      'bunqueue_jobs',
      'bunqueue_group_state',
      'bunqueue_queue_state',
      'bunqueue_event_prune_watermarks',
      'bunqueue_events',
      'bunqueue_event_commits',
      'bunqueue_brokers',
    ]) {
      await sql.unsafe(`DELETE FROM ${table} WHERE namespace = $1`, [value]);
    }
  } finally {
    await sql.close({ timeout: 5 });
  }
}

function job(queue: string, label: string, groupId?: string) {
  return createJob(generateJobId(), queue, {
    data: { label },
    groupId,
  });
}

function orderedJob(queue: string, label: string, id: string, timestamp: number) {
  return createJob(jobId(id), queue, {
    data: { label },
    groupId: 'A',
    timestamp,
  });
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanup(postgresUrl, value);
});

describe('PostgreSQL BullMQ Pro compatible groups', () => {
  test.skipIf(!postgresUrl)(
    'preserves insertion FIFO when custom IDs sort in the opposite order',
    async () => {
      const value = namespace();
      const store = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'fifo',
      });
      try {
        await store.initialize();
        const timestamp = Date.now();
        await store.insertMany([
          orderedJob('fifo', 'first', 'z-first', timestamp),
          orderedJob('fifo', 'second', 'a-second', timestamp),
        ]);

        const claimed = await store.claim('fifo', 2);
        expect(claimed.map(({ job: value }) => (value.data as { label: string }).label)).toEqual([
          'first',
          'second',
        ]);
      } finally {
        await store.close();
      }
    },
    30_000
  );

  test.skipIf(!postgresUrl)(
    'removes policyless group scheduler rows after the group becomes inactive',
    async () => {
      const value = namespace();
      const store = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'retention',
      });
      const sql = new SQL(postgresUrl!, { max: 1 });
      try {
        await store.initialize();
        await store.insertMany([
          job('retention', 'A1', 'A'),
          job('retention', 'B1', 'B'),
          job('retention', 'C1', 'C'),
        ]);
        const claimed = await store.claim('retention', 3);
        await Promise.all(claimed.map((entry) => store.complete(entry.job.id, entry.token)));

        const [state] = await sql<{ count: number | string | bigint }[]>`
          SELECT COUNT(*)::bigint AS count
          FROM bunqueue_group_state
          WHERE namespace = ${value} AND queue = 'retention'
        `;
        expect(Number(state.count)).toBe(0);
      } finally {
        await Promise.allSettled([store.close(), sql.close({ timeout: 5 })]);
      }
    },
    30_000
  );

  test.skipIf(!postgresUrl)(
    'rejects non-integer group controls instead of letting PostgreSQL round them',
    async () => {
      const value = namespace();
      const store = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'validation',
      });
      try {
        await store.initialize();

        await expect(store.setGroupRateLimit('validation', 'A', 1.5, 60_000)).rejects.toThrow(
          'max must be a positive safe integer'
        );
        await expect(store.setGroupRateLimit('validation', 'A', 1, 1.5)).rejects.toThrow(
          'duration must be a positive safe integer'
        );
        await expect(store.setGroupConcurrency('validation', 'A', 1.5)).rejects.toThrow(
          'concurrency must be a positive safe integer'
        );
        await expect(store.setGroupConcurrency('validation', 'A\0B', 1)).rejects.toThrow(
          /groupId/i
        );
        await expect(
          store.claim('validation', 1, 'validation', undefined, { concurrency: 1.5 })
        ).rejects.toThrow('group.concurrency must be a positive safe integer');

        expect(await store.getGroupRateLimit('validation', 'A')).toBeNull();
        expect(await store.getGroupConcurrency('validation', 'A')).toBeNull();

        await store.setGroupRateLimit('validation', 'A', 3, 60_000);
        await store.setGroupConcurrency('validation', 'A', 2);
        await expect(store.setGroupRateLimit('validation', 'A', 1.5, 1)).rejects.toThrow();
        await expect(store.setGroupConcurrency('validation', 'A', 1.5)).rejects.toThrow();
        expect(await store.getGroupRateLimit('validation', 'A')).toEqual({
          max: 3,
          duration: 60_000,
        });
        expect(await store.getGroupConcurrency('validation', 'A')).toBe(2);
      } finally {
        await store.close();
      }
    },
    30_000
  );

  test.skipIf(!postgresUrl)(
    'persists the documented safe-integer range for group controls',
    async () => {
      const value = namespace();
      const store = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'safe-integers',
      });
      try {
        await store.initialize();
        await store.setGroupRateLimit('safe-integers', 'A', Number.MAX_SAFE_INTEGER, 60_000);
        await store.setGroupConcurrency('safe-integers', 'A', Number.MAX_SAFE_INTEGER);

        expect(await store.getGroupRateLimit('safe-integers', 'A')).toEqual({
          max: Number.MAX_SAFE_INTEGER,
          duration: 60_000,
        });
        expect(await store.getGroupConcurrency('safe-integers', 'A')).toBe(Number.MAX_SAFE_INTEGER);

        await store.insert(job('safe-integers', 'A1', 'A'));
        const [claim] = await store.claim('safe-integers', 1, 'safe-integers', undefined, {
          concurrency: Number.MAX_SAFE_INTEGER,
          limit: { max: Number.MAX_SAFE_INTEGER, duration: 60_000 },
        });
        expect((claim.job.data as { label: string }).label).toBe('A1');
        await store.complete(claim.job.id, claim.token);
      } finally {
        await store.close();
      }
    },
    30_000
  );

  test.skipIf(!postgresUrl)(
    'shares round-robin, depth, concurrency and rate budgets across brokers',
    async () => {
      const value = namespace();
      const a = new PostgresQueueStore({ url: postgresUrl!, namespace: value, brokerId: 'a' });
      const b = new PostgresQueueStore({ url: postgresUrl!, namespace: value, brokerId: 'b' });
      try {
        await Promise.all([a.initialize(), b.initialize()]);

        await a.insertMany([
          job('round-robin', 'plain'),
          job('round-robin', 'A1', 'A'),
          job('round-robin', 'A2', 'A'),
          job('round-robin', 'B1', 'B'),
          job('round-robin', 'B2', 'B'),
        ]);
        expect(await b.getGroupJobsCount('round-robin', 'A')).toBe(2);
        expect(await b.getGroupJobsCount('round-robin')).toBe(4);
        const ordered = await b.claim('round-robin', 5);
        expect(ordered.map(({ job: value }) => (value.data as { label: string }).label)).toEqual([
          'plain',
          'A1',
          'B1',
          'A2',
          'B2',
        ]);
        await Promise.all(ordered.map((claim) => a.complete(claim.job.id, claim.token)));

        await a.insertMany([
          job('concurrency', 'A1', 'A'),
          job('concurrency', 'A2', 'A'),
          job('concurrency', 'B1', 'B'),
          job('concurrency', 'B2', 'B'),
        ]);
        const groupConcurrency = { concurrency: 1 };
        const [left, right] = await Promise.all([
          a.claim('concurrency', 10, 'a', undefined, groupConcurrency),
          b.claim('concurrency', 10, 'b', undefined, groupConcurrency),
        ]);
        const firstWave = [...left, ...right];
        expect(
          firstWave
            .map(({ job: value }) => value.groupId)
            .sort((left, right) => String(left).localeCompare(String(right)))
        ).toEqual(['A', 'B']);
        expect(await a.getGroupActiveCount('concurrency', 'A')).toBe(1);
        await Promise.all(firstWave.map((claim) => a.complete(claim.job.id, claim.token)));
        const secondWave = await b.claim('concurrency', 10, 'b', undefined, groupConcurrency);
        expect(
          secondWave
            .map(({ job: value }) => value.groupId)
            .sort((left, right) => String(left).localeCompare(String(right)))
        ).toEqual(['A', 'B']);
        await Promise.all(secondWave.map((claim) => b.complete(claim.job.id, claim.token)));

        await a.setGroupRateLimit('rate', 'A', 1, 60_000);
        await a.insertMany([
          job('rate', 'A1', 'A'),
          job('rate', 'A2', 'A'),
          job('rate', 'B1', 'B'),
          job('rate', 'B2', 'B'),
          job('rate', 'B3', 'B'),
        ]);
        const groupRate = { limit: { max: 2, duration: 60_000 } };
        const admitted = await b.claim('rate', 10, 'b', undefined, groupRate);
        expect(admitted.map(({ job: value }) => (value.data as { label: string }).label)).toEqual([
          'A1',
          'B1',
          'B2',
        ]);
        expect(await a.getGroupRateLimit('rate', 'A')).toEqual({ max: 1, duration: 60_000 });
        expect(await b.getGroupRateLimitTtl('rate', 'A', 1)).toBeGreaterThan(0);
        expect(await a.claim('rate', 10, 'a', undefined, groupRate)).toHaveLength(0);
      } finally {
        await Promise.allSettled([a.close(), b.close()]);
      }
    },
    30_000
  );
});
