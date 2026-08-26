import { afterAll, describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import { PostgresQueueManager } from '../src/application/postgresQueueManager';
import { TcpClient } from '../src/client/tcp/client';
import { createJob, generateJobId } from '../src/domain/types/job';
import { PostgresQueueStore } from '../src/infrastructure/persistence/postgres';
import { createTcpServer } from '../src/infrastructure/server/tcp';
import { expectedPostgresVersionPrefix } from './support/postgres-version';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const namespaces: string[] = [];

function namespace(label: string): string {
  const value = `test-${label}-${Date.now()}-${crypto.randomUUID()}`;
  namespaces.push(value);
  return value;
}

async function cleanupNamespace(url: string, value: string): Promise<void> {
  const sql = new SQL(url, { max: 2 });
  try {
    await sql.begin(async (tx) => {
      await tx`DELETE FROM bunqueue_metric_buckets WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_metric_totals WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_workers WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_crons WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_job_logs WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_repeat_links WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_flow_failures WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_dependencies WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_completions WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_jobs WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_queue_state WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_event_prune_watermarks WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_events WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_event_commits WHERE namespace = ${value}`;
      await tx`DELETE FROM bunqueue_brokers WHERE namespace = ${value}`;
    });
  } finally {
    await sql.close({ timeout: 5 });
  }
}

afterAll(async () => {
  if (!postgresUrl) return;
  for (const value of namespaces) await cleanupNamespace(postgresUrl, value);
});

describe(`PostgreSQL ${expectedPostgresVersionPrefix} multi-broker persistence`, () => {
  test.skipIf(!postgresUrl)(
    'does not rerun schema DDL while an initialized broker is serving traffic',
    async () => {
      const value = namespace('schema-reuse');
      const first = new PostgresQueueManager({
        postgres: {
          url: postgresUrl!,
          namespace: value,
          brokerId: 'schema-first',
        },
      });
      const blocker = new SQL(postgresUrl!, { max: 1 });
      const lockAcquired = Promise.withResolvers<undefined>();
      const releaseLock = Promise.withResolvers<undefined>();
      let second: PostgresQueueManager | null = null;
      let holdingTransaction: Promise<void> | null = null;
      try {
        await first.waitUntilReady();
        holdingTransaction = blocker.begin(async (tx) => {
          await tx`LOCK TABLE bunqueue_jobs IN ROW EXCLUSIVE MODE`;
          lockAcquired.resolve(undefined);
          await releaseLock.promise;
        });
        await lockAcquired.promise;
        second = new PostgresQueueManager({
          postgres: {
            url: postgresUrl!,
            namespace: value,
            brokerId: 'schema-second',
          },
        });
        const initializedWithoutDdlLock = await Promise.race([
          second.waitUntilReady().then(() => true),
          Bun.sleep(250).then(() => false),
        ]);
        expect(initializedWithoutDdlLock).toBe(true);
      } finally {
        releaseLock.resolve(undefined);
        await holdingTransaction;
        await Promise.allSettled([
          first.shutdownPostgres(),
          second?.shutdownPostgres() ?? Promise.resolve(),
        ]);
        await blocker.close({ timeout: 5 });
      }
    }
  );

  test.skipIf(!postgresUrl)(
    `uses PostgreSQL ${expectedPostgresVersionPrefix} and claims each job exactly once`,
    async () => {
      const value = namespace('claims');
      const a = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'claims-a',
      });
      const b = new PostgresQueueStore({
        url: postgresUrl!,
        namespace: value,
        brokerId: 'claims-b',
      });
      try {
        await Promise.all([a.initialize(), b.initialize()]);
        const [version] = await a.context.sql<{ server_version: string }[]>`SHOW server_version`;
        expect(version.server_version).toStartWith(expectedPostgresVersionPrefix);
        await a.insertMany(
          Array.from({ length: 200 }, (_, index) =>
            createJob(generateJobId(), 'claims', { data: { index } })
          )
        );

        const [first, second] = await Promise.all([a.claim('claims', 120), b.claim('claims', 120)]);
        const claimed = [...first, ...second];
        expect(first.length).toBeGreaterThan(0);
        expect(second.length).toBeGreaterThan(0);
        expect(claimed).toHaveLength(200);
        expect(new Set(claimed.map(({ job }) => job.id)).size).toBe(200);

        await Promise.all(
          claimed.map(({ job, token }, index) =>
            (index % 2 === 0 ? a : b).complete(job.id, token, { index })
          )
        );
        expect(await a.getCounts('claims')).toEqual({
          waiting: 0,
          prioritized: 0,
          delayed: 0,
          active: 0,
          completed: 200,
          failed: 0,
          waitingChildren: 0,
        });
      } finally {
        await Promise.allSettled([a.close(), b.close()]);
      }
    },
    15_000
  );

  test.skipIf(!postgresUrl)(
    'keeps high-contention shared-lock claims exclusively delivered',
    async () => {
      const value = namespace('contended-claims');
      const stores = ['contended-a', 'contended-b'].map(
        (brokerId) =>
          new PostgresQueueStore({
            url: postgresUrl!,
            namespace: value,
            brokerId,
            poolSize: 24,
            maxQueueEvents: 32,
          })
      );
      const totalJobs = 5_000;
      let stopped = false;
      try {
        await Promise.all(stores.map((store) => store.initialize()));
        await stores[0].insertMany(
          Array.from({ length: totalJobs }, (_, index) =>
            createJob(generateJobId(), 'contended-claims', { data: { index } })
          )
        );

        const claimedIds = new Set<string>();
        const consumers = stores.flatMap((store, storeIndex) =>
          Array.from({ length: 8 }, (_, consumerIndex) =>
            (async () => {
              try {
                while (!stopped && claimedIds.size < totalJobs) {
                  const claims = await store.claim(
                    'contended-claims',
                    50,
                    `consumer-${storeIndex}-${consumerIndex}`
                  );
                  if (claims.length === 0) {
                    await Bun.sleep(1);
                    continue;
                  }
                  for (const { job } of claims) {
                    if (claimedIds.has(String(job.id))) {
                      throw new Error(`Duplicate high-contention claim for ${String(job.id)}`);
                    }
                    claimedIds.add(String(job.id));
                  }
                  await store.completeMany(
                    claims.map(({ job, token }) => ({ id: job.id, token, result: null }))
                  );
                }
              } catch (error) {
                stopped = true;
                throw error;
              }
            })()
          )
        );
        await Promise.all(consumers);

        expect(claimedIds.size).toBe(totalJobs);
        expect(await stores[0].getCounts('contended-claims')).toMatchObject({
          active: 0,
          completed: totalJobs,
          waiting: 0,
        });
        const deadline = Date.now() + 5_000;
        let retainedEvents = -1;
        do {
          const [events] = await stores[0].context.sql<{ count: number | string | bigint }[]>`
            SELECT COUNT(*) AS count FROM bunqueue_events
            WHERE namespace = ${value} AND queue = 'contended-claims'
          `;
          retainedEvents = Number(events.count);
          if (retainedEvents === 32) break;
          await Bun.sleep(10);
        } while (Date.now() < deadline);
        expect(retainedEvents).toBe(32);
      } finally {
        stopped = true;
        await Promise.allSettled(stores.map((store) => store.close()));
      }
    },
    30_000
  );

  test.skipIf(!postgresUrl)('fences an expired owner and safely recovers its lease', async () => {
    const value = namespace('fencing');
    const a = new PostgresQueueStore({
      url: postgresUrl!,
      namespace: value,
      brokerId: 'fencing-a',
      leaseDurationMs: 1000,
    });
    const b = new PostgresQueueStore({
      url: postgresUrl!,
      namespace: value,
      brokerId: 'fencing-b',
      leaseDurationMs: 1000,
    });
    try {
      await Promise.all([a.initialize(), b.initialize()]);
      const admitted = await a.insert(
        createJob(generateJobId(), 'fencing', { data: {}, maxAttempts: 3, backoff: 0 })
      );
      const first = (await a.claim('fencing', 1))[0];
      expect(first).toBeDefined();
      await Bun.sleep(1050);
      expect((await a.complete(first.job.id, first.token)).applied).toBe(false);
      await b.recoverExpired();

      const second = (await b.claim('fencing', 1))[0];
      expect(second.job.id).toBe(admitted.job.id);
      expect(second.token).not.toBe(first.token);
      expect((await a.complete(first.job.id, first.token)).applied).toBe(false);
      expect((await b.complete(second.job.id, second.token)).applied).toBe(true);
    } finally {
      await Promise.allSettled([a.close(), b.close()]);
    }
  });

  test.skipIf(!postgresUrl)('runs two independent TCP brokers against one namespace', async () => {
    const value = namespace('tcp');
    const a = new PostgresQueueManager({
      postgres: {
        url: postgresUrl!,
        namespace: value,
        brokerId: 'tcp-a',
        leaseDurationMs: 5000,
        pollIntervalMs: 25,
      },
    });
    const b = new PostgresQueueManager({
      postgres: {
        url: postgresUrl!,
        namespace: value,
        brokerId: 'tcp-b',
        leaseDurationMs: 5000,
        pollIntervalMs: 25,
      },
    });
    await Promise.all([a.waitUntilReady(), b.waitUntilReady()]);
    const serverA = createTcpServer(a, { hostname: '127.0.0.1', port: 0 });
    const serverB = createTcpServer(b, { hostname: '127.0.0.1', port: 0 });
    const clientA = new TcpClient({
      host: '127.0.0.1',
      port: serverA.server.port,
      autoReconnect: false,
      pingInterval: 0,
      commandTimeout: 5000,
    });
    const clientB = new TcpClient({
      host: '127.0.0.1',
      port: serverB.server.port,
      autoReconnect: false,
      pingInterval: 0,
      commandTimeout: 5000,
    });
    try {
      await Promise.all([clientA.connect(), clientB.connect()]);
      const pushed = await clientA.send({
        cmd: 'PUSHB',
        queue: 'tcp',
        jobs: Array.from({ length: 100 }, (_, index) => ({ data: { index } })),
      });
      expect(pushed.ok).toBe(true);
      expect(pushed.ids).toHaveLength(100);

      const [responseA, responseB] = await Promise.all([
        clientA.send({ cmd: 'PULLB', queue: 'tcp', count: 60, owner: 'worker-a' }),
        clientB.send({ cmd: 'PULLB', queue: 'tcp', count: 60, owner: 'worker-b' }),
      ]);
      const jobsA = responseA.jobs as Array<{ id: string }>;
      const jobsB = responseB.jobs as Array<{ id: string }>;
      const allIds = [...jobsA, ...jobsB].map((job) => job.id);
      expect(jobsA.length).toBeGreaterThan(0);
      expect(jobsB.length).toBeGreaterThan(0);
      expect(allIds).toHaveLength(100);
      expect(new Set(allIds).size).toBe(100);

      const [ackA, ackB] = await Promise.all([
        clientA.send({ cmd: 'ACKB', ids: jobsA.map((job) => job.id), tokens: responseA.tokens }),
        clientB.send({ cmd: 'ACKB', ids: jobsB.map((job) => job.id), tokens: responseB.tokens }),
      ]);
      expect(ackA.ok).toBe(true);
      expect(ackB.ok).toBe(true);
      await Bun.sleep(200);
      expect(a.getQueueJobCounts('tcp')).toMatchObject({ active: 0, completed: 100 });

      await clientA.send({ cmd: 'PUSH', queue: 'tcp-control', data: { controlled: true } });
      expect((await clientA.send({ cmd: 'Pause', queue: 'tcp-control' })).ok).toBe(true);
      const paused = await clientB.send({
        cmd: 'PULL',
        queue: 'tcp-control',
        owner: 'worker-b',
      });
      expect(paused.job).toBeNull();
      expect((await clientB.send({ cmd: 'Resume', queue: 'tcp-control' })).ok).toBe(true);
      const resumed = await clientB.send({
        cmd: 'PULL',
        queue: 'tcp-control',
        owner: 'worker-b',
        lockTtl: 1000,
      });
      const resumedJob = resumed.job as { id: string };
      expect(resumedJob).toBeDefined();
      expect(
        (
          await clientB.send({
            cmd: 'JobHeartbeat',
            id: resumedJob.id,
            token: resumed.token,
            duration: 2000,
          })
        ).ok
      ).toBe(true);
      await Bun.sleep(1050);
      expect(
        (
          await clientB.send({
            cmd: 'ACK',
            id: resumedJob.id,
            token: resumed.token,
          })
        ).ok
      ).toBe(true);
    } finally {
      clientA.close();
      clientB.close();
      serverA.stop();
      serverB.stop();
      await Promise.allSettled([a.shutdownPostgres(), b.shutdownPostgres()]);
    }
  });
});
