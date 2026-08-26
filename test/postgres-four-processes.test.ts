import { describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import type { TcpClient } from '../src/client/tcp/client';
import {
  cleanupPostgresNamespace,
  crashPostgresProcessBroker,
  startPostgresProcessCluster,
  stopPostgresProcessCluster,
  type PostgresProcessBroker,
} from './support/postgres-process-cluster';
import {
  ackOnOtherBrokers,
  claimFromEveryBroker,
  consumeExactlyOnce,
  expectCompletedEverywhere,
  expectDatabaseCompleted,
  pullClaims,
  pushAcrossBrokers,
  waitFor,
} from './support/postgres-process-workload';
import { expectedPostgresVersionPrefix } from './support/postgres-version';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;

describe(`PostgreSQL ${expectedPostgresVersionPrefix} four-process topology`, () => {
  test.skipIf(!postgresUrl)(
    'runs four independent brokers against one database and survives a broker crash',
    async () => {
      const namespace = `test-four-processes-${Date.now()}-${crypto.randomUUID()}`;
      const sql = new SQL(postgresUrl!, { max: 4 });
      let brokers: PostgresProcessBroker[] = [];
      try {
        brokers = await startPostgresProcessCluster(postgresUrl!, namespace, 4);
        const clients = brokers.map(({ client }) => client);
        expect(new Set(brokers.map(({ process }) => process.pid)).size).toBe(4);
        expect(brokers.every(({ process }) => process.exitCode === null)).toBe(true);

        const [version] = await sql<{ version: string }[]>`
          SELECT current_setting('server_version') AS version
        `;
        expect(version.version).toStartWith(expectedPostgresVersionPrefix);
        const registered = await sql<{ broker_id: string }[]>`
          SELECT broker_id FROM bunqueue_brokers
          WHERE namespace = ${namespace}
          ORDER BY broker_id
        `;
        expect(registered.map((row) => row.broker_id)).toEqual([
          'process-1',
          'process-2',
          'process-3',
          'process-4',
        ]);
        for (const status of await Promise.all(
          clients.map((client) => client.send({ cmd: 'StorageStatus' }))
        )) {
          expect(status).toMatchObject({
            ok: true,
            data: { diskFull: false, error: null, since: null },
          });
        }

        const sharedIds = await pushAcrossBrokers(clients, 'four-shared', 1000);
        const sharedSeen = await consumeExactlyOnce(clients, 'four-shared', 1000);
        expect([...sharedSeen].sort()).toEqual([...sharedIds].sort());
        await expectCompletedEverywhere(clients, 'four-shared', 1000);
        await expectDatabaseCompleted(sql, namespace, 'four-shared', 1000);

        await exerciseGlobalPolicies(clients);
        await exercisePauseResume(clients);
        await exerciseCrashRecovery(sql, namespace, brokers);
      } finally {
        await stopPostgresProcessCluster(brokers);
        await sql.close({ timeout: 5 });
        await cleanupPostgresNamespace(postgresUrl!, namespace);
      }
    },
    90_000
  );
});

async function exerciseGlobalPolicies(clients: readonly TcpClient[]): Promise<void> {
  const concurrencyQueue = 'four-concurrency';
  expect(
    (await clients[0].send({ cmd: 'SetConcurrency', queue: concurrencyQueue, limit: 8 })).ok
  ).toBe(true);
  const concurrencyIds = await pushAcrossBrokers(clients, concurrencyQueue, 40);
  const limitedClaims = await claimFromEveryBroker(clients, concurrencyQueue, 40);
  expect(limitedClaims.flatMap(({ ids }) => ids)).toHaveLength(8);
  expect(new Set(limitedClaims.flatMap(({ ids }) => ids)).size).toBe(8);
  await ackOnOtherBrokers(clients, limitedClaims);
  expect((await clients[2].send({ cmd: 'ClearConcurrency', queue: concurrencyQueue })).ok).toBe(
    true
  );
  const remainingConcurrency = await consumeExactlyOnce(clients, concurrencyQueue, 32);
  expect(new Set([...limitedClaims.flatMap(({ ids }) => ids), ...remainingConcurrency]).size).toBe(
    concurrencyIds.length
  );
  await expectCompletedEverywhere(clients, concurrencyQueue, 40);

  const rateQueue = 'four-rate-limit';
  expect(
    (
      await clients[1].send({
        cmd: 'RateLimit',
        queue: rateQueue,
        limit: 12,
        duration: 60_000,
      })
    ).ok
  ).toBe(true);
  const rateIds = await pushAcrossBrokers(clients, rateQueue, 40);
  const rateClaims = await claimFromEveryBroker(clients, rateQueue, 40);
  expect(rateClaims.flatMap(({ ids }) => ids)).toHaveLength(12);
  expect(new Set(rateClaims.flatMap(({ ids }) => ids)).size).toBe(12);
  await ackOnOtherBrokers(clients, rateClaims);
  expect((await clients[3].send({ cmd: 'RateLimitClear', queue: rateQueue })).ok).toBe(true);
  const remainingRate = await consumeExactlyOnce(clients, rateQueue, 28);
  expect(new Set([...rateClaims.flatMap(({ ids }) => ids), ...remainingRate]).size).toBe(
    rateIds.length
  );
  await expectCompletedEverywhere(clients, rateQueue, 40);
}

async function exercisePauseResume(clients: readonly TcpClient[]): Promise<void> {
  const queue = 'four-paused';
  await pushAcrossBrokers(clients, queue, 4);
  expect((await clients[0].send({ cmd: 'Pause', queue })).ok).toBe(true);
  const paused = await Promise.all(
    clients.map((client, index) =>
      client.send({ cmd: 'PULL', queue, owner: `paused-${index}`, lockTtl: 5000 })
    )
  );
  expect(paused.every((response) => response.job === null)).toBe(true);
  expect((await clients[3].send({ cmd: 'Resume', queue })).ok).toBe(true);
  expect((await consumeExactlyOnce(clients, queue, 4)).size).toBe(4);
  await expectCompletedEverywhere(clients, queue, 4);
}

async function exerciseCrashRecovery(
  sql: SQL,
  namespace: string,
  brokers: readonly PostgresProcessBroker[]
): Promise<void> {
  const queue = 'four-crash-recovery';
  const clients = brokers.map(({ client }) => client);
  const pushedIds = await pushAcrossBrokers(clients, queue, 120, {
    backoff: 0,
    maxAttempts: 3,
  });
  const held = await pullClaims(clients[0], 0, queue, 24, 1000);
  expect(held.ids).toHaveLength(24);
  await crashPostgresProcessBroker(brokers[0]);
  const survivors = brokers.slice(1).map(({ client }) => client);

  await waitFor(async () => {
    const [row] = await sql<{ active: number }[]>`
      SELECT COUNT(*)::int AS active FROM bunqueue_jobs
      WHERE namespace = ${namespace}
        AND id = ANY(${sql.array(held.ids, 'TEXT')})
        AND state = 'active'
    `;
    return row.active === 0;
  }, 'surviving brokers to recover every expired lease');

  const staleAck = await survivors[0].send({
    cmd: 'ACKB',
    ids: held.ids,
    tokens: held.tokens,
  });
  expect(staleAck.ok).toBe(false);
  expect(String(staleAck.error)).toMatch(/invalid or expired lock token/i);
  for (const health of await Promise.all(survivors.map((client) => client.send({ cmd: 'Ping' })))) {
    expect(health.ok).toBe(true);
  }

  const recovered = await consumeExactlyOnce(survivors, queue, 120);
  expect([...recovered].sort()).toEqual([...pushedIds].sort());
  await expectCompletedEverywhere(survivors, queue, 120);
  await expectDatabaseCompleted(sql, namespace, queue, 120);
  const [retried] = await sql<{ count: number }[]>`
    SELECT COUNT(*)::int AS count FROM bunqueue_jobs
    WHERE namespace = ${namespace}
      AND id = ANY(${sql.array(held.ids, 'TEXT')})
      AND attempts = 1
  `;
  expect(retried.count).toBe(24);
}
