import { describe, expect, test } from 'bun:test';
import { SQL } from 'bun';
import { TcpClient } from '../src/client/tcp/client';
import {
  cleanupPostgresNamespace,
  crashPostgresProcessBroker,
  startPostgresProcessCluster,
  stopPostgresProcessCluster,
  type PostgresProcessBroker,
} from './support/postgres-process-cluster';
import {
  expectDatabaseCompleted,
  pullClaims,
  readPostgresDatabaseStats,
  subtractPostgresDatabaseStats,
  waitFor,
} from './support/postgres-process-workload';
import { expectedPostgresVersionPrefix } from './support/postgres-version';

const postgresUrl = Bun.env.BUNQUEUE_TEST_POSTGRES_URL;
const soakEnabled = Bun.env.BUNQUEUE_POSTGRES_TEN_BROKER_SOAK === '1';
const BROKERS = 10;
const STEADY_JOBS = 20_000;
const RECOVERY_JOBS = 5_000;
const BATCH_SIZE = 250;
const LEASE_DURATION_MS = 30_000;

interface DrainClient {
  readonly brokerIndex: number;
  readonly client: TcpClient;
}

interface DrainResult {
  readonly brokerClaims: number[];
  readonly elapsedMs: number;
  readonly ids: Set<string>;
}

describe(`PostgreSQL ${expectedPostgresVersionPrefix} ten-process soak topology`, () => {
  test.skipIf(!postgresUrl || !soakEnabled)(
    'runs realistic concurrent traffic and survives two broker crashes',
    async () => {
      const namespace = `test-ten-processes-${Date.now()}-${crypto.randomUUID()}`;
      const sql = new SQL(postgresUrl!, { max: 4 });
      const extraClients: TcpClient[] = [];
      let brokers: PostgresProcessBroker[] = [];
      try {
        brokers = await startPostgresProcessCluster(postgresUrl!, namespace, BROKERS, {
          leaseDurationMs: LEASE_DURATION_MS,
          pollIntervalMs: 250,
          poolSize: 4,
          shutdownTimeoutMs: 10_000,
        });
        const producerClients = brokers.map(({ client }) => client);
        const consumers = await connectConsumers(brokers, extraClients);
        const [version] = await sql<{ version: string }[]>`
          SELECT current_setting('server_version') AS version
        `;
        expect(version.version).toStartWith(expectedPostgresVersionPrefix);
        expect(new Set(brokers.map(({ process }) => process.pid)).size).toBe(BROKERS);
        await expectRegisteredBrokers(sql, namespace, BROKERS);

        const before = await readPostgresDatabaseStats(sql);
        const [steadyIds, steadyDrain] = await Promise.all([
          pushBatched(producerClients, 'ten-steady', STEADY_JOBS),
          drainExactlyOnce(consumers, producerClients, 'ten-steady', STEADY_JOBS, 90_000),
        ]);
        expect([...steadyDrain.ids].sort()).toEqual([...steadyIds].sort());
        await expectDatabaseCompleted(sql, namespace, 'ten-steady', STEADY_JOBS);

        const recoveryIds = await pushBatched(producerClients, 'ten-crash-recovery', RECOVERY_JOBS);
        const firstHeld = await pullClaims(
          brokers[0].client,
          0,
          'ten-crash-recovery',
          100,
          LEASE_DURATION_MS
        );
        const secondHeld = await pullClaims(
          brokers[1].client,
          1,
          'ten-crash-recovery',
          100,
          LEASE_DURATION_MS
        );
        expect(firstHeld.ids).toHaveLength(100);
        expect(secondHeld.ids).toHaveLength(100);
        expect(
          (await brokers[9].client.send({ cmd: 'Pause', queue: 'ten-crash-recovery' })).ok
        ).toBe(true);

        const crashedAt = performance.now();
        await Promise.all([
          crashPostgresProcessBroker(brokers[0]),
          crashPostgresProcessBroker(brokers[1]),
        ]);
        const survivors = brokers.slice(2).map(({ client }) => client);
        const survivorConsumers = consumers.filter(({ brokerIndex }) => brokerIndex >= 2);
        const heldIds = [...firstHeld.ids, ...secondHeld.ids];
        await waitFor(
          async () => {
            const [row] = await sql<{ active: number }[]>`
              SELECT COUNT(*)::int AS active FROM bunqueue_jobs
              WHERE namespace = ${namespace}
                AND id = ANY(${sql.array(heldIds, 'TEXT')})
                AND state = 'active'
            `;
            return row.active === 0;
          },
          'surviving brokers to recover every expired lease',
          90_000
        );
        const recoveryMs = performance.now() - crashedAt;
        const staleAck = await survivors[0].send({
          cmd: 'ACKB',
          ids: heldIds,
          tokens: [...firstHeld.tokens, ...secondHeld.tokens],
        });
        expect(staleAck.ok).toBe(false);
        expect(String(staleAck.error)).toMatch(/invalid or expired lock token/i);
        expect(
          (await survivors.at(-1)!.send({ cmd: 'Resume', queue: 'ten-crash-recovery' })).ok
        ).toBe(true);
        const recoveryDrain = await drainExactlyOnce(
          survivorConsumers,
          survivors,
          'ten-crash-recovery',
          RECOVERY_JOBS,
          90_000
        );
        expect([...recoveryDrain.ids].sort()).toEqual([...recoveryIds].sort());
        await expectDatabaseCompleted(sql, namespace, 'ten-crash-recovery', RECOVERY_JOBS);
        const [retried] = await sql<{ count: number }[]>`
          SELECT COUNT(*)::int AS count FROM bunqueue_jobs
          WHERE namespace = ${namespace}
            AND id = ANY(${sql.array(heldIds, 'TEXT')})
            AND attempts = 1
        `;
        expect(retried.count).toBe(heldIds.length);

        const after = await readPostgresDatabaseStats(sql);
        const databaseDelta = subtractPostgresDatabaseStats(before, after);
        expect(databaseDelta.deadlocks).toBe(0);
        expect(brokers.slice(2).every(({ process }) => process.exitCode === null)).toBe(true);
        console.log(
          `POSTGRES_TEN_BROKER_SOAK ${JSON.stringify({
            brokerProcesses: BROKERS,
            consumerConnections: consumers.length,
            databaseDelta,
            postgresVersion: version.version,
            recovery: {
              brokerClaims: recoveryDrain.brokerClaims,
              crashedBrokers: 2,
              elapsedMs: recoveryMs,
              jobs: RECOVERY_JOBS,
              recoveredLeases: heldIds.length,
            },
            steady: {
              brokerClaims: steadyDrain.brokerClaims,
              elapsedMs: steadyDrain.elapsedMs,
              jobs: STEADY_JOBS,
              jobsPerSecond: Math.round((STEADY_JOBS / steadyDrain.elapsedMs) * 100_000) / 100,
            },
          })}`
        );
      } finally {
        for (const client of extraClients) client.close();
        await stopPostgresProcessCluster(brokers);
        await sql.close({ timeout: 5 });
        await cleanupPostgresNamespace(postgresUrl!, namespace);
      }
    },
    180_000
  );
});

async function connectConsumers(
  brokers: readonly PostgresProcessBroker[],
  tracked: TcpClient[]
): Promise<DrainClient[]> {
  const consumers = await Promise.all(
    brokers.flatMap((broker, brokerIndex) =>
      Array.from({ length: 4 }, async () => {
        const client = new TcpClient({
          autoReconnect: false,
          commandTimeout: 60_000,
          connectTimeout: 5000,
          host: '127.0.0.1',
          pingInterval: 0,
          port: broker.port,
        });
        await client.connect();
        tracked.push(client);
        return { brokerIndex, client };
      })
    )
  );
  return consumers;
}

async function pushBatched(
  clients: readonly TcpClient[],
  queue: string,
  total: number
): Promise<Set<string>> {
  const accepted = new Set<string>();
  await Promise.all(
    clients.map(async (client, producerIndex) => {
      const start = Math.floor((total * producerIndex) / clients.length);
      const end = Math.floor((total * (producerIndex + 1)) / clients.length);
      for (let offset = start; offset < end; offset += BATCH_SIZE) {
        const size = Math.min(BATCH_SIZE, end - offset);
        const response = await client.send({
          cmd: 'PUSHB',
          jobs: Array.from({ length: size }, (_, index) => ({
            backoff: 0,
            data: { index: offset + index, payload: 'x'.repeat(512), producerIndex },
            maxAttempts: 3,
          })),
          queue,
        });
        if (!response.ok) throw new Error(`PUSHB failed: ${String(response.error)}`);
        for (const id of response.ids as string[]) {
          if (accepted.has(id)) throw new Error(`duplicate accepted ID ${id}`);
          accepted.add(id);
        }
      }
    })
  );
  expect(accepted.size).toBe(total);
  return accepted;
}

async function drainExactlyOnce(
  consumers: readonly DrainClient[],
  ackClients: readonly TcpClient[],
  queue: string,
  expected: number,
  timeoutMs: number
): Promise<DrainResult> {
  const ids = new Set<string>();
  const brokerClaims = Array.from({ length: BROKERS }, () => 0);
  const startedAt = performance.now();
  const deadline = Date.now() + timeoutMs;
  await Promise.all(
    consumers.map(async ({ brokerIndex, client }, consumerIndex) => {
      while (ids.size < expected) {
        if (Date.now() >= deadline) throw new Error(`timed out draining ${queue}: ${ids.size}`);
        const response = await client.send({
          cmd: 'PULLB',
          count: BATCH_SIZE,
          lockTtl: LEASE_DURATION_MS,
          owner: `${queue}-consumer-${consumerIndex}`,
          queue,
        });
        if (!response.ok) throw new Error(`PULLB failed: ${String(response.error)}`);
        const pulledIds = (response.jobs as Array<{ id: string }>).map(({ id }) => id);
        const tokens = response.tokens as string[];
        if (pulledIds.length === 0) {
          await Bun.sleep(10);
          continue;
        }
        if (tokens.length !== pulledIds.length) throw new Error('PULLB token count mismatch');
        for (const id of pulledIds) {
          if (ids.has(id)) throw new Error(`duplicate delivery for ${id} on ${queue}`);
          ids.add(id);
        }
        brokerClaims[brokerIndex] += pulledIds.length;
        const acknowledged = await ackClients[(consumerIndex + 1) % ackClients.length].send({
          cmd: 'ACKB',
          ids: pulledIds,
          tokens,
        });
        if (!acknowledged.ok) throw new Error(`ACKB failed: ${String(acknowledged.error)}`);
      }
    })
  );
  expect(ids.size).toBe(expected);
  return { brokerClaims, elapsedMs: performance.now() - startedAt, ids };
}

async function expectRegisteredBrokers(
  sql: SQL,
  namespace: string,
  expected: number
): Promise<void> {
  const rows = await sql<{ count: number }[]>`
    SELECT COUNT(*)::int AS count FROM bunqueue_brokers WHERE namespace = ${namespace}
  `;
  expect(rows).toEqual([{ count: expected }]);
}
