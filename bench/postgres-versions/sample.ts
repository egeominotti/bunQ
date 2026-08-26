import { SQL } from 'bun';
import { TcpClient } from '../../src/client/tcp/client';
import {
  startPostgresProcessCluster,
  stopPostgresProcessCluster,
  type PostgresProcessBroker,
} from '../../test/support/postgres-process-cluster';
import { distribution } from './stats';
import type { DatabaseDelta, PostgresVersionSample } from './types';

interface DatabaseStats {
  readonly blks_hit: number | string | bigint;
  readonly blks_read: number | string | bigint;
  readonly deadlocks: number | string | bigint;
  readonly temp_bytes: number | string | bigint;
  readonly wal_bytes: number | string | bigint;
  readonly xact_commit: number | string | bigint;
}

interface ConsumeOptions {
  readonly accepted: ReadonlySet<string>;
  readonly ackLatencies: number[];
  readonly brokerClaims: number[];
  readonly brokers: readonly PostgresProcessBroker[];
  readonly clients: readonly TcpClient[];
  readonly pullLatencies: number[];
}

const url = requiredEnv('BUNQUEUE_PG_BENCH_URL');
const postgresMajor = positiveInteger('BUNQUEUE_PG_BENCH_MAJOR');
const brokersCount = positiveInteger('BUNQUEUE_PG_BENCH_BROKERS');
const jobs = positiveInteger('BUNQUEUE_PG_BENCH_JOBS');
const batchSize = positiveInteger('BUNQUEUE_PG_BENCH_BATCH_SIZE');
const producerConnections = positiveInteger('BUNQUEUE_PG_BENCH_PRODUCERS');
const consumerConnections = positiveInteger('BUNQUEUE_PG_BENCH_CONSUMERS');
const poolSize = positiveInteger('BUNQUEUE_PG_BENCH_POOL_SIZE');
const pollIntervalMs = positiveInteger('BUNQUEUE_PG_BENCH_POLL_INTERVAL_MS');
const namespace = `bench-pg${postgresMajor}-${brokersCount}-${crypto.randomUUID()}`;
const queue = 'version-matrix';

function requiredEnv(name: string): string {
  const value = Bun.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function positiveInteger(name: string): number {
  const raw = requiredEnv(name);
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive safe integer, received ${raw}`);
  }
  return value;
}

function rate(total: number, elapsedMs: number): number {
  return Math.round((total / elapsedMs) * 100_000) / 100;
}

function numeric(value: number | string | bigint): number {
  return Number(value);
}

async function connect(port: number): Promise<TcpClient> {
  const client = new TcpClient({
    autoReconnect: false,
    commandTimeout: 60_000,
    connectTimeout: 5000,
    host: '127.0.0.1',
    pingInterval: 0,
    port,
  });
  await client.connect();
  return client;
}

async function readDatabaseStats(sql: SQL): Promise<DatabaseStats> {
  const [database] = await sql<
    Omit<DatabaseStats, 'wal_bytes'>[]
  >`SELECT xact_commit, deadlocks, temp_bytes, blks_read, blks_hit
    FROM pg_stat_database WHERE datname = current_database()`;
  const [wal] = await sql<{ wal_bytes: number | string | bigint }[]>`
    SELECT wal_bytes::text AS wal_bytes FROM pg_stat_wal
  `;
  return { ...database, wal_bytes: wal.wal_bytes };
}

function databaseDelta(before: DatabaseStats, after: DatabaseStats): DatabaseDelta {
  return {
    blksHit: numeric(after.blks_hit) - numeric(before.blks_hit),
    blksRead: numeric(after.blks_read) - numeric(before.blks_read),
    deadlocks: numeric(after.deadlocks) - numeric(before.deadlocks),
    tempBytes: numeric(after.temp_bytes) - numeric(before.temp_bytes),
    walBytes: numeric(after.wal_bytes) - numeric(before.wal_bytes),
    xactCommit: numeric(after.xact_commit) - numeric(before.xact_commit),
  };
}

async function pushJobs(
  clients: readonly TcpClient[],
  accepted: Set<string>,
  latencies: number[]
): Promise<void> {
  await Promise.all(
    clients.map(async (client, producerIndex) => {
      const start = Math.floor((jobs * producerIndex) / clients.length);
      const end = Math.floor((jobs * (producerIndex + 1)) / clients.length);
      for (let offset = start; offset < end; offset += batchSize) {
        const size = Math.min(batchSize, end - offset);
        const startedAt = performance.now();
        const response = await client.send({
          cmd: 'PUSHB',
          jobs: Array.from({ length: size }, (_, index) => ({
            data: { index: offset + index, payload: 'x'.repeat(100) },
          })),
          queue,
        });
        latencies.push(performance.now() - startedAt);
        if (!response.ok) throw new Error(`PUSHB failed: ${String(response.error)}`);
        for (const id of response.ids as string[]) {
          if (accepted.has(id)) throw new Error(`Duplicate accepted ID ${id}`);
          accepted.add(id);
        }
      }
    })
  );
}

async function consumeJobs(options: ConsumeOptions): Promise<Set<string>> {
  const { accepted, ackLatencies, brokerClaims, brokers, clients, pullLatencies } = options;
  const invoked = new Set<string>();
  const deadline = Date.now() + 120_000;
  await Promise.all(
    clients.map(async (client, consumerIndex) => {
      const brokerIndex = consumerIndex % brokers.length;
      const ackClient = brokers[(brokerIndex + 1) % brokers.length].client;
      while (invoked.size < jobs) {
        if (Date.now() >= deadline) {
          throw new Error(`Timed out after invoking ${invoked.size}/${jobs} jobs`);
        }
        const pullStartedAt = performance.now();
        const response = await client.send({
          cmd: 'PULLB',
          count: batchSize,
          lockTtl: 120_000,
          owner: `consumer-${consumerIndex}`,
          queue,
        });
        const pullElapsed = performance.now() - pullStartedAt;
        if (!response.ok) throw new Error(`PULLB failed: ${String(response.error)}`);
        const ids = (response.jobs as Array<{ id: string }>).map(({ id }) => id);
        const tokens = response.tokens as string[];
        if (ids.length === 0) {
          if (invoked.size < jobs) await Bun.sleep(1);
          continue;
        }
        pullLatencies.push(pullElapsed);
        if (tokens.length !== ids.length) throw new Error('PULLB returned misaligned tokens');
        for (const id of ids) {
          if (!accepted.has(id)) throw new Error(`Invoked unaccepted ID ${id}`);
          if (invoked.has(id)) throw new Error(`Duplicate invocation for ${id}`);
          invoked.add(id);
        }
        brokerClaims[brokerIndex] += ids.length;
        const ackStartedAt = performance.now();
        const acknowledged = await ackClient.send({ cmd: 'ACKB', ids, tokens });
        ackLatencies.push(performance.now() - ackStartedAt);
        if (!acknowledged.ok) throw new Error(`ACKB failed: ${String(acknowledged.error)}`);
      }
    })
  );
  return invoked;
}

async function assertDatabase(sql: SQL): Promise<number> {
  const rows = await sql<{ count: number; distinct_ids: number; state: string }[]>`
    SELECT state, COUNT(*)::int AS count, COUNT(DISTINCT id)::int AS distinct_ids
    FROM bunqueue_jobs WHERE namespace = ${namespace} AND queue = ${queue}
    GROUP BY state ORDER BY state
  `;
  if (
    rows.length !== 1 ||
    rows[0].state !== 'completed' ||
    rows[0].count !== jobs ||
    rows[0].distinct_ids !== jobs
  ) {
    throw new Error(`Authoritative state mismatch: ${JSON.stringify(rows)}`);
  }
  return rows[0].count;
}

async function main(): Promise<void> {
  const sql = new SQL(url, { max: 4 });
  let brokers: PostgresProcessBroker[] = [];
  const clients: TcpClient[] = [];
  try {
    brokers = await startPostgresProcessCluster(url, namespace, brokersCount, {
      pollIntervalMs,
      poolSize,
    });
    const producerClients = await Promise.all(
      Array.from({ length: producerConnections }, (_, index) =>
        connect(brokers[index % brokers.length].port)
      )
    );
    const consumerClients = await Promise.all(
      Array.from({ length: consumerConnections }, (_, index) =>
        connect(brokers[index % brokers.length].port)
      )
    );
    clients.push(...producerClients, ...consumerClients);

    const [settings] = await sql<Record<string, string>[]>`
      SELECT current_setting('server_version') AS server_version,
        current_setting('fsync') AS fsync,
        current_setting('synchronous_commit') AS synchronous_commit,
        current_setting('full_page_writes') AS full_page_writes,
        current_setting('shared_buffers') AS shared_buffers,
        current_setting('work_mem') AS work_mem,
        current_setting('max_connections') AS max_connections
    `;
    if (!settings.server_version.startsWith(`${postgresMajor}.`)) {
      throw new Error(`Expected PostgreSQL ${postgresMajor}, received ${settings.server_version}`);
    }

    const before = await readDatabaseStats(sql);
    const accepted = new Set<string>();
    const pushLatencies: number[] = [];
    const pullLatencies: number[] = [];
    const ackLatencies: number[] = [];
    const brokerClaims = Array.from({ length: brokersCount }, () => 0);

    const lifecycleStartedAt = performance.now();
    await pushJobs(producerClients, accepted, pushLatencies);
    const admissionMs = performance.now() - lifecycleStartedAt;
    if (accepted.size !== jobs) throw new Error(`Accepted ${accepted.size}/${jobs} unique IDs`);

    const processingStartedAt = performance.now();
    const invoked = await consumeJobs({
      accepted,
      ackLatencies,
      brokerClaims,
      brokers,
      clients: consumerClients,
      pullLatencies,
    });
    const processingMs = performance.now() - processingStartedAt;
    const lifecycleMs = performance.now() - lifecycleStartedAt;
    if (invoked.size !== jobs) throw new Error(`Invoked ${invoked.size}/${jobs} unique IDs`);

    const completed = await assertDatabase(sql);
    const delta = databaseDelta(before, await readDatabaseStats(sql));
    if (delta.deadlocks !== 0) throw new Error(`Observed ${delta.deadlocks} PostgreSQL deadlocks`);

    const sample: PostgresVersionSample = {
      accepted: accepted.size,
      ackLatencyMs: distribution(ackLatencies),
      admissionJobsPerSecond: rate(jobs, admissionMs),
      admissionMs,
      batchSize,
      brokerClaims,
      brokers: brokersCount,
      completed,
      consumerConnections,
      databaseDelta: delta,
      duplicateInvocations: 0,
      jobs,
      lifecycleJobsPerSecond: rate(jobs, lifecycleMs),
      lifecycleMs,
      postgresMajor,
      pollIntervalMs,
      postgresSettings: settings,
      postgresVersion: settings.server_version,
      producerConnections,
      poolSize,
      processingJobsPerSecond: rate(jobs, processingMs),
      processingMs,
      pullLatencyMs: distribution(pullLatencies),
      pushLatencyMs: distribution(pushLatencies),
      uniqueAccepted: accepted.size,
      uniqueInvoked: invoked.size,
    };
    console.log(`POSTGRES_VERSION_SAMPLE ${JSON.stringify(sample)}`);
  } finally {
    for (const client of clients) client.close();
    await stopPostgresProcessCluster(brokers);
    await sql.close({ timeout: 5 });
  }
}

await main();
