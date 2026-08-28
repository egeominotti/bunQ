import { SQL } from 'bun';
import { TcpClient } from '../../src/client/tcp/client';
import {
  capturePostgresProcessOutput,
  type PostgresProcessOutput,
  type PostgresProcessRetryDiagnostics,
} from './postgres-process-output';
import { isBindCollision } from './bind-collision';

type BrokerProcess = ReturnType<typeof Bun.spawn>;

const STARTUP_TIMEOUT_MS = 30_000;
const leasedPorts = new Set<number>();

export interface PostgresProcessBroker {
  readonly brokerId: string;
  readonly client: TcpClient;
  readonly output: PostgresProcessOutput;
  readonly port: number;
  readonly process: BrokerProcess;
  released: boolean;
  terminated: boolean;
  termination?: Promise<void>;
}

export interface PostgresProcessClusterOptions {
  readonly leaseDurationMs?: number;
  readonly lockTimeoutMs?: number;
  readonly logFormat?: 'human' | 'json';
  readonly logLevel?: 'error' | 'warn';
  readonly pollIntervalMs?: number;
  readonly poolSize?: number;
  readonly shutdownTimeoutMs?: number;
}

export async function startPostgresProcessCluster(
  url: string,
  namespace: string,
  size: number,
  options: PostgresProcessClusterOptions = {}
): Promise<PostgresProcessBroker[]> {
  const starts = Array.from({ length: size }, (_, index) =>
    startPostgresProcessBroker(url, namespace, `process-${index + 1}`, options)
  );
  const results = await Promise.allSettled(starts);
  const brokers = results.flatMap((result) =>
    result.status === 'fulfilled' ? [result.value] : []
  );
  const failures = results.flatMap((result) =>
    result.status === 'rejected' ? [String(result.reason)] : []
  );
  if (failures.length === 0) return brokers;
  await stopPostgresProcessCluster(brokers);
  throw new Error(`PostgreSQL broker cluster startup failed:\n${failures.join('\n')}`);
}

export async function stopPostgresProcessCluster(
  brokers: readonly PostgresProcessBroker[]
): Promise<void> {
  // Teardown stays best-effort so every broker is stopped; diagnostics is the strict output barrier.
  await Promise.allSettled(brokers.map((broker) => stopPostgresProcessBroker(broker)));
}

export async function crashPostgresProcessBroker(broker: PostgresProcessBroker): Promise<void> {
  broker.termination ??= terminatePostgresProcessBroker(broker, true);
  await broker.termination;
}

export async function cleanupPostgresNamespace(url: string, namespace: string): Promise<void> {
  const sql = new SQL(url, { max: 2 });
  try {
    await sql.begin(async (tx) => {
      await tx`DELETE FROM bunqueue_metric_buckets WHERE namespace = ${namespace}`;
      await tx`DELETE FROM bunqueue_metric_totals WHERE namespace = ${namespace}`;
      await tx`DELETE FROM bunqueue_workers WHERE namespace = ${namespace}`;
      await tx`DELETE FROM bunqueue_crons WHERE namespace = ${namespace}`;
      await tx`DELETE FROM bunqueue_job_logs WHERE namespace = ${namespace}`;
      await tx`DELETE FROM bunqueue_repeat_links WHERE namespace = ${namespace}`;
      await tx`DELETE FROM bunqueue_flow_failures WHERE namespace = ${namespace}`;
      await tx`DELETE FROM bunqueue_dependencies WHERE namespace = ${namespace}`;
      await tx`DELETE FROM bunqueue_completions WHERE namespace = ${namespace}`;
      await tx`DELETE FROM bunqueue_jobs WHERE namespace = ${namespace}`;
      await tx`DELETE FROM bunqueue_queue_state WHERE namespace = ${namespace}`;
      await tx`DELETE FROM bunqueue_event_prune_watermarks WHERE namespace = ${namespace}`;
      await tx`DELETE FROM bunqueue_events WHERE namespace = ${namespace}`;
      await tx`DELETE FROM bunqueue_event_commits WHERE namespace = ${namespace}`;
      await tx`DELETE FROM bunqueue_brokers WHERE namespace = ${namespace}`;
    });
  } finally {
    await sql.close({ timeout: 5 });
  }
}

async function attemptPostgresProcessBroker(
  url: string,
  namespace: string,
  brokerId: string,
  options: PostgresProcessClusterOptions
): Promise<PostgresProcessBroker> {
  const port = reservePortPair();
  const process = Bun.spawn([globalThis.process.execPath, 'run', 'src/main.ts'], {
    cwd: globalThis.process.cwd(),
    env: {
      ...globalThis.process.env,
      BUNQUEUE_BROKER_ID: brokerId,
      BUNQUEUE_DATA_PATH: '',
      BUNQUEUE_EMBEDDED: '',
      BUNQUEUE_POSTGRES_LEASE_DURATION_MS: String(options.leaseDurationMs ?? 1000),
      BUNQUEUE_POSTGRES_LOCK_TIMEOUT_MS: String(options.lockTimeoutMs ?? 5000),
      BUNQUEUE_POSTGRES_NAMESPACE: namespace,
      BUNQUEUE_POSTGRES_POLL_INTERVAL_MS: String(options.pollIntervalMs ?? 25),
      BUNQUEUE_POSTGRES_POOL_SIZE: String(options.poolSize ?? 12),
      BUNQUEUE_POSTGRES_URL: url,
      BUNQUEUE_STORAGE_DRIVER: 'postgres',
      BQ_DATA_PATH: '',
      DATA_PATH: '',
      HOST: '127.0.0.1',
      HTTP_PORT: String(port + 1),
      LOG_FORMAT: options.logFormat === 'json' ? 'json' : '',
      LOG_LEVEL: options.logLevel ?? 'error',
      SHUTDOWN_TIMEOUT_MS: String(options.shutdownTimeoutMs ?? 1500),
      S3_BACKUP_ENABLED: '',
      SQLITE_PATH: '',
      STATS_INTERVAL_MS: '600000',
      TCP_PORT: String(port),
    },
    stderr: 'pipe',
    stdout: 'pipe',
  });
  const output = capturePostgresProcessOutput(brokerId, process.stdout, process.stderr);
  try {
    await waitUntilReady(process, port + 1);
    const client = new TcpClient({
      autoReconnect: false,
      commandTimeout: 15_000,
      connectTimeout: 5000,
      host: '127.0.0.1',
      pingInterval: 0,
      port,
    });
    await client.connect();
    const hello = await client.send({ cmd: 'Hello' });
    if (hello.ok !== true || hello.server !== 'bunqueue') {
      throw new Error(`unexpected broker handshake: ${JSON.stringify(hello)}`);
    }
    return {
      brokerId,
      client,
      output,
      port,
      process,
      released: false,
      terminated: false,
    };
  } catch (error) {
    const exited = process.exitCode !== null;
    if (!exited) process.kill('SIGKILL');
    await process.exited;
    const outputError = await output.settle().then(
      () => '',
      (settleError: unknown) => `\n${String(settleError)}`
    );
    const detail = `${String(error)}\n${output.snapshot()}${outputError}`.trim();
    if (exited && isBindCollision(detail)) {
      // Another process owns this pair: keep it leased so this worker never
      // draws it again, and let the caller retry with a different pair. Only a
      // broker that actually exited is retried, so a readiness timeout keeps
      // its diagnostics instead of being hidden behind five more attempts.
      throw new PortConflictError(detail);
    }
    leasedPorts.delete(port);
    leasedPorts.delete(port + 1);
    throw new Error(detail);
  }
}

class PortConflictError extends Error {}

/**
 * Reserving a port pair releases the probe sockets before the broker binds
 * them, so a concurrent test worker can win the race. Retry with a fresh pair
 * instead of failing the suite on an addressing conflict.
 */
async function startPostgresProcessBroker(
  url: string,
  namespace: string,
  brokerId: string,
  options: PostgresProcessClusterOptions
): Promise<PostgresProcessBroker> {
  let lastConflict: unknown;
  for (let attempt = 0; attempt < 5; attempt++) {
    try {
      return await attemptPostgresProcessBroker(url, namespace, brokerId, options);
    } catch (error) {
      if (!(error instanceof PortConflictError)) throw error;
      lastConflict = error;
    }
  }
  throw new Error(`broker ${brokerId} lost five port races: ${String(lastConflict)}`);
}

export async function postgresProcessRetryDiagnostics(
  brokers: readonly PostgresProcessBroker[]
): Promise<PostgresProcessRetryDiagnostics> {
  const live = brokers.filter(({ terminated }) => !terminated);
  if (live.length > 0) {
    throw new Error(`cannot collect PostgreSQL diagnostics from ${live.length} live broker(s)`);
  }
  const settlements = await Promise.allSettled(brokers.map(({ output }) => output.settle()));
  const failures = settlements.flatMap((result) =>
    result.status === 'rejected' ? [result.reason] : []
  );
  if (failures.length === 1) throw failures[0];
  if (failures.length > 1) {
    throw new AggregateError(failures, `${failures.length} broker output captures were incomplete`);
  }
  return brokers.reduce<PostgresProcessRetryDiagnostics>(
    (total, { output }) => {
      const current = output.diagnostics();
      return {
        ackbFailures: total.ackbFailures + current.ackbFailures,
        transactionRetries: total.transactionRetries + current.transactionRetries,
      };
    },
    { ackbFailures: 0, transactionRetries: 0 }
  );
}

async function stopPostgresProcessBroker(broker: PostgresProcessBroker): Promise<void> {
  broker.termination ??= terminatePostgresProcessBroker(broker, false);
  await broker.termination;
}

async function terminatePostgresProcessBroker(
  broker: PostgresProcessBroker,
  crash: boolean
): Promise<void> {
  try {
    try {
      broker.client.close();
    } catch {
      // Client cleanup must not prevent process termination and output draining.
    }
    if (broker.process.exitCode === null) {
      broker.process.kill(crash ? 'SIGKILL' : 'SIGTERM');
      if (crash) {
        await broker.process.exited;
      } else if (
        !(await Promise.race([
          broker.process.exited.then(() => true),
          Bun.sleep(5000).then(() => false),
        ])) &&
        broker.process.exitCode === null
      ) {
        broker.process.kill('SIGKILL');
        await broker.process.exited;
      }
    }
    broker.terminated = true;
    await broker.output.settle();
  } finally {
    releaseBrokerPort(broker);
  }
}

function releaseBrokerPort(broker: PostgresProcessBroker): void {
  if (broker.released) return;
  broker.released = true;
  leasedPorts.delete(broker.port);
  leasedPorts.delete(broker.port + 1);
}

function reservePortPair(): number {
  for (let attempt = 0; attempt < 100; attempt++) {
    const port = 20_000 + Math.floor(Math.random() * 20_000);
    if (leasedPorts.has(port) || leasedPorts.has(port + 1)) continue;
    let tcp: ReturnType<typeof Bun.listen> | null = null;
    let http: ReturnType<typeof Bun.listen> | null = null;
    try {
      tcp = probePort(port);
      http = probePort(port + 1);
      leasedPorts.add(port);
      leasedPorts.add(port + 1);
      return port;
    } catch {
      // Another process owns this pair; retry with a different adjacent pair.
    } finally {
      http?.stop(true);
      tcp?.stop(true);
    }
  }
  throw new Error('unable to reserve an adjacent TCP/HTTP port pair');
}

function probePort(port: number): ReturnType<typeof Bun.listen> {
  return Bun.listen({
    hostname: '127.0.0.1',
    port,
    socket: { data() {} },
  });
}

async function waitUntilReady(process: BrokerProcess, httpPort: number): Promise<void> {
  const deadline = Date.now() + STARTUP_TIMEOUT_MS;
  while (Date.now() < deadline) {
    if (process.exitCode !== null) throw new Error(`broker exited with ${process.exitCode}`);
    try {
      const response = await fetch(`http://127.0.0.1:${httpPort}/ready`, {
        signal: AbortSignal.timeout(500),
      });
      const body = (await response.json()) as { ok?: boolean; ready?: boolean };
      if (response.ok && body.ok === true && body.ready === true) return;
    } catch {
      // The broker may still be initializing PostgreSQL and the schema.
    }
    await Bun.sleep(25);
  }
  throw new Error(`broker readiness timed out on HTTP port ${httpPort}`);
}
