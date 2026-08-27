import { afterEach, describe, expect, test } from 'bun:test';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { existsSync, readdirSync } from 'node:fs';
import { disposeServer, Driver, startServer } from '../sdk/conformance/harness';
import { terminateChild } from '../sdk/conformance/process-lifecycle';

const REPO_ROOT = new URL('../', import.meta.url).pathname;
const DRIVER_FIXTURE = new URL('./fixtures/conformance-env-driver.ts', import.meta.url).pathname;
const SIGNAL_FIXTURE = new URL('./fixtures/conformance-signal-child.ts', import.meta.url).pathname;
const BROKER_ONLY_VARIABLES = [
  'AUTH_TOKENS',
  'BQ_DATA_PATH',
  'BQ_TOKEN',
  'BUNQUEUE_BROKER_ID',
  'BUNQUEUE_CLOUD_API_KEY',
  'BUNQUEUE_CLOUD_SIGNING_SECRET',
  'BUNQUEUE_CLOUD_URL',
  'BUNQUEUE_CONFORMANCE_POSTGRES_URL',
  'BUNQUEUE_DATA_PATH',
  'BUNQUEUE_TEST_POSTGRES_URL',
  'BUNQUEUE_POSTGRES_IDLE_TRANSACTION_TIMEOUT_MS',
  'BUNQUEUE_POSTGRES_LEASE_DURATION_MS',
  'BUNQUEUE_POSTGRES_LOCK_TIMEOUT_MS',
  'BUNQUEUE_POSTGRES_MAX_CONCURRENT_OPERATIONS',
  'BUNQUEUE_POSTGRES_MAX_QUEUED_OPERATIONS',
  'BUNQUEUE_POSTGRES_MAX_SNAPSHOT_JOBS',
  'BUNQUEUE_POSTGRES_MAX_SNAPSHOT_PAYLOAD_BYTES',
  'BUNQUEUE_POSTGRES_NAMESPACE',
  'BUNQUEUE_POSTGRES_POLL_INTERVAL_MS',
  'BUNQUEUE_POSTGRES_POOL_SIZE',
  'BUNQUEUE_POSTGRES_STATEMENT_TIMEOUT_MS',
  'BUNQUEUE_POSTGRES_URL',
  'BUNQUEUE_STORAGE_DRIVER',
  'BUNQUEUE_TOKEN',
  'CUSTOM_API_KEY',
  'CUSTOM_API_TOKEN',
  'CUSTOM_PASSWORD',
  'CUSTOM_SECRET',
  'DATABASE_URL',
  'DATA_PATH',
  'AWS_ACCESS_KEY_ID',
  'AWS_CONTAINER_AUTHORIZATION_TOKEN',
  'AWS_SECRET_ACCESS_KEY',
  'AWS_SESSION_TOKEN',
  'AWS_WEB_IDENTITY_TOKEN_FILE',
  'GOOGLE_APPLICATION_CREDENTIALS',
  'GITHUB_TOKEN',
  'METRICS_AUTH',
  'NEXT_API_KEY',
  'NPM_TOKEN',
  'PGDATABASE',
  'PGHOST',
  'PGPASSFILE',
  'PGPASSWORD',
  'PGPORT',
  'PGUSER',
  'POSTGRES_DB',
  'POSTGRES_PASSWORD',
  'POSTGRES_URL',
  'POSTGRES_USER',
  'S3_ACCESS_KEY_ID',
  'S3_SECRET_ACCESS_KEY',
  'S3_SESSION_TOKEN',
  'SDK_POSTGRES_URL',
  'SQLITE_PATH',
  'service-token',
] as const;
const SAFE_DRIVER_VARIABLES = [
  'AUTHORITY_URL',
  'CARGO_REGISTRIES_CRATES_IO_PROTOCOL',
  'GITHUB_ACTIONS',
  'TOKENIZERS_PARALLELISM',
] as const;
const TRACKED_VARIABLES = [...BROKER_ONLY_VARIABLES, ...SAFE_DRIVER_VARIABLES] as const;
const originalEnvironment = new Map(TRACKED_VARIABLES.map((name) => [name, process.env[name]]));

function temporaryConformanceDirectories(): string[] {
  return readdirSync(REPO_ROOT)
    .filter((name) => name.startsWith('.conformance-tmp-'))
    .sort();
}

afterEach(() => {
  for (const name of TRACKED_VARIABLES) {
    const original = originalEnvironment.get(name);
    if (original === undefined) delete process.env[name];
    else process.env[name] = original;
  }
});

describe('SDK conformance harness lifecycle', () => {
  test('a client driver cannot observe broker-only storage variables', async () => {
    for (const name of BROKER_ONLY_VARIABLES) process.env[name] = `secret-${name}`;

    const driver = new Driver(`bun ${DRIVER_FIXTURE}`);
    try {
      const answer = await driver.must('inspectEnvironment', { names: BROKER_ONLY_VARIABLES });
      expect(answer.brokerOnlyEnvironment).toEqual({});
    } finally {
      await driver.terminate();
    }
  });

  test('the secret filter preserves non-secret toolchain variables', async () => {
    for (const name of SAFE_DRIVER_VARIABLES) process.env[name] = `safe-${name}`;

    const driver = new Driver(`bun ${DRIVER_FIXTURE}`);
    try {
      const answer = await driver.must('inspectEnvironment', { names: SAFE_DRIVER_VARIABLES });
      expect(answer.brokerOnlyEnvironment).toEqual(
        Object.fromEntries(SAFE_DRIVER_VARIABLES.map((name) => [name, `safe-${name}`]))
      );
    } finally {
      await driver.terminate();
    }
  });

  test('a failed broker startup removes its temporary SQLite directory', async () => {
    delete process.env.BUNQUEUE_CONFORMANCE_POSTGRES_URL;
    const before = temporaryConformanceDirectories();

    await expect(
      startServer(
        { BUNQUEUE_STORAGE_DRIVER: 'unsupported-for-regression' },
        { startupTimeoutMs: 1000 }
      )
    ).rejects.toThrow('server did not start');

    expect(temporaryConformanceDirectories()).toEqual(before);
  }, 20_000);

  test('child termination escalates when SIGTERM is ignored', async () => {
    const child = spawn(process.execPath, [SIGNAL_FIXTURE, '--ignore-term'], {
      stdio: ['ignore', 'pipe', 'inherit'],
    });
    await once(child.stdout!, 'data');

    const result = await terminateChild(child, {
      termTimeoutMs: 50,
      killTimeoutMs: 1000,
    });

    expect(result.forced).toBe(true);
    expect(child.signalCode).toBe('SIGKILL');
  });

  test('child termination observes a graceful SIGTERM exit', async () => {
    const child = spawn(process.execPath, [SIGNAL_FIXTURE], {
      stdio: ['ignore', 'pipe', 'inherit'],
    });
    await once(child.stdout!, 'data');

    const result = await terminateChild(child, {
      termTimeoutMs: 1000,
      killTimeoutMs: 1000,
    });

    expect(result.forced).toBe(false);
    expect(child.signalCode).toBe('SIGTERM');
  });

  test('successful broker disposal is shared, ordered, and idempotent', async () => {
    delete process.env.BUNQUEUE_CONFORMANCE_POSTGRES_URL;
    const server = await startServer();
    expect(existsSync(server.dataDir)).toBe(true);

    const first = disposeServer(server);
    const second = disposeServer(server);
    expect(second).toBe(first);
    await Promise.all([first, second]);

    expect(server.proc.exitCode !== null || server.proc.signalCode !== null).toBe(true);
    expect(existsSync(server.dataDir)).toBe(false);
  });
});
