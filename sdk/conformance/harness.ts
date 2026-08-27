import { type ChildProcess, spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import { SQL } from 'bun';
import { terminateChild, waitForChildExit } from './process-lifecycle';
import { Wire } from './wire';

const REPO_ROOT = new URL('../../', import.meta.url).pathname;
const DRIVER_BLOCKED_ENVIRONMENT = new Set([
  'AUTH_TOKENS',
  'DATABASE_URL',
  'DATA_PATH',
  'DOCKER_CONFIG',
  'KUBECONFIG',
  'METRICS_AUTH',
  'NETRC',
  'SSH_AUTH_SOCK',
  'SDK_POSTGRES_URL',
  'SQLITE_PATH',
  'TLS_CA_FILE',
  'TLS_CERT_FILE',
  'TLS_KEY_FILE',
]);
const DRIVER_BLOCKED_PREFIXES = ['AWS_', 'BQ_', 'BUNQUEUE_', 'POSTGRES_', 'S3_'];
const DRIVER_SECRET_SEGMENT =
  /(?:^|[_-])(?:ACCESS_KEY(?:_ID)?|API_KEY|AUTH(?:ORIZATION)?|BEARER|CREDENTIALS?|OAUTH|PASS(?:WORD|WD)?|PRIVATE_KEY|SECRET|SESSION_TOKEN|SIGNING_(?:KEY|SECRET)|TOKEN)(?:[_-]|$)/i;

function isDriverEnvironmentBlocked(name: string): boolean {
  const normalized = name.toUpperCase();
  return (
    DRIVER_BLOCKED_ENVIRONMENT.has(normalized) ||
    DRIVER_BLOCKED_PREFIXES.some((prefix) => normalized.startsWith(prefix)) ||
    /^PG[A-Z0-9_]/.test(normalized) ||
    DRIVER_SECRET_SEGMENT.test(name)
  );
}

interface PostgresStorage {
  readonly namespace: string;
  readonly url: string;
}

export interface ConformanceServer {
  readonly dataDir: string;
  readonly port: number;
  readonly postgres: PostgresStorage | null;
  readonly proc: ChildProcess;
  readonly wire: Wire;
  disposePromise?: Promise<void>;
}

interface StartServerOptions {
  readonly startupTimeoutMs?: number;
}

async function freePort(): Promise<number> {
  const { createServer } = await import('node:net');
  return new Promise((resolve) => {
    const server = createServer();
    server.listen(0, '127.0.0.1', () => {
      const port = (server.address() as { port: number }).port;
      server.close(() => resolve(port));
    });
  });
}

export async function startServer(
  extraEnv: Record<string, string> = {},
  options: StartServerOptions = {}
): Promise<ConformanceServer> {
  const port = await freePort();
  const httpPort = await freePort();
  const dataDir = `${REPO_ROOT}.conformance-tmp-${Math.random().toString(36).slice(2, 8)}`;
  const postgresUrl = process.env.BUNQUEUE_CONFORMANCE_POSTGRES_URL?.trim() || null;
  const postgresNamespace = postgresUrl
    ? `conformance-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`
    : null;
  const postgres =
    postgresUrl && postgresNamespace ? { url: postgresUrl, namespace: postgresNamespace } : null;
  const { mkdirSync } = await import('node:fs');
  mkdirSync(dataDir, { recursive: true });
  const storageEnv = postgresUrl
    ? {
        BUNQUEUE_BROKER_ID: `conformance-${Math.random().toString(36).slice(2, 10)}`,
        BUNQUEUE_DATA_PATH: '',
        BUNQUEUE_POSTGRES_NAMESPACE: postgresNamespace ?? '',
        BUNQUEUE_POSTGRES_URL: postgresUrl,
        BUNQUEUE_STORAGE_DRIVER: 'postgres',
        BQ_DATA_PATH: '',
        DATA_PATH: '',
        SQLITE_PATH: '',
      }
    : {
        BUNQUEUE_DATA_PATH: `${dataDir}/bunq.db`,
        BUNQUEUE_POSTGRES_URL: '',
        BUNQUEUE_STORAGE_DRIVER: 'sqlite',
      };
  const proc = spawn('bun', ['src/main.ts'], {
    cwd: REPO_ROOT,
    env: {
      ...process.env,
      AUTH_TOKENS: '',
      TCP_PORT: String(port),
      HTTP_PORT: String(httpPort),
      ...storageEnv,
      ...extraEnv,
    },
    stdio: 'ignore',
  });
  const startupTimeoutMs = options.startupTimeoutMs ?? 15_000;
  const deadline = Date.now() + startupTimeoutMs;
  let startupError: Error;
  try {
    while (Date.now() < deadline) {
      if (proc.exitCode !== null || proc.signalCode !== null) {
        throw new Error(
          `server did not start because it exited with ${proc.signalCode ?? proc.exitCode}`
        );
      }
      const wire = new Wire();
      try {
        await wire.connect(port, extraEnv.AUTH_TOKENS?.split(',')[0]);
        return { port, proc, wire, dataDir, postgres };
      } catch {
        wire.close();
        await new Promise((resolve) => setTimeout(resolve, 150));
      }
    }
    throw new Error(`server did not start within ${startupTimeoutMs}ms`);
  } catch (error) {
    startupError = error instanceof Error ? error : new Error(String(error));
  }

  try {
    await terminateChild(proc);
  } catch (terminationError) {
    throw new AggregateError(
      [startupError, terminationError],
      `${startupError.message}; broker termination also failed`
    );
  }
  try {
    await cleanupServerStorage({ dataDir, postgres });
  } catch (cleanupError) {
    throw new AggregateError(
      [startupError, cleanupError],
      `${startupError.message}; storage cleanup also failed`
    );
  }
  throw startupError;
}

export async function cleanupServerStorage(server: {
  readonly dataDir: string;
  readonly postgres: PostgresStorage | null;
}): Promise<void> {
  const { rmSync } = await import('node:fs');
  rmSync(server.dataDir, { recursive: true, force: true });
  if (!server.postgres) return;

  const sql = new SQL(server.postgres.url, { max: 1 });
  const namespace = server.postgres.namespace;
  try {
    const relations = await sql<{ jobs: string | null }[]>`
      SELECT to_regclass('bunqueue_jobs')::text AS jobs
    `;
    if (!relations[0]?.jobs) return;
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

async function disposeServerOnce(server: ConformanceServer): Promise<void> {
  server.wire.close();
  await terminateChild(server.proc);
  await cleanupServerStorage(server);
}

export function disposeServer(server: ConformanceServer): Promise<void> {
  server.disposePromise ??= disposeServerOnce(server);
  return server.disposePromise;
}

function driverEnvironment(): NodeJS.ProcessEnv {
  return Object.fromEntries(
    Object.entries(process.env).filter(([name]) => !isDriverEnvironmentBlocked(name))
  );
}

export class Driver {
  private readonly proc: ChildProcess;
  private readonly pending = new Map<
    number,
    {
      reject: (error: Error) => void;
      resolve: (value: Record<string, unknown>) => void;
      timer: ReturnType<typeof setTimeout>;
    }
  >();
  private counter = 0;
  private readonly processGroup: boolean;

  constructor(command: string) {
    this.processGroup = process.platform !== 'win32';
    this.proc = spawn('sh', ['-c', command], {
      cwd: new URL('.', import.meta.url).pathname,
      detached: this.processGroup,
      env: driverEnvironment(),
      stdio: ['pipe', 'pipe', 'inherit'],
    });
    const output = this.proc.stdout;
    if (!output) throw new Error('driver stdout is unavailable');
    const lines = createInterface({ input: output });
    lines.on('line', (line) => {
      try {
        const message = JSON.parse(line) as { id: number };
        const pending = this.pending.get(message.id);
        if (!pending) return;
        clearTimeout(pending.timer);
        pending.resolve(message as unknown as Record<string, unknown>);
        this.pending.delete(message.id);
      } catch {
        /* non-JSON driver noise on stdout: ignore */
      }
    });
    this.proc.once('exit', (code, signal) => {
      const error = new Error(`driver exited with ${signal ?? code ?? 'unknown status'}`);
      for (const pending of this.pending.values()) {
        clearTimeout(pending.timer);
        pending.reject(error);
      }
      this.pending.clear();
    });
  }

  async op(
    op: string,
    fields: Record<string, unknown> = {},
    timeoutMs = 30_000
  ): Promise<Record<string, unknown>> {
    const id = ++this.counter;
    const answer = new Promise<Record<string, unknown>>((resolve, reject) => {
      const timer = setTimeout(() => {
        if (this.pending.delete(id))
          reject(new Error(`driver op '${op}' timed out after ${timeoutMs}ms`));
      }, timeoutMs);
      timer.unref?.();
      this.pending.set(id, { reject, resolve, timer });
    });
    const input = this.proc.stdin;
    if (!input) throw new Error('driver stdin is unavailable');
    input.write(`${JSON.stringify({ id, op, ...fields })}\n`);
    return await answer;
  }

  /** Like op(), but throws when the driver answers ok: false. */
  async must(
    op: string,
    fields: Record<string, unknown> = {},
    timeoutMs = 30_000
  ): Promise<Record<string, unknown>> {
    const answer = await this.op(op, fields, timeoutMs);
    if (answer.ok !== true)
      throw new Error(`driver ${op} failed: ${String(answer.error ?? 'unknown')}`);
    return answer;
  }

  async terminate(): Promise<void> {
    this.proc.stdin?.end();
    if (await waitForChildExit(this.proc, 1000)) return;
    await terminateChild(this.proc, { killProcessGroup: this.processGroup });
  }
}
