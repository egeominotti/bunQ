import { TcpClient } from '../../src/client/tcp/client';
import { isBindCollision } from '../support/bind-collision';

type Subprocess = ReturnType<typeof Bun.spawn>;

const STARTUP_ATTEMPTS = 5;
const STARTUP_TIMEOUT_MS = 30_000;
const leasedPorts = new Set<number>();

export interface StartedModelBroker {
  client: TcpClient;
  port: number;
  process: Subprocess;
  stderr: Promise<string>;
}

export async function startModelBroker(
  dbPath: string,
  initialPort?: number
): Promise<StartedModelBroker> {
  const startupRetries: string[] = [];
  let port = initialPort !== undefined && claimPortPair(initialPort) ? initialPort : freePortPair();

  for (let attempt = 1; attempt <= STARTUP_ATTEMPTS; attempt++) {
    if (attempt > 1) port = freePortPair();
    const process = Bun.spawn([globalThis.process.execPath, 'run', 'src/main.ts'], {
      cwd: globalThis.process.cwd(),
      env: {
        ...globalThis.process.env,
        BUNQUEUE_EMBEDDED: '',
        BUNQUEUE_DATA_PATH: dbPath,
        HTTP_PORT: String(port + 1),
        LOG_LEVEL: 'error',
        TCP_PORT: String(port),
      },
      stderr: 'pipe',
      stdout: 'ignore',
    });
    const stderr = new Response(process.stderr).text();
    const readiness = await waitBrokerReady(process, port + 1);

    if (readiness !== 'ready') {
      const detail = await stopProcess(process, stderr);
      releasePortPair(port);
      if (readiness === 'exited' && isBindCollision(detail) && attempt < STARTUP_ATTEMPTS) {
        startupRetries.push(detail.trim());
        continue;
      }
      throw startupError(port, readiness, detail, startupRetries);
    }

    const client = new TcpClient({
      autoReconnect: false,
      commandTimeout: 5000,
      connectTimeout: 5000,
      host: '127.0.0.1',
      pingInterval: 0,
      port,
    });
    try {
      await client.connect();
      const hello = await client.send({ cmd: 'Hello' });
      if (hello.ok !== true || hello.server !== 'bunqueue') {
        throw new Error(`unexpected startup handshake: ${JSON.stringify(hello)}`);
      }
      if (process.exitCode !== null) throw new Error(`broker exited with ${process.exitCode}`);
      return { client, port, process, stderr };
    } catch (error) {
      client.close();
      const detail = await stopProcess(process, stderr);
      releasePortPair(port);
      const message = error instanceof Error ? error.message : String(error);
      const handshakeDetail = `${message}\n${detail}`.trim();
      if (attempt < STARTUP_ATTEMPTS) {
        startupRetries.push(handshakeDetail);
        continue;
      }
      throw startupError(port, 'handshake', handshakeDetail, startupRetries);
    }
  }

  throw startupError(port, 'exited', 'startup retry budget exhausted', startupRetries);
}

export async function stopModelBroker(broker: StartedModelBroker): Promise<void> {
  broker.client.close();
  try {
    await stopProcess(broker.process, broker.stderr);
  } finally {
    releasePortPair(broker.port);
  }
}

function freePortPair(): number {
  for (let attempt = 0; attempt < 100; attempt++) {
    const port = 20_000 + Math.floor(Math.random() * 20_000);
    if (leasedPorts.has(port) || leasedPorts.has(port + 1)) continue;
    let tcpListener: { stop(): void } | null = null;
    let httpListener: { stop(): void } | null = null;
    try {
      tcpListener = probePort(port);
      httpListener = probePort(port + 1);
      if (!claimPortPair(port)) continue;
      return port;
    } catch {
      // Try another adjacent TCP/HTTP pair.
    } finally {
      httpListener?.stop();
      tcpListener?.stop();
    }
  }
  throw new Error('unable to reserve a model-test TCP/HTTP port pair');
}

function claimPortPair(port: number): boolean {
  if (leasedPorts.has(port) || leasedPorts.has(port + 1)) return false;
  leasedPorts.add(port);
  leasedPorts.add(port + 1);
  return true;
}

function releasePortPair(port: number): void {
  leasedPorts.delete(port);
  leasedPorts.delete(port + 1);
}

function probePort(port: number): { stop(): void } {
  return Bun.listen({
    hostname: '127.0.0.1',
    port,
    socket: {
      data() {
        /* Port probe intentionally ignores bytes. */
      },
    },
  });
}

async function waitBrokerReady(
  process: Subprocess,
  httpPort: number
): Promise<'ready' | 'exited' | 'timeout'> {
  const deadline = Date.now() + STARTUP_TIMEOUT_MS;
  while (Date.now() < deadline) {
    if (process.exitCode !== null) return 'exited';
    try {
      const response = await fetch(`http://127.0.0.1:${httpPort}/ready`, {
        signal: AbortSignal.timeout(500),
      });
      const body = (await response.json()) as { ok?: boolean; ready?: boolean };
      if (response.ok && body.ok === true && body.ready === true) return 'ready';
    } catch {
      // The process may still be booting; retry until exit or deadline.
    }
    await Bun.sleep(25);
  }
  return process.exitCode === null ? 'timeout' : 'exited';
}

async function stopProcess(process: Subprocess, stderr: Promise<string>): Promise<string> {
  if (process.exitCode === null) process.kill(9);
  await process.exited;
  return stderr.catch((error: unknown) => `unable to read broker stderr: ${String(error)}`);
}

function startupError(
  port: number,
  phase: 'exited' | 'handshake' | 'timeout',
  detail: string,
  startupRetries: readonly string[]
): Error {
  const diagnostics = [
    `model-test broker startup ${phase} on TCP ${port} / HTTP ${port + 1}`,
    detail.trim() || 'broker produced no stderr',
  ];
  if (startupRetries.length > 0) {
    diagnostics.push(`prior startup failures retried: ${startupRetries.length}`);
  }
  return new Error(diagnostics.join('\n'));
}
