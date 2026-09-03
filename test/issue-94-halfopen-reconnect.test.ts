/**
 * Issue #94 — Worker never recovers from a half-open TCP connection.
 *
 * https://github.com/egeominotti/bunqueue/issues/94
 *
 * A TCP connection becomes "half-open" when the socket dies WITHOUT a FIN/RST:
 * the host is suspended/hibernated, or a NAT / load-balancer silently drops an
 * idle connection. From the client's TCP stack the socket still looks alive —
 * writes succeed (they go into the kernel send buffer), no `error`/`close`
 * event ever fires. The ONLY application-level signal that the peer is gone is
 * that responses stop coming back.
 *
 * The client ships the machinery to handle this: a periodic health-check `ping`
 * (pingInterval), a max-consecutive-failure threshold (maxPingFailures), and
 * auto-reconnect with backoff. The bug report claims a failed ping does NOT
 * conclude the connection is dead, so reconnect is never triggered and the
 * worker stalls at throughput 0 forever.
 *
 * We reproduce the EXACT client-visible condition with a mock server that keeps
 * the TCP socket fully open but stops answering application frames ("black
 * hole"). New connections opened AFTER the black-hole are served normally — so
 * a client that correctly concludes "dead → reconnect" recovers immediately,
 * and a client that doesn't, hangs forever.
 *
 * Three levels of assertion:
 *   1. TcpClient: a half-open socket must trigger reconnect (the report's core
 *      claim — client.ts ping/reconnect path).
 *   2. TcpClient: a command issued after the half-open must eventually succeed
 *      (full recovery, not just an event).
 *   3. Worker: job throughput must resume after the connection goes half-open
 *      (the integration-level symptom the reporter actually observed).
 */

import { describe, it, expect, afterEach } from 'bun:test';
import { TcpClient } from '../src/client/tcp/client';
import { Worker } from '../src/client/worker';
import { FrameParser } from '../src/infrastructure/server/protocol';
import { pack, unpack } from 'msgpackr';
import type { Socket, TCPSocketListener } from 'bun';

// ---------------------------------------------------------------------------
// Controllable mock server
// ---------------------------------------------------------------------------

interface SockState {
  parser: FrameParser;
  /** When true, every frame received on this socket is swallowed (no reply). */
  dead: boolean;
}

interface MockController {
  readonly port: number;
  /** Total sockets the server has accepted (proxy for "reconnect happened"). */
  connectionsAccepted(): number;
  /** Number of sockets currently open and NOT black-holed. */
  liveConnections(): number;
  /** Black-hole every CURRENTLY open socket. New sockets stay healthy. */
  goHalfOpen(): void;
  stop(): void;
}

interface MockOptions {
  /** When true, PULL/PULLB hand out an endless stream of unique jobs. */
  serveJobs?: boolean;
}

function randomPort(): number {
  return 20000 + Math.floor(Math.random() * 20000);
}

const activeServers: TCPSocketListener<unknown>[] = [];

function startMockServer(opts: MockOptions = {}): MockController {
  const port = randomPort();
  const open = new Set<Socket<SockState>>();
  let accepted = 0;
  let jobSeq = 0;

  const reply = (sock: Socket<SockState>, payload: Record<string, unknown>): void => {
    sock.write(FrameParser.frame(pack(payload)));
  };

  const respond = (sock: Socket<SockState>, cmd: Record<string, unknown>): void => {
    const reqId = cmd.reqId as string | undefined;
    const name = cmd.cmd as string;

    if (name === 'Ping') {
      reply(sock, { reqId, ok: true, data: { pong: true } });
      return;
    }

    if (name === 'PULL') {
      if (opts.serveJobs) {
        const id = `job-${++jobSeq}`;
        reply(sock, { reqId, ok: true, job: { id, data: { name: 'task' } }, token: `tok-${id}` });
      } else {
        reply(sock, { reqId, ok: true, job: null });
      }
      return;
    }

    if (name === 'PULLB') {
      if (opts.serveJobs) {
        const count = Math.min((cmd.count as number | undefined) ?? 1, 10);
        const jobs: Array<Record<string, unknown>> = [];
        const tokens: string[] = [];
        for (let i = 0; i < count; i++) {
          const id = `job-${++jobSeq}`;
          jobs.push({ id, data: { name: 'task' } });
          tokens.push(`tok-${id}`);
        }
        reply(sock, { reqId, ok: true, jobs, tokens });
      } else {
        reply(sock, { reqId, ok: true, jobs: [], tokens: [] });
      }
      return;
    }

    // Auth, ACK, ACKB, FAIL, RegisterWorker, Heartbeat, JobHeartbeat, etc.
    reply(sock, { reqId, ok: true });
  };

  const server = Bun.listen<SockState>({
    hostname: 'localhost',
    port,
    socket: {
      open(sock) {
        accepted += 1;
        sock.data = { parser: new FrameParser(), dead: false };
        open.add(sock);
      },
      data(sock, data) {
        const state = sock.data;
        const frames = state.parser.addData(new Uint8Array(data));
        // Half-open: the kernel ACKs the bytes (socket stays "alive") but the
        // application never answers. Exactly what a suspended host / NAT drop
        // looks like to the client.
        if (state.dead) return;
        for (const frame of frames) {
          const cmd = unpack(frame) as Record<string, unknown>;
          respond(sock, cmd);
        }
      },
      close(sock) {
        open.delete(sock);
      },
      error(sock) {
        open.delete(sock);
      },
    },
  });
  activeServers.push(server);

  return {
    port,
    connectionsAccepted: () => accepted,
    liveConnections: () => [...open].filter((s) => !s.data.dead).length,
    goHalfOpen: () => {
      for (const sock of open) sock.data.dead = true;
    },
    stop: () => server.stop(true),
  };
}

afterEach(async () => {
  while (activeServers.length) {
    const s = activeServers.pop();
    try {
      s?.stop(true);
    } catch {
      /* already stopped */
    }
  }
  // Let force-closed sockets fire their client-side close/error handlers and any
  // residual command timeouts drain before the next test, so a stuck half-open
  // client from one test cannot perturb the timing of the next one.
  await Bun.sleep(150);
});

/** Poll `fn` until it returns true or the deadline elapses. */
async function waitFor(
  fn: () => boolean | Promise<boolean>,
  timeoutMs: number,
  intervalMs = 50
): Promise<boolean> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await fn()) return true;
    await Bun.sleep(intervalMs);
  }
  return false;
}

// ---------------------------------------------------------------------------
// Fast client config: shrink every timer so a normally-multi-minute recovery
// happens in well under a second.
// ---------------------------------------------------------------------------
const FAST = {
  pingInterval: 120,
  commandTimeout: 150,
  maxPingFailures: 2,
  reconnectDelay: 50,
  maxReconnectDelay: 200,
  connectTimeout: 1000,
  autoReconnect: true,
} as const;

describe('Issue #94: half-open TCP connection recovery', () => {
  it('TcpClient concludes a half-open socket is dead and attempts reconnect', async () => {
    const mock = startMockServer();
    const client = new TcpClient({ host: 'localhost', port: mock.port, ...FAST });

    let reconnecting = false;
    let unhealthy = false;
    client.on('reconnecting', () => {
      reconnecting = true;
    });
    client.on('health', (d) => {
      if (d.type === 'unhealthy') unhealthy = true;
    });

    await client.connect();
    // Sanity: a ping round-trips on the healthy connection.
    const pong = await client.send({ cmd: 'Ping' });
    expect((pong.data as { pong?: boolean }).pong).toBe(true);
    expect(mock.connectionsAccepted()).toBe(1);

    // Kill the connection silently — no FIN/RST, socket stays "alive".
    mock.goHalfOpen();

    // The health-check ping must FAIL repeatedly, conclude the link is dead,
    // and fire the reconnect path (regression guard for the ping-based path).
    const concluded = await waitFor(() => unhealthy && reconnecting, 4000, 50);

    client.close();

    expect(unhealthy).toBe(true);
    expect(reconnecting).toBe(true);
    expect(concluded).toBe(true);
  }, 10000);

  it('TcpClient fully recovers: a command issued after half-open eventually succeeds', async () => {
    const mock = startMockServer();
    const client = new TcpClient({ host: 'localhost', port: mock.port, ...FAST });

    await client.connect();
    // Round-trip first so the server's `open` handler has definitely run
    // (connect() resolves on the CLIENT open; the server counter is async).
    await client.send({ cmd: 'Ping' });
    expect(mock.connectionsAccepted()).toBe(1);

    mock.goHalfOpen();

    // A correctly-recovering client opens a fresh connection (which the server
    // serves normally) and Ping succeeds again. A stuck client never reconnects
    // and every Ping times out forever.
    const recovered = await waitFor(
      async () => {
        if (mock.connectionsAccepted() < 2) return false;
        try {
          const r = await client.send({ cmd: 'Ping' });
          return (r.data as { pong?: boolean }).pong === true;
        } catch {
          return false;
        }
      },
      6000,
      100
    );

    client.close();

    expect(mock.connectionsAccepted()).toBeGreaterThanOrEqual(2);
    expect(recovered).toBe(true);
  }, 12000);

  it('recovers from a half-open socket via command timeouts ALONE (health-ping disabled)', async () => {
    // This is the root-cause gap from the report: "repeated command timeouts
    // don't conclude the connection is dead." Today the ONLY thing that calls
    // forceReconnect() is the health-check ping. If pings are disabled (a
    // documented option, pingInterval=0) — or merely set longer than a worker's
    // command timeout — a worker that keeps issuing PULLs whose responses never
    // arrive will time out forever WITHOUT ever reconnecting.
    //
    // A worker pull loop is exactly "issue command → it times out → retry on the
    // same dead client". We model that loop directly. With ping OFF, the command
    // timeouts are the only possible recovery signal.
    const mock = startMockServer();
    const client = new TcpClient({
      host: 'localhost',
      port: mock.port,
      ...FAST,
      pingInterval: 0, // health-check ping DISABLED
    });

    await client.connect();
    await client.send({ cmd: 'Ping' });
    expect(mock.connectionsAccepted()).toBe(1);

    mock.goHalfOpen();

    // Keep issuing commands like a worker's pull loop. Each rejects with
    // "Command timeout" while the socket is half-open. The client must conclude
    // the link is dead from the timeouts alone and open a fresh connection.
    const recovered = await waitFor(
      async () => {
        try {
          const r = await client.send({ cmd: 'Ping' });
          return (r.data as { pong?: boolean }).pong === true && mock.connectionsAccepted() >= 2;
        } catch {
          return false;
        }
      },
      6000,
      60
    );

    client.close();

    expect(mock.connectionsAccepted()).toBeGreaterThanOrEqual(2);
    expect(recovered).toBe(true);
  }, 12000);

  it('Worker resumes job throughput after its connection goes half-open', async () => {
    const mock = startMockServer({ serveJobs: true });

    let processed = 0;
    const worker = new Worker(
      'q-issue-94',
      async () => {
        processed += 1;
        return { ok: true };
      },
      {
        embedded: false,
        concurrency: 2,
        connection: {
          host: 'localhost',
          port: mock.port,
          pingInterval: 120,
          commandTimeout: 150,
        },
      }
    );
    // A half-open link is expected to surface transient command timeouts before
    // the health tracker forces a reconnect. Workers use EventEmitter error
    // semantics, so keep the required listener attached while asserting that
    // throughput recovers after those transport errors.
    worker.on('error', () => undefined);

    // Warm-up: confirm the worker is actually pulling and processing.
    const warmed = await waitFor(() => processed > 3, 4000, 50);
    expect(warmed).toBe(true);

    // Sever every live connection silently.
    mock.goHalfOpen();
    const atHole = processed;

    // After the half-open, a healthy worker reconnects and throughput climbs
    // again. A stuck worker stays pinned at `atHole` forever (the reported
    // "throughput stays at 0 indefinitely").
    const resumed = await waitFor(() => processed > atHole + 5, 10000, 100);

    // Force-close: a graceful close() drains the buffer / flushes ACKs over the
    // (possibly still-reconnecting) link, which is irrelevant to what we assert
    // and only adds shutdown latency to the test.
    await worker.close(true);

    expect(resumed).toBe(true);
    expect(processed).toBeGreaterThan(atHole + 5);
  }, 15000);
});
