/**
 * Native TLS support — server (TCP + HTTP), client SDK, config resolution, CLI flags.
 *
 * Server: createTcpServer/createHttpServer accept `tls: { certFile, keyFile }`
 * and terminate TLS natively (no reverse proxy needed).
 * Client: ConnectionOptions accept `tls: true | { rejectUnauthorized?, caFile? }`.
 * Config: TLS_CERT_FILE/TLS_KEY_FILE env vars + server.tlsCertFile/tlsKeyFile
 * in bunqueue.config.ts; partial config (cert without key) must fail fast.
 *
 * Certificates are generated at runtime with openssl (SAN: localhost + 127.0.0.1).
 */

import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pack, unpack } from 'msgpackr';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';
import { createHttpServer } from '../src/infrastructure/server/http';
import { FrameParser } from '../src/infrastructure/server/protocol';
import { Queue, Worker, shutdownManager } from '../src/client';

const TLS_TCP_PORT = 18791;
const TLS_HTTP_PORT = 18792;
const PLAIN_TCP_PORT = 18793;

let certDir: string;
let CERT: string;
let KEY: string;

let qm: QueueManager;
let tlsTcpServer: TcpServer;
let plainTcpServer: TcpServer;
let tlsHttpServer: ReturnType<typeof createHttpServer>;

const open: Array<{ close: () => Promise<void> | void }> = [];

/** Open a raw socket (optionally TLS) to a port, send Ping, return the response or null. */
async function rawPing(
  port: number,
  tls: false | { rejectUnauthorized?: boolean; caFile?: string },
  host = '127.0.0.1',
  timeoutMs = 3000
): Promise<Record<string, unknown> | null> {
  return new Promise((resolve) => {
    const parser = new FrameParser();
    let done = false;
    const finish = (value: Record<string, unknown> | null, sock?: { end: () => void }) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      try {
        sock?.end();
      } catch {
        /* already closed */
      }
      resolve(value);
    };
    const timer = setTimeout(() => finish(null), timeoutMs);

    const connectOpts: Record<string, unknown> = {
      hostname: host,
      port,
      socket: {
        open(sock: { write: (d: Uint8Array) => void }) {
          sock.write(FrameParser.frame(pack({ cmd: 'Ping', reqId: 'tls-test' })));
        },
        data(sock: { end: () => void }, data: Buffer) {
          try {
            const frames = parser.addData(new Uint8Array(data));
            for (const frame of frames) {
              finish(unpack(frame) as Record<string, unknown>, sock);
              return;
            }
          } catch {
            finish(null, sock);
          }
        },
        close() {
          finish(null);
        },
        error() {
          finish(null);
        },
        connectError() {
          finish(null);
        },
      },
    };
    if (tls) {
      connectOpts.tls = {
        ...(tls.rejectUnauthorized !== undefined && { rejectUnauthorized: tls.rejectUnauthorized }),
        ...(tls.caFile !== undefined && { ca: Bun.file(tls.caFile) }),
      };
    }
    void (Bun.connect as (o: unknown) => Promise<unknown>)(connectOpts).catch(() => finish(null));
  });
}

beforeAll(() => {
  certDir = mkdtempSync(join(tmpdir(), 'bq-tls-test-'));
  CERT = join(certDir, 'cert.pem');
  KEY = join(certDir, 'key.pem');
  const gen = Bun.spawnSync([
    'openssl', 'req', '-x509', '-newkey', 'rsa:2048',
    '-keyout', KEY, '-out', CERT, '-days', '7', '-nodes',
    '-subj', '/CN=localhost',
    '-addext', 'subjectAltName=DNS:localhost,IP:127.0.0.1',
  ]);
  if (gen.exitCode !== 0) {
    throw new Error(`openssl cert generation failed: ${gen.stderr.toString()}`);
  }

  qm = new QueueManager(); // in-memory
  tlsTcpServer = createTcpServer(qm, {
    port: TLS_TCP_PORT,
    hostname: '127.0.0.1',
    tls: { certFile: CERT, keyFile: KEY },
  });
  plainTcpServer = createTcpServer(qm, { port: PLAIN_TCP_PORT, hostname: '127.0.0.1' });
  tlsHttpServer = createHttpServer(qm, {
    port: TLS_HTTP_PORT,
    hostname: '127.0.0.1',
    tls: { certFile: CERT, keyFile: KEY },
  });
});

afterAll(async () => {
  for (const c of open) await c.close();
  tlsTcpServer.stop();
  plainTcpServer.stop();
  tlsHttpServer.stop();
  qm.shutdown();
  shutdownManager();
  rmSync(certDir, { recursive: true, force: true });
});

describe('TCP server with TLS', () => {
  test('TLS client completes handshake and gets Ping response', async () => {
    const resp = await rawPing(TLS_TCP_PORT, { rejectUnauthorized: false });
    expect(resp).not.toBeNull();
    expect(resp?.ok).toBe(true);
  });

  test('TLS client with caFile verifies the server certificate (localhost SAN)', async () => {
    const resp = await rawPing(TLS_TCP_PORT, { caFile: CERT }, 'localhost');
    expect(resp).not.toBeNull();
    expect(resp?.ok).toBe(true);
  });

  test('plaintext client gets no valid response from TLS server', async () => {
    const resp = await rawPing(TLS_TCP_PORT, false, '127.0.0.1', 1500);
    expect(resp).toBeNull();
  });

  test('plain server unchanged: plaintext client still works (regression guard)', async () => {
    const resp = await rawPing(PLAIN_TCP_PORT, false);
    expect(resp).not.toBeNull();
    expect(resp?.ok).toBe(true);
  });

  test('missing cert file fails fast with a clear error', () => {
    expect(() =>
      createTcpServer(qm, {
        port: 18799,
        hostname: '127.0.0.1',
        tls: { certFile: join(certDir, 'nope.pem'), keyFile: KEY },
      })
    ).toThrow(/cert/i);
  });

  test('missing key file fails fast with a clear error', () => {
    expect(() =>
      createTcpServer(qm, {
        port: 18799,
        hostname: '127.0.0.1',
        tls: { certFile: CERT, keyFile: join(certDir, 'nope.pem') },
      })
    ).toThrow(/key/i);
  });
});

describe('Queue/Worker SDK over TLS', () => {
  test('Queue.add + getWaitingAsync roundtrip over TLS', async () => {
    const q = new Queue<{ n: number }>('tls-roundtrip', {
      embedded: false,
      connection: { host: '127.0.0.1', port: TLS_TCP_PORT, tls: { rejectUnauthorized: false } },
      autoBatch: { enabled: false },
    });
    open.push(q);

    await q.add('job', { n: 42 });
    const waiting = await q.getWaitingAsync<{ n: number }>();
    expect(waiting.length).toBe(1);
    expect(waiting[0].data.n).toBe(42);
  });

  test('Worker processes a job over TLS', async () => {
    const q = new Queue<{ msg: string }>('tls-worker', {
      embedded: false,
      connection: { host: '127.0.0.1', port: TLS_TCP_PORT, tls: { rejectUnauthorized: false } },
      autoBatch: { enabled: false },
    });
    open.push(q);
    await q.add('task', { msg: 'hello-tls' });

    const processed: string[] = [];
    const worker = new Worker<{ msg: string }>(
      'tls-worker',
      async (job) => {
        processed.push(job.data.msg);
        return { done: true };
      },
      {
        embedded: false,
        connection: { host: '127.0.0.1', port: TLS_TCP_PORT, tls: { rejectUnauthorized: false } },
        concurrency: 1,
        heartbeatInterval: 0,
      }
    );
    open.push(worker);

    const deadline = Date.now() + 10_000;
    while (processed.length < 1 && Date.now() < deadline) {
      await Bun.sleep(50);
    }
    expect(processed).toEqual(['hello-tls']);
  }, 15_000);

  test('SDK without tls option cannot talk to TLS server (fails, no hang)', async () => {
    const q = new Queue('tls-plain-fail', {
      embedded: false,
      connection: { host: '127.0.0.1', port: TLS_TCP_PORT, commandTimeout: 1500 },
      autoBatch: { enabled: false },
    });
    open.push(q);
    expect(q.add('job', { x: 1 })).rejects.toThrow();
  });
});

describe('HTTP server with TLS', () => {
  test('https /health responds', async () => {
    const resp = await fetch(`https://127.0.0.1:${TLS_HTTP_PORT}/health`, {
      // @ts-expect-error Bun-specific fetch option
      tls: { rejectUnauthorized: false },
      signal: AbortSignal.timeout(3000),
    });
    expect(resp.ok).toBe(true);
    const body = (await resp.json()) as { ok?: boolean; status?: string };
    expect(body).toBeTruthy();
  });

  test('plain http against TLS HTTP server fails', async () => {
    expect(
      fetch(`http://127.0.0.1:${TLS_HTTP_PORT}/health`, { signal: AbortSignal.timeout(1500) })
    ).rejects.toThrow();
  });
});

describe('TLS config resolution', () => {
  test('resolveServerConfig reads TLS_CERT_FILE / TLS_KEY_FILE env vars', async () => {
    const { resolveServerConfig } = await import('../src/config');
    const prevCert = Bun.env.TLS_CERT_FILE;
    const prevKey = Bun.env.TLS_KEY_FILE;
    Bun.env.TLS_CERT_FILE = '/x/cert.pem';
    Bun.env.TLS_KEY_FILE = '/x/key.pem';
    try {
      const cfg = resolveServerConfig(null) as unknown as Record<string, unknown>;
      expect(cfg.tlsCertFile).toBe('/x/cert.pem');
      expect(cfg.tlsKeyFile).toBe('/x/key.pem');
    } finally {
      if (prevCert === undefined) delete Bun.env.TLS_CERT_FILE;
      else Bun.env.TLS_CERT_FILE = prevCert;
      if (prevKey === undefined) delete Bun.env.TLS_KEY_FILE;
      else Bun.env.TLS_KEY_FILE = prevKey;
    }
  });

  test('config file server.tlsCertFile/tlsKeyFile wins over env', async () => {
    const { resolveServerConfig } = await import('../src/config');
    const prevCert = Bun.env.TLS_CERT_FILE;
    Bun.env.TLS_CERT_FILE = '/env/cert.pem';
    try {
      const cfg = resolveServerConfig({
        server: { tlsCertFile: '/file/cert.pem', tlsKeyFile: '/file/key.pem' },
      }) as unknown as Record<string, unknown>;
      expect(cfg.tlsCertFile).toBe('/file/cert.pem');
      expect(cfg.tlsKeyFile).toBe('/file/key.pem');
    } finally {
      if (prevCert === undefined) delete Bun.env.TLS_CERT_FILE;
      else Bun.env.TLS_CERT_FILE = prevCert;
    }
  });

  test('resolveTlsServerOptions: null when unset, throws on partial config', async () => {
    const mod = (await import('../src/config')) as unknown as Record<string, unknown>;
    const resolveTls = mod.resolveTlsServerOptions as
      | ((cfg: { tlsCertFile?: string; tlsKeyFile?: string }) => unknown)
      | undefined;
    expect(typeof resolveTls).toBe('function');
    expect(resolveTls?.({})).toBeNull();
    expect(resolveTls?.({ tlsCertFile: CERT, tlsKeyFile: KEY })).toEqual({
      certFile: CERT,
      keyFile: KEY,
    });
    expect(() => resolveTls?.({ tlsCertFile: CERT })).toThrow(/key/i);
    expect(() => resolveTls?.({ tlsKeyFile: KEY })).toThrow(/cert/i);
  });
});

describe('CLI global TLS flags', () => {
  async function parseWith(argv: string[]): Promise<Record<string, unknown>> {
    const { parseGlobalOptions } = await import('../src/cli/index');
    const prev = process.argv;
    process.argv = ['bun', 'cli', ...argv];
    try {
      const { options } = parseGlobalOptions();
      return options as unknown as Record<string, unknown>;
    } finally {
      process.argv = prev;
    }
  }

  test('--tls enables TLS with verification on', async () => {
    const options = await parseWith(['stats', '--tls']);
    expect(options.tls).toEqual(true);
  });

  test('--tls-ca implies TLS and carries the CA file', async () => {
    const options = await parseWith(['stats', '--tls-ca', '/ca.pem']);
    expect(options.tls).toEqual({ caFile: '/ca.pem' });
  });

  test('--tls-no-verify implies TLS with rejectUnauthorized=false', async () => {
    const options = await parseWith(['stats', '--tls-no-verify']);
    expect(options.tls).toEqual({ rejectUnauthorized: false });
  });

  test('no TLS flags → tls undefined (plaintext default unchanged)', async () => {
    const options = await parseWith(['stats']);
    expect(options.tls).toBeUndefined();
  });

  test('--tls-ca followed by another flag does not swallow it', async () => {
    const options = await parseWith(['stats', '--tls-ca', '--json']);
    expect(options.tls).toBeUndefined(); // ca ignored with warning
    expect(options.json).toBe(true); // --json still parsed
  });

  test('server flags --tls-cert/--tls-key pass through to command args', async () => {
    const { parseGlobalOptions } = await import('../src/cli/index');
    const prev = process.argv;
    process.argv = ['bun', 'cli', 'start', '--tls-cert', '/c.pem', '--tls-key', '/k.pem'];
    try {
      const { options, commandArgs } = parseGlobalOptions();
      expect(options.tls).toBeUndefined();
      expect(commandArgs).toContain('--tls-cert');
      expect(commandArgs).toContain('--tls-key');
    } finally {
      process.argv = prev;
    }
  });
});

describe('TLS pool/client sharing isolation', () => {
  test('shared pools for TLS and plaintext to the same host:port are distinct', async () => {
    const { getSharedPool } = await import('../src/client/tcpPool');
    const plain = getSharedPool({ host: '127.0.0.1', port: 19111 });
    const tls = getSharedPool({ host: '127.0.0.1', port: 19111, tls: { caFile: '/ca.pem' } });
    try {
      expect(plain).not.toBe(tls);
    } finally {
      plain.close();
      tls.close();
    }
  });

  test('shared TCP clients for TLS and plaintext targets are distinct', async () => {
    const { getSharedTcpClient, closeSharedTcpClient } = await import('../src/client/tcp');
    const plain = getSharedTcpClient({ host: '127.0.0.1', port: 19112 });
    const tls = getSharedTcpClient({ host: '127.0.0.1', port: 19112, tls: true });
    try {
      expect(plain).not.toBe(tls);
      // Same config → same instance (sharing still works)
      expect(getSharedTcpClient({ host: '127.0.0.1', port: 19112 })).toBe(plain);
    } finally {
      closeSharedTcpClient();
    }
  });
});
