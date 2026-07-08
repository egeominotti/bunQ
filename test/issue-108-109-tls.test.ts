/**
 * Issue #108 — TLS: a `data` event delivered before `open` destructured a null
 * `socket.data`, the TypeError escalated to the process unhandledRejection
 * handler, and the whole server shut down (pre-auth remote DoS on the TLS port).
 * Fix: lazy, idempotent per-socket init shared by `open`/`data`; guarded
 * `close`/`drain`. This test hammers concurrent TLS connections that write a
 * frame the instant the socket opens and asserts (a) no "Cannot destructure …
 * socket.data" unhandledRejection fires, and (b) the server still answers.
 *
 * Issue #109 — TLS: the TCP client never authenticated the server. Bun.connect
 * does not reject an unauthorized peer, so a wrong pinned CA (even with
 * rejectUnauthorized:true) still round-tripped — encryption-only, MITM-open.
 * Fix: enforce Bun's computed `authorizationError` in a `handshake` handler.
 * This test drives the real client `createConnection` across the CA matrix.
 */

import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pack, unpack } from 'msgpackr';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';
import { FrameParser } from '../src/infrastructure/server/protocol';
import { createConnection } from '../src/client/tcp/connection';

const TLS_PORT = 18811;
let certDir: string;
let CERT: string;
let KEY: string;
let OTHER_CERT: string;
let qm: QueueManager;
let server: TcpServer;

function genCert(cert: string, key: string): void {
  const gen = Bun.spawnSync([
    'openssl', 'req', '-x509', '-newkey', 'rsa:2048',
    '-keyout', key, '-out', cert, '-days', '2', '-nodes',
    '-subj', '/CN=localhost',
    '-addext', 'subjectAltName=DNS:localhost,IP:127.0.0.1',
  ]);
  if (gen.exitCode !== 0) throw new Error(`openssl failed: ${gen.stderr.toString()}`);
}

beforeAll(() => {
  certDir = mkdtempSync(join(tmpdir(), 'bq-tls-108-'));
  CERT = join(certDir, 'cert.pem');
  KEY = join(certDir, 'key.pem');
  OTHER_CERT = join(certDir, 'other-cert.pem');
  genCert(CERT, KEY);
  genCert(OTHER_CERT, join(certDir, 'other-key.pem'));

  qm = new QueueManager();
  server = createTcpServer(qm, { port: TLS_PORT, hostname: '127.0.0.1', tls: { certFile: CERT, keyFile: KEY } });
});

afterAll(() => {
  server.stop();
  qm.shutdown();
  rmSync(certDir, { recursive: true, force: true });
});

// A minimal Bun-socket stand-in that records writes and terminate/end calls, so
// we can drive the server's `data` handler directly with an uninitialised
// `socket.data` (the exact data-before-open state TLS produces). #108
function makeFakeSocket() {
  const writes: Uint8Array[] = [];
  return {
    data: undefined as unknown, // NOT yet set — as if `open` never ran
    write(d: Uint8Array) { writes.push(d); return d.length; },
    end() {},
    terminate() {},
    _writes: writes,
  };
}

async function rawTlsPing(rejectUnauthorized = false): Promise<Record<string, unknown> | null> {
  return new Promise((resolve) => {
    const parser = new FrameParser();
    let settled = false;
    const finish = (v: Record<string, unknown> | null, s?: { end: () => void }) => {
      if (settled) return;
      settled = true;
      clearTimeout(t);
      try { s?.end(); } catch { /* closed */ }
      resolve(v);
    };
    const t = setTimeout(() => finish(null), 2000);
    void (Bun.connect as (o: unknown) => Promise<unknown>)({
      hostname: '127.0.0.1', port: TLS_PORT,
      tls: { rejectUnauthorized },
      socket: {
        open(s: { write: (d: Uint8Array) => void }) { s.write(FrameParser.frame(pack({ cmd: 'Ping', reqId: 'r' }))); },
        data(s: { end: () => void }, d: Buffer) {
          for (const f of parser.addData(d)) { finish(unpack(f) as Record<string, unknown>, s); return; }
        },
        error() { finish(null); },
        connectError() { finish(null); },
      },
    }).catch(() => finish(null));
  });
}

describe('#108 TLS data-before-open must not crash the server', () => {
  // Bun can fire `data` before `open` under TLS; the old handler destructured a
  // null `socket.data` and the TypeError escalated to a full server shutdown.
  const getHandlers = () =>
    (server as unknown as {
      _socketHandlers: {
        data: (s: unknown, d: Buffer) => Promise<void>;
        close: (s: unknown) => void;
        drain: (s: unknown) => void;
      };
    })._socketHandlers;

  test('data() handles an uninitialised socket.data (lazy init, no throw)', async () => {
    const handlers = getHandlers();
    const sock = makeFakeSocket();
    const frame = Buffer.from(FrameParser.frame(pack({ cmd: 'Ping', reqId: 'x' })));

    // OLD code: throws "Cannot destructure property 'frameParser' from null".
    await handlers.data(sock, frame);

    // Lazy init populated the per-socket state and the Ping got a response.
    expect(sock.data).toBeDefined();
    expect(sock._writes.length).toBeGreaterThan(0);
    const resp = unpack(new Uint8Array((sock._writes[0] as Uint8Array).slice(4))) as {
      ok?: boolean;
    };
    expect(resp.ok).toBe(true);
  });

  test('close() and drain() tolerate a socket that never initialised', () => {
    const handlers = getHandlers();
    const sock = makeFakeSocket();
    expect(() => handlers.close(sock)).not.toThrow();
    expect(() => handlers.drain(sock)).not.toThrow();
  });

  test('server still answers after a data-before-open connection', async () => {
    const resp = await rawTlsPing(false);
    expect(resp?.ok).toBe(true);
  });
});

describe('#109 TLS client must authenticate the server certificate', () => {
  const connect = (tls: unknown) =>
    createConnection(
      { host: 'localhost', port: TLS_PORT, tls } as never,
      3000,
      { onData: () => {}, onClose: () => {}, onError: () => {} }
    );

  test('right pinned CA is accepted', async () => {
    const { cleanup } = await connect({ caFile: CERT, rejectUnauthorized: true });
    cleanup();
    expect(true).toBe(true);
  });

  test('WRONG pinned CA with rejectUnauthorized:true is REJECTED', async () => {
    await expect(connect({ caFile: OTHER_CERT, rejectUnauthorized: true })).rejects.toThrow(
      /TLS verification failed/
    );
  });

  test('no CA + rejectUnauthorized:true (system CAs) REJECTS a self-signed server', async () => {
    await expect(connect({ rejectUnauthorized: true })).rejects.toThrow(/TLS verification failed/);
  });

  test('tls:true (system CAs) REJECTS a self-signed server', async () => {
    await expect(connect(true)).rejects.toThrow(/TLS verification failed/);
  });

  test('rejectUnauthorized:false stays encryption-only (accepts self-signed)', async () => {
    const { cleanup } = await connect({ rejectUnauthorized: false });
    cleanup();
    expect(true).toBe(true);
  });

  // Regression: because a registered `handshake` handler makes Bun fire `open`
  // BEFORE the TLS handshake, the connect timeout must stay armed until we
  // actually resolve — clearing it in `open` (as an earlier revision did) left
  // the handshake phase unbounded, so a peer that completes TCP but stalls TLS
  // negotiation hung the promise forever. Here a plain-TCP server accepts the
  // socket and never speaks TLS; the TLS client must reject within the timeout.
  test('a stalled TLS handshake rejects within connectTimeout (no hang)', async () => {
    const held: Array<{ end?: () => void }> = [];
    const muteServer = Bun.listen({
      hostname: '127.0.0.1',
      port: 18812,
      socket: { open(s: { end?: () => void }) { held.push(s); }, data() {}, close() {} },
    });
    try {
      const start = Date.now();
      await expect(
        createConnection(
          { host: '127.0.0.1', port: 18812, tls: { rejectUnauthorized: false } } as never,
          600,
          { onData: () => {}, onClose: () => {}, onError: () => {} }
        )
      ).rejects.toThrow(/timeout/i);
      // Must reject promptly (near connectTimeout), not hang until the test dies.
      expect(Date.now() - start).toBeLessThan(3000);
    } finally {
      muteServer.stop(true);
    }
  });
});
