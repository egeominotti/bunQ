/**
 * FUZZ (category 5) — TCP wire-protocol fuzzing.
 *
 * Goal: prove the server never crashes, never leaks the connection into a wedged
 * state, and never violates the pre-auth gate — no matter what garbage arrives
 * on the socket. Every case here writes RAW bytes with `Bun.connect`, bypassing
 * TcpClient so we control the exact wire image.
 *
 * Wire format (src/infrastructure/server/protocol.ts):
 *   frame = u32 BE length prefix + msgpack(body)     (msgpackr pack/unpack)
 *   MAX_FRAME_SIZE = 64MB → FrameSizeError → error response + socket.end()
 *   decode error → errorResponse('Invalid command format') (socket stays alive)
 *   !cmd.cmd     → errorResponse('Invalid command')
 *   unknown cmd  → error 'Unknown command: X'
 *
 * The invariant under test across ALL cases: after we throw garbage at a
 * connection, the SERVER PROCESS is still up and a FRESH connection can still
 * complete a normal PUSH/PULL round-trip. A crash (unhandledRejection taking the
 * process down, #108 class) or a wedged accept loop fails that invariant.
 */

import { describe, it, expect, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';
import { TcpClient } from '../src/client/tcp/client';
import { FrameParser } from '../src/infrastructure/server/protocol';
import { pack, unpack } from 'msgpackr';

function randomPort(): number {
  return 30000 + Math.floor(Math.random() * 25000);
}
/** A port confirmed free right now (bind+close), to avoid cross-test collisions. */
function getFreePort(): number {
  for (let i = 0; i < 50; i++) {
    const port = randomPort();
    try {
      const l = Bun.listen({ hostname: '127.0.0.1', port, socket: { data() {} } });
      l.stop();
      return port;
    } catch {
      /* taken — try another */
    }
  }
  throw new Error('no free port found');
}

interface Harness {
  qm: QueueManager;
  server: TcpServer;
  port: number;
  clients: TcpClient[];
  raws: Array<{ end: () => void }>;
}

let harness: Harness | null = null;

function startServer(authTokens?: string[], idleTimeoutMs = 5000): Harness {
  const qm = new QueueManager();
  const port = getFreePort();
  const server = createTcpServer(qm, {
    hostname: '127.0.0.1',
    port,
    authTokens,
    idleTimeoutMs,
  });
  const h: Harness = { qm, server, port, clients: [], raws: [] };
  harness = h;
  return h;
}

async function openClient(h: Harness, token?: string): Promise<TcpClient> {
  const client = new TcpClient({
    host: '127.0.0.1',
    port: h.port,
    token,
    autoReconnect: false,
    pingInterval: 0,
    commandTimeout: 2000,
    connectTimeout: 2000,
  });
  await client.connect();
  h.clients.push(client);
  return client;
}

/**
 * A minimal raw TCP peer. Collects inbound bytes, parses them into frames
 * (msgpack decoded), and lets a test write arbitrary bytes. No framing help on
 * the write side — you send exactly what you pass.
 */
interface RawPeer {
  write(bytes: Uint8Array): void;
  /** Wait until at least one decoded response frame is received (or timeout). */
  nextResponse(timeoutMs?: number): Promise<Record<string, unknown> | null>;
  responses(): Record<string, unknown>[];
  closed(): boolean;
  end(): void;
}

async function openRaw(h: Harness): Promise<RawPeer> {
  const parser = new FrameParser();
  const responses: Record<string, unknown>[] = [];
  // Consume cursor: nextResponse() returns the next UNCONSUMED response, so a
  // response that arrived before the call is not missed (the bug a snapshot of
  // responses.length would cause on fast round-trips).
  let consumed = 0;
  let isClosed = false;

  // Outbound backpressure buffer: Bun's socket.write may accept fewer bytes than
  // offered (large writes) or defer flushing (tiny writes). We buffer the tail
  // and flush it on `drain`, mirroring how a real client must behave. Without
  // this a 2MB frame is truncated on the wire and the server sees a permanent
  // partial frame (slowloris timer kills it) — a harness bug, not a server bug.
  let pending = new Uint8Array(0);
  const pump = (sock: { write: (b: Uint8Array) => number; flush?: () => void }) => {
    if (pending.length === 0) return;
    const written = sock.write(pending);
    pending = written >= pending.length ? new Uint8Array(0) : pending.slice(written);
    sock.flush?.();
  };

  const socket = await Bun.connect({
    hostname: '127.0.0.1',
    port: h.port,
    socket: {
      data(_s, data: Buffer) {
        let frames: Uint8Array[];
        try {
          frames = parser.addData(data);
        } catch {
          return; // FrameSizeError on the client parser — irrelevant here
        }
        for (const f of frames) {
          try {
            responses.push(unpack(f) as Record<string, unknown>);
          } catch {
            /* ignore undecodable inbound */
          }
        }
      },
      drain(s) {
        pump(s as unknown as { write: (b: Uint8Array) => number; flush?: () => void });
      },
      close() {
        isClosed = true;
      },
      error() {
        isClosed = true;
      },
    },
  });

  const peer: RawPeer = {
    write(bytes: Uint8Array) {
      // Append to the pending tail then try to flush as much as possible now.
      if (pending.length === 0) {
        pending = bytes;
      } else {
        const merged = new Uint8Array(pending.length + bytes.length);
        merged.set(pending);
        merged.set(bytes, pending.length);
        pending = merged;
      }
      pump(socket as unknown as { write: (b: Uint8Array) => number; flush?: () => void });
    },
    async nextResponse(timeoutMs = 1500) {
      const start = Date.now();
      for (;;) {
        if (consumed < responses.length) return responses[consumed++] ?? null;
        if (Date.now() - start >= timeoutMs) return null;
        await Bun.sleep(10);
      }
    },
    responses: () => responses,
    closed: () => isClosed,
    end() {
      try {
        socket.end();
      } catch {
        /* ignore */
      }
    },
  };
  h.raws.push(peer);
  return peer;
}

/** Frame a raw msgpack body (helper mirroring FrameParser.frame). */
function frameOf(body: Uint8Array): Uint8Array {
  return FrameParser.frame(body);
}

/** A u32-BE length prefix in front of an arbitrary payload (no msgpack help). */
function withLenPrefix(len: number, payload: Uint8Array): Uint8Array {
  const out = new Uint8Array(4 + payload.length);
  out[0] = (len >>> 24) & 0xff;
  out[1] = (len >>> 16) & 0xff;
  out[2] = (len >>> 8) & 0xff;
  out[3] = len & 0xff;
  out.set(payload, 4);
  return out;
}

/** Prove the server is still alive: a fresh client completes PUSH+PULL. */
async function assertServerAlive(h: Harness, token?: string): Promise<void> {
  const c = await openClient(h, token);
  const q = `alive-${Math.random().toString(36).slice(2)}`;
  const push = await c.send({ cmd: 'PUSH', queue: q, data: { ping: 1 } });
  expect(push.ok).toBe(true);
  const pull = await c.send({ cmd: 'PULL', queue: q });
  expect(pull.ok).toBe(true);
  expect((pull.job as { data: { ping: number } } | undefined)?.data?.ping).toBe(1);
}

afterEach(async () => {
  if (harness) {
    for (const c of harness.clients) {
      try {
        c.close();
      } catch {
        /* ignore */
      }
    }
    for (const r of harness.raws) {
      try {
        r.end();
      } catch {
        /* ignore */
      }
    }
    try {
      harness.server.stop();
    } catch {
      /* ignore */
    }
    try {
      harness.qm.shutdown();
    } catch {
      /* ignore */
    }
    harness = null;
  }
  await Bun.sleep(100);
});

describe('FUZZ protocol — malformed frames never crash or wedge the server', () => {
  it('corrupt msgpack body (valid length prefix, garbage payload) → error, socket alive, server alive', async () => {
    const h = startServer();
    const raw = await openRaw(h);

    // Length prefix says 8 bytes, payload is 8 bytes of non-msgpack garbage.
    const garbage = new Uint8Array([0xff, 0xff, 0xff, 0xff, 0x00, 0x01, 0x02, 0x03]);
    raw.write(withLenPrefix(garbage.length, garbage));

    const resp = await raw.nextResponse();
    expect(resp).not.toBeNull();
    expect(resp?.ok).toBe(false);
    expect(String(resp?.error)).toContain('Invalid command format');
    expect(raw.closed()).toBe(false); // decode error must NOT close the socket

    await assertServerAlive(h);
  });

  it('valid msgpack but not an object (e.g. a bare integer / array / string)', async () => {
    const h = startServer();
    const raw = await openRaw(h);

    const bodies = [pack(42), pack('hello'), pack([1, 2, 3]), pack(null), pack(true)];
    for (const body of bodies) {
      raw.write(frameOf(body));
    }
    // EACH frame must be individually rejected (no .cmd) without crashing —
    // consume one response per frame and assert every one is an error.
    for (let i = 0; i < bodies.length; i++) {
      const resp = await raw.nextResponse();
      expect(resp?.ok).toBe(false);
    }
    expect(raw.closed()).toBe(false);

    await assertServerAlive(h);
  });

  it('object without a `cmd` field → Invalid command', async () => {
    const h = startServer();
    const raw = await openRaw(h);
    raw.write(frameOf(pack({ notCmd: 'PUSH', queue: 'x', reqId: 'r1' })));
    const resp = await raw.nextResponse();
    expect(resp?.ok).toBe(false);
    expect(String(resp?.error)).toContain('Invalid command');
    await assertServerAlive(h);
  });

  it('unknown command string → Unknown command error, connection reusable', async () => {
    const h = startServer();
    const raw = await openRaw(h);
    raw.write(frameOf(pack({ cmd: 'TOTALLY_NOT_A_COMMAND', reqId: 'r2' })));
    const resp = await raw.nextResponse();
    expect(resp?.ok).toBe(false);
    expect(String(resp?.error)).toContain('Unknown command');
    // Same socket must still serve a valid command afterwards.
    raw.write(frameOf(pack({ cmd: 'Ping', reqId: 'r3' })));
    const ping = await raw.nextResponse();
    expect(ping).not.toBeNull();
    expect(raw.closed()).toBe(false);
  });

  it('cmd field present but non-string type (number/object) does not throw', async () => {
    const h = startServer();
    const raw = await openRaw(h);
    raw.write(frameOf(pack({ cmd: 12345, reqId: 'r4' })));
    raw.write(frameOf(pack({ cmd: { nested: true }, reqId: 'r5' })));
    // Both non-string cmd frames must be rejected (not routed, not crashed).
    for (let i = 0; i < 2; i++) {
      const resp = await raw.nextResponse();
      expect(resp?.ok).toBe(false);
    }
    await assertServerAlive(h);
  });

  it('zero-length frame (length prefix = 0) → unpack of empty buffer handled', async () => {
    const h = startServer();
    const raw = await openRaw(h);
    raw.write(withLenPrefix(0, new Uint8Array(0)));
    // Server should respond with an error (empty body is not a valid command)
    // OR silently ignore — either way it must NOT crash and the socket survives.
    await Bun.sleep(200);
    expect(raw.closed()).toBe(false);
    await assertServerAlive(h);
  });

  it('length prefix LIES (says 1000, only 4 bytes sent) → server waits, does not crash, slowloris timer eventually closes', async () => {
    const h = startServer(undefined, 500); // aggressive slowloris timeout
    const raw = await openRaw(h);
    // Declare a 1000-byte frame, send only 4 bytes of it. Legal partial frame.
    raw.write(withLenPrefix(1000, new Uint8Array([1, 2, 3, 4])));
    // Server must not respond (frame incomplete) and must not crash.
    const resp = await raw.nextResponse(300);
    expect(resp).toBeNull();
    // Slowloris mitigation closes the stalled partial-frame connection.
    await Bun.sleep(700);
    expect(raw.closed()).toBe(true);
    // The rest of the server is unharmed.
    await assertServerAlive(h);
  });

  it('frame length exceeds MAX_FRAME_SIZE (declares >64MB) → error + socket.end(), server survives', async () => {
    const h = startServer();
    const raw = await openRaw(h);
    // Declare a frame just over 64MB without actually sending the bytes.
    const huge = 64 * 1024 * 1024 + 1;
    raw.write(withLenPrefix(huge, new Uint8Array([0, 0, 0, 0])));
    const resp = await raw.nextResponse();
    expect(resp?.ok).toBe(false);
    expect(String(resp?.error)).toContain('Frame too large');
    // Server ends this socket; a fresh one still works.
    await assertServerAlive(h);
  });

  it('giant-but-legal payload (~2MB msgpack) is accepted and processed', async () => {
    const h = startServer();
    const raw = await openRaw(h);
    // A legal, in-bounds large job: a 2MB string. Under the 10MB job-data cap
    // and the 64MB frame cap, so it must be accepted.
    const big = 'x'.repeat(2 * 1024 * 1024);
    raw.write(frameOf(pack({ cmd: 'PUSH', queue: 'big-q', data: { blob: big }, reqId: 'big' })));
    const resp = await raw.nextResponse(4000);
    expect(resp).not.toBeNull();
    expect(resp?.ok).toBe(true);
    expect(raw.closed()).toBe(false);
  });

  it('torn frames: a single valid frame split across many tiny writes reassembles correctly', async () => {
    const h = startServer();
    const raw = await openRaw(h);
    const full = frameOf(pack({ cmd: 'Ping', reqId: 'torn' }));
    // Dribble one byte at a time — exercises the partial-frame buffer path.
    for (const b of full) {
      raw.write(new Uint8Array([b]));
      await Bun.sleep(2);
    }
    const resp = await raw.nextResponse(2000);
    expect(resp).not.toBeNull();
    expect(raw.closed()).toBe(false);
  });

  it('coalesced frames: many valid frames in ONE write are all processed (pipelining)', async () => {
    const h = startServer();
    const raw = await openRaw(h);
    const N = 50;
    const parts: Uint8Array[] = [];
    for (let i = 0; i < N; i++) {
      parts.push(frameOf(pack({ cmd: 'PUSH', queue: 'coalesced', data: { i }, reqId: `c${i}` })));
    }
    const total = parts.reduce((n, p) => n + p.length, 0);
    const merged = new Uint8Array(total);
    let off = 0;
    for (const p of parts) {
      merged.set(p, off);
      off += p.length;
    }
    raw.write(merged);
    // Wait for all responses.
    const deadline = Date.now() + 4000;
    while (raw.responses().length < N && Date.now() < deadline) await Bun.sleep(20);
    expect(raw.responses().length).toBe(N);
    expect(raw.responses().every((r) => r.ok === true)).toBe(true);
  });

  it('random-byte fuzz barrage (100 random frames) never crashes the server', async () => {
    const h = startServer();
    const raw = await openRaw(h);
    // Deterministic PRNG (no Math.random in the frame content path so failures
    // are reproducible): a simple LCG seeded by index.
    let seed = 0x12345678;
    const rnd = () => {
      seed = (seed * 1103515245 + 12345) & 0x7fffffff;
      return seed;
    };
    for (let i = 0; i < 100; i++) {
      const len = rnd() % 64; // small bodies
      const body = new Uint8Array(len);
      for (let j = 0; j < len; j++) body[j] = rnd() & 0xff;
      raw.write(withLenPrefix(len, body));
    }
    await Bun.sleep(500);
    // The server must still answer a fresh, valid client.
    await assertServerAlive(h);
  });
});

describe('FUZZ protocol — pre-auth gate (AUTH_TOKENS set)', () => {
  it('mutating commands are rejected pre-auth and do NOT mutate state; Auth unlocks', async () => {
    const h = startServer(['s3cr3t']);
    const raw = await openRaw(h);

    // PUSH before Auth → Not authenticated, and no job must be created.
    raw.write(frameOf(pack({ cmd: 'PUSH', queue: 'gated', data: { x: 1 }, reqId: 'p1' })));
    const denied = await raw.nextResponse();
    expect(denied?.ok).toBe(false);
    expect(String(denied?.error)).toContain('Not authenticated');

    // Verify nothing landed: an authenticated client sees an empty queue.
    const authed = await openClient(h, 's3cr3t');
    const counts = await authed.send({ cmd: 'GetJobCounts', queue: 'gated' });
    expect(counts.ok).toBe(true);
    const c = counts.counts as Record<string, number> | undefined;
    expect((c?.waiting ?? 0) + (c?.active ?? 0)).toBe(0);

    // Now Auth on the raw socket, then PUSH succeeds.
    raw.write(frameOf(pack({ cmd: 'Auth', token: 's3cr3t', reqId: 'a1' })));
    const authResp = await raw.nextResponse();
    expect(authResp?.ok).toBe(true);
    raw.write(frameOf(pack({ cmd: 'PUSH', queue: 'gated', data: { x: 2 }, reqId: 'p2' })));
    const ok = await raw.nextResponse();
    expect(ok?.ok).toBe(true);
  });

  it('wrong Auth token is rejected and leaves the connection unauthenticated', async () => {
    const h = startServer(['right-token']);
    const raw = await openRaw(h);
    raw.write(frameOf(pack({ cmd: 'Auth', token: 'wrong-token', reqId: 'a2' })));
    const bad = await raw.nextResponse();
    expect(bad?.ok).toBe(false);
    expect(String(bad?.error)).toContain('Invalid token');
    // Still gated afterwards.
    raw.write(frameOf(pack({ cmd: 'PUSH', queue: 'gated2', data: { x: 1 }, reqId: 'p3' })));
    const denied = await raw.nextResponse();
    expect(denied?.ok).toBe(false);
    expect(String(denied?.error)).toContain('Not authenticated');
  });

  it('garbage frames pre-auth do not bypass the gate and do not crash', async () => {
    const h = startServer(['tok']);
    const raw = await openRaw(h);
    // Corrupt frame pre-auth.
    raw.write(withLenPrefix(4, new Uint8Array([0xde, 0xad, 0xbe, 0xef])));
    const r1 = await raw.nextResponse();
    expect(r1?.ok).toBe(false);
    // Unknown command pre-auth → still gated (auth checked before routing, but
    // Auth itself is the only bypass).
    raw.write(frameOf(pack({ cmd: 'Drain', queue: 'q', reqId: 'g1' })));
    const r2 = await raw.nextResponse();
    expect(r2?.ok).toBe(false);
    expect(String(r2?.error)).toContain('Not authenticated');
    await assertServerAlive(h, 'tok');
  });
});
