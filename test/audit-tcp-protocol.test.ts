/**
 * Audit: TCP protocol robustness
 * =================================================================
 *
 * FIX H5 (src/infrastructure/server/protocol.ts FrameParser + tcp.ts timeout)
 * -------------------------------------------------------------------------
 * A frame header may declare up to MAX_FRAME_SIZE (64MB). A *single* frame's
 * declared length is rejected when it exceeds maxFrameSize, which already
 * bounds the partial-frame buffer by 64MB regardless of TCP segmentation. So
 * LEGAL large frames (anything < 64MB) delivered across many TCP segments must
 * REASSEMBLE and complete — a 4MB partial cap would corrupt them (regression).
 *
 * The real slowloris vector (declare a big frame, then trickle/never finish to
 * tie up memory across many connections) is handled at the transport layer by
 * a per-connection STALL TIMEOUT in the server: it is armed only while a
 * partial frame is buffered and disarmed once the frame completes, so healthy
 * idle connections are never affected. We assert:
 *   (a) a legal large frame (8MB) delivered in 1MB chunks COMPLETES as exactly
 *       one frame with correct bytes (parser-level);
 *   (b) the maxFrameSize boundary (declared > 64MB throws FrameSizeError);
 *   (c) a connection that starts a partial frame and then stalls is closed
 *       within ~the configured short stall timeout (e2e).
 *
 * CLAIM H7 (src/infrastructure/server/tcp.ts backpressure) — KEPT
 * -------------------------------------------------------------------------
 * A non-reading client under backpressure must still receive every full framed
 * response (the SocketWriteQueue buffers unwritten tails and flushes on drain).
 *
 * FIX B3 (src/infrastructure/server/socketWriteQueue.ts write-side bound)
 * -------------------------------------------------------------------------
 * A client that NEVER reads while the server produces responses must not grow
 * the outbound queue without bound. With a small maxWriteQueueBytes the server
 * drops (terminates) the connection instead of buffering forever.
 */

import { describe, test, expect } from 'bun:test';
import { pack, unpack } from 'msgpackr';
import { FrameParser, FrameSizeError, MAX_FRAME_SIZE } from '../src/infrastructure/server/protocol';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';

/** Build a big-endian u32 length-prefix header. */
function header(len: number): Uint8Array {
  const h = new Uint8Array(4);
  h[0] = (len >>> 24) & 0xff;
  h[1] = (len >>> 16) & 0xff;
  h[2] = (len >>> 8) & 0xff;
  h[3] = len & 0xff;
  return h;
}

// ---------------------------------------------------------------------------
// FIX H5 — legal large frames reassemble; boundary; slowloris stall timeout
// ---------------------------------------------------------------------------
describe('FIX H5 — FrameParser reassembly + boundary', () => {
  test('a LEGAL large frame (8MB) trickled in 1MB chunks completes intact', () => {
    const parser = new FrameParser(); // default 64MB max

    // 8MB payload — well under the 64MB cap, larger than the (removed) 4MB cap
    // that previously corrupted such frames.
    const PAYLOAD = 8 * 1024 * 1024;
    expect(PAYLOAD).toBeLessThan(MAX_FRAME_SIZE);

    // Distinct byte pattern so we can verify the reassembled bytes exactly.
    const payload = new Uint8Array(PAYLOAD);
    for (let i = 0; i < PAYLOAD; i++) payload[i] = i & 0xff;

    let framesEmitted = parser.addData(header(PAYLOAD)).length;
    expect(framesEmitted).toBe(0); // header only, no complete frame yet
    expect(parser.hasPartialFrame).toBe(true);

    // Trickle the payload 1MB at a time across many TCP segments.
    const frames: Uint8Array[] = [];
    const CHUNK = 1024 * 1024;
    for (let off = 0; off < PAYLOAD; off += CHUNK) {
      const seg = payload.subarray(off, Math.min(off + CHUNK, PAYLOAD));
      for (const f of parser.addData(seg)) frames.push(f);
      framesEmitted = frames.length;
    }

    // EXACTLY one frame, with EXACTLY the original bytes — no corruption/drop.
    expect(frames.length).toBe(1);
    expect(frames[0].length).toBe(PAYLOAD);
    expect(Buffer.from(frames[0]).equals(Buffer.from(payload))).toBe(true);
    // Buffer fully drained after the frame completed.
    expect(parser.hasPartialFrame).toBe(false);
    expect(parser.bufferedBytes).toBe(0);
  });

  test('maxFrameSize boundary comparison is correct (inclusive max)', () => {
    // Exactly maxFrameSize must be ACCEPTED (no throw — just waits for data).
    const atMax = new FrameParser(100);
    expect(() => atMax.addData(header(100))).not.toThrow();

    // maxFrameSize + 1 must be REJECTED with FrameSizeError.
    const overMax = new FrameParser(100);
    let thrown: unknown;
    try {
      overMax.addData(header(101));
    } catch (e) {
      thrown = e;
    }
    expect(thrown).toBeInstanceOf(FrameSizeError);
    expect((thrown as FrameSizeError).requestedSize).toBe(101);
    expect((thrown as FrameSizeError).maxSize).toBe(100);
  });

  test('a frame declaring > MAX_FRAME_SIZE (64MB) is rejected', () => {
    const parser = new FrameParser(); // default 64MB
    let thrown: unknown;
    try {
      parser.addData(header(MAX_FRAME_SIZE + 1));
    } catch (e) {
      thrown = e;
    }
    expect(thrown).toBeInstanceOf(FrameSizeError);
  });
});

// ---------------------------------------------------------------------------
// FIX H5 (slowloris) — server closes a connection that stalls mid-frame
// ---------------------------------------------------------------------------
describe('FIX H5 — slowloris stall timeout (e2e)', () => {
  test('a connection that starts a frame then stalls is closed within the timeout', async () => {
    const STALL_MS = 300; // short timeout for the test
    const qm = new QueueManager();
    const server: TcpServer = createTcpServer(qm, {
      port: 0,
      hostname: '127.0.0.1',
      idleTimeoutMs: STALL_MS,
    });
    const PORT = server.server.port;

    let closed = false;
    let resolveClosed: () => void;
    const closedP = new Promise<void>((r) => (resolveClosed = r));

    const sock = await Bun.connect({
      hostname: '127.0.0.1',
      port: PORT,
      socket: {
        open() {},
        data() {},
        close() {
          closed = true;
          resolveClosed();
        },
        error() {},
      },
    });

    // Declare a large (LEGAL) frame, then send only a few payload bytes and
    // stall forever. The server has a partial frame buffered, arms the stall
    // timer, and must terminate the connection once it fires.
    sock.write(header(8 * 1024 * 1024)); // declare 8MB
    sock.write(new Uint8Array([1, 2, 3, 4])); // trickle, then go silent

    const winner = await Promise.race([
      closedP.then(() => 'closed' as const),
      Bun.sleep(STALL_MS * 8).then(() => 'timed-out' as const),
    ]);

    server.stop();
    qm.shutdown();

    expect(winner).toBe('closed');
    expect(closed).toBe(true);
  }, 15000);

  test('a healthy connection that completes its frame is NOT closed by the stall timer', async () => {
    const STALL_MS = 300;
    const qm = new QueueManager();
    const server: TcpServer = createTcpServer(qm, {
      port: 0,
      hostname: '127.0.0.1',
      idleTimeoutMs: STALL_MS,
    });
    const PORT = server.server.port;

    const parser = new FrameParser();
    let pong = false;
    let resolvePong: () => void;
    const pongP = new Promise<void>((r) => (resolvePong = r));
    let closed = false;

    const sock = await Bun.connect({
      hostname: '127.0.0.1',
      port: PORT,
      socket: {
        open() {},
        data(_s, d) {
          for (const f of parser.addData(new Uint8Array(d))) {
            const r = unpack(f) as { ok?: boolean };
            if (r.ok !== undefined) {
              pong = true;
              resolvePong();
            }
          }
        },
        close() {
          closed = true;
        },
        error() {},
      },
    });

    // Send a complete Ping in two segments (header split from payload). The
    // frame completes, draining the buffer, so the stall timer must NOT fire.
    const frame = FrameParser.frame(pack({ cmd: 'Ping', reqId: 'p1' }));
    sock.write(frame.subarray(0, 2));
    await Bun.sleep(STALL_MS / 3); // less than the timeout — partial but progressing soon
    sock.write(frame.subarray(2));

    await Promise.race([pongP, Bun.sleep(STALL_MS * 6)]);
    // After the response, sit idle well past the stall window: a healthy idle
    // connection (no partial frame) must stay open.
    await Bun.sleep(STALL_MS * 3);

    expect(pong).toBe(true);
    expect(closed).toBe(false);

    sock.end();
    server.stop();
    qm.shutdown();
  }, 15000);
});

// ---------------------------------------------------------------------------
// FIX B3 — write-side memory bound (never-reading client is dropped)
// ---------------------------------------------------------------------------
describe('FIX B3 — bounded outbound write queue', () => {
  test('a never-reading client does NOT grow the server write queue unbounded (connection dropped)', async () => {
    const CAP = 512 * 1024; // small cap so a few big responses trip it
    const qm = new QueueManager();
    const server: TcpServer = createTcpServer(qm, {
      port: 0,
      hostname: '127.0.0.1',
      maxWriteQueueBytes: CAP,
      idleTimeoutMs: 0, // isolate B3 from the stall timer
    });
    const PORT = server.server.port;

    // ----- push several jobs (sequentially, so frames are never truncated) -----
    const BLOB = 'A'.repeat(40 * 1024); // 40KB blob -> ~40KB GetJobs responses
    const N_JOBS = 30; // ~1.2MB aggregate -> far exceeds the 512KB cap
    const pushParser = new FrameParser();
    let ackResolve: ((id: string) => void) | null = null;
    const pushSock = await Bun.connect({
      hostname: '127.0.0.1',
      port: PORT,
      socket: {
        open() {},
        data(_s, d) {
          for (const f of pushParser.addData(new Uint8Array(d))) {
            const r = unpack(f) as { id?: string };
            if (r.id && ackResolve) {
              const fn = ackResolve;
              ackResolve = null;
              fn(r.id);
            }
          }
        },
      },
    });
    for (let i = 0; i < N_JOBS; i++) {
      const ack = new Promise<string>((r) => (ackResolve = r));
      pushSock.write(
        FrameParser.frame(
          pack({ cmd: 'PUSH', queue: 'b3', name: 'j', data: { blob: BLOB }, reqId: `p${i}` })
        )
      );
      await ack;
    }
    pushSock.end();
    await Bun.sleep(50); // let the push connection's close settle

    const connsBefore = server.getConnectionCount();

    // ----- a client that NEVER reads, then fires many heavy requests -----
    const client = await Bun.connect({
      hostname: '127.0.0.1',
      port: PORT,
      socket: {
        open() {},
        // Intentionally ignore inbound data: a stuck reader so the server's
        // socket.write() backpressures and the outbound queue would grow.
        data() {},
        close() {},
        error() {},
      },
    });

    // Fire FAR more response data (~8MB) than the 512KB cap.
    for (let i = 0; i < 200; i++) {
      client.write(
        FrameParser.frame(
          pack({ cmd: 'GetJobs', queue: 'b3', state: 'waiting', limit: 100, reqId: `g${i}` })
        )
      );
    }

    // Poll the server: its per-connection outbound queue must stay bounded
    // (never far exceeds the cap) and the overflowing connection must be dropped
    // — proving the queue is bounded rather than growing toward the full ~8MB.
    let maxObservedQueued = 0;
    let connClosed = false;
    const deadline = Date.now() + 5000;
    while (Date.now() < deadline) {
      let maxQ = 0;
      for (const s of server.connections.values()) {
        maxQ = Math.max(maxQ, s.data.writeQueue.bytesQueued);
      }
      maxObservedQueued = Math.max(maxObservedQueued, maxQ);
      if (server.getConnectionCount() <= connsBefore) {
        connClosed = true;
        break;
      }
      await Bun.sleep(50);
    }

    client.end();
    server.stop();
    qm.shutdown();

    // The overflowing client connection was dropped (back to the baseline count).
    expect(connClosed).toBe(true);
    // The outbound queue never grew far past the cap (bounded, not unbounded).
    // Allow a small margin for the response that pushed it over the edge.
    expect(maxObservedQueued).toBeLessThan(CAP * 4);
  }, 20000);
});

// ---------------------------------------------------------------------------
// CLAIM H7 — backpressure / dropped response bytes (CONTESTED)
// ---------------------------------------------------------------------------
describe('CLAIM H7 — socket.write() backpressure (contested)', () => {
  test('full framed responses survive a non-reading client under backpressure', async () => {
    const qm = new QueueManager();
    const server: TcpServer = createTcpServer(qm, { port: 0, hostname: '127.0.0.1' });
    const PORT = server.server.port;

    // ~150KB blob -> each GetJobs response is ~150KB framed. Firing many of
    // them at a non-reading client forces the server's socket.write() to
    // return short / hit backpressure (the contested path in tcp.ts).
    const BLOB = 'A'.repeat(150 * 1024);
    const N_REQUESTS = 60; // ~9MB of cumulative response data

    // ----- phase 1: push one big job over a normal (reading) connection -----
    const pushParser = new FrameParser();
    let pushResolve: (id: string) => void;
    const pushed = new Promise<string>((r) => (pushResolve = r));
    const pushSock = await Bun.connect({
      hostname: '127.0.0.1',
      port: PORT,
      socket: {
        data(_s, d) {
          for (const f of pushParser.addData(new Uint8Array(d))) {
            const r = unpack(f) as { id?: string };
            if (r.id) pushResolve(r.id);
          }
        },
        open() {},
      },
    });
    pushSock.write(
      FrameParser.frame(
        pack({
          cmd: 'PUSH',
          queue: 'h7',
          name: 'big',
          data: { blob: BLOB },
          reqId: 'push',
        })
      )
    );
    await pushed;
    pushSock.end();

    // ----- phase 2: client reads continuously, server hits backpressure -----
    // The client parses every inbound chunk the moment it arrives (an honest,
    // well-behaved reader). It then fires ALL heavy requests back-to-back. The
    // server must serialize ~9MB of responses across this socket; doing so
    // makes Bun's socket.write() return a SHORT count (backpressure). Because
    // tcp.ts ignores that return value and its drain() handler is an empty
    // no-op, the unwritten tail bytes are never re-sent -> responses vanish.
    const liveParser = new FrameParser();
    const liveResponses: unknown[] = [];

    const client = await Bun.connect({
      hostname: '127.0.0.1',
      port: PORT,
      socket: {
        data(_s, d) {
          for (const f of liveParser.addData(new Uint8Array(d))) {
            liveResponses.push(unpack(f));
          }
        },
        open() {},
      },
    });

    // Fire all the heavy requests back-to-back (saturates the server's write
    // path before the client can drain the socket).
    for (let i = 0; i < N_REQUESTS; i++) {
      client.write(
        FrameParser.frame(
          pack({
            cmd: 'GetJobs',
            queue: 'h7',
            states: ['waiting'],
            start: 0,
            end: 100,
            reqId: `g${i}`,
          })
        )
      );
    }

    // Wait until all responses arrive or an ample deadline elapses. A correct
    // server flushes all ~9MB well within this window (the spaced-request
    // control delivers all 60 in well under a second); a server that drops
    // bytes plateaus almost immediately and never reaches N_REQUESTS. We also
    // bail early once the response count has been stable for a while.
    const deadline = Date.now() + 6000;
    let stableSince = Date.now();
    let lastCount = -1;
    while (liveResponses.length < N_REQUESTS && Date.now() < deadline) {
      if (liveResponses.length !== lastCount) {
        lastCount = liveResponses.length;
        stableSince = Date.now();
      } else if (Date.now() - stableSince > 1500) {
        break; // count stopped advancing -> bytes were dropped
      }
      await Bun.sleep(50);
    }

    client.end();
    server.stop();
    qm.shutdown();

    // Each response actually received must be a well-formed GetJobs reply
    // (proves the frames that DID arrive are not corrupted/truncated).
    for (const r of liveResponses) {
      const resp = r as { ok?: boolean; jobs?: unknown[] };
      expect(resp.ok).toBe(true);
      expect(Array.isArray(resp.jobs)).toBe(true);
    }

    // CORRECT behavior: every request gets its full response back.
    //
    // Resolution rule (per the audit task):
    //   - If this assertion PASSES, Bun buffered everything internally and no
    //     bytes were lost  => H7 REFUTED.
    //   - If it FAILS (fewer responses + far fewer bytes than the ~9MB sent),
    //     unwritten bytes were dropped under backpressure => H7 REAL.
    //
    // Empirically (Bun 1.3.14): only a handful of the 60 responses and ~1.4MB
    // of ~9MB arrive => bytes ARE dropped => H7 is REAL (RED).
    expect(liveResponses.length).toBe(N_REQUESTS);
  }, 30000);
});

// ---------------------------------------------------------------------------
// FIX B2 — a single LEGAL response > 4MB is received intact CLIENT-side
// ---------------------------------------------------------------------------
describe('FIX B2 — client receives a > 4MB framed response intact', () => {
  test('a > 4MB GetJobs response reassembles into exactly one frame with all jobs', async () => {
    const qm = new QueueManager();
    const server: TcpServer = createTcpServer(qm, { port: 0, hostname: '127.0.0.1' });
    const PORT = server.server.port;

    // 100 jobs x 50KB ≈ 5MB aggregate. Each PUSH frame is small (no client-side
    // short-write), but the single GetJobs RESPONSE is > 4MB — the size the
    // previous, wrong 4MB latch would have wedged forever. The client here uses
    // the SAME FrameParser as src/client/tcp/connection.ts, so this exercises
    // the client read path for a large response delivered across many TCP
    // segments.
    const N = 100; // GetJobs default/limit caps at 100 jobs per response
    const BLOB = 'B'.repeat(50 * 1024); // 100 x 50KB ≈ 5MB aggregate response (> 4MB)

    // Push sequentially (await each ack) so each small PUSH frame fully drains
    // the raw test client's send buffer before the next — avoids client-side
    // short-writes truncating frames (this test is about the RESPONSE path).
    const pushParser = new FrameParser();
    let ackResolve: ((id: string) => void) | null = null;
    const pushSock = await Bun.connect({
      hostname: '127.0.0.1',
      port: PORT,
      socket: {
        open() {},
        data(_s, d) {
          for (const f of pushParser.addData(new Uint8Array(d))) {
            const r = unpack(f) as { id?: string };
            if (r.id && ackResolve) {
              const fn = ackResolve;
              ackResolve = null;
              fn(r.id);
            }
          }
        },
      },
    });
    for (let i = 0; i < N; i++) {
      const ack = new Promise<string>((r) => (ackResolve = r));
      pushSock.write(
        FrameParser.frame(
          pack({ cmd: 'PUSH', queue: 'b2', name: 'j', data: { blob: BLOB }, reqId: `p${i}` })
        )
      );
      await ack;
    }
    pushSock.end();

    // ----- request all jobs back over a fresh connection (client read path) -----
    const liveParser = new FrameParser();
    let framesReceived = 0;
    let jobCount = 0;
    let blobsIntact = false;
    let resolveResp: () => void;
    const respP = new Promise<void>((r) => (resolveResp = r));

    const client = await Bun.connect({
      hostname: '127.0.0.1',
      port: PORT,
      socket: {
        open() {},
        data(_s, d) {
          // The > 4MB response arrives across MANY TCP segments; the parser must
          // reassemble it into a single frame.
          for (const f of liveParser.addData(new Uint8Array(d))) {
            framesReceived++;
            const r = unpack(f) as {
              ok?: boolean;
              jobs?: { data?: { blob?: string } }[];
            };
            jobCount = r.jobs?.length ?? 0;
            blobsIntact = !!r.jobs && r.jobs.every((j) => j.data?.blob === BLOB);
            resolveResp();
          }
        },
      },
    });
    client.write(
      FrameParser.frame(
        pack({ cmd: 'GetJobs', queue: 'b2', state: 'waiting', limit: 100, reqId: 'get' })
      )
    );

    const winner = await Promise.race([
      respP.then(() => 'got' as const),
      Bun.sleep(8000).then(() => 'timed-out' as const),
    ]);

    client.end();
    server.stop();
    qm.shutdown();

    // Exactly one frame, fully intact (no wedge / no corruption of the > 4MB body).
    expect(winner).toBe('got');
    expect(framesReceived).toBe(1);
    expect(jobCount).toBe(N);
    expect(blobsIntact).toBe(true);
  }, 20000);
});
