/**
 * Bug reproduction: BUG H6 — TcpClient pipelining mode leaks in-flight commands
 * on a malformed/unparseable response frame.
 *
 * File: src/client/tcp/client.ts:216-255 (handleData)
 *
 * In pipelining mode (the default: pipelining=true, maxInFlight=100), every
 * command sent via send()/processQueue() is tracked in the CommandQueue's
 * `inFlightByReqId` map, matched back to its caller by reqId. The legacy
 * `currentCommand` slot is NEVER set in pipelining mode.
 *
 * When the server emits a frame that `unpack()` cannot parse (garbage / a
 * partially-corrupt msgpack payload), handleData()'s catch block runs:
 *
 *     } catch {
 *       const current = this.commands.getCurrentCommand(); // === null in pipelining
 *       if (current) { ... current.reject(...) }            // never entered
 *     }
 *
 * Because `currentCommand` is null, NO in-flight command is rejected. Every
 * caller whose command is sitting in `inFlightByReqId` hangs forever — its
 * Promise never settles (until/unless a per-command timeout fires far later).
 *
 * CORRECT behavior: a malformed frame should settle the pending in-flight
 * commands (reject them, or otherwise unblock the callers) promptly. This test
 * asserts that ALL concurrently-issued command promises settle within a short
 * window. Under the current buggy code, the non-current in-flight commands
 * hang and the race against the short timeout WINS -> assertion fails (RED).
 */

import { describe, it, expect, afterEach } from 'bun:test';
import { TcpClient } from '../src/client/tcp/client';
import { FrameParser } from '../src/infrastructure/server/protocol';
import { unpack } from 'msgpackr';
import type { Socket, TCPSocketListener } from 'bun';

let mockServer: TCPSocketListener<unknown> | null = null;

afterEach(() => {
  if (mockServer) {
    mockServer.stop(true);
    mockServer = null;
  }
});

/** Pick a random high port to avoid collisions. */
function randomPort(): number {
  return 20000 + Math.floor(Math.random() * 20000);
}

/**
 * Build a framed message whose payload is NOT valid msgpack.
 *
 * 0xc1 is the "never used" byte in the msgpack spec; unpack() throws on it.
 * We wrap it in the normal 4-byte big-endian length prefix so the client's
 * FrameParser happily extracts a complete frame and hands it to handleData(),
 * where unpack() then throws — exercising the buggy catch path.
 */
function malformedFrame(): Uint8Array {
  return FrameParser.frame(new Uint8Array([0xc1, 0xc1, 0xc1, 0xc1]));
}

/**
 * Start a mock TCP server that:
 *   1. Speaks the bunqueue frame protocol (4-byte BE length + msgpack payload).
 *   2. Counts incoming command frames.
 *   3. Once it has received `expectCount` commands (i.e. all of them are
 *      in-flight on the client), it emits a SINGLE malformed frame.
 *
 * It deliberately never sends a valid response, so the ONLY way for the
 * client's command promises to settle is via correct handling of the
 * malformed frame in handleData().
 */
function startMockGarbageServer(
  port: number,
  expectCount: number
): Promise<TCPSocketListener<unknown>> {
  return new Promise((resolve) => {
    const server = Bun.listen({
      hostname: 'localhost',
      port,
      socket: {
        // One parser per connection so partial frames are reassembled.
        open(sock: Socket<{ parser: FrameParser; seen: number }>) {
          sock.data = { parser: new FrameParser(), seen: 0 };
        },
        data(sock: Socket<{ parser: FrameParser; seen: number }>, data: Buffer) {
          const state = sock.data;
          const frames = state.parser.addData(new Uint8Array(data));
          for (const frame of frames) {
            // Sanity: confirm the client really is speaking msgpack to us.
            unpack(frame);
            state.seen += 1;
          }
          if (state.seen >= expectCount) {
            // All commands are now in-flight on the client. Reply with garbage.
            sock.write(malformedFrame());
          }
        },
        close() {},
        error() {},
      },
    });
    mockServer = server;
    resolve(server);
  });
}

describe('BUG H6: TcpClient pipelining leaks in-flight commands on malformed frame', () => {
  it('settles ALL concurrent in-flight commands when the server sends a malformed frame', async () => {
    const port = randomPort();
    const CONCURRENCY = 5;
    await startMockGarbageServer(port, CONCURRENCY);

    const client = new TcpClient({
      host: 'localhost',
      port,
      autoReconnect: false,
      connectTimeout: 2000,
      // Long command timeout so the per-command timeout CANNOT mask the bug.
      // If commands settle quickly it is because handleData() handled the
      // malformed frame, NOT because they timed out.
      commandTimeout: 25000,
      pingInterval: 0,
      pipelining: true,
      maxInFlight: 100,
    });

    await client.connect();

    // Issue several concurrent commands. All are tracked in inFlightByReqId;
    // none populate the legacy `currentCommand` slot.
    const settled = new Array<boolean>(CONCURRENCY).fill(false);
    const cmdPromises = Array.from({ length: CONCURRENCY }, (_, i) =>
      client
        .send({ cmd: 'Ping', n: i })
        .then(() => {
          settled[i] = true;
        })
        .catch(() => {
          // Rejection is a perfectly correct way to settle — what matters is
          // that the caller is UNBLOCKED rather than hanging forever.
          settled[i] = true;
        })
    );

    expect(client.getInFlightCount()).toBe(CONCURRENCY);

    // Wait until every command promise settles, OR a short deadline elapses.
    // Under the bug, the non-current in-flight commands never settle, so the
    // timeout branch wins.
    const HANG_DEADLINE_MS = 3000;
    const allSettled = Promise.all(cmdPromises).then(() => 'all-settled' as const);
    const deadline = new Promise<'timed-out'>((resolve) =>
      setTimeout(() => resolve('timed-out'), HANG_DEADLINE_MS)
    );

    const outcome = await Promise.race([allSettled, deadline]);

    const hung = settled
      .map((s, i) => (s ? -1 : i))
      .filter((i) => i >= 0);

    // CORRECT behavior: every concurrent command settled promptly.
    // BUGGY behavior: handleData() only checks `currentCommand` (null in
    // pipelining), so the in-flight commands hang -> `hung` is non-empty and
    // the race resolves to 'timed-out'.
    expect(hung).toEqual([]);
    expect(outcome).toBe('all-settled');
    expect(client.getInFlightCount()).toBe(0);

    client.close();
    // Drain any chained rejections so they don't surface as unhandled.
    await Promise.allSettled(cmdPromises);
  });

  it('does not leave any command pending after a malformed frame (single command)', async () => {
    const port = randomPort();
    await startMockGarbageServer(port, 1);

    const client = new TcpClient({
      host: 'localhost',
      port,
      autoReconnect: false,
      connectTimeout: 2000,
      commandTimeout: 25000,
      pingInterval: 0,
      pipelining: true,
    });

    await client.connect();

    let didSettle = false;
    const p = client
      .send({ cmd: 'Ping' })
      .then(() => {
        didSettle = true;
      })
      .catch(() => {
        didSettle = true;
      });

    const HANG_DEADLINE_MS = 3000;
    const outcome = await Promise.race([
      p.then(() => 'settled' as const),
      new Promise<'timed-out'>((resolve) =>
        setTimeout(() => resolve('timed-out'), HANG_DEADLINE_MS)
      ),
    ]);

    expect(didSettle).toBe(true);
    expect(outcome).toBe('settled');
    expect(client.getInFlightCount()).toBe(0);

    client.close();
    await Promise.allSettled([p]);
  });
});
