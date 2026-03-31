/**
 * Bug reproduction: TcpClient connected state not reset on auth failure
 *
 * When authentication fails in doConnect(), the client sets connected=true
 * BEFORE calling authenticate(). If auth throws, connected remains true
 * and subsequent connect() calls return immediately (line 55: if (this.connected) return).
 *
 * File: src/client/tcp/client.ts, lines 113-120
 */

import { describe, it, expect, afterEach } from 'bun:test';
import { TcpClient } from '../src/client/tcp/client';
import { FrameParser } from '../src/infrastructure/server/protocol';
import { pack, unpack } from 'msgpackr';
import type { Socket, TCPSocketListener } from 'bun';

let mockServer: TCPSocketListener<unknown> | null = null;

afterEach(() => {
  if (mockServer) {
    mockServer.stop(true);
    mockServer = null;
  }
});

/**
 * Start a mock TCP server that accepts connections but rejects Auth commands.
 * Speaks the same frame protocol (4-byte big-endian length + msgpack payload).
 */
function startMockAuthRejectServer(port: number): Promise<TCPSocketListener<unknown>> {
  return new Promise((resolve) => {
    const server = Bun.listen({
      hostname: 'localhost',
      port,
      socket: {
        data(sock: Socket<unknown>, data: Buffer) {
          const parser = new FrameParser();
          const frames = parser.addData(new Uint8Array(data));
          for (const frame of frames) {
            const msg = unpack(frame) as Record<string, unknown>;
            if (msg.cmd === 'Auth') {
              // Reject authentication
              const response = pack({ ok: false, reqId: msg.reqId, error: 'Invalid token' });
              const framed = FrameParser.frame(response);
              sock.write(framed);
            }
          }
        },
        open() {},
        close() {},
        error() {},
      },
    });
    mockServer = server;
    resolve(server);
  });
}

describe('Bug: TcpClient auth failure leaves connected=true', () => {
  it('should set connected=false when authentication fails', async () => {
    const port = 16789;
    await startMockAuthRejectServer(port);

    const client = new TcpClient({
      host: 'localhost',
      port,
      token: 'wrong-token',
      autoReconnect: false,
      connectTimeout: 2000,
      commandTimeout: 2000,
      pingInterval: 0,
    });

    // connect() should throw because auth fails
    let connectError: Error | null = null;
    try {
      await client.connect();
    } catch (err) {
      connectError = err as Error;
    }

    expect(connectError).not.toBeNull();
    expect(connectError!.message).toContain('Authentication failed');

    // BUG: After auth failure, connected should be false but it's true
    // This is the bug we're reproducing
    const isConnected = client.isConnected();
    const state = client.getState();

    // These assertions document the EXPECTED correct behavior.
    // Currently they FAIL because of the bug: connected remains true.
    expect(isConnected).toBe(false);
    expect(state).toBe('disconnected');

    client.close();
  });

  it('should allow reconnection after auth failure', async () => {
    const port = 16790;
    await startMockAuthRejectServer(port);

    const client = new TcpClient({
      host: 'localhost',
      port,
      token: 'wrong-token',
      autoReconnect: false,
      connectTimeout: 2000,
      commandTimeout: 2000,
      pingInterval: 0,
    });

    // First connect fails auth
    try {
      await client.connect();
    } catch {
      // expected
    }

    // BUG: Second connect() returns immediately without actually connecting
    // because connected is still true from the first attempt.
    // After fixing, connect() should attempt a real connection again.
    let secondError: Error | null = null;
    try {
      await client.connect();
    } catch (err) {
      secondError = err as Error;
    }

    // If the bug is fixed, the second connect() should also throw auth failure
    // (not return silently as if already connected)
    expect(secondError).not.toBeNull();
    expect(secondError!.message).toContain('Authentication failed');

    client.close();
  });
});
