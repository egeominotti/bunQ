import { describe, expect, test } from 'bun:test';
import { createServer, type Socket } from 'node:net';
import { once } from 'node:events';
import { pack, unpack } from 'msgpackr';
import { FrameParser } from '../src/infrastructure/server/protocol/frameParser';
import { createConnection as nativeConnection } from '../src/client/tcp/transport';
import {
  createConnection,
  tlsRequiresVerification,
} from '../sdk/typescript/src/canonical-transport/transport';

describe('portable canonical TCP transport', () => {
  test.each([
    ['native', nativeConnection],
    ['portable', createConnection],
  ] as const)(
    '%s preserves correlated errors and unsolicited events through fragmented frames',
    async (_name, connect) => {
      const sockets = new Set<Socket>();
      const server = createServer((socket) => {
        sockets.add(socket);
        socket.on('close', () => sockets.delete(socket));
        const parser = new FrameParser();
        socket.on('data', (data) => {
          for (const payload of parser.addData(data)) {
            const command = unpack(payload);
            const response = FrameParser.frame(
              pack({ ok: false, reqId: command.reqId, error: 'denied' })
            );
            socket.write(response.subarray(0, 2));
            socket.write(
              Buffer.concat([
                response.subarray(2),
                FrameParser.frame(
                  pack({
                    type: 'event',
                    event: { eventType: 'waiting', queue: 'q', jobId: '1', timestamp: 1 },
                  })
                ),
              ])
            );
          }
        });
      });
      server.listen(0, '127.0.0.1');
      await once(server, 'listening');
      const port = (server.address() as { port: number }).port;
      const frames: Record<string, unknown>[] = [];
      let resolveFrames!: () => void;
      const received = new Promise<void>((resolve) => {
        resolveFrames = resolve;
      });
      let connection: Awaited<ReturnType<typeof createConnection>> | undefined;
      try {
        connection = await connect({ host: '127.0.0.1', port }, 1000, {
          onData: (frame) => {
            frames.push(unpack(frame));
            if (frames.length === 2) resolveFrames();
          },
          onClose: () => {},
          onError: (error) => {
            throw error;
          },
        });
        connection.socket.write(FrameParser.frame(pack({ cmd: 'Ping', reqId: '7' })));
        await received;
        expect(frames[0]).toEqual({ ok: false, reqId: '7', error: 'denied' });
        expect(frames[1].type).toBe('event');
      } finally {
        connection?.socket.end();
        for (const socket of sockets) socket.destroy();
        server.close();
        await once(server, 'close');
      }
    }
  );

  test('TLS verifies by default and requires explicit opt out', () => {
    expect(tlsRequiresVerification(undefined)).toBe(false);
    expect(tlsRequiresVerification(true)).toBe(true);
    expect(tlsRequiresVerification({})).toBe(true);
    expect(tlsRequiresVerification({ rejectUnauthorized: false })).toBe(false);
  });

  test('rejects a refused connection without retaining a socket', async () => {
    const server = createServer();
    server.listen(0, '127.0.0.1');
    await once(server, 'listening');
    const port = (server.address() as { port: number }).port;
    server.close();
    await once(server, 'close');
    let resolveClosed!: () => void;
    const closed = new Promise<void>((resolve) => {
      resolveClosed = resolve;
    });
    await expect(
      createConnection({ host: '127.0.0.1', port }, 1000, {
        onData: () => {},
        onClose: resolveClosed,
        onError: () => {},
      })
    ).rejects.toThrow(`Failed to connect to 127.0.0.1:${port}`);
    await closed;
  });

  test('rejects oversized frames and closes the peer connection', async () => {
    const sockets = new Set<Socket>();
    const server = createServer((socket) => {
      sockets.add(socket);
      socket.on('close', () => sockets.delete(socket));
      socket.on('data', () => {
        const header = Buffer.alloc(4);
        header.writeUInt32BE(64 * 1024 * 1024 + 1);
        socket.write(header);
      });
    });
    server.listen(0, '127.0.0.1');
    await once(server, 'listening');
    const port = (server.address() as { port: number }).port;
    let error: Error | undefined;
    let resolveClosed!: () => void;
    const closed = new Promise<void>((resolve) => {
      resolveClosed = resolve;
    });
    let connection: Awaited<ReturnType<typeof createConnection>> | undefined;
    try {
      connection = await createConnection({ host: '127.0.0.1', port }, 1000, {
        onData: () => {
          throw new Error('Oversized frame was accepted');
        },
        onClose: resolveClosed,
        onError: (reason) => {
          error = reason;
        },
      });
      connection.socket.write('request');
      await closed;
      expect(error?.message).toBe('Frame too large: 67108865 bytes exceeds maximum 67108864');
    } finally {
      connection?.socket.end();
      for (const socket of sockets) socket.destroy();
      server.close();
      await once(server, 'close');
    }
  });
});
