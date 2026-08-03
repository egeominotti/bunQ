import { expect, test } from 'bun:test';
import type { Socket } from 'bun';
import { TcpClient } from '../src/client/tcp';
import { FrameParser } from '../src/infrastructure/server/protocol';
import { decodeMessagePack, encodeMessagePack } from '../src/shared/msgpack';

test('TcpClient.close fully closes a connection even when the peer allows half-open sockets', async () => {
  const parsers = new WeakMap<Socket<undefined>, FrameParser>();
  let resolveClosed = () => undefined;
  const closed = new Promise<void>((resolve) => {
    resolveClosed = resolve;
  });
  const server = Bun.listen({
    hostname: '127.0.0.1',
    port: 0,
    allowHalfOpen: true,
    socket: {
      open(socket) {
        parsers.set(socket, new FrameParser());
      },
      data(socket, data) {
        const parser = parsers.get(socket);
        if (!parser) return;
        for (const frame of parser.addData(data)) {
          const request = decodeMessagePack<Record<string, unknown>>(frame);
          socket.write(
            FrameParser.frame(
              encodeMessagePack({ ok: true, data: { pong: true }, reqId: request.reqId })
            )
          );
        }
      },
      close() {
        resolveClosed();
      },
      error() {
        // The assertion is driven by peer close, not by mock-server diagnostics.
      },
    },
  });
  const client = new TcpClient({
    host: '127.0.0.1',
    port: server.port,
    autoReconnect: false,
    pingInterval: 0,
  });

  try {
    await client.send({ cmd: 'Ping' });
    client.close();
    const peerObservedFullClose = await Promise.race([
      closed.then(() => true),
      Bun.sleep(250).then(() => false),
    ]);
    expect(peerObservedFullClose).toBe(true);
  } finally {
    client.close();
    server.stop(true);
  }
}, 3_000);
