import { afterEach, expect, test } from 'bun:test';
import { pack, unpack } from 'msgpackr';
import { QueueManager } from '../src/application/queueManager';
import { FrameParser } from '../src/infrastructure/server/protocol';
import { getRateLimiter, stopRateLimiter } from '../src/infrastructure/server/rateLimiter';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';

let manager: QueueManager | null = null;
let server: TcpServer | null = null;
let socket: { end(): void; write(data: Uint8Array): number } | null = null;

afterEach(() => {
  socket?.end();
  server?.stop();
  manager?.shutdown();
  stopRateLimiter();
  socket = null;
  server = null;
  manager = null;
});

test('protocol overload responses preserve the triggering reqId', async () => {
  stopRateLimiter();
  getRateLimiter({ maxRequests: 1, windowMs: 60_000, cleanupIntervalMs: 0 });
  manager = new QueueManager();
  server = createTcpServer(manager, { hostname: '127.0.0.1', port: 0 });

  const parser = new FrameParser();
  const responses: Record<string, unknown>[] = [];
  let wake: (() => void) | null = null;
  socket = await Bun.connect({
    hostname: '127.0.0.1',
    port: server.server.port,
    socket: {
      data(_socket, data) {
        for (const frame of parser.addData(data)) {
          responses.push(unpack(frame) as Record<string, unknown>);
        }
        wake?.();
        wake = null;
      },
    },
  });

  const send = async (reqId: string): Promise<Record<string, unknown>> => {
    socket!.write(FrameParser.frame(pack({ cmd: 'Ping', reqId })));
    const deadline = Date.now() + 2_000;
    while (responses.length === 0 && Date.now() < deadline) {
      await new Promise<void>((resolve) => {
        wake = resolve;
        setTimeout(resolve, 25);
      });
    }
    const response = responses.shift();
    if (!response) throw new Error(`Timed out waiting for ${reqId}`);
    return response;
  };

  expect(await send('allowed-1')).toMatchObject({ ok: true, reqId: 'allowed-1' });
  expect(await send('limited-2')).toEqual({
    ok: false,
    error: 'Rate limit exceeded',
    reqId: 'limited-2',
  });
});
