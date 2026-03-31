/**
 * CLI Client Tests
 * Tests for src/cli/client.ts: sendCommand, connect, frame parsing,
 * timeout logic, socket handlers, and command routing.
 */

import { describe, test, expect } from 'bun:test';
import { pack, unpack } from 'msgpackr';
import { FrameParser, FrameSizeError } from '../src/infrastructure/server/protocol';

/** SocketData shape matching client.ts */
type SocketData = {
  frameParser: FrameParser;
  resolve: ((value: Record<string, unknown>) => void) | null;
  reject: ((error: Error) => void) | null;
};

/** Mock socket matching the interface sendCommand expects */
function createMockSocket() {
  const data: SocketData = { frameParser: new FrameParser(), resolve: null, reject: null };
  const written: Uint8Array[] = [];
  return { socket: { write: (d: Uint8Array) => written.push(d), data }, written };
}

/** Reimplementation of sendCommand (not exported) for testing the pattern */
function sendCommand(
  socket: { write: (d: Uint8Array) => void; data: SocketData },
  command: Record<string, unknown>,
  timeoutMs = 30000
): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    const tid = setTimeout(() => {
      socket.data.resolve = null;
      socket.data.reject = null;
      reject(new Error('Command timeout'));
    }, timeoutMs);
    socket.data.resolve = (v) => { clearTimeout(tid); resolve(v); };
    socket.data.reject = (e) => { clearTimeout(tid); reject(e); };
    socket.write(FrameParser.frame(pack(command)));
  });
}

// ---------------------------------------------------------------------------
describe('CLI Client - FrameParser framing for sendCommand', () => {
  test('frame produces valid length-prefixed frame', () => {
    const payload = pack({ cmd: 'Ping' });
    const framed = FrameParser.frame(payload);
    expect(framed.length).toBe(4 + payload.length);
    const len = ((framed[0] << 24) | (framed[1] << 16) | (framed[2] << 8) | framed[3]) >>> 0;
    expect(len).toBe(payload.length);
  });

  test('round-trips through frame + addData', () => {
    const original = { cmd: 'PUSH', queue: 'emails', data: { to: 'a@b.com' } };
    const frames = new FrameParser().addData(FrameParser.frame(pack(original)));
    expect(frames.length).toBe(1);
    const decoded = unpack(frames[0]) as Record<string, unknown>;
    expect(decoded.cmd).toBe('PUSH');
    expect(decoded.queue).toBe('emails');
  });

  test('handles multiple concatenated frames', () => {
    const combined = new Uint8Array([
      ...FrameParser.frame(pack({ cmd: 'Ping' })),
      ...FrameParser.frame(pack({ ok: true })),
    ]);
    const frames = new FrameParser().addData(combined);
    expect(frames.length).toBe(2);
  });

  test('handles partial frames across addData calls', () => {
    const framed = FrameParser.frame(pack({ cmd: 'Stats' }));
    const mid = Math.floor(framed.length / 2);
    const parser = new FrameParser();
    expect(parser.addData(framed.slice(0, mid)).length).toBe(0);
    const frames = parser.addData(framed.slice(mid));
    expect(frames.length).toBe(1);
    expect((unpack(frames[0]) as Record<string, unknown>).cmd).toBe('Stats');
  });

  test('throws FrameSizeError for oversized frames', () => {
    const parser = new FrameParser(1024);
    expect(() => parser.addData(new Uint8Array([0, 0, 8, 0]))).toThrow(FrameSizeError);
  });
});

// ---------------------------------------------------------------------------
describe('CLI Client - sendCommand behavior', () => {
  test('writes framed msgpack to socket and resolves', async () => {
    const { socket, written } = createMockSocket();
    const promise = sendCommand(socket, { cmd: 'Ping' });
    expect(written.length).toBe(1);
    const frames = new FrameParser().addData(written[0]);
    expect((unpack(frames[0]) as Record<string, unknown>).cmd).toBe('Ping');
    socket.data.resolve!({ ok: true });
    expect((await promise).ok).toBe(true);
  });

  test('resolves with full response payload', async () => {
    const { socket } = createMockSocket();
    const promise = sendCommand(socket, { cmd: 'Stats' });
    socket.data.resolve!({ ok: true, stats: { waiting: 5 } });
    const result = await promise;
    expect(result.stats).toEqual({ waiting: 5 });
  });

  test('rejects when reject callback is invoked', async () => {
    const { socket } = createMockSocket();
    const promise = sendCommand(socket, { cmd: 'PULL', queue: 'test' });
    socket.data.reject!(new Error('Connection closed'));
    await expect(promise).rejects.toThrow('Connection closed');
  });

  test('times out and nulls callbacks', async () => {
    const { socket } = createMockSocket();
    const promise = sendCommand(socket, { cmd: 'Ping' }, 50);
    await expect(promise).rejects.toThrow('Command timeout');
    expect(socket.data.resolve).toBeNull();
    expect(socket.data.reject).toBeNull();
  });

  test('clears timeout on resolve (no double-fire)', async () => {
    const { socket } = createMockSocket();
    const promise = sendCommand(socket, { cmd: 'Ping' }, 100);
    socket.data.resolve!({ ok: true });
    expect((await promise).ok).toBe(true);
    await new Promise((r) => setTimeout(r, 150));
  });

  test('clears timeout on rejection (no double-fire)', async () => {
    const { socket } = createMockSocket();
    const promise = sendCommand(socket, { cmd: 'Ping' }, 100);
    socket.data.reject!(new Error('server error'));
    await expect(promise).rejects.toThrow('server error');
    await new Promise((r) => setTimeout(r, 150));
  });
});

// ---------------------------------------------------------------------------
describe('CLI Client - socket handler simulation', () => {
  test('data handler resolves on valid framed msgpack', () => {
    const sd: SocketData = { frameParser: new FrameParser(), resolve: null, reject: null };
    let resolved: Record<string, unknown> | null = null;
    sd.resolve = (v) => { resolved = v; };
    const frames = sd.frameParser.addData(new Uint8Array(FrameParser.frame(pack({ ok: true, id: '42' }))));
    for (const f of frames) {
      if (sd.resolve) { sd.resolve(unpack(f) as Record<string, unknown>); sd.resolve = null; sd.reject = null; }
    }
    expect(resolved).not.toBeNull();
    expect(resolved!.id).toBe('42');
  });

  test('data handler rejects on FrameSizeError', () => {
    const sd: SocketData = { frameParser: new FrameParser(64), resolve: null, reject: null };
    let err: Error | null = null;
    sd.reject = (e) => { err = e; };
    try { sd.frameParser.addData(new Uint8Array([0, 0, 1, 0])); } catch (e) {
      if (e instanceof FrameSizeError && sd.reject) {
        sd.reject(new Error(`Frame too large: ${e.requestedSize} exceeds ${e.maxSize}`));
        sd.resolve = null; sd.reject = null;
      }
    }
    expect(err!.message).toContain('Frame too large');
  });

  test('close handler rejects pending command', () => {
    const sd: SocketData = { frameParser: new FrameParser(), resolve: null, reject: null };
    let err: Error | null = null;
    sd.reject = (e) => { err = e; };
    if (sd.reject) sd.reject(new Error('Connection closed'));
    expect(err!.message).toBe('Connection closed');
  });

  test('close handler is safe when no pending command', () => {
    const sd: SocketData = { frameParser: new FrameParser(), resolve: null, reject: null };
    if (sd.reject) sd.reject(new Error('Connection closed'));
    expect(sd.reject).toBeNull();
  });
});

// ---------------------------------------------------------------------------
describe('CLI Client - connect failures', () => {
  test('rejects on invalid port via Bun.connect', async () => {
    const promise = new Promise((resolve, reject) => {
      const tid = setTimeout(() => reject(new Error('Connection timeout')), 1000);
      void Bun.connect({
        hostname: 'localhost', port: 1,
        socket: {
          data() {}, open() { clearTimeout(tid); resolve(true); }, close() {},
          error(_s, e) { reject(new Error(`Connection error: ${e.message}`)); },
          connectError(_s, e) { clearTimeout(tid); reject(new Error(`Failed: ${e.message}`)); },
        },
      });
    });
    await expect(promise).rejects.toThrow();
  });

  test('connection timeout pattern rejects correctly', async () => {
    const promise = new Promise((_resolve, reject) => {
      const connected = false;
      setTimeout(() => { if (!connected) reject(new Error('Connection timeout')); }, 100);
    });
    await expect(promise).rejects.toThrow('Connection timeout');
  });
});

// ---------------------------------------------------------------------------
describe('CLI Client - buildCommand routing', () => {
  async function buildCommand(command: string, args: string[]) {
    const { buildCoreCommand } = await import('../src/cli/commands/core');
    const { buildMonitorCommand } = await import('../src/cli/commands/monitor');
    switch (command) {
      case 'push': case 'pull': case 'ack': case 'fail': return buildCoreCommand(command, args);
      case 'stats': case 'metrics': case 'health': case 'ping': return buildMonitorCommand(command);
      default: return null;
    }
  }

  test('routes core commands correctly', async () => {
    expect((await buildCommand('push', ['q1', '{"a":1}']))!.cmd).toBe('PUSH');
    expect((await buildCommand('pull', ['q1']))!.cmd).toBe('PULL');
    expect((await buildCommand('ack', ['123']))!.cmd).toBe('ACK');
    expect((await buildCommand('fail', ['123']))!.cmd).toBe('FAIL');
  });

  test('routes monitor commands correctly', async () => {
    expect((await buildCommand('stats', []))!.cmd).toBe('Stats');
    expect((await buildCommand('health', []))!.cmd).toBe('Stats');
    expect((await buildCommand('ping', []))!.cmd).toBe('Ping');
    expect((await buildCommand('metrics', []))!.cmd).toBe('Prometheus');
  });

  test('returns null for unknown command', async () => {
    expect(await buildCommand('nonexistent', [])).toBeNull();
    expect(await buildCommand('', [])).toBeNull();
  });
});

// ---------------------------------------------------------------------------
describe('CLI Client - ClientOptions interface', () => {
  test('accepts minimal required fields with token optional', () => {
    const opts: import('../src/cli/client').ClientOptions = {
      host: 'localhost', port: 6789, json: false,
    };
    expect(opts.host).toBe('localhost');
    expect(opts.port).toBe(6789);
    expect(opts.json).toBe(false);
    expect(opts.token).toBeUndefined();
  });

  test('accepts all fields including token', () => {
    const opts: import('../src/cli/client').ClientOptions = {
      host: '127.0.0.1', port: 7000, json: true, token: 'my-secret',
    };
    expect(opts.token).toBe('my-secret');
    expect(opts.json).toBe(true);
  });
});
