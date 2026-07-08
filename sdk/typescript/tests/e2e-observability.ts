/** E2E: observability — telemetry sink, lifecycle events, injectable logger. */

import { createServer } from 'node:net';
import { type Logger, Queue, type TelemetryEvent } from '../dist/index.js';
import { assert, getPort, qname, test } from './harness.ts';

function closedPort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.listen(0, '127.0.0.1', () => {
      const { port } = srv.address() as { port: number };
      srv.close(() => resolve(port)); // free the port so connects are refused
    });
    srv.on('error', reject);
  });
}

test('observability: telemetry captures connect + per-command latency', async () => {
  const events: TelemetryEvent[] = [];
  const queue = new Queue(qname('obs'), {
    host: '127.0.0.1',
    port: getPort(),
    onTelemetry: (e) => events.push(e),
  });
  try {
    await queue.add('t', { x: 1 });
    assert(
      events.some((e) => e.type === 'connect'),
      'a connect telemetry event fires'
    );
    const cmd = events.find((e) => e.type === 'command' && e.cmd === 'PUSH');
    assert(cmd !== undefined, 'a PUSH command telemetry event fires');
    assert(
      cmd?.type === 'command' && cmd.ok === true && cmd.durationMs >= 0,
      'command event carries ok + non-negative latency'
    );
  } finally {
    queue.close();
  }
});

test('observability: connection emits connect + disconnect lifecycle events', async () => {
  const queue = new Queue(qname('obs-life'), { host: '127.0.0.1', port: getPort() });
  let connected = false;
  let disconnected = false;
  queue.connection.on('connect', () => {
    connected = true;
  });
  queue.connection.on('disconnect', () => {
    disconnected = true;
  });
  await queue.add('t', { x: 1 });
  assert(connected, 'connect event fired');
  queue.close();
  await new Promise((r) => setTimeout(r, 50));
  assert(disconnected, 'disconnect event fired on close');
});

test('observability: reconnect_scheduled telemetry fires when a connect fails', async () => {
  const port = await closedPort();
  const events: TelemetryEvent[] = [];
  const queue = new Queue(qname('obs-recon'), {
    host: '127.0.0.1',
    port,
    onTelemetry: (e) => events.push(e),
  });
  try {
    await queue.add('t', { x: 1 }).catch(() => {}); // connect refused
    assert(
      events.some((e) => e.type === 'reconnect_scheduled'),
      'reconnect_scheduled telemetry fires on a failed connect'
    );
  } finally {
    queue.close();
  }
});

test('observability: injectable logger receives structured log lines', async () => {
  const lines: string[] = [];
  const logger: Logger = {
    debug: (m) => lines.push(`debug ${m}`),
    info: (m) => lines.push(`info ${m}`),
    warn: (m) => lines.push(`warn ${m}`),
    error: (m) => lines.push(`error ${m}`),
  };
  const queue = new Queue(qname('obs-log'), { host: '127.0.0.1', port: getPort(), logger });
  try {
    await queue.add('t', { x: 1 });
    assert(
      lines.some((l) => l === 'info connected'),
      `logger received the 'connected' info line, got: ${JSON.stringify(lines)}`
    );
  } finally {
    queue.close();
  }
});
