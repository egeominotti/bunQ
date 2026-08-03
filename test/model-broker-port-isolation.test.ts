import { afterEach, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  startModelBroker,
  stopModelBroker,
  type StartedModelBroker,
} from './model-based/queue-model-broker';

type BunServer = ReturnType<typeof Bun.serve>;

let broker: StartedModelBroker | null = null;
let decoy: BunServer | null = null;
let directory = '';

afterEach(async () => {
  if (broker) await stopModelBroker(broker);
  broker = null;
  decoy?.stop(true);
  decoy = null;
  if (directory) rmSync(directory, { recursive: true, force: true });
  directory = '';
});

test('model broker rejects a pair whose HTTP readiness port belongs to another server', async () => {
  const candidatePort = reserveAdjacentPorts();
  decoy = Bun.serve({
    hostname: '0.0.0.0',
    port: candidatePort + 1,
    fetch(request) {
      if (new URL(request.url).pathname === '/ready') {
        return Response.json({ ok: true, ready: true });
      }
      return new Response('decoy', { status: 404 });
    },
  });
  directory = mkdtempSync(join(tmpdir(), 'bunqueue-model-port-isolation-'));

  broker = await startModelBroker(join(directory, 'queue.db'), candidatePort);

  expect(broker.port).not.toBe(candidatePort);
  const hello = await broker.client.send({ cmd: 'Hello' });
  expect(hello.ok).toBe(true);
  expect(hello.server).toBe('bunqueue');
});

function reserveAdjacentPorts(): number {
  for (let attempt = 0; attempt < 100; attempt++) {
    const port = 20_000 + Math.floor(Math.random() * 20_000);
    let tcp: ReturnType<typeof Bun.listen> | null = null;
    let http: ReturnType<typeof Bun.listen> | null = null;
    try {
      tcp = probe(port);
      http = probe(port + 1);
      return port;
    } catch {
      // Try another adjacent pair.
    } finally {
      http?.stop();
      tcp?.stop();
    }
  }
  throw new Error('unable to reserve adjacent ports for the model isolation regression');
}

function probe(port: number): ReturnType<typeof Bun.listen> {
  return Bun.listen({
    hostname: '127.0.0.1',
    port,
    socket: {
      data() {
        // Port reservation only.
      },
    },
  });
}
