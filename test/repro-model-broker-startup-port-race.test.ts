import { expect, test } from 'bun:test';
import { RealQueue } from './model-based/queue-model-harness';

test('parallel model brokers acquire distinct ready TCP/HTTP pairs', async () => {
  const brokers: RealQueue[] = [];

  try {
    await Promise.all(
      Array.from({ length: 6 }, async (_, index) => {
        const real = await RealQueue.create(`parallel-startup-${index}-${crypto.randomUUID()}`);
        brokers.push(real);
        const hello = await real.send({ cmd: 'Hello' });
        expect(hello.ok).toBe(true);
        expect(hello.server).toBe('bunqueue');
      })
    );

    const tcpPorts = brokers.map((broker) => broker.port);
    const allPorts = tcpPorts.flatMap((port) => [port, port + 1]);
    expect(new Set(tcpPorts).size).toBe(brokers.length);
    expect(new Set(allPorts).size).toBe(allPorts.length);
  } finally {
    await Promise.all(brokers.map((broker) => broker.dispose()));
  }
}, 30_000);
