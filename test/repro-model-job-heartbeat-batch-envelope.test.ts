import { expect, test } from 'bun:test';
import { RealQueue } from './model-based/queue-model-harness';

test('JobHeartbeatB returns the renewed count inside the protocol data envelope', async () => {
  const real = await RealQueue.create(`heartbeat-envelope-${crypto.randomUUID()}`);

  try {
    const pushed = await real.send({
      cmd: 'PUSH',
      data: { source: 'heartbeat-envelope-regression' },
      durable: true,
      queue: real.queue,
    });
    const pulled = await real.send({
      cmd: 'PULL',
      lockTtl: 60_000,
      owner: 'heartbeat-envelope-worker',
      queue: real.queue,
      timeout: 0,
    });

    expect((pulled.job as { id?: string } | null)?.id).toBe(pushed.id);
    expect(typeof pulled.token).toBe('string');

    const response = await real.send({
      cmd: 'JobHeartbeatB',
      ids: [String(pushed.id)],
      tokens: [String(pulled.token)],
    });

    expect(response.ok).toBe(true);
    expect(response.data).toEqual({ ok: true, count: 1 });
    expect(response.count).toBeUndefined();
  } finally {
    await real.dispose();
  }
});
