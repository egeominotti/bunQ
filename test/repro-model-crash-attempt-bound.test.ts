import { expect, test } from 'bun:test';
import { RealQueue } from './model-based/queue-model-harness';

test('crash recovery honors maxAttempts before requeueing a stalled job', async () => {
  const real = await RealQueue.create(`attempt-bound-regression-${Date.now()}`);
  const id = 'model-crash-attempt-bound';
  try {
    await real.send({
      cmd: 'SetStallConfig',
      config: { maxStalls: 10 },
      queue: real.queue,
    });
    await real.send({
      backoff: 60000,
      cmd: 'PUSH',
      data: { generation: 1 },
      durable: true,
      jobId: id,
      maxAttempts: 1,
      queue: real.queue,
    });
    const pulled = await real.send({
      cmd: 'PULL',
      lockTtl: 60000,
      owner: 'attempt-bound-worker',
      queue: real.queue,
      timeout: 0,
    });
    expect((pulled.job as { id?: string })?.id).toBe(id);

    await real.crashRestart();

    expect((await real.send({ cmd: 'GetState', id })).state).toBe('failed');
    const response = await real.send({ cmd: 'GetJob', id });
    const job = response.job as { attempts?: number; maxAttempts?: number };
    expect(job.attempts).toBe(1);
    expect(job.attempts).toBeLessThanOrEqual(job.maxAttempts ?? 0);
  } finally {
    await real.dispose();
  }
}, 30000);
