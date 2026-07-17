import { expect, test } from 'bun:test';
import { RealQueue } from './model-based/queue-model-harness';

test('repeated crash recovery preserves stallCount and reaches DLQ exactly once', async () => {
  const real = await RealQueue.create(`stall-bound-regression-${Date.now()}`);
  const id = 'model-repeated-crash-stall-bound';
  try {
    await real.send({
      cmd: 'SetStallConfig',
      config: { maxStalls: 2 },
      queue: real.queue,
    });
    await real.send({
      backoff: 60000,
      cmd: 'PUSH',
      data: { generation: 1 },
      durable: true,
      jobId: id,
      maxAttempts: 3,
      queue: real.queue,
    });

    const first = await real.send({
      cmd: 'PULL',
      lockTtl: 60000,
      owner: 'crash-worker-1',
      queue: real.queue,
      timeout: 0,
    });
    expect((first.job as { id?: string })?.id).toBe(id);
    await real.crashRestart();
    expect((await real.send({ cmd: 'GetState', id })).state).toBe('delayed');

    await real.send({ cmd: 'Promote', id });
    const second = await real.send({
      cmd: 'PULL',
      lockTtl: 60000,
      owner: 'crash-worker-2',
      queue: real.queue,
      timeout: 0,
    });
    expect((second.job as { id?: string })?.id).toBe(id);
    await real.crashRestart();

    expect((await real.send({ cmd: 'GetState', id })).state).toBe('failed');
    const dlq = await real.send({ cmd: 'Dlq', count: 10, queue: real.queue });
    expect((dlq.jobs as { id: string }[]).map((job) => job.id)).toEqual([id]);

    await real.crashRestart();
    const afterIdempotentRestart = await real.send({ cmd: 'Dlq', count: 10, queue: real.queue });
    expect((afterIdempotentRestart.jobs as { id: string }[]).map((job) => job.id)).toEqual([id]);
  } finally {
    await real.dispose();
  }
}, 30000);
