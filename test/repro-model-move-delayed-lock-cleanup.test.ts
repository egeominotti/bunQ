import { expect, test } from 'bun:test';
import { RealQueue } from './model-based/queue-model-harness';

test('moving an active job to delayed releases its live lease exactly once', async () => {
  const real = await RealQueue.create(`move-delayed-lock-${Date.now()}`);

  try {
    const pushed = await real.send({
      cmd: 'PUSH',
      data: { source: 'model-regression' },
      durable: true,
      jobId: 'move-delayed-lock-job',
      queue: real.queue,
    });
    const pulled = await real.send({
      cmd: 'PULL',
      lockTtl: 60_000,
      owner: 'move-delayed-worker',
      queue: real.queue,
      timeout: 0,
    });
    expect((pulled.job as { id: string }).id).toBe(pushed.id);
    expect(await liveOwnership(real.port)).toEqual({
      clientJobs: 1,
      clientJobsTotal: 1,
      jobLocks: 1,
    });

    expect(
      (
        await real.send({
          cmd: 'MoveToDelayed',
          delay: 60_000,
          id: pushed.id,
          token: pulled.token,
        })
      ).ok
    ).toBe(true);

    expect((await real.send({ cmd: 'GetState', id: pushed.id })).state).toBe('delayed');
    expect(await liveOwnership(real.port)).toEqual({
      clientJobs: 0,
      clientJobsTotal: 0,
      jobLocks: 0,
    });
  } finally {
    await real.dispose();
  }
});

async function liveOwnership(tcpPort: number): Promise<{
  clientJobs: number;
  clientJobsTotal: number;
  jobLocks: number;
}> {
  const response = await fetch(`http://127.0.0.1:${tcpPort + 1}/stats`);
  const body = (await response.json()) as {
    collections: { clientJobs: number; clientJobsTotal: number; jobLocks: number };
  };
  return {
    clientJobs: body.collections.clientJobs,
    clientJobsTotal: body.collections.clientJobsTotal,
    jobLocks: body.collections.jobLocks,
  };
}
