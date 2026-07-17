import { expect, test } from 'bun:test';
import { RealQueue } from './model-based/queue-model-harness';

test('Update persists job data across a broker crash and restart', async () => {
  const real = await RealQueue.create(`update-data-regression-${Date.now()}`);
  const id = 'model-update-data-persistence';
  try {
    await real.send({
      cmd: 'PUSH',
      data: { generation: 1 },
      durable: true,
      jobId: id,
      queue: real.queue,
    });
    const updated = await real.send({
      cmd: 'Update',
      data: { generation: 2 },
      id,
    });
    expect(updated.ok).toBe(true);

    await real.crashRestart();

    const response = await real.send({ cmd: 'GetJob', id });
    const job = response.job as { data?: { generation?: number } };
    expect(job.data?.generation).toBe(2);
  } finally {
    await real.dispose();
  }
}, 30000);
