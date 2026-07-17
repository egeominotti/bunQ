import { expect, test } from 'bun:test';
import { RealQueue } from './model-based/queue-model-harness';

test('ChangePriority persists scheduling order across a broker crash and restart', async () => {
  const real = await RealQueue.create(`priority-regression-${Date.now()}`);
  const id = 'model-priority-persistence';
  try {
    await real.send({
      cmd: 'PUSH',
      data: { generation: 1 },
      jobId: id,
      priority: 0,
      queue: real.queue,
    });
    const changed = await real.send({ cmd: 'ChangePriority', id, priority: 3 });
    expect(changed.ok).toBe(true);

    await real.crashRestart();

    const response = await real.send({ cmd: 'GetJob', id });
    const job = response.job as { priority?: number };
    expect(job.priority).toBe(3);
    const state = await real.send({ cmd: 'GetState', id });
    expect(state.state).toBe('prioritized');
  } finally {
    await real.dispose();
  }
}, 30000);

test('ChangePriority persists the LIFO tie-break used after restart', async () => {
  const real = await RealQueue.create(`priority-lifo-regression-${Date.now()}`);
  const olderId = 'model-priority-lifo-a';
  const newerId = 'model-priority-lifo-b';
  try {
    for (const id of [olderId, newerId]) {
      await real.send({
        cmd: 'PUSH',
        data: { id },
        durable: true,
        jobId: id,
        priority: 0,
        queue: real.queue,
      });
      const changed = await real.send({ cmd: 'ChangePriority', id, lifo: true, priority: 3 });
      expect(changed.ok).toBe(true);
    }

    await real.crashRestart();

    const response = await real.send({
      cmd: 'PULL',
      lockTtl: 60000,
      owner: 'priority-regression-worker',
      queue: real.queue,
      timeout: 0,
    });
    const job = response.job as { id?: string; lifo?: boolean; priority?: number };
    expect(job.id).toBe(newerId);
    expect(job.priority).toBe(3);
    expect(job.lifo).toBe(true);
  } finally {
    await real.dispose();
  }
}, 30000);
