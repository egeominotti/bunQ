import { expect, test } from 'bun:test';
import { RealQueue } from './model-based/queue-model-harness';

interface PulledJob {
  id: string;
}

async function addFlow(real: RealQueue): Promise<{ childId: string; parentId: string }> {
  const childId = 'ackb-flow-child';
  const parentId = 'ackb-flow-parent';
  await real.send({
    cmd: 'PUSH',
    data: { role: 'child' },
    durable: true,
    jobId: childId,
    priority: 2,
    queue: real.queue,
  });
  await real.send({
    childrenIds: [childId],
    cmd: 'PUSH',
    data: { role: 'parent' },
    dependsOn: [childId],
    durable: true,
    jobId: parentId,
    priority: 1,
    queue: real.queue,
  });
  return { childId, parentId };
}

test('flow PULLB and ACKB use top-level success/error envelopes', async () => {
  const real = await RealQueue.create(`ackb-envelope-${crypto.randomUUID()}`);

  try {
    const { childId, parentId } = await addFlow(real);
    const pulled = await real.send({
      cmd: 'PULLB',
      count: 2,
      lockTtl: 60_000,
      owner: 'ackb-envelope-worker',
      queue: real.queue,
      timeout: 0,
    });
    const jobs = pulled.jobs as PulledJob[];
    const tokens = pulled.tokens as string[];
    expect(jobs.map((job) => job.id)).toEqual([childId]);
    expect(tokens).toHaveLength(1);

    const rejected = await real.send({
      cmd: 'ACKB',
      ids: [childId],
      results: [{ completedBy: 'wrong-token' }],
      tokens: ['not-the-pull-token'],
    });
    expect(rejected.ok).toBe(false);
    expect(String(rejected.error)).toContain('lock token');
    expect((await real.send({ cmd: 'GetState', id: childId })).state).toBe('active');

    const accepted = await real.send({
      cmd: 'ACKB',
      ids: [childId],
      results: [{ completedBy: childId }],
      tokens,
    });
    expect(accepted.ok).toBe(true);
    expect(accepted.count).toBeUndefined();
    expect(accepted.data).toBeUndefined();
    expect((await real.send({ cmd: 'GetResult', id: childId })).result).toEqual({
      completedBy: childId,
    });
    expect((await real.send({ cmd: 'GetState', id: parentId })).state).toBe('prioritized');
  } finally {
    await real.dispose();
  }
});

test('ACKB keeps ids, tokens, and results aligned by array index', async () => {
  const real = await RealQueue.create(`ackb-order-${crypto.randomUUID()}`);

  try {
    const { childId } = await addFlow(real);
    const independentId = 'ackb-independent';
    await real.send({
      cmd: 'PUSH',
      data: { role: 'independent' },
      durable: true,
      jobId: independentId,
      priority: 3,
      queue: real.queue,
    });
    const pulled = await real.send({
      cmd: 'PULLB',
      count: 2,
      lockTtl: 60_000,
      owner: 'ackb-order-worker',
      queue: real.queue,
      timeout: 0,
    });
    const jobs = pulled.jobs as PulledJob[];
    const tokens = pulled.tokens as string[];
    expect(jobs.map((job) => job.id)).toEqual([independentId, childId]);
    expect(tokens).toHaveLength(jobs.length);

    const results = jobs.map((job, index) => ({ id: job.id, index }));
    const response = await real.send({
      cmd: 'ACKB',
      ids: jobs.map((job) => job.id),
      results,
      tokens,
    });
    expect(response.ok).toBe(true);
    for (const [index, job] of jobs.entries()) {
      expect((await real.send({ cmd: 'GetResult', id: job.id })).result).toEqual(results[index]);
    }
  } finally {
    await real.dispose();
  }
});
