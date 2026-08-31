import { afterEach, describe, expect, test } from 'bun:test';
import { Queue, shutdownManager } from '../src/client';
import { getSharedManager } from '../src/client/manager';

function queueName(label: string): string {
  return `groups-pro-${label}-${Bun.randomUUIDv7()}`;
}

afterEach(() => {
  shutdownManager();
});

describe('BullMQ Pro compatible groups', () => {
  test('accepts group.id and serves groups round-robin with FIFO ordering', async () => {
    const name = queueName('round-robin');
    const queue = new Queue<{ label: string }>(name, { embedded: true });
    const manager = getSharedManager();

    await queue.add('plain', { label: 'plain' });
    await queue.add('grouped', { label: 'A1' }, { group: { id: 'A' } });
    await queue.add('grouped', { label: 'A2' }, { group: { id: 'A' } });
    await queue.add('grouped', { label: 'B1' }, { group: { id: 'B' } });
    await queue.add('grouped', { label: 'B2' }, { group: { id: 'B' } });

    const jobs = await manager.pullBatch(name, 5);
    expect(jobs.map((job) => (job.data as { label: string }).label)).toEqual([
      'plain',
      'A1',
      'B1',
      'A2',
      'B2',
    ]);
    expect(jobs.slice(1).map((job) => job.groupId)).toEqual(['A', 'B', 'A', 'B']);
  });

  test('enforces shared group concurrency while preserving independent groups', async () => {
    const name = queueName('concurrency');
    const queue = new Queue<{ label: string }>(name, { embedded: true });
    const manager = getSharedManager();

    for (const label of ['A1', 'A2', 'B1', 'B2']) {
      await queue.add('grouped', { label }, { group: { id: label[0] } });
    }

    const firstWave = await manager.pullBatch(name, 10, 0, undefined, { concurrency: 1 });
    expect(firstWave.map((job) => (job.data as { label: string }).label)).toEqual(['A1', 'B1']);
    expect(await queue.getGroupActiveCount('A')).toBe(1);
    expect(await queue.getGroupActiveCount('B')).toBe(1);

    for (const job of firstWave) await manager.ack(job.id, {});
    const secondWave = await manager.pullBatch(name, 10, 0, undefined, { concurrency: 1 });
    expect(secondWave.map((job) => (job.data as { label: string }).label)).toEqual(['A2', 'B2']);
  });

  test('reports per-group depth and total grouped depth without counting active jobs', async () => {
    const name = queueName('depth');
    const queue = new Queue<{ label: string }>(name, { embedded: true });
    const manager = getSharedManager();

    for (const label of ['A1', 'A2', 'A3', 'B1']) {
      await queue.add('grouped', { label }, { group: { id: label[0] } });
    }

    expect(await queue.getGroupJobsCount('A')).toBe(3);
    expect(await queue.getGroupJobsCount('B')).toBe(1);
    expect(await queue.getGroupsJobsCount()).toBe(4);

    const [active] = await manager.pullBatch(name, 1, 0, undefined, { concurrency: 1 });
    expect(active.groupId).toBe('A');
    expect(await queue.getGroupJobsCount('A')).toBe(2);
    expect(await queue.getGroupActiveCount('A')).toBe(1);
    expect(await queue.getGroupsJobsCount()).toBe(3);
  });

  test('applies a default rate budget per group and a stored per-group override', async () => {
    const name = queueName('rate');
    const queue = new Queue<{ label: string }>(name, { embedded: true });
    const manager = getSharedManager();

    await queue.setGroupRateLimit('A', 1, 60_000);
    for (const label of ['A1', 'A2', 'B1', 'B2', 'B3']) {
      await queue.add('grouped', { label }, { group: { id: label[0] } });
    }

    const group = { limit: { max: 2, duration: 60_000 } };
    const admitted = await manager.pullBatch(name, 10, 0, undefined, group);
    expect(admitted.map((job) => (job.data as { label: string }).label)).toEqual([
      'A1',
      'B1',
      'B2',
    ]);
    expect(await queue.getGroupRateLimit('A')).toEqual({ max: 1, duration: 60_000 });
    expect(await queue.getGroupRateLimitTtl('A', 1)).toBeGreaterThan(0);
    expect(await manager.pullBatch(name, 10, 0, undefined, group)).toEqual([]);

    expect(await queue.removeGroupRateLimit('A')).toBe(1);
    expect(await queue.getGroupRateLimit('A')).toBeNull();
  });

  test('rejects invalid group controls without changing embedded overrides', async () => {
    const queue = new Queue(queueName('validation'), { embedded: true });
    await queue.setGroupRateLimit('A', 3, 60_000);
    await queue.setGroupConcurrency('A', 2);

    await expect(queue.setGroupRateLimit('A', 1.5, 60_000)).rejects.toThrow(
      'max must be a positive safe integer'
    );
    await expect(queue.setGroupRateLimit('A', 3, 1.5)).rejects.toThrow(
      'duration must be a positive safe integer'
    );
    await expect(queue.setGroupConcurrency('A', 1.5)).rejects.toThrow(
      'concurrency must be a positive safe integer'
    );

    expect(await queue.getGroupRateLimit('A')).toEqual({ max: 3, duration: 60_000 });
    expect(await queue.getGroupConcurrency('A')).toBe(2);
  });
});
