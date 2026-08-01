import { afterEach, describe, expect, test } from 'bun:test';
import { QueueGroup, shutdownManager, type Queue } from '../../src/client';
import { closeTcpHarness, startTcpHarness, type TcpHarness } from './tcp-harness';

let harness: TcpHarness | null = null;

function setup(): { group: QueueGroup; queue: Queue<{ value: number }> } {
  harness = startTcpHarness();
  const group = new QueueGroup(`group-${Bun.randomUUIDv7()}`);
  const queue = group.getQueue<{ value: number }>('alpha', {
    embedded: false,
    connection: {
      host: '127.0.0.1',
      port: harness.port,
      poolSize: 1,
      pingInterval: 0,
    },
    autoBatch: { enabled: false },
  });
  harness.queues.push(queue as Queue<unknown>);
  return { group, queue };
}

afterEach(async () => {
  await closeTcpHarness(harness);
  harness = null;
  shutdownManager();
});

describe('QueueGroup bulk methods operate on queues created over TCP', () => {
  test('listQueues includes TCP queues created in the group', async () => {
    const { group, queue } = setup();
    await queue.add('task', { value: 1 });
    expect(await group.listQueuesAsync()).toContain('alpha');
  });

  test('pauseAll pauses every TCP queue in the group', async () => {
    const { group, queue } = setup();
    const second = group.getQueue<{ value: number }>('beta', {
      embedded: false,
      connection: { host: '127.0.0.1', port: harness!.port, poolSize: 1, pingInterval: 0 },
      autoBatch: { enabled: false },
    });
    harness!.queues.push(second as Queue<unknown>);
    await queue.add('task', { value: 1 });
    await second.add('task', { value: 2 });

    await group.pauseAllAsync();
    expect(await queue.isPausedAsync()).toBe(true);
    expect(await second.isPausedAsync()).toBe(true);
  });

  test('resumeAll resumes every paused TCP queue in the group', async () => {
    const { group, queue } = setup();
    await queue.pauseAsync();
    expect(await queue.isPausedAsync()).toBe(true);

    await group.resumeAllAsync();
    expect(await queue.isPausedAsync()).toBe(false);
  });

  test('drainAll removes waiting jobs from every TCP queue in the group', async () => {
    const { group, queue } = setup();
    await queue.add('task', { value: 1 });
    await queue.add('task', { value: 2 });

    expect(await group.drainAllAsync()).toBe(2);
    expect(await queue.countAsync()).toBe(0);
  });

  test('obliterateAll removes all data from every TCP queue in the group', async () => {
    const { group, queue } = setup();
    const job = await queue.add('task', { value: 1 });

    await group.obliterateAllAsync();
    expect(await queue.getJob(job.id)).toBeNull();
  });
});
