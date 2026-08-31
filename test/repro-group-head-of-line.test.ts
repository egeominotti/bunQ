import { describe, expect, test } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { shardIndex } from '../src/shared/hash';

function findCollidingQueues(): [string, string] {
  const firstByShard = new Map<number, string>();
  for (let i = 0; i < 10_000; i++) {
    const queue = `group-wakeup-collision-${i}`;
    const idx = shardIndex(queue);
    const existing = firstByShard.get(idx);
    if (existing) return [existing, queue];
    firstByShard.set(idx, queue);
  }
  throw new Error('Unable to find two queues on the same shard');
}

describe('FIFO group head-of-line regression', () => {
  test('pull skips a blocked group head and returns a job from a free group', async () => {
    const qm = new QueueManager();
    const baseTimestamp = Date.now() - 1_000;

    try {
      const a1 = await qm.push('group-hol-single', {
        data: { label: 'A1' },
        groupId: 'group-a',
        timestamp: baseTimestamp,
      });
      const a2 = await qm.push('group-hol-single', {
        data: { label: 'A2' },
        groupId: 'group-a',
        timestamp: baseTimestamp + 1,
      });
      const b1 = await qm.push('group-hol-single', {
        data: { label: 'B1' },
        groupId: 'group-b',
        timestamp: baseTimestamp + 2,
      });

      const group = { concurrency: 1 };
      const first = await qm.pull('group-hol-single', 0, undefined, group);
      const second = await qm.pull('group-hol-single', 0, undefined, group);

      expect(first?.id).toBe(a1.id);
      expect(second?.id).toBe(b1.id);
      expect(await qm.getJobState(a2.id)).toBe('waiting');
    } finally {
      qm.shutdown();
    }
  });

  test('pullBatch continues past a blocked group head', async () => {
    const qm = new QueueManager();
    const baseTimestamp = Date.now() - 1_000;

    try {
      const a1 = await qm.push('group-hol-batch', {
        data: { label: 'A1' },
        groupId: 'group-a',
        timestamp: baseTimestamp,
      });
      const a2 = await qm.push('group-hol-batch', {
        data: { label: 'A2' },
        groupId: 'group-a',
        timestamp: baseTimestamp + 1,
      });
      const b1 = await qm.push('group-hol-batch', {
        data: { label: 'B1' },
        groupId: 'group-b',
        timestamp: baseTimestamp + 2,
      });

      const batch = await qm.pullBatch('group-hol-batch', 3, 0, undefined, {
        concurrency: 1,
      });

      expect(batch.map((job) => job.id)).toEqual([a1.id, b1.id]);
      expect(await qm.getJobState(a2.id)).toBe('waiting');
    } finally {
      qm.shutdown();
    }
  });

  test('a blocked pull does not consume a rate-limit token', async () => {
    const qm = new QueueManager();

    try {
      const a1 = await qm.push('group-hol-rate-limit', {
        data: { label: 'A1' },
        groupId: 'group-a',
      });
      const a2 = await qm.push('group-hol-rate-limit', {
        data: { label: 'A2' },
        groupId: 'group-a',
      });
      qm.setRateLimit('group-hol-rate-limit', 2);

      const group = { concurrency: 1 };
      expect((await qm.pull('group-hol-rate-limit', 0, undefined, group))?.id).toBe(a1.id);
      expect(await qm.pull('group-hol-rate-limit', 0, undefined, group)).toBeNull();

      await qm.ack(a1.id, {});
      expect((await qm.pull('group-hol-rate-limit', 0, undefined, group))?.id).toBe(a2.id);
    } finally {
      qm.shutdown();
    }
  });

  test('releasing a group wakes a long-poll pull', async () => {
    const qm = new QueueManager();

    try {
      const a1 = await qm.push('group-hol-wakeup', {
        data: { label: 'A1' },
        groupId: 'group-a',
      });
      const a2 = await qm.push('group-hol-wakeup', {
        data: { label: 'A2' },
        groupId: 'group-a',
      });

      const group = { concurrency: 1 };
      expect((await qm.pull('group-hol-wakeup', 0, undefined, group))?.id).toBe(a1.id);

      const startedAt = Date.now();
      const nextPull = qm.pull('group-hol-wakeup', 1_500, undefined, group);
      await Bun.sleep(25);
      await qm.ack(a1.id, {});

      expect((await nextPull)?.id).toBe(a2.id);
      expect(Date.now() - startedAt).toBeLessThan(1_000);
    } finally {
      qm.shutdown();
    }
  });

  test('releasing queue concurrency wakes a long-poll pull promptly', async () => {
    const qm = new QueueManager();
    const queue = 'concurrency-slot-wakeup';

    try {
      qm.setConcurrency(queue, 1);
      const first = await qm.push(queue, { data: { label: 'first' } });
      const second = await qm.push(queue, { data: { label: 'second' } });

      expect((await qm.pull(queue, 0))?.id).toBe(first.id);

      const startedAt = Date.now();
      const nextPull = qm.pull(queue, 1_500);
      await Bun.sleep(25);
      await qm.ack(first.id, {});

      expect((await nextPull)?.id).toBe(second.id);
      expect(Date.now() - startedAt).toBeLessThan(500);
    } finally {
      qm.shutdown();
    }
  });

  test('a colliding queue cannot consume another queue group wake-up', async () => {
    const qm = new QueueManager();
    const [otherQueue, targetQueue] = findCollidingQueues();

    try {
      const first = await qm.push(targetQueue, {
        data: { label: 'first' },
        groupId: 'serial',
      });
      const second = await qm.push(targetQueue, {
        data: { label: 'second' },
        groupId: 'serial',
      });
      const group = { concurrency: 1 };
      expect((await qm.pull(targetQueue, 0, undefined, group))?.id).toBe(first.id);

      // Register the unrelated waiter first so a shard-global FIFO notifier
      // would incorrectly hand it the target queue's wake-up.
      const unrelatedPull = qm.pull(otherQueue, 800);
      await Bun.sleep(20);
      const startedAt = Date.now();
      const targetPull = qm.pull(targetQueue, 800, undefined, group);
      await Bun.sleep(20);
      await qm.ack(first.id, {});

      expect((await targetPull)?.id).toBe(second.id);
      expect(Date.now() - startedAt).toBeLessThan(500);
      expect(await unrelatedPull).toBeNull();
    } finally {
      qm.shutdown();
    }
  });
});
