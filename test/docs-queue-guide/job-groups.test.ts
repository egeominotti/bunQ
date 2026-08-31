/**
 * Executable proof for /guide/queue/job-groups/
 * ("Job Groups: FIFO, Fairness and Backpressure").
 */

import { afterEach, describe, expect, test } from 'bun:test';
import {
  type CoreE2eHarness,
  MODES,
  closeHarness,
  startHarness,
  waitUntil,
} from '../docs-guide-support';

let harness: CoreE2eHarness | null = null;

afterEach(async () => {
  await closeHarness(harness);
  harness = null;
});

for (const mode of MODES) {
  describe(`queue guide · job groups [${mode}]`, () => {
    test('plain jobs precede fair group rotation while each group remains FIFO', async () => {
      harness = await startHarness('job-groups-order', mode);
      const queue = harness.queue<{ label: string }>('webhooks');
      await queue.pauseAsync();
      const timestamp = Date.now();

      await queue.add('deliver', { label: 'plain' }, { durable: true, timestamp });
      await queue.add(
        'deliver',
        { label: 'A1' },
        { durable: true, group: { id: 'tenant-a' }, jobId: 'z-first', timestamp }
      );
      await queue.add(
        'deliver',
        { label: 'A2' },
        { durable: true, group: { id: 'tenant-a' }, jobId: 'a-second', timestamp }
      );
      await queue.add(
        'deliver',
        { label: 'B1' },
        { durable: true, group: { id: 'tenant-b' }, jobId: 'y-first', timestamp }
      );
      await queue.add(
        'deliver',
        { label: 'B2' },
        { durable: true, group: { id: 'tenant-b' }, jobId: 'b-second', timestamp }
      );

      const order: string[] = [];
      harness.worker<{ label: string }, string>(
        queue.name,
        (job) => {
          order.push(job.data.label);
          return job.data.label;
        },
        { batchSize: 1, concurrency: 1 }
      );
      await queue.resumeAsync();

      await waitUntil(() => order.length === 5, 'all grouped jobs to complete', 15_000);
      expect(order).toEqual(['plain', 'A1', 'B1', 'A2', 'B2']);
    }, 20_000);

    test('depth, active count, policy CRUD, TTL semantics, and validation are observable', async () => {
      harness = await startHarness('job-groups-control', mode);
      const queue = harness.queue('webhooks');
      await queue.add('A1', {}, { durable: true, group: { id: 'tenant-a' } });
      await queue.add('A2', {}, { durable: true, group: { id: 'tenant-a' } });
      await queue.add('B1', {}, { durable: true, group: { id: 'tenant-b' } });

      expect(await queue.getGroupJobsCount('tenant-a')).toBe(2);
      expect(await queue.getGroupsJobsCount()).toBe(3);
      expect(await queue.getGroupActiveCount('tenant-a')).toBe(0);
      expect(await queue.getGroupRateLimitTtl('tenant-a', 1)).toBe(-2);

      await queue.setGroupRateLimit('tenant-a', 5, 1_000);
      await queue.setGroupConcurrency('tenant-a', 1);
      expect(await queue.getGroupRateLimit('tenant-a')).toEqual({ max: 5, duration: 1_000 });
      expect(await queue.getGroupConcurrency('tenant-a')).toBe(1);

      await expect(queue.add('empty', {}, { group: { id: '' } })).rejects.toThrow(/non-empty/);
      await expect(queue.add('nul', {}, { group: { id: 'a\0b' } })).rejects.toThrow(/NUL/);
      await expect(queue.add('fraction', {}, { group: { id: 1.5 } })).rejects.toThrow(
        /safe integer/
      );

      expect(await queue.removeGroupRateLimit('tenant-a')).toBe(1);
      expect(await queue.removeGroupConcurrency('tenant-a')).toBe(1);
      expect(await queue.getGroupRateLimit('tenant-a')).toBeNull();
      expect(await queue.getGroupConcurrency('tenant-a')).toBeNull();
    });

    test('a local concurrency override composes with the worker group default', async () => {
      harness = await startHarness('job-groups-concurrency', mode);
      const queue = harness.queue<{ label: string }>('webhooks');
      await queue.pauseAsync();
      await queue.setGroupConcurrency('A', 1);
      await queue.addBulk(
        ['A1', 'A2', 'B1', 'B2'].map((label) => ({
          name: label,
          data: { label },
          opts: { durable: true, group: { id: label[0] } },
        }))
      );

      const active = new Map<string, number>();
      const peak = new Map<string, number>();
      let completed = 0;
      harness.worker<{ label: string }, string>(
        queue.name,
        async (job) => {
          const groupId = String(job.opts.group?.id);
          const current = (active.get(groupId) ?? 0) + 1;
          active.set(groupId, current);
          peak.set(groupId, Math.max(peak.get(groupId) ?? 0, current));
          await Bun.sleep(100);
          active.set(groupId, current - 1);
          completed++;
          return job.data.label;
        },
        { batchSize: 4, concurrency: 4, group: { concurrency: 2 } }
      );
      await queue.resumeAsync();

      await waitUntil(() => completed === 4, 'all concurrency fixtures to complete', 15_000);
      await waitUntil(
        async () => (await queue.getGroupActiveCount('A')) === 0,
        'the group concurrency slot to be released',
        10_000
      );
      expect(Object.fromEntries(peak)).toEqual({ A: 1, B: 2 });
      expect(await queue.getGroupActiveCount('A')).toBe(0);
    }, 20_000);

    test('a local rate override delays the next claim and survives broker restart', async () => {
      harness = await startHarness('job-groups-rate', mode);
      const queue = harness.queue<{ label: string }>('webhooks');
      await queue.pauseAsync();
      await queue.setGroupRateLimit('A', 1, 500);
      const timestamp = Date.now();
      const first = await queue.add(
        'A1',
        { label: 'A1' },
        { durable: true, group: { id: 'A' }, jobId: 'z-first', timestamp }
      );
      const second = await queue.add(
        'A2',
        { label: 'A2' },
        { durable: true, group: { id: 'A' }, jobId: 'a-second', timestamp }
      );

      await harness.restartBroker();
      const afterRestart = await queue.add(
        'B1',
        { label: 'B1' },
        { durable: true, group: { id: 'B' } }
      );
      const [afterRestartBulk] = await queue.addBulk([
        { name: 'C1', data: { label: 'C1' }, opts: { durable: true, group: { id: 'C' } } },
      ]);
      expect(await queue.getGroupRateLimit('A')).toEqual({ max: 1, duration: 500 });
      expect(await queue.getGroupJobsCount('A')).toBe(2);
      expect(await queue.getGroupsJobsCount()).toBe(4);

      const starts: Array<{ label: string; time: number }> = [];
      harness.worker<{ label: string }, string>(
        queue.name,
        (job) => {
          if (job.opts.group?.id === 'A') {
            starts.push({ label: job.data.label, time: Date.now() });
          }
          return job.data.label;
        },
        {
          batchSize: 2,
          concurrency: 2,
          group: { limit: { max: 2, duration: 500 } },
        }
      );
      await queue.resumeAsync();
      await waitUntil(() => starts.length === 1, 'the first rate-limited claim', 10_000);
      const ttl = await queue.getGroupRateLimitTtl('A', 1);
      await waitUntil(() => starts.length === 2, 'the second rate-limited claim', 10_000);

      expect(starts.map((entry) => entry.label)).toEqual(['A1', 'A2']);
      expect(starts[1]!.time - starts[0]!.time).toBeGreaterThanOrEqual(350);
      expect(ttl).toBeGreaterThan(0);
      expect(ttl).toBeLessThanOrEqual(500);
      await waitUntil(
        async () =>
          (await queue.getJobState(first.id)) === 'completed' &&
          (await queue.getJobState(second.id)) === 'completed' &&
          (await queue.getJobState(afterRestart.id)) === 'completed' &&
          (await queue.getJobState(afterRestartBulk.id)) === 'completed',
        'all post-restart acknowledgements to persist',
        10_000
      );

      await queue.obliterateAsync();
      expect(await queue.getGroupRateLimit('A')).toBeNull();
    }, 25_000);
  });
}
