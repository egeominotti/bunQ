/**
 * Executable proof for /guide/cron/recipes/
 * ("Cron Recipes: Intervals, Timezones, Chained Repeats").
 */

import { afterEach, describe, expect, test } from 'bun:test';
import {
  type CoreE2eHarness,
  MODES,
  closeHarness,
  startHarness,
  waitForState,
  waitUntil,
} from '../docs-guide-support';

let harness: CoreE2eHarness | null = null;

afterEach(async () => {
  await closeHarness(harness);
  harness = null;
});

function schedulerId(label: string): string {
  return `${label}-${crypto.randomUUID().slice(0, 8)}`;
}

for (const mode of MODES) {
  describe(`cron guide · recipes [${mode}]`, () => {
    test('every fires on fixed-rate slots anchored to the schedule', async () => {
      harness = await startHarness('cron-recipes', mode);
      const queue = harness.queue<{ check: string }>('system');
      const starts: number[] = [];

      harness.worker<{ check: string }, boolean>(queue.name, async () => {
        starts.push(Date.now());
        return true;
      });
      await queue.upsertJobScheduler(
        schedulerId('heartbeat'),
        { every: 300 },
        { data: { check: 'health' } }
      );

      await waitUntil(() => starts.length >= 3, 'three fixed-rate slots', 30_000);
      const firstGap = starts[1] - starts[0];
      const secondGap = starts[2] - starts[1];
      expect(firstGap).toBeGreaterThan(100);
      expect(firstGap).toBeLessThan(1_500);
      expect(secondGap).toBeLessThan(1_500);
    }, 60_000);

    test('limit stops the scheduler after the configured number of runs', async () => {
      harness = await startHarness('cron-recipes', mode);
      const queue = harness.queue('system');
      let runs = 0;

      harness.worker(queue.name, async () => {
        runs++;
        return true;
      });
      const id = schedulerId('capped');
      const info = await queue.upsertJobScheduler(
        id,
        { every: 200, limit: 2 },
        { data: { check: 'health' } }
      );
      expect(info?.limit).toBe(2);

      await waitUntil(() => runs >= 2, 'both capped runs', 30_000);
      await waitUntil(
        async () => (await queue.getJobScheduler(id)) === null,
        'the exhausted scheduler to be removed',
        30_000
      );
      const after = runs;
      await Bun.sleep(600);
      expect(runs).toBe(after);
    }, 60_000);

    test('preventOverlap defaults to true and blocks a second live generation', async () => {
      harness = await startHarness('cron-recipes', mode);
      const queue = harness.queue('system');
      let started = 0;
      let release = (): void => undefined;
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });

      harness.worker(queue.name, async () => {
        started++;
        await held;
        return true;
      });
      await queue.upsertJobScheduler(
        schedulerId('overlapping'),
        { every: 150 },
        { data: { check: 'health' } }
      );

      await waitUntil(() => started === 1, 'the first generation to start', 20_000);
      await Bun.sleep(1_200); // several ticks pass while the first job is active

      expect(started).toBe(1);
      const counts = await queue.getJobCountsAsync();
      expect(counts.active).toBe(1);
      expect(counts.waiting + counts.prioritized).toBe(0);
      release();
    }, 60_000);

    test('a timezone changes when the same pattern next fires', async () => {
      harness = await startHarness('cron-recipes', mode);
      const queue = harness.queue('reports');

      const rome = await queue.upsertJobScheduler(
        schedulerId('rome'),
        { pattern: '0 18 * * 1-5', timezone: 'Europe/Rome' },
        { data: { type: 'summary' } }
      );
      const newYork = await queue.upsertJobScheduler(
        schedulerId('new-york'),
        { pattern: '0 18 * * 1-5', timezone: 'America/New_York' },
        { data: { type: 'summary' } }
      );

      expect(rome?.next).toBeGreaterThan(0);
      expect(newYork?.next).toBeGreaterThan(0);
      // Same wall-clock pattern in two zones cannot resolve to the same instant.
      expect(rome?.next).not.toBe(newYork?.next);
    }, 20_000);

    test('repeat re-enqueues after each completion, limit successors later', async () => {
      harness = await startHarness('cron-recipes', mode);
      const queue = harness.queue<{ source: string }>('sync');
      let runs = 0;

      harness.worker<{ source: string }, boolean>(queue.name, async () => {
        runs++;
        return true;
      });
      await queue.add('sync', { source: 'crm' }, { repeat: { every: 100, limit: 2 } });

      // total executions = initial job + at most `limit` repeats
      await waitUntil(() => runs >= 3, `initial run plus two repeats (saw ${runs})`, 30_000);
      await Bun.sleep(800);
      expect(runs).toBe(3);
    }, 60_000);

    test('a failed terminal job does not create a successor', async () => {
      harness = await startHarness('cron-recipes', mode);
      const queue = harness.queue('sync');
      let runs = 0;

      harness.worker(queue.name, async () => {
        runs++;
        throw new Error('always');
      });
      const job = await queue.add(
        'sync',
        { source: 'crm' },
        { repeat: { every: 100, limit: 5 }, attempts: 1, durable: true }
      );

      await waitForState(queue, job.id, 'failed', 20_000);
      const after = runs;
      await Bun.sleep(800);
      expect(runs).toBe(after);
      expect(runs).toBe(1);
    }, 60_000);

    test('repeat.pattern on add() follows the cron schedule', async () => {
      harness = await startHarness('cron-recipes', mode);
      const queue = harness.queue('sync');
      const starts: number[] = [];

      harness.worker(queue.name, async () => {
        starts.push(Date.now());
        return true;
      });
      await queue.add(
        'sync',
        { source: 'crm' },
        { repeat: { pattern: '* * * * * *', limit: 2, tz: 'UTC' } }
      );

      await waitUntil(() => starts.length >= 3, 'the initial run plus two cron repeats', 20_000);
      expect(await queue.getJobSchedulersCount()).toBe(0);
      expect(starts[2] - starts[1]).toBeGreaterThan(400);
      await Bun.sleep(1_200);
      expect(starts).toHaveLength(3);
    }, 40_000);
  });
}
