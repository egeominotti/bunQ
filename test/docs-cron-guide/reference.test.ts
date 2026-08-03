/**
 * Executable proof for /guide/cron/reference/
 * ("Cron Reference: Expressions, Options, MCP").
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

function schedulerId(label: string): string {
  return `${label}-${crypto.randomUUID().slice(0, 8)}`;
}

const CHEAT_SHEET = ['0 9 * * *', '*/15 * * * *', '0 0 * * MON', '0 0 1 * *'];
const SHORTCUTS = ['@daily', '@hourly', '@weekly', '@monthly', '@yearly', '@midnight'];

for (const mode of MODES) {
  describe(`cron guide · reference [${mode}]`, () => {
    test('every cheat-sheet expression resolves to a future run', async () => {
      harness = await startHarness('cron-reference', mode);
      const queue = harness.queue('expressions');

      for (const pattern of CHEAT_SHEET) {
        const info = await queue.upsertJobScheduler(
          schedulerId('cheat'),
          { pattern },
          { data: { pattern } }
        );
        expect(info, pattern).not.toBeNull();
        expect(info?.pattern, pattern).toBe(pattern);
        expect(info?.next, pattern).toBeGreaterThan(Date.now());
      }
    }, 30_000);

    test('the documented shortcuts are accepted', async () => {
      harness = await startHarness('cron-reference', mode);
      const queue = harness.queue('shortcuts');

      for (const pattern of SHORTCUTS) {
        const info = await queue.upsertJobScheduler(
          schedulerId('shortcut'),
          { pattern },
          { data: { pattern } }
        );
        expect(info, pattern).not.toBeNull();
        expect(info?.next, pattern).toBeGreaterThan(Date.now());
      }
    }, 30_000);

    test('the six-field form with leading seconds is accepted', async () => {
      harness = await startHarness('cron-reference', mode);
      const queue = harness.queue('seconds');

      const info = await queue.upsertJobScheduler(
        schedulerId('with-seconds'),
        { pattern: '30 0 9 * * *' },
        { data: {} }
      );

      expect(info).not.toBeNull();
      expect(info?.next).toBeGreaterThan(Date.now());
    }, 20_000);

    test('day-of-week accepts both 0 and 7 for Sunday', async () => {
      harness = await startHarness('cron-reference', mode);
      const queue = harness.queue('sunday');

      const zero = await queue.upsertJobScheduler(
        schedulerId('sunday-0'),
        { pattern: '0 0 * * 0' },
        { data: {} }
      );
      const seven = await queue.upsertJobScheduler(
        schedulerId('sunday-7'),
        { pattern: '0 0 * * 7' },
        { data: {} }
      );

      expect(zero?.next).toBe(seven?.next);
    }, 20_000);

    test('immediately fires once right away on first creation', async () => {
      harness = await startHarness('cron-reference', mode);
      const queue = harness.queue('immediate');
      let runs = 0;

      harness.worker(queue.name, async () => {
        runs++;
        return true;
      });
      await queue.upsertJobScheduler(
        schedulerId('now'),
        { pattern: '0 9 1 1 *', immediately: true },
        { data: {} }
      );

      await waitUntil(() => runs === 1, 'the immediate run', 20_000);
    }, 40_000);

    test('immediately defaults to false', async () => {
      harness = await startHarness('cron-reference', mode);
      const queue = harness.queue('not-immediate');
      let runs = 0;

      harness.worker(queue.name, async () => {
        runs++;
        return true;
      });
      // Next 1 January at 09:00: nothing may fire now.
      await queue.upsertJobScheduler(schedulerId('later'), { pattern: '0 9 1 1 *' }, { data: {} });
      await Bun.sleep(800);

      expect(runs).toBe(0);
    }, 30_000);

    test('skipIfNoWorker skips a run while no worker is registered', async () => {
      harness = await startHarness('cron-reference', mode);
      const queue = harness.queue('skip-no-worker');

      await queue.upsertJobScheduler(
        schedulerId('skipping'),
        { every: 150, skipIfNoWorker: true },
        { data: {} }
      );
      await Bun.sleep(1_000);
      expect(await queue.countAsync()).toBe(0);

      // With a worker registered the schedule starts producing again.
      let runs = 0;
      harness.worker(queue.name, async () => {
        runs++;
        return true;
      });
      await waitUntil(() => runs >= 1, 'a run once a worker exists', 20_000);
    }, 40_000);

    test('skipIfNoWorker defaults to false', async () => {
      harness = await startHarness('cron-reference', mode);
      const queue = harness.queue('no-skip');

      await queue.upsertJobScheduler(schedulerId('unskipped'), { every: 150 }, { data: {} });

      await waitUntil(
        async () => (await queue.countAsync()) >= 1,
        'a job to be produced without any worker',
        20_000
      );
    }, 40_000);

    test('scheduler every advances from the slot, not from completion', async () => {
      harness = await startHarness('cron-reference', mode);
      const queue = harness.queue('fixed-rate');
      const starts: number[] = [];

      harness.worker(
        queue.name,
        async () => {
          starts.push(Date.now());
          await Bun.sleep(250); // processing takes longer than one slot
          return true;
        },
        { concurrency: 4 }
      );
      await queue.upsertJobScheduler(
        schedulerId('fixed'),
        { every: 200, preventOverlap: false },
        { data: {} }
      );

      await waitUntil(() => starts.length >= 3, 'three slots', 30_000);
      // A completion-anchored schedule would space runs by >= 250ms + 200ms.
      expect(starts[2] - starts[0]).toBeLessThan(1_200);
    }, 60_000);
  });
}
