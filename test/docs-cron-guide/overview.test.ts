/**
 * Executable proof for /guide/cron/ ("Cron Jobs in Bun") and
 * /guide/queue/schedulers/ ("Job Schedulers from the Queue").
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Queue } from '../../src/client';
import { QueueManager } from '../../src/application/queueManager';
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

for (const mode of MODES) {
  describe(`cron guide · overview [${mode}]`, () => {
    test('upsertJobScheduler creates a named schedule', async () => {
      harness = await startHarness('cron-overview', mode);
      const queue = harness.queue('reports');
      const id = schedulerId('daily-report');

      const info = await queue.upsertJobScheduler(
        id,
        { pattern: '0 9 * * *' },
        { name: 'daily-report', data: { type: 'sales' } }
      );

      expect(info).not.toBeNull();
      expect(info?.id).toBe(id);
      expect(info?.pattern).toBe('0 9 * * *');
      expect(info?.next).toBeGreaterThan(Date.now());
      expect(await queue.getJobSchedulersCount()).toBe(1);
    }, 20_000);

    // SchedulerInfo.name consistently identifies the spawned job template.
    test('SchedulerInfo.name is the same value on write and on read', async () => {
      harness = await startHarness('cron-overview', mode);
      const queue = harness.queue('reports');
      const id = schedulerId('daily-report');

      const written = await queue.upsertJobScheduler(
        id,
        { pattern: '0 9 * * *' },
        { name: 'generate-report', data: {} }
      );
      const read = await queue.getJobScheduler(id);

      expect(read?.name).toBe(written?.name);
    }, 20_000);

    test('a worker processes the jobs the scheduler produces', async () => {
      harness = await startHarness('cron-overview', mode);
      const queue = harness.queue<{ type: string }>('reports');
      const seen: string[] = [];

      harness.worker<{ type: string }, boolean>(queue.name, async (job) => {
        seen.push(job.data.type);
        return true;
      });
      await queue.upsertJobScheduler(
        schedulerId('fast-report'),
        { every: 200 },
        { name: 'daily-report', data: { type: 'sales' } }
      );

      await waitUntil(() => seen.length >= 2, 'two scheduled runs', 30_000);
      expect(seen.every((type) => type === 'sales')).toBe(true);
    }, 60_000);

    test('upserting the same id replaces the definition instead of duplicating', async () => {
      harness = await startHarness('cron-overview', mode);
      const queue = harness.queue('reports');
      const id = schedulerId('daily-report');

      await queue.upsertJobScheduler(id, { pattern: '0 9 * * *' }, { data: { v: 1 } });
      await queue.upsertJobScheduler(id, { pattern: '0 18 * * *' }, { data: { v: 2 } });

      expect(await queue.getJobSchedulersCount()).toBe(1);
      expect((await queue.getJobScheduler(id))?.pattern).toBe('0 18 * * *');
    }, 20_000);

    test('a scheduler id is global to the broker and moves between queues', async () => {
      harness = await startHarness('cron-overview', mode);
      const first = harness.queue('reports');
      const second = harness.queue('invoices');
      const id = schedulerId('shared');

      await first.upsertJobScheduler(id, { pattern: '0 9 * * *' }, { data: { queue: 'first' } });
      expect(await first.getJobSchedulersCount()).toBe(1);

      // Reusing the ID on another queue moves the definition there.
      await second.upsertJobScheduler(id, { pattern: '0 9 * * *' }, { data: { queue: 'second' } });

      await waitUntil(
        async () => (await first.getJobSchedulersCount()) === 0,
        'the definition to leave the first queue'
      );
      expect(await second.getJobSchedulersCount()).toBe(1);
    }, 20_000);

    test('get, list, count and remove round-trip a scheduler', async () => {
      harness = await startHarness('cron-overview', mode);
      const queue = harness.queue('reports');
      const id = schedulerId('daily-report');

      await queue.upsertJobScheduler(
        id,
        { pattern: '0 9 * * *' },
        { name: 'generate-report', data: { type: 'daily' } }
      );

      const scheduler = await queue.getJobScheduler(id);
      expect(scheduler?.id).toBe(id);
      expect(scheduler?.name).toBe('generate-report');
      const list = await queue.getJobSchedulers(0, 100);
      expect(list.some((entry) => entry.id === id && entry.name === 'generate-report')).toBe(true);
      expect(await queue.getJobSchedulersCount()).toBe(1);

      await queue.removeJobScheduler(id);
      await waitUntil(
        async () => (await queue.getJobSchedulersCount()) === 0,
        'the scheduler to be removed'
      );
      expect(await queue.getJobScheduler(id)).toBeNull();
    }, 20_000);

    test('an interval schedule is accepted alongside the cron form', async () => {
      harness = await startHarness('cron-overview', mode);
      const queue = harness.queue('reports');
      const id = schedulerId('hourly');

      const info = await queue.upsertJobScheduler(id, { every: 3_600_000 }, { data: {} });

      expect(info?.every).toBe(3_600_000);
      expect(info?.pattern).toBeUndefined();
    }, 20_000);

    test('scheduler lists are filtered to the current queue', async () => {
      harness = await startHarness('cron-overview', mode);
      const reports = harness.queue('reports');
      const invoices = harness.queue('invoices');

      await reports.upsertJobScheduler(schedulerId('r'), { pattern: '0 9 * * *' }, { data: {} });
      await invoices.upsertJobScheduler(schedulerId('i'), { pattern: '0 9 * * *' }, { data: {} });

      expect(await reports.getJobSchedulersCount()).toBe(1);
      expect(await invoices.getJobSchedulersCount()).toBe(1);
      expect((await reports.getJobSchedulers(0, 100)).length).toBe(1);
    }, 20_000);

    test('scheduler lists honor inclusive pagination and next-run ordering', async () => {
      harness = await startHarness('cron-overview-pagination', mode);
      const queue = harness.queue('reports');
      const definitions = [
        { id: schedulerId('fourth'), every: 400_000 },
        { id: schedulerId('first'), every: 100_000 },
        { id: schedulerId('third'), every: 300_000 },
        { id: schedulerId('second'), every: 200_000 },
      ];

      for (const definition of definitions) {
        await queue.upsertJobScheduler(definition.id, { every: definition.every }, { data: {} });
      }

      const ascendingPage = await queue.getJobSchedulers(1, 2, true);
      expect(ascendingPage.map((entry) => entry.id)).toEqual([
        definitions[3].id,
        definitions[2].id,
      ]);

      const descendingPage = await queue.getJobSchedulers(0, 1, false);
      expect(descendingPage.map((entry) => entry.id)).toEqual([
        definitions[0].id,
        definitions[2].id,
      ]);

      const tiedIds = [schedulerId('tie-c'), schedulerId('tie-a'), schedulerId('tie-b')];
      for (const id of tiedIds) {
        await queue.upsertJobScheduler(id, { pattern: '0 9 * * *' }, { data: {} });
      }

      const ascendingTies = (await queue.getJobSchedulers(0, -1, true))
        .map((entry) => entry.id)
        .filter((id) => tiedIds.includes(id));
      expect(ascendingTies).toEqual([...tiedIds].sort());

      const defaultDescendingTies = (await queue.getJobSchedulers())
        .map((entry) => entry.id)
        .filter((id) => tiedIds.includes(id));
      expect(defaultDescendingTies).toEqual([...tiedIds].sort().reverse());
    }, 20_000);

    test('the Bun client applies prefixKey to the scheduler id', async () => {
      harness = await startHarness('cron-overview', mode);
      const name = `prefixed-${crypto.randomUUID().slice(0, 8)}`;
      const dev = new Queue(name, harness.queueOptions({ prefixKey: 'dev:' }));
      const prod = new Queue(name, harness.queueOptions({ prefixKey: 'prod:' }));
      harness.addCleanup(() => dev.close());
      harness.addCleanup(() => prod.close());

      await dev.upsertJobScheduler('nightly', { pattern: '0 2 * * *' }, { data: { env: 'dev' } });
      await prod.upsertJobScheduler('nightly', { pattern: '0 3 * * *' }, { data: { env: 'prod' } });

      // Same logical ID, two independent definitions.
      expect(await dev.getJobSchedulersCount()).toBe(1);
      expect(await prod.getJobSchedulersCount()).toBe(1);
      expect((await dev.getJobScheduler('nightly'))?.pattern).toBe('0 2 * * *');
      expect((await prod.getJobScheduler('nightly'))?.pattern).toBe('0 3 * * *');
    }, 20_000);
  });
}

describe('cron guide · overview [durability]', () => {
  test('a schedule survives a broker restart', async () => {
    const dataDir = mkdtempSync(join(tmpdir(), 'bunqueue-docs-cron-restart-'));
    const dataPath = join(dataDir, 'queue.db');
    let manager = new QueueManager({ dataPath });

    try {
      manager.addCron({
        name: 'daily-report',
        queue: 'reports',
        data: { type: 'sales' },
        schedule: '0 9 * * *',
      });
      await Bun.sleep(100);
      expect(manager.listCrons().map((cron) => cron.name)).toContain('daily-report');
      manager.shutdown();

      manager = new QueueManager({ dataPath });
      await waitUntil(
        () => manager.listCrons().some((cron) => cron.name === 'daily-report'),
        'the schedule to be restored'
      );
      const restored = manager.listCrons().find((cron) => cron.name === 'daily-report');
      expect(restored?.schedule).toBe('0 9 * * *');
      expect(restored?.queue).toBe('reports');
    } finally {
      manager.shutdown();
      rmSync(dataDir, { recursive: true, force: true });
    }
  }, 30_000);
});
