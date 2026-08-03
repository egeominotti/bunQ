/**
 * Executable proof for /guide/queue/querying/
 * ("Querying Jobs: State, Counts and Results").
 */

import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../../src/application/queueManager';
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

const DOCUMENTED_STATES = [
  'waiting',
  'prioritized',
  'delayed',
  'active',
  'completed',
  'failed',
  'waiting-children',
  'unknown',
];

const DOCUMENTED_COUNT_KEYS = [
  'waiting',
  'prioritized',
  'active',
  'completed',
  'failed',
  'delayed',
  'waiting-children',
  'paused',
];

for (const mode of MODES) {
  describe(`queue guide · querying [${mode}]`, () => {
    test('getJob and getJobState answer for one job', async () => {
      harness = await startHarness('querying', mode);
      const queue = harness.queue<{ key: string }>('reads');

      const added = await queue.add('job-name', { key: 'value' }, { durable: true });
      const job = await queue.getJob(added.id);
      const state = await queue.getJobState(added.id);

      expect(job?.id).toBe(added.id);
      expect(job?.name).toBe('job-name');
      expect(DOCUMENTED_STATES).toContain(state);
      expect(state).toBe('waiting');
      expect(await queue.getJob('does-not-exist')).toBeNull();
      expect(await queue.getJobState('does-not-exist')).toBe('unknown');
    });

    test('every documented job state is reachable and reported', async () => {
      harness = await startHarness('querying', mode);
      const queue = harness.queue('states');
      await queue.pauseAsync();

      const waiting = await queue.add('waiting', {}, { durable: true });
      const prioritized = await queue.add('prioritized', {}, { priority: 5, durable: true });
      const delayed = await queue.add('delayed', {}, { delay: 60_000, durable: true });
      await queue.resumeAsync();

      expect(await queue.getJobState(waiting.id)).toBe('waiting');
      expect(await queue.getJobState(prioritized.id)).toBe('prioritized');
      expect(await queue.getJobState(delayed.id)).toBe('delayed');

      const failing = harness.queue('states-failed');
      harness.worker(failing.name, async () => {
        throw new Error('nope');
      });
      const failed = await failing.add('failed', {}, { attempts: 1, durable: true });
      await waitForState(failing, failed.id, 'failed');

      const ok = harness.queue('states-completed');
      let release = (): void => undefined;
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });
      harness.worker(ok.name, async () => {
        await held;
        return true;
      });
      const completed = await ok.add('completed', {}, { durable: true });
      await waitForState(ok, completed.id, 'active');
      release();
      await waitForState(ok, completed.id, 'completed');
    }, 20_000);

    test('getJobCountsAsync returns every documented state key', async () => {
      harness = await startHarness('querying', mode);
      const queue = harness.queue('counts');
      await queue.pauseAsync();
      await queue.add('a', {}, { durable: true });
      await queue.add('b', {}, { delay: 60_000, durable: true });

      const counts = await queue.getJobCountsAsync();

      expect(Object.keys(counts).sort()).toEqual([...DOCUMENTED_COUNT_KEYS].sort());
      expect(counts.paused).toBe(1);
      expect(counts.delayed).toBe(1);
    });

    test('getJobsAsync filters by state and honours start/end', async () => {
      harness = await startHarness('querying', mode);
      const queue = harness.queue('lists');
      harness.worker(queue.name, async () => {
        throw new Error('always fails');
      });
      for (let i = 0; i < 5; i++) {
        await queue.add(`fail-${i}`, { i }, { attempts: 1, durable: true });
      }
      await waitUntil(async () => (await queue.getFailedCount()) === 5, 'five failed jobs', 15_000);

      const page = await queue.getJobsAsync({ state: 'failed', start: 0, end: 3 });
      expect(page).toHaveLength(3);
      const all = await queue.getJobsAsync({ state: 'failed', start: 0, end: 50 });
      expect(all).toHaveLength(5);
    }, 30_000);

    test('the async read API works in this mode', async () => {
      harness = await startHarness('querying', mode);
      const queue = harness.queue('async-reads');
      await queue.add('a', {}, { durable: true });
      await queue.add('b', {}, { priority: 3, durable: true });

      expect(await queue.countAsync()).toBe(2);
      expect((await queue.getJobCountsAsync()).waiting).toBe(1);
      expect(await queue.getCountsPerPriorityAsync()).toMatchObject({ 0: 1, 3: 1 });
      expect(await queue.isPausedAsync()).toBe(false);
      await queue.pauseAsync();
      expect(await queue.isPausedAsync()).toBe(true);
      expect(await queue.getJobsAsync({ end: -1 })).toHaveLength(2);
    });

    test('the sync read API is embedded-only, as documented', async () => {
      harness = await startHarness('querying', mode);
      const queue = harness.queue('sync-reads');
      await queue.add('a', {}, { durable: true });
      await queue.add('b', {}, { priority: 3, durable: true });
      await queue.pauseAsync();
      // The sync path reads persisted rows, so let the write buffer settle.
      await waitUntil(async () => (await queue.countAsync()) === 2, 'both jobs persisted');

      if (mode === 'embedded') {
        expect(queue.count()).toBe(2);
        expect(queue.getJobCounts().paused).toBe(2);
        expect(queue.getJobs({ end: -1 })).toHaveLength(2);
        expect(queue.getCountsPerPriority()).toMatchObject({ 0: 1, 3: 1 });
        expect(queue.isPaused()).toBe(true);
      } else {
        expect(queue.count()).toBe(0);
        expect(queue.getJobs({ end: -1 })).toEqual([]);
        expect(queue.getCountsPerPriority()).toEqual({});
        expect(queue.isPaused()).toBe(false);
        // The async twin is authoritative over TCP.
        expect(await queue.countAsync()).toBe(2);
        expect(await queue.isPausedAsync()).toBe(true);
      }
    });

    test('per-state shortcuts and their count twins agree', async () => {
      harness = await startHarness('querying', mode);
      const queue = harness.queue('shortcuts');
      await queue.pauseAsync();
      await queue.add('w1', {}, { durable: true });
      await queue.add('w2', {}, { durable: true });
      await queue.add('d1', {}, { delay: 60_000, durable: true });
      await queue.resumeAsync();

      expect(await queue.getWaitingAsync(0, 10)).toHaveLength(2);
      expect(await queue.getDelayedAsync(0, 10)).toHaveLength(1);
      expect(await queue.getActiveAsync(0, 10)).toEqual([]);
      expect(await queue.getCompletedAsync(0, 10)).toEqual([]);
      expect(await queue.getFailedAsync(0, 10)).toEqual([]);

      expect(await queue.getWaitingCount()).toBe(2);
      expect(await queue.getDelayedCount()).toBe(1);
      expect(await queue.getActiveCount()).toBe(0);
      expect(await queue.getCompletedCount()).toBe(0);
      expect(await queue.getFailedCount()).toBe(0);

      if (mode === 'embedded') {
        expect(queue.getWaiting(0, 10)).toHaveLength(2);
        expect(queue.getDelayed(0, 10)).toHaveLength(1);
      } else {
        expect(queue.getWaiting(0, 10)).toEqual([]);
      }
    });

    test('getPrioritized returns only jobs with priority > 0', async () => {
      harness = await startHarness('querying', mode);
      const queue = harness.queue('prioritized');
      await queue.pauseAsync();

      const plain = await queue.add('plain', {}, { durable: true });
      const high = await queue.add('high', {}, { priority: 10, durable: true });
      await queue.resumeAsync();

      const prioritized = await queue.getPrioritized(0, 10);
      expect(prioritized.map((job) => job.id)).toEqual([high.id]);
      expect(prioritized.map((job) => job.id)).not.toContain(plain.id);
      expect(await queue.getPrioritizedCount()).toBe(1);
    });

    test('getWaitingChildren selects the dedicated state', async () => {
      harness = await startHarness('querying', mode);
      const queue = harness.queue('children');
      const flow = harness.flow();

      await flow.add({
        name: 'parent',
        queueName: queue.name,
        data: { parent: true },
        children: [{ name: 'child', queueName: queue.name, data: { child: true } }],
      });
      await waitUntil(
        async () => (await queue.getWaitingChildrenCount()) === 1,
        'the parent to be waiting on its child',
        10_000
      );

      const waitingChildren = await queue.getWaitingChildren(0, 10);
      expect(waitingChildren).toHaveLength(1);
      expect(await queue.getJobState(waitingChildren[0].id)).toBe('waiting-children');
      expect(
        (await queue.getJobsAsync({ state: 'waiting-children', start: 0, end: 10 })).map(
          (job) => job.id
        )
      ).toEqual([waitingChildren[0].id]);
    }, 20_000);

    test('end: -1 is an exhaustive read across paging', async () => {
      harness = await startHarness('querying', mode);
      const queue = harness.queue('exhaustive');
      await queue.pauseAsync();
      const total = 1_050; // above the 1,000-row TCP page size
      await queue.addBulk(
        Array.from({ length: total }, (_, index) => ({ name: `job-${index}`, data: { index } }))
      );
      await waitUntil(async () => (await queue.countAsync()) === total, 'every job persisted');

      const exhaustive = await queue.getJobsAsync({ end: -1 });
      expect(exhaustive).toHaveLength(total);
      expect(new Set(exhaustive.map((job) => job.id)).size).toBe(total);

      const firstPage = await queue.getJobsAsync({ start: 0, end: 100 });
      expect(firstPage).toHaveLength(100);

      await queue.resumeAsync();
      expect(await queue.getWaitingAsync(0, -1)).toHaveLength(total);
      if (mode === 'embedded') expect(queue.getJobs({ end: -1 })).toHaveLength(total);
    }, 60_000);

    test('asc: false reverses the stable order', async () => {
      harness = await startHarness('querying', mode);
      const queue = harness.queue('ordering');
      await queue.pauseAsync();
      const ids: string[] = [];
      for (let i = 0; i < 5; i++) {
        ids.push((await queue.add(`job-${i}`, { i }, { durable: true })).id);
      }

      const ascending = await queue.getJobsAsync({ start: 0, end: -1, asc: true });
      const descending = await queue.getJobsAsync({ start: 0, end: -1, asc: false });

      expect(ascending.map((job) => job.id)).toEqual(ids);
      expect(descending.map((job) => job.id)).toEqual([...ids].reverse());
    });

    test('paused jobs are counted as paused and return to their logical state', async () => {
      harness = await startHarness('querying', mode);
      const queue = harness.queue('pausing');
      await queue.pauseAsync();

      const plain = await queue.add('plain', {}, { durable: true });
      const high = await queue.add('high', {}, { priority: 7, durable: true });

      const paused = await queue.getJobCountsAsync();
      expect(paused.paused).toBe(2);
      expect(paused.waiting).toBe(0);
      expect(paused.prioritized).toBe(0);

      await queue.resumeAsync();
      const resumed = await queue.getJobCountsAsync();
      expect(resumed.paused).toBe(0);
      expect(resumed.waiting).toBe(1);
      expect(resumed.prioritized).toBe(1);
      expect(await queue.getJobState(plain.id)).toBe('waiting');
      expect(await queue.getJobState(high.id)).toBe('prioritized');
    });

    test('a job in the DLQ stays visible through the failed views', async () => {
      harness = await startHarness('querying', mode);
      const queue = harness.queue('dlq-visibility');
      harness.worker(queue.name, async () => {
        throw new Error('permanent');
      });

      const job = await queue.add('doomed', { key: 'value' }, { attempts: 1, durable: true });
      await waitForState(queue, job.id, 'failed', 15_000);

      expect((await queue.getJobCountsAsync()).failed).toBe(1);
      expect(await queue.getFailedCount()).toBe(1);
      expect((await queue.getJob(job.id))?.id).toBe(job.id);
      expect((await queue.getFailedAsync(0, 10)).map((entry) => entry.id)).toEqual([job.id]);
      expect((await queue.getDlqAsync()).map((entry) => entry.job.id)).toEqual([job.id]);
    }, 30_000);
  });
}

describe('queue guide · querying [durability]', () => {
  test('pause state survives a broker restart', async () => {
    const dataDir = mkdtempSync(join(tmpdir(), 'bunqueue-docs-pause-restart-'));
    const dataPath = join(dataDir, 'queue.db');
    const name = 'paused-queue';
    let manager = new QueueManager({ dataPath });

    try {
      await manager.push(name, { data: { name: 'job' }, durable: true });
      manager.pause(name);
      expect(manager.isPaused(name)).toBe(true);
      await Bun.sleep(100);
      manager.shutdown();

      manager = new QueueManager({ dataPath });
      await waitUntil(() => manager.isPaused(name), 'pause state to be restored');
      expect(manager.isPaused(name)).toBe(true);
    } finally {
      manager.shutdown();
      rmSync(dataDir, { recursive: true, force: true });
    }
  }, 30_000);
});
