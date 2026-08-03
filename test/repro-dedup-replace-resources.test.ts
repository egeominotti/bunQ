import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import type { SqliteStorage } from '../src/infrastructure/persistence/sqlite';
import {
  type CoreE2eHarness,
  MODES,
  closeHarness,
  startHarness,
  waitForState,
  waitUntil,
} from './docs-guide-support';

let harness: CoreE2eHarness | null = null;

afterEach(async () => {
  await closeHarness(harness);
  harness = null;
});

async function createPinnedDedupOwner(activeHarness: CoreE2eHarness, label: string) {
  const queue = activeHarness.queue<{ generation: number }>(`${label}-owners`);
  const completionQueue = activeHarness.queue(`${label}-completed-dependencies`);
  const blockerQueue = activeHarness.queue(`${label}-blocking-dependencies`);
  await queue.pauseAsync();
  const manager = activeHarness.brokerManager();
  const storage = (manager as unknown as { storage: SqliteStorage }).storage;
  const blocker = await blockerQueue.add('blocker', {}, { durable: true });
  activeHarness.worker(completionQueue.name, async () => true);
  const completed = await completionQueue.add(
    'completed',
    {},
    { removeOnComplete: true, durable: true }
  );
  const completionTracker = (manager as unknown as { depCompletions: { has(id: string): boolean } })
    .depCompletions;
  await waitUntil(
    () => completionTracker.has(completed.id),
    'remove-on-complete dependency evidence'
  );
  const key = `${label}-key-${crypto.randomUUID()}`;
  const owner = await queue.add(
    'owner',
    { generation: 0 },
    {
      dependsOn: [completed.id, blocker.id],
      deduplication: { id: key, ttl: 300_000, replace: true },
      durable: true,
    }
  );
  await waitForState(queue, owner.id, 'waiting-children');
  return { queue, manager, storage, key, owner };
}

for (const mode of MODES) {
  describe(`dedup replace resources [${mode}]`, () => {
    test('retires temporal, delayed, and custom-id ownership exactly once', async () => {
      harness = await startHarness('dedup-replace-resources', mode);
      const queue = harness.queue<{ generation: number }>('resources');
      await queue.pauseAsync();
      const manager = harness.brokerManager();
      const key = `resource-${crypto.randomUUID()}`;
      const oldCustomId = `old-${crypto.randomUUID()}`;
      const newCustomId = `new-${crypto.randomUUID()}`;

      const first = await queue.add(
        'task',
        { generation: 1 },
        {
          jobId: oldCustomId,
          delay: 60_000,
          deduplication: { id: key, ttl: 300_000, replace: true },
          durable: true,
        }
      );
      expect(manager.getMemoryStats()).toMatchObject({
        queuedTotal: 1,
        customIdMap: 1,
        temporalIndexTotal: 1,
      });
      expect(manager.getStats().delayed).toBe(1);

      const replacement = await queue.add(
        'task',
        { generation: 2 },
        {
          jobId: newCustomId,
          deduplication: { id: key, ttl: 300_000, replace: true },
          durable: true,
        }
      );

      expect(await queue.getJobState(first.id)).toBe('unknown');
      expect(await queue.getJobState(replacement.id)).toBe('waiting');
      expect(manager.getJobByCustomId(oldCustomId)).toBeNull();
      expect(manager.getJobByCustomId(newCustomId)?.id).toBe(replacement.id);
      expect(manager.getMemoryStats()).toMatchObject({
        jobIndex: 1,
        queuedTotal: 1,
        customIdMap: 1,
        temporalIndexTotal: 1,
        delayedHeapTotal: 0,
      });
      expect(manager.getStats().delayed).toBe(0);
      expect(await queue.getJobCountsAsync()).toMatchObject({ paused: 1, delayed: 0 });
    });

    test('keeps the old owner intact when durable replacement persistence fails', async () => {
      harness = await startHarness('dedup-replace-rollback', mode);
      const queue = harness.queue<{ generation: number }>('rollback');
      await queue.pauseAsync();
      const manager = harness.brokerManager();
      const storage = (manager as unknown as { storage: SqliteStorage }).storage;
      const replaceJob = storage.replaceJob;
      const key = `rollback-${crypto.randomUUID()}`;
      const oldCustomId = `old-${crypto.randomUUID()}`;
      const rejectedCustomId = `rejected-${crypto.randomUUID()}`;
      const first = await queue.add(
        'task',
        { generation: 1 },
        {
          jobId: oldCustomId,
          deduplication: { id: key, ttl: 300_000, replace: true },
          durable: true,
        }
      );

      storage.replaceJob = () => {
        throw new Error('injected dedup replacement failure');
      };
      try {
        await expect(
          queue.add(
            'task',
            { generation: 2 },
            {
              jobId: rejectedCustomId,
              deduplication: { id: key, ttl: 300_000, replace: true },
              durable: true,
            }
          )
        ).rejects.toThrow('injected dedup replacement failure');
      } finally {
        storage.replaceJob = replaceJob;
      }

      expect(await queue.getJobState(first.id)).toBe('waiting');
      expect(await queue.getDeduplicationJobId(key)).toBe(first.id);
      expect(manager.getJobByCustomId(oldCustomId)?.id).toBe(first.id);
      expect(manager.getJobByCustomId(rejectedCustomId)).toBeNull();
      expect(manager.getMemoryStats()).toMatchObject({
        jobIndex: 1,
        queuedTotal: 1,
        customIdMap: 1,
        temporalIndexTotal: 1,
      });
      expect(await queue.getJobCountsAsync()).toMatchObject({ paused: 1 });
    });

    test('finalizes the accepted addBulk prefix when a later replacement fails', async () => {
      harness = await startHarness('dedup-replace-batch-prefix', mode);
      const queue = harness.queue<{ generation: number }>('batch-prefix');
      await queue.pauseAsync();
      const manager = harness.brokerManager();
      const storage = (manager as unknown as { storage: SqliteStorage }).storage;
      const replaceJob = storage.replaceJob;
      const key = `batch-prefix-${crypto.randomUUID()}`;
      const prefixId = `accepted-prefix-${crypto.randomUUID()}`;
      const rejectedId = `rejected-replace-${crypto.randomUUID()}`;
      const pushedBefore = manager.getStats().totalPushed;
      const owner = await queue.add(
        'task',
        { generation: 0 },
        {
          deduplication: { id: key, ttl: 300_000, replace: true },
          durable: true,
        }
      );

      storage.replaceJob = () => {
        throw new Error('injected batch replacement failure');
      };
      try {
        await expect(
          queue.addBulk([
            {
              name: 'task',
              data: { generation: 1 },
              opts: { jobId: prefixId, durable: true },
            },
            {
              name: 'task',
              data: { generation: 2 },
              opts: {
                jobId: rejectedId,
                deduplication: { id: key, ttl: 300_000, replace: true },
                durable: true,
              },
            },
          ])
        ).rejects.toThrow('injected batch replacement failure');
      } finally {
        storage.replaceJob = replaceJob;
      }

      expect(manager.getJobByCustomId(prefixId)?.id).toBe(prefixId);
      expect(manager.getJobByCustomId(rejectedId)).toBeNull();
      expect(storage.getJob(prefixId)?.id).toBe(prefixId);
      expect(await queue.getJobState(owner.id)).toBe('waiting');
      expect(await queue.getDeduplicationJobId(key)).toBe(owner.id);
      expect(await queue.getJobCountsAsync()).toMatchObject({ paused: 2 });
      expect(manager.getStats().totalPushed - pushedBefore).toBe(2n);
    });

    test('keeps an active owner intact when replacement persistence fails', async () => {
      harness = await startHarness('dedup-active-replace-rollback', mode);
      const queue = harness.queue<{ generation: number }>('active-rollback');
      const manager = harness.brokerManager();
      const storage = (manager as unknown as { storage: SqliteStorage }).storage;
      const transferActiveDedupJob = storage.transferActiveDedupJob;
      const key = `active-rollback-${crypto.randomUUID()}`;
      const oldCustomId = `old-active-${crypto.randomUUID()}`;
      const rejectedCustomId = `rejected-active-${crypto.randomUUID()}`;
      let release = (): void => undefined;
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });

      harness.worker(queue.name, async () => {
        await held;
        return true;
      });
      const first = await queue.add(
        'task',
        { generation: 1 },
        {
          jobId: oldCustomId,
          deduplication: { id: key, ttl: 300_000, replace: true },
          durable: true,
        }
      );
      await waitForState(queue, first.id, 'active');

      storage.transferActiveDedupJob = () => {
        throw new Error('injected active replacement failure');
      };
      try {
        await expect(
          queue.add(
            'task',
            { generation: 2 },
            {
              jobId: rejectedCustomId,
              deduplication: { id: key, ttl: 300_000, replace: true },
              durable: true,
            }
          )
        ).rejects.toThrow('injected active replacement failure');
      } finally {
        storage.transferActiveDedupJob = transferActiveDedupJob;
      }

      expect(await queue.getJobState(first.id)).toBe('active');
      expect(await queue.getJobState(rejectedCustomId)).toBe('unknown');
      expect(await queue.getDeduplicationJobId(key)).toBe(first.id);
      expect(manager.getJobByCustomId(oldCustomId)?.id).toBe(first.id);
      expect(manager.getJobByCustomId(rejectedCustomId)).toBeNull();
      expect(await queue.getJobCountsAsync()).toMatchObject({ active: 1, waiting: 0 });
      release();
      await waitForState(queue, first.id, 'completed');
    }, 20_000);

    test('rejects replacement while another live job depends on the owner', async () => {
      harness = await startHarness('dedup-replace-dependent-owner', mode);
      const queue = harness.queue<{ generation: number }>('dependent-owner');
      const consumers = harness.queue('dependent-consumers');
      await queue.pauseAsync();
      const manager = harness.brokerManager();
      const key = `dependent-owner-${crypto.randomUUID()}`;
      const oldCustomId = `old-dependent-${crypto.randomUUID()}`;
      const rejectedCustomId = `rejected-dependent-${crypto.randomUUID()}`;
      const first = await queue.add(
        'task',
        { generation: 1 },
        {
          jobId: oldCustomId,
          deduplication: { id: key, ttl: 300_000, replace: true },
          durable: true,
        }
      );
      const dependent = await consumers.add(
        'consumer',
        {},
        { dependsOn: [first.id], durable: true }
      );
      await waitForState(consumers, dependent.id, 'waiting-children');

      await expect(
        queue.add(
          'task',
          { generation: 2 },
          {
            jobId: rejectedCustomId,
            deduplication: { id: key, ttl: 300_000, replace: true },
            durable: true,
          }
        )
      ).rejects.toThrow(
        `Cannot replace deduplicated job ${first.id} because live jobs depend on it`
      );

      expect(await queue.getJobState(first.id)).toBe('waiting');
      expect(await consumers.getJobState(dependent.id)).toBe('waiting-children');
      expect(await queue.getDeduplicationJobId(key)).toBe(first.id);
      expect(manager.getJobByCustomId(oldCustomId)?.id).toBe(first.id);
      expect(manager.getJobByCustomId(rejectedCustomId)).toBeNull();
      expect((await manager.getJob(dependent.id))?.dependsOn).toEqual([first.id]);
      expect(manager.getMemoryStats()).toMatchObject({
        jobIndex: 2,
        queuedTotal: 1,
        waitingDepsTotal: 1,
        customIdMap: 1,
      });
    });

    test('accounts a committed replacement when completion-pin cleanup fails', async () => {
      harness = await startHarness('dedup-replace-pin-cleanup', mode);
      const { queue, manager, storage, key, owner } = await createPinnedDedupOwner(
        harness,
        'single-pin-cleanup'
      );
      const unpinDependencyCompletions = storage.unpinDependencyCompletions;
      const replacementId = `replacement-pin-cleanup-${crypto.randomUUID()}`;
      const pushedBefore = manager.getStats().totalPushed;

      storage.unpinDependencyCompletions = () => {
        throw new Error('injected completion-pin cleanup failure');
      };
      try {
        await expect(
          queue.add(
            'replacement',
            { generation: 1 },
            {
              jobId: replacementId,
              deduplication: { id: key, ttl: 300_000, replace: true },
              durable: true,
            }
          )
        ).rejects.toThrow('injected completion-pin cleanup failure');
      } finally {
        storage.unpinDependencyCompletions = unpinDependencyCompletions;
      }

      expect(await queue.getJobState(owner.id)).toBe('unknown');
      expect(await queue.getJobState(replacementId)).toBe('waiting');
      expect(storage.getJob(replacementId)?.id).toBe(replacementId);
      expect(await queue.getDeduplicationJobId(key)).toBe(replacementId);
      expect(manager.getStats().totalPushed - pushedBefore).toBe(1n);
    });

    test('finalizes an accepted batch when completion-pin cleanup fails', async () => {
      harness = await startHarness('dedup-batch-pin-cleanup', mode);
      const { queue, manager, storage, key, owner } = await createPinnedDedupOwner(
        harness,
        'batch-pin-cleanup'
      );
      const unpinDependencyCompletions = storage.unpinDependencyCompletions;
      const prefixId = `prefix-pin-cleanup-${crypto.randomUUID()}`;
      const replacementId = `replacement-pin-cleanup-${crypto.randomUUID()}`;
      const pushedBefore = manager.getStats().totalPushed;

      storage.unpinDependencyCompletions = () => {
        throw new Error('injected batch completion-pin cleanup failure');
      };
      try {
        await expect(
          queue.addBulk([
            {
              name: 'prefix',
              data: { generation: 1 },
              opts: { jobId: prefixId, durable: true },
            },
            {
              name: 'replacement',
              data: { generation: 2 },
              opts: {
                jobId: replacementId,
                deduplication: { id: key, ttl: 300_000, replace: true },
                durable: true,
              },
            },
          ])
        ).rejects.toThrow('injected batch completion-pin cleanup failure');
      } finally {
        storage.unpinDependencyCompletions = unpinDependencyCompletions;
      }

      expect(await queue.getJobState(owner.id)).toBe('unknown');
      expect(await queue.getJobState(prefixId)).toBe('waiting');
      expect(await queue.getJobState(replacementId)).toBe('waiting');
      expect(storage.getJob(prefixId)?.id).toBe(prefixId);
      expect(storage.getJob(replacementId)?.id).toBe(replacementId);
      expect(await queue.getDeduplicationJobId(key)).toBe(replacementId);
      expect(manager.getStats().totalPushed - pushedBefore).toBe(2n);
    });

    for (const oldOutcome of ['completed', 'failed'] as const) {
      test(`a stale ${oldOutcome} acknowledgement cannot release the successor key`, async () => {
        harness = await startHarness(`dedup-active-stale-${oldOutcome}`, mode);
        const queue = harness.queue<{ generation: number }>(`active-stale-${oldOutcome}`);
        const key = `active-stale-${oldOutcome}-${crypto.randomUUID()}`;
        let releaseOld = (): void => undefined;
        let releaseSuccessor = (): void => undefined;
        const oldHeld = new Promise<void>((resolve) => {
          releaseOld = resolve;
        });
        const successorHeld = new Promise<void>((resolve) => {
          releaseSuccessor = resolve;
        });
        harness.addCleanup(() => {
          releaseOld();
          releaseSuccessor();
        });

        harness.worker<{ generation: number }, boolean>(queue.name, async (job) => {
          if (job.data.generation === 1) {
            await oldHeld;
            if (oldOutcome === 'failed') throw new Error('expected predecessor failure');
            return true;
          }
          await successorHeld;
          return true;
        });
        const first = await queue.add(
          'task',
          { generation: 1 },
          {
            attempts: 1,
            deduplication: { id: key, ttl: 300_000, replace: true },
            durable: true,
          }
        );
        await waitForState(queue, first.id, 'active');
        const successor = await queue.add(
          'task',
          { generation: 2 },
          {
            deduplication: { id: key, ttl: 300_000, replace: true },
            durable: true,
          }
        );

        releaseOld();
        await waitForState(queue, first.id, oldOutcome);
        await waitForState(queue, successor.id, 'active');
        const duplicate = await queue.add(
          'task',
          { generation: 3 },
          { deduplication: { id: key, ttl: 300_000 }, durable: true }
        );

        expect(duplicate.id).toBe(successor.id);
        expect(await queue.getDeduplicationJobId(key)).toBe(successor.id);
        expect(await queue.getJobCountsAsync()).toMatchObject({ active: 1, waiting: 0 });
        releaseSuccessor();
        await waitForState(queue, successor.id, 'completed');
      }, 20_000);
    }
  });
}

describe('dedup replace active durability', () => {
  test('restart before acknowledgement restores the successor as the only key owner', async () => {
    const dataDir = mkdtempSync(join(tmpdir(), 'bunqueue-dedup-active-restart-'));
    const dataPath = join(dataDir, 'queue.db');
    const queue = `active-restart-${crypto.randomUUID()}`;
    const key = `active-restart-key-${crypto.randomUUID()}`;
    let manager = new QueueManager({ dataPath });

    try {
      const first = await manager.push(queue, {
        data: { generation: 1 },
        uniqueKey: key,
        durable: true,
        dedup: { ttl: 300_000, replace: true },
      });
      expect((await manager.pull(queue, 1_000))?.id).toBe(first.id);
      const successor = await manager.push(queue, {
        data: { generation: 2 },
        uniqueKey: key,
        durable: true,
        dedup: { ttl: 300_000, replace: true },
      });
      const storage = (manager as unknown as { storage: SqliteStorage }).storage;

      expect(storage.getJob(first.id)?.uniqueKey).toBeNull();
      expect(storage.getJob(successor.id)?.uniqueKey).toBe(key);
      manager.shutdown();
      manager = new QueueManager({ dataPath });
      await waitUntil(
        () => manager.getQueueJobCounts(queue).waiting === 2,
        'recovery to restore predecessor and successor'
      );

      expect(manager.getDeduplicationJobId(queue, key)).toBe(successor.id);
      const duplicate = await manager.push(queue, {
        data: { generation: 3 },
        uniqueKey: key,
        durable: true,
        dedup: { ttl: 300_000 },
      });
      expect(duplicate.id).toBe(successor.id);
      expect(manager.getQueueJobCounts(queue).waiting).toBe(2);
    } finally {
      manager.shutdown();
      rmSync(dataDir, { recursive: true, force: true });
    }
  }, 30_000);
});
