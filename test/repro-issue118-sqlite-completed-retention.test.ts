import { afterEach, describe, expect, setSystemTime, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { cleanup } from '../src/application/cleanupTasks';
import { QueueManager } from '../src/application/queueManager';
import type { BackgroundContext, QueueManagerConfig } from '../src/application/types';
import { normalizeCompletedRetentionMs } from '../src/application/types/config';
import { resolveServerConfig } from '../src/config/resolve';
import { jobId } from '../src/domain/types/job';
import { processingShardIndex, shardIndex } from '../src/shared/hash';
import type { RWLock } from '../src/shared/lock';

interface Harness {
  readonly directory: string;
  readonly path: string;
  readonly manager: QueueManager;
}

const active: Harness[] = [];

function createHarness(config: QueueManagerConfig = {}): Harness {
  const directory = mkdtempSync(join(tmpdir(), 'bunqueue-issue118-'));
  const path = join(directory, 'queue.db');
  const manager = new QueueManager({
    dataPath: path,
    maxCompletedJobs: 3,
    maxJobResults: 2,
    ...config,
  });
  const harness = { directory, path, manager };
  active.push(harness);
  return harness;
}

function backgroundContext(manager: QueueManager): BackgroundContext {
  return (
    manager as unknown as {
      contextFactory: { getBackgroundContext(): BackgroundContext };
    }
  ).contextFactory.getBackgroundContext();
}

function suspendWriteBuffer(manager: QueueManager): { readonly pendingCount: number } {
  const buffer = (
    manager as unknown as {
      storage: {
        writeBuffer: {
          timer: ReturnType<typeof setInterval> | null;
          readonly pendingCount: number;
        };
      };
    }
  ).storage.writeBuffer;
  if (buffer.timer) clearInterval(buffer.timer);
  buffer.timer = null;
  return buffer;
}

async function completeJobs(
  manager: QueueManager,
  queue: string,
  count: number
): Promise<string[]> {
  const ids: string[] = [];
  for (let index = 0; index < count; index++) {
    const id = `issue118-${index.toString().padStart(4, '0')}`;
    const pushed = await manager.push(queue, {
      customId: id,
      data: { index },
      durable: true,
    });
    const pulled = await manager.pull(queue, 0);
    expect(pulled?.id).toBe(pushed.id);
    await manager.ack(pushed.id, { value: index });
    ids.push(String(pushed.id));
  }
  return ids;
}

function persistedCounts(path: string, queue: string): { jobs: number; results: number } {
  const database = new Database(path, { readonly: true });
  try {
    const jobs = database
      .query<{ count: number }, [string]>(
        "SELECT COUNT(*) AS count FROM jobs WHERE queue = ? AND state = 'completed'"
      )
      .get(queue)?.count;
    const results = database
      .query<{ count: number }, []>('SELECT COUNT(*) AS count FROM job_results')
      .get()?.count;
    return { jobs: jobs ?? 0, results: results ?? 0 };
  } finally {
    database.close();
  }
}

async function waitFor(predicate: () => boolean, timeoutMs = 1_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate() && Date.now() < deadline) await Bun.sleep(10);
  expect(predicate()).toBe(true);
}

afterEach(() => {
  for (const harness of active.splice(0)) {
    harness.manager.shutdown();
    rmSync(harness.directory, { recursive: true, force: true });
  }
});

describe('issue #118 SQLite completed retention', () => {
  test('memory and SQLite clean use the same binary ID tie-breaker', async () => {
    const memory = new QueueManager({ maxCompletedJobs: 10 });
    const sqlite = createHarness({ maxCompletedJobs: 10 }).manager;
    const ids = ['tie-a', 'tie-_', 'tie-A', 'tie--'];
    const expected = ['tie--', 'tie-A', 'tie-_', 'tie-a'];
    const now = Date.now();
    setSystemTime(new Date(now));
    try {
      for (const [manager, queue] of [
        [memory, 'issue118-binary-memory'],
        [sqlite, 'issue118-binary-sqlite'],
      ] as const) {
        for (const id of ids) {
          const pushed = await manager.push(queue, { customId: id, data: { id }, durable: true });
          expect((await manager.pull(queue, 0))?.id).toBe(pushed.id);
          await manager.ack(pushed.id);
        }
      }

      expect(memory.clean('issue118-binary-memory', 0, 'completed', 10)).toEqual(expected);
      expect(sqlite.clean('issue118-binary-sqlite', 0, 'completed', 10)).toEqual(expected);
    } finally {
      setSystemTime();
      memory.shutdown();
    }
  });

  test('expired queue policy rows cannot recreate a queue after cleanup and restart', async () => {
    const original = createHarness({ cleanupIntervalMs: 60_000 });
    const queue = 'issue118-expired-policy-restart';
    const realNow = Date.now();
    setSystemTime(new Date(realNow - 1_000));
    try {
      const pushed = await original.manager.push(queue, {
        data: { expires: true },
        durable: true,
        removeOnComplete: true,
      });
      expect((await original.manager.pull(queue, 0))?.id).toBe(pushed.id);
      await original.manager.ack(pushed.id);
      original.manager.setRateLimit(queue, 1, 1_000, 500);

      setSystemTime(new Date(realNow));
      await cleanup(backgroundContext(original.manager));
      expect(original.manager.listQueues()).not.toContain(queue);

      original.manager.shutdown();
      active.splice(active.indexOf(original), 1);
      const restarted = new QueueManager({ dataPath: original.path, cleanupIntervalMs: 60_000 });
      active.push({ directory: original.directory, path: original.path, manager: restarted });

      expect(restarted.listQueues()).not.toContain(queue);
    } finally {
      setSystemTime();
    }
  });

  test('policy-only queues are registered immediately and remain restart-consistent', async () => {
    const original = createHarness({ cleanupIntervalMs: 60_000 });
    const persistedQueues = [
      'issue118-policy-pause',
      'issue118-policy-rate',
      'issue118-policy-concurrency',
      'issue118-policy-stall',
      'issue118-policy-dlq',
      'issue118-policy-group-rate',
      'issue118-policy-group-concurrency',
      'issue118-policy-group-pause',
    ];
    const transientQueue = 'issue118-policy-group-manual-rate';

    original.manager.pause(persistedQueues[0]);
    original.manager.setRateLimit(persistedQueues[1], 2, 1_000);
    original.manager.setConcurrency(persistedQueues[2], 2);
    original.manager.setStallConfig(persistedQueues[3], { maxStalls: 9 });
    original.manager.setDlqConfig(persistedQueues[4], { maxEntries: 9 });
    await original.manager.setGroupRateLimit(persistedQueues[5], 'group', 2, 1_000);
    await original.manager.setGroupConcurrency(persistedQueues[6], 'group', 2);
    await original.manager.pauseGroup(persistedQueues[7], 'group');
    await original.manager.rateLimitGroup(transientQueue, 'group', 1_000);

    expect(original.manager.listQueues()).toEqual(
      expect.arrayContaining([...persistedQueues, transientQueue])
    );

    original.manager.shutdown();
    active.splice(active.indexOf(original), 1);
    const restarted = new QueueManager({ dataPath: original.path, cleanupIntervalMs: 60_000 });
    active.push({ directory: original.directory, path: original.path, manager: restarted });

    expect(restarted.listQueues()).toEqual(expect.arrayContaining(persistedQueues));
    expect(restarted.listQueues()).not.toContain(transientQueue);
  });

  test('live queue policies remain registered when their SQLite write fails', () => {
    const { manager } = createHarness({ cleanupIntervalMs: 60_000 });
    const storage = (
      manager as unknown as {
        storage: {
          saveQueueState(): void;
          saveGroupPaused(): void;
        };
      }
    ).storage;
    storage.saveQueueState = () => {
      throw new Error('forced queue policy write failure');
    };
    storage.saveGroupPaused = () => {
      throw new Error('forced group policy write failure');
    };

    const queuePolicies = [
      ['issue118-policy-failure-pause', () => manager.pause('issue118-policy-failure-pause')],
      [
        'issue118-policy-failure-rate',
        () => manager.setRateLimit('issue118-policy-failure-rate', 2, 1_000),
      ],
      [
        'issue118-policy-failure-concurrency',
        () => manager.setConcurrency('issue118-policy-failure-concurrency', 2),
      ],
      [
        'issue118-policy-failure-stall',
        () => manager.setStallConfig('issue118-policy-failure-stall', { maxStalls: 9 }),
      ],
      [
        'issue118-policy-failure-dlq',
        () => manager.setDlqConfig('issue118-policy-failure-dlq', { maxEntries: 9 }),
      ],
    ] as const;
    for (const [queue, mutate] of queuePolicies) {
      expect(mutate).toThrow('forced queue policy write failure');
      expect(manager.listQueues()).toContain(queue);
    }

    const groupQueue = 'issue118-policy-failure-group-pause';
    expect(() => manager.pauseGroup(groupQueue, 'group')).toThrow(
      'forced group policy write failure'
    );
    expect(manager.listQueues()).toContain(groupQueue);
  });

  test('obliterate removes completed rows beyond the hot cache before restart', async () => {
    const original = createHarness({ maxCompletedJobs: 1 });
    const queue = 'issue118-obliterate-cold-completed';
    const ids = await completeJobs(original.manager, queue, 3);
    expect(original.manager.getMemoryStats().completedJobs).toBe(1);
    expect(original.manager.getQueueJobCounts(queue).completed).toBe(3);

    original.manager.obliterate(queue);

    expect(original.manager.getQueueJobCounts(queue).completed).toBe(0);
    expect(original.manager.getStats().completed).toBe(0);
    expect(persistedCounts(original.path, queue)).toEqual({ jobs: 0, results: 0 });
    for (const id of ids) expect(original.manager.getResult(id)).toBeUndefined();
    original.manager.shutdown();
    active.splice(active.indexOf(original), 1);

    const restarted = new QueueManager({ dataPath: original.path });
    active.push({ directory: original.directory, path: original.path, manager: restarted });
    expect(restarted.listQueues()).not.toContain(queue);
    expect(restarted.getStats().completed).toBe(0);
  });

  test('memory-only obliterate clears results whose completed jobs left the hot cache', async () => {
    const manager = new QueueManager({ maxCompletedJobs: 1, maxJobResults: 10 });
    const queue = 'issue118-memory-obliterate-cold-results';
    try {
      const ids = await completeJobs(manager, queue, 3);

      manager.obliterate(queue);

      for (const id of ids) expect(manager.getResult(id)).toBeUndefined();
    } finally {
      manager.shutdown();
    }
  });

  test('memory-only obliterate clears logs whose completed jobs left the hot cache', async () => {
    const manager = new QueueManager({ maxCompletedJobs: 1, maxJobLogs: 10 });
    const queue = 'issue118-memory-obliterate-cold-logs';
    const ids: string[] = [];
    try {
      for (let index = 0; index < 3; index++) {
        const pushed = await manager.push(queue, { data: { index } });
        expect((await manager.pull(queue, 0))?.id).toBe(pushed.id);
        expect(manager.addLog(pushed.id, `log-${index}`)).toBe(true);
        await manager.ack(pushed.id);
        ids.push(String(pushed.id));
      }

      manager.obliterate(queue);

      for (const id of ids) expect(manager.getLogs(id)).toEqual([]);
    } finally {
      manager.shutdown();
    }
  });

  test('completed custom-ID reuse starts without logs or results from the old generation', async () => {
    const manager = new QueueManager({ maxCompletedJobs: 10, maxJobLogs: 10, maxJobResults: 10 });
    const oldQueue = 'issue118-completed-log-generation-old';
    const newQueue = 'issue118-completed-log-generation-new';
    try {
      const old = await manager.push(oldQueue, {
        customId: 'issue118-completed-log-generation',
        data: { generation: 'old' },
      });
      expect(manager.addLog(old.id, 'old completed log')).toBe(true);
      expect((await manager.pull(oldQueue, 0))?.id).toBe(old.id);
      await manager.ack(old.id, { generation: 'old' });

      const current = await manager.push(newQueue, {
        customId: String(old.id),
        data: { generation: 'new' },
      });

      expect(current.id).toBe(old.id);
      expect(manager.getLogs(current.id)).toEqual([]);
      expect(manager.getResult(current.id)).toBeUndefined();
      expect(manager.getJobIndex().get(current.id)).toMatchObject({
        type: 'queue',
        queueName: newQueue,
      });
    } finally {
      manager.shutdown();
    }
  });

  test('DLQ custom-ID reuse retires every result and log owner from the old generation', async () => {
    const manager = new QueueManager({ maxJobLogs: 10, maxJobResults: 10 });
    const oldQueue = 'issue118-dlq-log-generation-old';
    const newQueue = 'issue118-dlq-log-generation-new';
    try {
      const old = await manager.push(oldQueue, {
        customId: 'issue118-dlq-log-generation',
        data: { generation: 'old' },
      });
      expect(manager.addLog(old.id, 'old DLQ log')).toBe(true);
      expect(await manager.discard(old.id)).toBe(true);
      const auxiliary = manager as unknown as {
        jobResults: { set(id: string, value: unknown): void };
        jobResultQueues: Map<string, string>;
        jobLogQueues: Map<string, string>;
      };
      auxiliary.jobResults.set(old.id, { generation: 'stale' });
      auxiliary.jobResultQueues.set(old.id, oldQueue);

      const current = await manager.push(newQueue, {
        customId: String(old.id),
        data: { generation: 'new' },
      });

      expect(current.id).toBe(old.id);
      expect(manager.getLogs(current.id)).toEqual([]);
      expect(manager.getResult(current.id)).toBeUndefined();
      expect(auxiliary.jobLogQueues.has(current.id)).toBe(false);
      expect(auxiliary.jobResultQueues.has(current.id)).toBe(false);
      expect(manager.getDlq(oldQueue)).toEqual([]);
      expect(manager.getJobIndex().get(current.id)).toMatchObject({
        type: 'queue',
        queueName: newQueue,
      });
    } finally {
      manager.shutdown();
    }
  });

  test('obliterate preserves runtime state when authoritative SQLite deletion fails', async () => {
    const { manager } = createHarness();
    const queue = 'issue118-obliterate-delete-failure';
    const pushed = await manager.push(queue, { data: { survives: true }, durable: true });
    const storage = (
      manager as unknown as {
        storage: { deleteJobsForQueue(queueName: string): unknown };
      }
    ).storage;
    storage.deleteJobsForQueue = () => {
      throw new Error('forced queue delete failure');
    };

    expect(() => manager.obliterate(queue)).toThrow('forced queue delete failure');
    expect(manager.listQueues()).toContain(queue);
    expect(manager.getQueueJobCounts(queue).waiting).toBe(1);
    expect(await manager.getJobState(pushed.id)).toBe('waiting');
  });

  test('obliterate keeps every runtime projection when durable DLQ deletion fails', async () => {
    const { manager, path } = createHarness();
    const queue = 'issue118-obliterate-dlq-failure';
    const failed = await manager.push(queue, {
      data: { terminal: true },
      durable: true,
      maxAttempts: 1,
    });
    expect((await manager.pull(queue, 0))?.id).toBe(failed.id);
    await manager.fail(failed.id, 'terminal failure');
    const waiting = await manager.push(queue, {
      data: { waiting: true },
      durable: true,
    });
    manager.pause(queue);

    const database = new Database(path);
    try {
      database.exec(`
        CREATE TRIGGER issue118_fail_dlq_delete
        BEFORE DELETE ON dlq
        BEGIN
          SELECT RAISE(ABORT, 'forced dlq delete failure');
        END
      `);
    } finally {
      database.close();
    }

    expect(() => manager.obliterate(queue)).toThrow('forced dlq delete failure');
    expect(manager.listQueues()).toContain(queue);
    expect(manager.isPaused(queue)).toBe(true);
    expect(manager.getDlq(queue).map((job) => job.id)).toEqual([failed.id]);
    expect(manager.getQueueJobCounts(queue).waiting).toBe(1);
    expect(await manager.getJobState(waiting.id)).toBe('waiting');

    const retryDatabase = new Database(path);
    try {
      expect(
        retryDatabase
          .query<{ count: number }, [string]>('SELECT COUNT(*) AS count FROM jobs WHERE queue = ?')
          .get(queue)?.count
      ).toBe(1);
      expect(
        retryDatabase
          .query<{ count: number }, [string]>('SELECT COUNT(*) AS count FROM dlq WHERE queue = ?')
          .get(queue)?.count
      ).toBe(1);
      expect(
        retryDatabase
          .query<{ count: number }, [string]>(
            'SELECT COUNT(*) AS count FROM queue_state WHERE name = ?'
          )
          .get(queue)?.count
      ).toBe(1);
      retryDatabase.exec('DROP TRIGGER issue118_fail_dlq_delete');
    } finally {
      retryDatabase.close();
    }

    manager.obliterate(queue);
    expect(manager.listQueues()).not.toContain(queue);
    expect(manager.getDlq(queue)).toEqual([]);
    expect(await manager.getJobState(waiting.id)).toBe('unknown');

    const verification = new Database(path, { readonly: true });
    try {
      for (const table of ['jobs', 'dlq', 'queue_state']) {
        const owner = table === 'queue_state' ? 'name' : 'queue';
        expect(
          verification
            .query<{ count: number }, [string]>(
              `SELECT COUNT(*) AS count FROM ${table} WHERE ${owner} = ?`
            )
            .get(queue)?.count
        ).toBe(0);
      }
    } finally {
      verification.close();
    }
  });

  test('obliterate preserves a newer buffered generation with the same ID in another queue', async () => {
    const { manager } = createHarness({ maxCompletedJobs: 1, maxJobResults: 10 });
    const oldQueue = 'issue118-old-generation-obliterate';
    const newQueue = 'issue118-new-generation-obliterate';
    const sharedId = 'issue118-shared-obliterate-generation';
    const old = await manager.push(oldQueue, {
      customId: sharedId,
      data: { generation: 'old' },
      durable: true,
    });
    expect((await manager.pull(oldQueue, 0))?.id).toBe(old.id);
    await manager.ack(old.id, { generation: 'old' });
    await completeJobs(manager, oldQueue, 1);
    const buffer = suspendWriteBuffer(manager);

    const current = await manager.push(newQueue, {
      customId: sharedId,
      data: { generation: 'new' },
    });
    expect(buffer.pendingCount).toBe(1);

    manager.obliterate(oldQueue);

    expect(manager.getJobIndex().get(current.id)).toMatchObject({
      type: 'queue',
      queueName: newQueue,
    });
    expect(await manager.getJobState(current.id)).toBe('waiting');
    expect(manager.getShards()[shardIndex(newQueue)].getQueue(newQueue).find(current.id)?.id).toBe(
      current.id
    );
    expect(buffer.pendingCount).toBe(1);
    expect(manager.getResult(current.id)).toBeUndefined();
  });

  test('custom-ID admission retires the old completion before clean', async () => {
    const { manager } = createHarness({ maxCompletedJobs: 1, maxJobResults: 10 });
    const oldQueue = 'issue118-old-generation-clean';
    const newQueue = 'issue118-new-generation-clean';
    const sharedId = 'issue118-shared-clean-generation';
    const old = await manager.push(oldQueue, {
      customId: sharedId,
      data: { generation: 'old' },
      durable: true,
    });
    expect((await manager.pull(oldQueue, 0))?.id).toBe(old.id);
    await manager.ack(old.id, { generation: 'old' });
    const [fillerId] = await completeJobs(manager, oldQueue, 1);
    const buffer = suspendWriteBuffer(manager);
    const current = await manager.push(newQueue, {
      customId: sharedId,
      data: { generation: 'new' },
    });

    expect(manager.getResult(current.id)).toBeUndefined();
    expect(manager.clean(oldQueue, 0, 'completed', 100)).toEqual([fillerId]);

    expect(manager.getJobIndex().get(current.id)).toMatchObject({
      type: 'queue',
      queueName: newQueue,
    });
    expect(await manager.getJobState(current.id)).toBe('waiting');
    expect(manager.getShards()[shardIndex(newQueue)].getQueue(newQueue).find(current.id)?.id).toBe(
      current.id
    );
    expect(buffer.pendingCount).toBe(1);
  });

  test('obliterate removes orphan flow-failure rows by child queue ownership', () => {
    const { manager, path } = createHarness();
    const queue = 'issue118-orphan-flow-child';
    const database = new Database(path);
    try {
      database
        .prepare(
          `INSERT INTO flow_failures
           (parent_id, child_id, child_queue, mode, error, created_at)
           VALUES (?, ?, ?, ?, ?, ?)`
        )
        .run('missing-parent', 'missing-child', queue, 'fail', 'orphan', Date.now());
    } finally {
      database.close();
    }

    manager.obliterate(queue);

    const verification = new Database(path, { readonly: true });
    try {
      expect(
        verification
          .query<{ count: number }, [string]>(
            'SELECT COUNT(*) AS count FROM flow_failures WHERE child_queue = ?'
          )
          .get(queue)?.count
      ).toBe(0);
    } finally {
      verification.close();
    }
  });

  test('obliterate removes flow failures owned by a parent that exists only in its DLQ', async () => {
    const { manager, path } = createHarness();
    const queue = 'issue118-dlq-parent-flow-owner';
    const failed = await manager.push(queue, {
      data: { parent: true },
      durable: true,
      maxAttempts: 1,
    });
    expect((await manager.pull(queue, 0))?.id).toBe(failed.id);
    await manager.fail(failed.id, 'terminal parent');

    const database = new Database(path);
    try {
      database
        .prepare(
          `INSERT INTO flow_failures
           (parent_id, child_id, child_queue, mode, error, created_at)
           VALUES (?, ?, ?, ?, ?, ?)`
        )
        .run(failed.id, 'other-child', 'other-queue', 'ignore', 'child failed', Date.now());
    } finally {
      database.close();
    }

    manager.obliterate(queue);

    const verification = new Database(path, { readonly: true });
    try {
      expect(
        verification
          .query<{ count: number }, [string]>(
            'SELECT COUNT(*) AS count FROM flow_failures WHERE parent_id = ?'
          )
          .get(String(failed.id))?.count
      ).toBe(0);
    } finally {
      verification.close();
    }
  });

  test('obliterate preserves auxiliary state for a same-ID DLQ generation in another queue', async () => {
    const harness = createHarness();
    const { manager, path } = harness;
    const targetQueue = 'issue118-stale-dlq-owner';
    const currentQueue = 'issue118-current-dlq-owner';
    const sharedId = 'issue118-shared-dlq-generation';
    const failed = await manager.push(currentQueue, {
      customId: sharedId,
      data: { generation: 'current' },
      durable: true,
      maxAttempts: 1,
    });
    expect((await manager.pull(currentQueue, 0))?.id).toBe(failed.id);
    await manager.fail(failed.id, 'terminal current generation');

    const database = new Database(path);
    try {
      database
        .prepare(
          `INSERT INTO dlq (job_id, queue, entry, entered_at)
           SELECT job_id, ?, entry, entered_at - 1 FROM dlq WHERE queue = ?`
        )
        .run(targetQueue, currentQueue);
      database
        .prepare(
          `INSERT INTO flow_failures
           (parent_id, child_id, child_queue, mode, error, created_at)
           VALUES (?, ?, ?, ?, ?, ?)`
        )
        .run(sharedId, 'other-child', 'other-queue', 'ignore', 'child failed', Date.now());
    } finally {
      database.close();
    }

    manager.shutdown();
    const recovered = new QueueManager({
      dataPath: path,
      maxCompletedJobs: 3,
      maxJobResults: 2,
    });
    (harness as { manager: QueueManager }).manager = recovered;
    expect(recovered.getJobIndex().get(failed.id)).toMatchObject({
      type: 'dlq',
      queueName: currentQueue,
    });
    const recoveredStorage = (
      recovered as unknown as {
        storage: { storeResult(id: string, value: unknown): void };
      }
    ).storage;
    recoveredStorage.storeResult(sharedId, { generation: 'current' });
    const recoveredDatabase = new Database(path);
    try {
      recoveredDatabase
        .prepare(
          `INSERT OR REPLACE INTO flow_failures
           (parent_id, child_id, child_queue, mode, error, created_at)
           VALUES (?, ?, ?, ?, ?, ?)`
        )
        .run(sharedId, 'other-child', 'other-queue', 'ignore', 'child failed', Date.now());
    } finally {
      recoveredDatabase.close();
    }

    recovered.obliterate(targetQueue);

    expect(recovered.getDlq(currentQueue).map((job) => job.id)).toEqual([failed.id]);
    expect(recovered.getJobIndex().get(failed.id)).toMatchObject({
      type: 'dlq',
      queueName: currentQueue,
    });
    expect(await recovered.getJobState(failed.id)).toBe('failed');
    expect(recovered.getResult(failed.id)).toEqual({ generation: 'current' });
    const verification = new Database(path, { readonly: true });
    try {
      expect(
        verification
          .query<{ count: number }, [string]>('SELECT COUNT(*) AS count FROM dlq WHERE queue = ?')
          .get(targetQueue)?.count
      ).toBe(0);
      expect(
        verification
          .query<{ count: number }, [string]>('SELECT COUNT(*) AS count FROM dlq WHERE queue = ?')
          .get(currentQueue)?.count
      ).toBe(1);
      expect(
        verification
          .query<{ count: number }, [string]>(
            'SELECT COUNT(*) AS count FROM flow_failures WHERE parent_id = ?'
          )
          .get(sharedId)?.count
      ).toBe(1);
    } finally {
      verification.close();
    }
  });

  test('a stale same-ID jobs row cannot erase auxiliary state owned by another queue DLQ', async () => {
    const harness = createHarness();
    const { manager, path } = harness;
    const staleQueue = 'issue118-stale-job-owner';
    const currentQueue = 'issue118-current-dlq-over-job';
    const sharedId = 'issue118-shared-job-dlq-generation';
    const failed = await manager.push(currentQueue, {
      customId: sharedId,
      data: { generation: 'current' },
      durable: true,
      maxAttempts: 1,
    });
    expect((await manager.pull(currentQueue, 0))?.id).toBe(failed.id);
    await manager.fail(failed.id, 'terminal current generation');
    const stale = await manager.push(staleQueue, {
      data: { generation: 'stale' },
      durable: true,
    });
    const storage = (
      manager as unknown as {
        storage: { storeResult(id: string, value: unknown): void };
      }
    ).storage;
    storage.storeResult(sharedId, { generation: 'current' });
    manager.shutdown();

    const database = new Database(path);
    try {
      database.prepare('UPDATE jobs SET id = ? WHERE id = ?').run(sharedId, stale.id);
    } finally {
      database.close();
    }

    const recovered = new QueueManager({
      dataPath: path,
      maxCompletedJobs: 3,
      maxJobResults: 2,
    });
    (harness as { manager: QueueManager }).manager = recovered;
    expect(recovered.getJobIndex().get(failed.id)).toMatchObject({
      type: 'dlq',
      queueName: currentQueue,
    });
    const recoveredDatabase = new Database(path);
    try {
      recoveredDatabase
        .prepare(
          `INSERT INTO flow_failures
           (parent_id, child_id, child_queue, mode, error, created_at)
           VALUES (?, ?, ?, ?, ?, ?)`
        )
        .run(sharedId, 'other-child', 'other-queue', 'ignore', 'child failed', Date.now());
    } finally {
      recoveredDatabase.close();
    }

    recovered.obliterate(staleQueue);

    expect(recovered.getDlq(currentQueue).map((job) => job.id)).toEqual([failed.id]);
    expect(recovered.getJobIndex().get(failed.id)).toMatchObject({
      type: 'dlq',
      queueName: currentQueue,
    });
    expect(recovered.getResult(failed.id)).toEqual({ generation: 'current' });
    const verification = new Database(path, { readonly: true });
    try {
      expect(
        verification
          .query<{ count: number }, [string]>(
            'SELECT COUNT(*) AS count FROM job_results WHERE job_id = ?'
          )
          .get(sharedId)?.count
      ).toBe(1);
      expect(
        verification
          .query<{ count: number }, [string]>(
            'SELECT COUNT(*) AS count FROM flow_failures WHERE parent_id = ?'
          )
          .get(sharedId)?.count
      ).toBe(1);
    } finally {
      verification.close();
    }
  });

  test('completed clean preserves auxiliary state owned by a same-ID DLQ generation', async () => {
    const harness = createHarness({ maxCompletedJobs: 1, maxJobResults: 10 });
    const { manager, path } = harness;
    const staleQueue = 'issue118-stale-completed-owner';
    const currentQueue = 'issue118-current-dlq-over-completed';
    const sharedId = 'issue118-shared-completed-dlq-generation';
    const failed = await manager.push(currentQueue, {
      customId: sharedId,
      data: { generation: 'current' },
      durable: true,
      maxAttempts: 1,
    });
    expect((await manager.pull(currentQueue, 0))?.id).toBe(failed.id);
    await manager.fail(failed.id, 'terminal current generation');
    const stale = await manager.push(staleQueue, {
      data: { generation: 'stale' },
      durable: true,
    });
    expect((await manager.pull(staleQueue, 0))?.id).toBe(stale.id);
    await manager.ack(stale.id);
    const filler = await manager.push('issue118-clean-generation-filler', {
      data: { filler: true },
      durable: true,
    });
    expect((await manager.pull('issue118-clean-generation-filler', 0))?.id).toBe(filler.id);
    await manager.ack(filler.id);
    const storage = (
      manager as unknown as {
        storage: { storeResult(id: string, value: unknown): void };
      }
    ).storage;
    storage.storeResult(sharedId, { generation: 'current' });
    manager.shutdown();

    const database = new Database(path);
    try {
      database
        .prepare("UPDATE jobs SET id = ?, completed_at = 0 WHERE id = ? AND state = 'completed'")
        .run(sharedId, stale.id);
    } finally {
      database.close();
    }

    const recovered = new QueueManager({
      dataPath: path,
      maxCompletedJobs: 1,
      maxJobResults: 10,
    });
    (harness as { manager: QueueManager }).manager = recovered;
    expect(recovered.getJobIndex().get(failed.id)).toMatchObject({
      type: 'dlq',
      queueName: currentQueue,
    });
    const recoveredDatabase = new Database(path);
    try {
      recoveredDatabase
        .prepare(
          `INSERT INTO flow_failures
           (parent_id, child_id, child_queue, mode, error, created_at)
           VALUES (?, ?, ?, ?, ?, ?)`
        )
        .run(sharedId, 'other-child', 'other-queue', 'ignore', 'child failed', Date.now());
    } finally {
      recoveredDatabase.close();
    }

    expect(recovered.clean(staleQueue, 0, 'completed', 100)).toEqual([sharedId]);

    expect(recovered.getDlq(currentQueue).map((job) => job.id)).toEqual([failed.id]);
    expect(recovered.getJobIndex().get(failed.id)).toMatchObject({
      type: 'dlq',
      queueName: currentQueue,
    });
    expect(recovered.getResult(failed.id)).toEqual({ generation: 'current' });
    const verification = new Database(path, { readonly: true });
    try {
      expect(
        verification
          .query<{ count: number }, [string]>(
            'SELECT COUNT(*) AS count FROM job_results WHERE job_id = ?'
          )
          .get(sharedId)?.count
      ).toBe(1);
      expect(
        verification
          .query<{ count: number }, [string]>(
            'SELECT COUNT(*) AS count FROM flow_failures WHERE parent_id = ?'
          )
          .get(sharedId)?.count
      ).toBe(1);
    } finally {
      verification.close();
    }
  });

  test('obliterate fences an ACK that already extracted its processing job', async () => {
    const { manager } = createHarness();
    const queue = 'issue118-obliterate-ack-gap';
    const pushed = await manager.push(queue, { data: { late: true }, durable: true });
    expect((await manager.pull(queue, 0))?.id).toBe(pushed.id);
    const internals = manager as unknown as {
      shardLocks: RWLock[];
      processingShards: Map<string, unknown>[];
    };
    const queueLock = internals.shardLocks[shardIndex(queue)];
    const guard = await queueLock.acquireWrite();
    try {
      const pendingAck = manager.ack(pushed.id, { late: true });
      await waitFor(
        () =>
          !internals.processingShards[processingShardIndex(pushed.id)].has(pushed.id) &&
          queueLock.getState().writerWaiting === 1
      );

      manager.obliterate(queue);
      guard.release();

      await expect(pendingAck).rejects.toThrow('not found or not in processing state');
      expect(await manager.getJobState(pushed.id)).toBe('unknown');
      expect(manager.getResult(pushed.id)).toBeUndefined();
      expect(manager.getQueueJobCounts(queue).completed).toBe(0);
      expect(manager.listQueues()).not.toContain(queue);
    } finally {
      guard.release();
    }
  });

  test('obliterate fences a failure that already extracted its processing job', async () => {
    const { manager } = createHarness();
    const queue = 'issue118-obliterate-fail-gap';
    const pushed = await manager.push(queue, {
      data: { late: true },
      durable: true,
      maxAttempts: 1,
    });
    expect((await manager.pull(queue, 0))?.id).toBe(pushed.id);
    const internals = manager as unknown as {
      shardLocks: RWLock[];
      processingShards: Map<string, unknown>[];
    };
    const queueLock = internals.shardLocks[shardIndex(queue)];
    const guard = await queueLock.acquireWrite();
    try {
      const pendingFailure = manager.fail(pushed.id, 'late failure');
      await waitFor(
        () =>
          !internals.processingShards[processingShardIndex(pushed.id)].has(pushed.id) &&
          queueLock.getState().writerWaiting === 1
      );

      manager.obliterate(queue);
      guard.release();

      await expect(pendingFailure).rejects.toThrow('not found or not in processing state');
      expect(await manager.getJobState(pushed.id)).toBe('unknown');
      expect(manager.getDlqCount(queue)).toBe(0);
      expect(manager.getQueueJobCounts(queue).failed).toBe(0);
      expect(manager.listQueues()).not.toContain(queue);
    } finally {
      guard.release();
    }
  });

  test('completed clean removes cold rows and results in deterministic oldest-first pages', async () => {
    const { manager, path } = createHarness();
    const queue = 'issue118-clean';
    const ids = await completeJobs(manager, queue, 12);

    expect(manager.getMemoryStats().completedJobs).toBe(3);
    expect(persistedCounts(path, queue)).toEqual({ jobs: 12, results: 12 });

    const schema = new Database(path, { readonly: true });
    let maintenanceObjects: string[];
    try {
      maintenanceObjects = schema
        .query<{ name: string }, []>(
          "SELECT name FROM sqlite_master WHERE name = 'completed_job_counts' OR name LIKE 'idx_jobs_completed_retention%' OR name LIKE 'trg_jobs_completed_%' ORDER BY name"
        )
        .all()
        .map((row) => row.name);
    } finally {
      schema.close();
    }
    expect(maintenanceObjects).toHaveLength(7);

    expect(manager.clean(queue, 0, 'completed', 5)).toEqual(ids.slice(0, 5));
    expect(persistedCounts(path, queue)).toEqual({ jobs: 7, results: 7 });

    expect(manager.clean(queue, 0, 'completed', 100)).toEqual(ids.slice(5));
    expect(persistedCounts(path, queue)).toEqual({ jobs: 0, results: 0 });
    for (const id of ids) {
      expect(await manager.getJobState(id)).toBe('unknown');
      expect(manager.getResult(id)).toBeUndefined();
    }
  });

  test('stats and queue counts report persisted completed rows beyond the hot-cache cap', async () => {
    const { manager } = createHarness();
    const queue = 'issue118-counts';
    await completeJobs(manager, queue, 12);

    expect(manager.getMemoryStats().completedJobs).toBe(3);
    expect(manager.getStats().completed).toBe(12);
    expect(manager.getQueueJobCounts(queue).completed).toBe(12);
    expect(manager.getQueuesSummary()[0]?.counts.completed).toBe(12);
  });

  test('retryCompleted can requeue one persisted completion outside the hot cache', async () => {
    const { manager } = createHarness({ maxCompletedJobs: 1, maxJobResults: 10 });
    const queue = 'issue118-retry-cold-id';
    const ids = await completeJobs(manager, queue, 3);

    expect(manager.retryCompleted(queue, jobId(ids[0]))).toBe(1);

    expect(await manager.getJobState(ids[0])).toBe('waiting');
    expect(manager.getQueueJobCounts(queue)).toMatchObject({ waiting: 1, completed: 2 });
    expect(manager.getResult(ids[0])).toBeUndefined();
  });

  test('specific retry accepts a cold completed row with a null completion timestamp', async () => {
    const { manager, path } = createHarness({ maxCompletedJobs: 1, maxJobResults: 10 });
    const queue = 'issue118-retry-cold-null-timestamp';
    const ids = await completeJobs(manager, queue, 3);
    const database = new Database(path);
    try {
      database
        .prepare("UPDATE jobs SET completed_at = NULL WHERE id = ? AND state = 'completed'")
        .run(ids[0]);
    } finally {
      database.close();
    }

    expect(manager.retryCompleted(queue, jobId(ids[0]))).toBe(1);
    expect(await manager.getJobState(ids[0])).toBe('waiting');
    expect(manager.getQueueJobCounts(queue)).toMatchObject({ waiting: 1, completed: 2 });
  });

  test('bulk retryCompleted selects persisted cold completions oldest-first up to limit', async () => {
    const { manager } = createHarness({ maxCompletedJobs: 1, maxJobResults: 10 });
    const queue = 'issue118-retry-cold-bulk';
    const ids = await completeJobs(manager, queue, 5);

    expect(manager.retryCompleted(queue, undefined, { limit: 3 })).toBe(3);

    expect(manager.getQueueJobCounts(queue)).toMatchObject({ waiting: 3, completed: 2 });
    for (const id of ids.slice(0, 3)) expect(await manager.getJobState(id)).toBe('waiting');
    for (const id of ids.slice(3)) expect(await manager.getJobState(id)).toBe('completed');
  });

  test('batch queue counts cross the SQLite parameter chunk boundary', async () => {
    const { manager } = createHarness();
    const completedQueue = 'issue118-counts-chunk-target';
    await completeJobs(manager, completedQueue, 1);
    const queueNames = Array.from({ length: 500 }, (_, index) => `issue118-empty-${index}`);
    queueNames.push(completedQueue);

    const counts = manager.getQueueJobCountsBatch(queueNames);

    expect(counts.size).toBe(501);
    expect(counts.get(queueNames[0])?.completed).toBe(0);
    expect(counts.get(completedQueue)?.completed).toBe(1);
  });

  test('optional automatic retention deletes expired completed rows in bounded sweeps', async () => {
    const { manager, path } = createHarness({
      completedRetentionMs: 0,
      cleanupIntervalMs: 10,
    });
    const queue = 'issue118-automatic-retention';
    const ids = await completeJobs(manager, queue, 12);

    for (let attempt = 0; attempt < 100; attempt++) {
      if (persistedCounts(path, queue).jobs === 0) break;
      await Bun.sleep(20);
    }

    expect(persistedCounts(path, queue)).toEqual({ jobs: 0, results: 0 });
    expect(manager.getStats().completed).toBe(0);
    expect(await manager.getJobState(ids[0])).toBe('unknown');
  });

  test('cleanup keeps completed-only queues registered while durable rows remain', async () => {
    const { manager } = createHarness({
      maxCompletedJobs: 1,
      cleanupIntervalMs: 10,
    });
    const queue = 'issue118-completed-only-live';
    await completeJobs(manager, queue, 3);

    await Bun.sleep(100);

    expect(manager.listQueues()).toContain(queue);
    expect(manager.getQueuesSummary().find((entry) => entry.name === queue)?.counts.completed).toBe(
      3
    );
  });

  test('cleanup unregisters a recovered completed-only queue after its last row expires', async () => {
    const original = createHarness({ maxCompletedJobs: 1 });
    const queue = 'issue118-completed-only-restart';
    await completeJobs(original.manager, queue, 1);
    original.manager.shutdown();
    active.splice(active.indexOf(original), 1);

    const restarted = new QueueManager({
      dataPath: original.path,
      maxCompletedJobs: 1,
      completedRetentionMs: 0,
      cleanupIntervalMs: 10,
    });
    active.push({ directory: original.directory, path: original.path, manager: restarted });
    expect(restarted.listQueues()).toContain(queue);

    await waitFor(() => restarted.getStats().completed === 0);
    await waitFor(() => !restarted.listQueues().includes(queue));

    expect(restarted.getQueuesSummary().some((entry) => entry.name === queue)).toBe(false);
  });

  test('server config separates the hot cache cap from optional durable retention', () => {
    const configured = resolveServerConfig({
      storage: { maxCompletedJobs: 321, completedRetentionMs: 86_400_000 },
    });
    expect(configured.maxCompletedJobs).toBe(321);
    expect(configured.completedRetentionMs).toBe(86_400_000);

    const disabled = resolveServerConfig({
      storage: { maxCompletedJobs: 0, completedRetentionMs: -1 },
    });
    expect(disabled.maxCompletedJobs).toBe(50_000);
    expect(disabled.completedRetentionMs).toBeNull();
    expect(normalizeCompletedRetentionMs(12.9)).toBe(12);
    expect(normalizeCompletedRetentionMs(0)).toBe(0);
    expect(normalizeCompletedRetentionMs(Number.MAX_SAFE_INTEGER + 1)).toBeNull();
  });

  test('invalid direct retention values cannot destructively expire completed jobs', async () => {
    for (const [label, completedRetentionMs] of [
      ['negative', -1],
      ['nan', Number.NaN],
      ['infinity', Number.POSITIVE_INFINITY],
    ] as const) {
      const { manager } = createHarness({ completedRetentionMs, cleanupIntervalMs: 10 });
      const queue = `issue118-invalid-retention-${label}`;
      const [id] = await completeJobs(manager, queue, 1);

      await Bun.sleep(100);

      expect(manager.getStats().completed).toBe(1);
      expect(await manager.getJobState(id)).toBe('completed');
    }
  });

  test('clean protects a cold completed dependency until its live consumer is removed', async () => {
    const { manager, path } = createHarness({ dependencyCheckMs: 60_000 });
    const queue = 'issue118-dependency-protection';
    const dependency = await manager.push(queue, {
      customId: 'issue118-protected-dependency',
      data: { dependency: true },
      durable: true,
    });
    const consumer = await manager.push(queue, {
      customId: 'issue118-protected-consumer',
      data: { consumer: true },
      dependsOn: [dependency.id],
      delay: 60_000,
      durable: true,
    });
    expect((await manager.pull(queue, 0))?.id).toBe(dependency.id);
    await manager.ack(dependency.id, { protected: true });

    for (let index = 0; index < 5; index++) {
      const filler = await manager.push(queue, {
        customId: `issue118-protected-filler-${index}`,
        data: { index },
        durable: true,
      });
      expect((await manager.pull(queue, 0))?.id).toBe(filler.id);
      await manager.ack(filler.id, { index });
    }
    expect(manager.getMemoryStats().completedJobs).toBe(3);

    const firstClean = manager.clean(queue, 0, 'completed', 100);
    expect(firstClean).not.toContain(String(dependency.id));
    expect(firstClean).toHaveLength(5);
    expect(persistedCounts(path, queue).jobs).toBe(1);
    expect(await manager.getJobState(dependency.id)).toBe('completed');

    expect(await manager.cancel(consumer.id)).toBe(true);
    expect(manager.clean(queue, 0, 'completed', 100)).toEqual([String(dependency.id)]);
    expect(persistedCounts(path, queue)).toEqual({ jobs: 0, results: 0 });
  });
});
