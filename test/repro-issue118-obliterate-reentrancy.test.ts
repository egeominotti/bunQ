import { afterEach, describe, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { cleanup } from '../src/application/cleanupTasks';
import { DependencyCompletionTracker } from '../src/application/dependencyCompletions';
import { processPendingDependencies } from '../src/application/dependencyProcessor';
import { QueueManager } from '../src/application/queueManager';
import type { BackgroundContext, QueueManagerConfig } from '../src/application/types';
import { jobId, MAX_TIMELINE_ENTRIES, type Job, type JobId } from '../src/domain/types/job';

interface SqliteHarness {
  readonly directory: string;
  readonly path: string;
  manager: QueueManager;
}

const active: SqliteHarness[] = [];

afterEach(() => {
  for (const harness of active.splice(0)) {
    harness.manager.shutdown();
    rmSync(harness.directory, { force: true, recursive: true });
  }
});

function createHarness(config: QueueManagerConfig = {}): SqliteHarness {
  const directory = mkdtempSync(join(tmpdir(), 'bunqueue-issue118-obliterate-'));
  const path = join(directory, 'queue.db');
  const harness = {
    directory,
    path,
    manager: new QueueManager({ dataPath: path, cleanupIntervalMs: 60_000, ...config }),
  };
  active.push(harness);
  return harness;
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

function injectTransientBufferedWrites(manager: QueueManager): {
  readonly pendingCount: number;
  readonly calls: number;
  countCompletedJobs(): number;
  flushWriteBuffer(): number;
  getJobStateRaw(jobId: JobId): string | null;
  getResult(jobId: JobId): unknown;
  getCriticalLosses(): readonly unknown[];
  restore(): void;
} {
  const storage = (
    manager as unknown as {
      storage: {
        writeBuffer: {
          timer: ReturnType<typeof setInterval> | null;
          readonly pendingCount: number;
        };
        batchManager: {
          insertJobsBatch(jobs: Job[]): { transient: Job[]; conflicts: Job[]; error?: Error };
        };
        countCompletedJobs(): number;
        flushWriteBuffer(): number;
        getJobStateRaw(jobId: JobId): string | null;
        getResult(jobId: JobId): unknown;
        getCriticalLosses(): readonly unknown[];
      };
    }
  ).storage;
  if (storage.writeBuffer.timer) clearInterval(storage.writeBuffer.timer);
  storage.writeBuffer.timer = null;
  const original = storage.batchManager.insertJobsBatch;
  let calls = 0;
  storage.batchManager.insertJobsBatch = (jobs) => {
    calls++;
    return {
      transient: [...jobs],
      conflicts: [],
      error: new Error('injected transient'),
    };
  };
  return {
    get pendingCount() {
      return storage.writeBuffer.pendingCount;
    },
    get calls() {
      return calls;
    },
    countCompletedJobs: () => storage.countCompletedJobs(),
    flushWriteBuffer: () => storage.flushWriteBuffer(),
    getJobStateRaw: (target) => storage.getJobStateRaw(target),
    getResult: (target) => storage.getResult(target),
    getCriticalLosses: () => storage.getCriticalLosses(),
    restore: () => {
      storage.batchManager.insertJobsBatch = original;
    },
  };
}

function restart(harness: SqliteHarness, config: QueueManagerConfig = {}): QueueManager {
  harness.manager.shutdown();
  harness.manager = new QueueManager({
    dataPath: harness.path,
    cleanupIntervalMs: 60_000,
    ...config,
  });
  return harness.manager;
}

function completionTracker(manager: QueueManager): DependencyCompletionTracker {
  return (manager as unknown as { depCompletions: DependencyCompletionTracker }).depCompletions;
}

function backgroundContext(manager: QueueManager): BackgroundContext {
  return (
    manager as unknown as {
      contextFactory: { getBackgroundContext(): BackgroundContext };
    }
  ).contextFactory.getBackgroundContext();
}

async function completeRemoved(
  manager: QueueManager,
  queue: string,
  customId: string
): Promise<JobId> {
  const job = await manager.push(queue, {
    customId,
    data: { queue },
    removeOnComplete: true,
  });
  expect((await manager.pull(queue))?.id).toBe(job.id);
  await manager.ack(job.id);
  return job.id;
}

describe('issue #118 obliterate admission fences', () => {
  test('queries include accepted jobs that are still owned by the SQLite buffer', async () => {
    const harness = createHarness();
    const { manager } = harness;
    const queue = 'issue118-buffered-query';
    const buffer = suspendWriteBuffer(manager);
    const ids = await manager.pushBatch(
      queue,
      Array.from({ length: 3 }, (_, index) => ({ data: { index } }))
    );

    expect(buffer.pendingCount).toBe(3);
    const ascending = manager.getJobs(queue, { start: 0, end: 100, asc: true });
    const descending = manager.getJobs(queue, { start: 0, end: 100, asc: false });
    expect(ascending.map((job) => job.id)).toEqual(ids);
    expect(descending.map((job) => job.id)).toEqual([...ids].reverse());
    expect(buffer.pendingCount).toBe(3);
  });

  test('mixed persisted and buffered pages keep SQLite binary ID ordering after flush', async () => {
    const harness = createHarness();
    const { manager } = harness;
    const queue = 'issue118-buffered-binary-order';
    const timestamp = Date.now();
    const persisted = await manager.push(queue, {
      customId: '\uE000',
      data: { tier: 'persisted' },
      timestamp,
      durable: true,
    });
    const buffer = suspendWriteBuffer(manager);
    const buffered = await manager.push(queue, {
      customId: '\u{10000}',
      data: { tier: 'buffered' },
      timestamp,
    });

    const readPages = (): JobId[] =>
      [
        ...manager.getJobs(queue, { start: 0, end: 1, asc: true }),
        ...manager.getJobs(queue, { start: 1, end: 2, asc: true }),
      ].map((job) => job.id);

    expect(buffer.pendingCount).toBe(1);
    expect(readPages()).toEqual([persisted.id, buffered.id]);
    (
      manager as unknown as {
        storage: { flushWriteBuffer(): number };
      }
    ).storage.flushWriteBuffer();
    expect(buffer.pendingCount).toBe(0);
    expect(readPages()).toEqual([persisted.id, buffered.id]);
  });

  test('SQLite admissions reject custom IDs that cannot round-trip as Unicode text', async () => {
    const harness = createHarness();
    const { manager } = harness;
    const queue = 'issue118-buffered-well-formed-ids';

    for (const customId of ['\uD800', '\uDC00']) {
      await expect(
        manager.push(queue, {
          customId,
          data: { rejected: true },
          durable: true,
        })
      ).rejects.toThrow('well-formed Unicode');
    }

    const replacement = await manager.push(queue, {
      customId: '\uFFFD',
      data: { valid: true },
      durable: true,
    });
    expect(replacement.id).toBe('\uFFFD');
    expect(manager.getJobs(queue, { start: 0, end: 3 }).map(({ id }) => id)).toEqual(['\uFFFD']);
  });

  test('pushBatch preserves an explicitly thrown undefined storage failure', async () => {
    const harness = createHarness();
    const { manager } = harness;
    const storage = (
      manager as unknown as {
        storage: { withDeferredBufferFlush<T>(operation: () => T): T };
      }
    ).storage;
    const original = storage.withDeferredBufferFlush;
    let escaped = false;
    storage.withDeferredBufferFlush = () => {
      throw undefined;
    };

    try {
      try {
        await manager.pushBatch('issue118-undefined-batch-error', [{ data: { value: 1 } }]);
      } catch (error) {
        escaped = true;
        expect(error).toBeUndefined();
      }
      expect(escaped).toBe(true);
    } finally {
      storage.withDeferredBufferFlush = original;
    }
  });

  test('a transient buffered insert follows its current active state in queries', async () => {
    const harness = createHarness();
    const { manager } = harness;
    const queue = 'issue118-buffered-active-state';
    const storage = injectTransientBufferedWrites(manager);
    const job = await manager.push(queue, { data: { transient: true } });

    try {
      expect(() => storage.flushWriteBuffer()).toThrow('remains buffered');
      expect((await manager.pull(queue))?.id).toBe(job.id);
      expect(await manager.getJobState(job.id)).toBe('active');
      expect(manager.getJobs(queue, { state: 'waiting' }).map(({ id }) => id)).not.toContain(
        job.id
      );
      expect(manager.getJobs(queue, { state: 'active' }).map(({ id }) => id)).toContain(job.id);
    } finally {
      storage.restore();
    }
    storage.flushWriteBuffer();
    expect(storage.getJobStateRaw(job.id)).toBe('active');
  });

  test('a transient active buffered insert retries with its current lifecycle state', async () => {
    const harness = createHarness();
    const { manager } = harness;
    const queue = 'issue118-buffered-active-retry';
    const storage = injectTransientBufferedWrites(manager);
    const job = await manager.push(queue, { data: { transient: true } });

    try {
      expect(() => storage.flushWriteBuffer()).toThrow('remains buffered');
      expect((await manager.pull(queue))?.id).toBe(job.id);
      expect(storage.getJobStateRaw(job.id)).toBeNull();
    } finally {
      storage.restore();
    }
    storage.flushWriteBuffer();
    expect(storage.getJobStateRaw(job.id)).toBe('active');
  });

  test('an explicit retry clears transient active state before the buffered insert succeeds', async () => {
    const harness = createHarness();
    const { manager } = harness;
    const queue = 'issue118-buffered-explicit-retry';
    const storage = injectTransientBufferedWrites(manager);
    const job = await manager.push(queue, { data: { transient: true }, backoff: 0 });
    const activeJob = await manager.pull(queue);
    expect(activeJob?.id).toBe(job.id);
    while (activeJob && activeJob.timeline.length < MAX_TIMELINE_ENTRIES) {
      activeJob.timeline.push({ state: 'active', timestamp: Date.now() });
    }

    await manager.fail(job.id, 'retry after transient admission');
    expect(await manager.getJobState(job.id)).toBe('waiting');
    expect(manager.getJobs(queue, { state: 'waiting' }).map(({ id }) => id)).toContain(job.id);
    expect(storage.getJobStateRaw(job.id)).toBeNull();

    storage.restore();
    storage.flushWriteBuffer();
    expect(storage.getJobStateRaw(job.id)).toBe('waiting');
    const recovered = restart(harness);
    expect(await recovered.getJobState(job.id)).toBe('waiting');
  });

  test('a full timeline cannot erase a buffered waiting-children transition', async () => {
    const harness = createHarness();
    const { manager } = harness;
    const queue = 'issue118-buffered-waiting-children';
    const storage = injectTransientBufferedWrites(manager);
    const job = await manager.push(queue, { data: { transient: true } });
    const activeJob = await manager.pull(queue);
    expect(activeJob?.id).toBe(job.id);
    while (activeJob && activeJob.timeline.length < MAX_TIMELINE_ENTRIES) {
      activeJob.timeline.push({ state: 'active', timestamp: Date.now() });
    }

    expect(await manager.moveToWaitingChildren(job.id)).toBe(true);
    expect(await manager.getJobState(job.id)).toBe('waiting-children');
    storage.restore();
    storage.flushWriteBuffer();
    expect(storage.getJobStateRaw(job.id)).toBe('waiting-children');

    const recovered = restart(harness);
    expect(await recovered.getJobState(job.id)).toBe('waiting-children');
    expect(await recovered.pull(queue)).toBeNull();
  });

  test('a full timeline cannot erase buffered dependency promotion', async () => {
    const harness = createHarness();
    const { manager } = harness;
    const queue = 'issue118-buffered-dependency-promotion';
    const dependencyId = jobId('issue118-buffered-completed-dependency');
    const storage = injectTransientBufferedWrites(manager);
    const dependent = await manager.push(queue, {
      data: { transient: true },
      dependsOn: [dependencyId],
    });
    const pending = await manager.getJob(dependent.id);
    while (pending && pending.timeline.length < MAX_TIMELINE_ENTRIES) {
      pending.timeline.push({ state: 'waiting-children', timestamp: Date.now() });
    }

    const context = backgroundContext(manager);
    context.completedJobs.add(dependencyId);
    context.pendingDepChecks.add(dependencyId);
    await processPendingDependencies(context);
    expect(await manager.getJobState(dependent.id)).toBe('waiting');

    storage.restore();
    storage.flushWriteBuffer();
    expect(storage.getJobStateRaw(dependent.id)).toBe('waiting');
    const recovered = restart(harness);
    expect((await recovered.pull(queue))?.id).toBe(dependent.id);
  });

  test('a transient completed insert cannot resurrect after retry and restart', async () => {
    const harness = createHarness();
    const { manager } = harness;
    const queue = 'issue118-buffered-completed-retry';
    const storage = injectTransientBufferedWrites(manager);
    const job = await manager.push(queue, { data: { transient: true } });

    try {
      expect(() => storage.flushWriteBuffer()).toThrow('remains buffered');
      expect((await manager.pull(queue))?.id).toBe(job.id);
      await manager.ack(job.id, { persisted: 'result' });
      expect(storage.getJobStateRaw(job.id)).toBeNull();
    } finally {
      storage.restore();
    }
    storage.flushWriteBuffer();
    expect(storage.getJobStateRaw(job.id)).toBe('completed');
    expect(storage.countCompletedJobs()).toBe(1);
    expect(storage.getResult(job.id)).toEqual({ persisted: 'result' });

    const recovered = restart(harness);
    expect(await recovered.getJobState(job.id)).toBe('completed');
    expect(recovered.getResult(job.id)).toEqual({ persisted: 'result' });
  });

  test('batch lifecycle transitions cannot consume the retry budget during backoff', async () => {
    const harness = createHarness();
    const { manager } = harness;
    const queue = 'issue118-buffered-batch-backoff';
    const storage = injectTransientBufferedWrites(manager);
    const ids = await manager.pushBatch(
      queue,
      Array.from({ length: 10 }, (_, index) => ({ data: { index } }))
    );

    try {
      expect(storage.calls).toBe(0);
      const active = await manager.pullBatch(queue, ids.length, 0);
      expect(active.map(({ id }) => id)).toEqual(ids);
      expect(storage.calls).toBe(1);
      expect(storage.pendingCount).toBe(ids.length);
      expect(storage.getCriticalLosses()).toHaveLength(0);

      await manager.ackBatch(ids);
      expect(storage.calls).toBe(1);
      expect(storage.pendingCount).toBe(ids.length);
      expect(storage.getCriticalLosses()).toHaveLength(0);
    } finally {
      storage.restore();
    }

    storage.flushWriteBuffer();
    for (const id of ids) expect(storage.getJobStateRaw(id)).toBe('completed');
  });

  test('reusing an evicted buffered custom ID retires its completed generation', async () => {
    const harness = createHarness({ maxCompletedJobs: 1, maxJobResults: 10 });
    const { manager } = harness;
    const queue = 'issue118-buffered-custom-reuse';
    const storage = injectTransientBufferedWrites(manager);
    const first = await manager.push(queue, {
      customId: 'issue118-buffered-generation-a',
      data: { generation: 'old-a' },
    });
    expect((await manager.pull(queue))?.id).toBe(first.id);
    await manager.ack(first.id, { generation: 'old-a' });
    const second = await manager.push(queue, {
      customId: 'issue118-buffered-generation-b',
      data: { generation: 'old-b' },
    });
    expect((await manager.pull(queue))?.id).toBe(second.id);
    await manager.ack(second.id, { generation: 'old-b' });

    const current = await manager.push(queue, {
      customId: String(first.id),
      data: { generation: 'new-a' },
    });
    expect(current.id).toBe(first.id);
    expect(storage.pendingCount).toBe(2);
    expect(manager.getResult(first.id)).toBeUndefined();

    storage.restore();
    storage.flushWriteBuffer();
    expect(storage.getJobStateRaw(first.id)).toBe('waiting');
    expect(storage.getResult(first.id)).toBeNull();

    const recovered = restart(harness, { maxCompletedJobs: 1, maxJobResults: 10 });
    expect(await recovered.getJobState(first.id)).toBe('waiting');
    expect(recovered.getResult(first.id)).toBeUndefined();
  });

  test('empty-queue cleanup preserves a current buffered completion during retry backoff', async () => {
    const harness = createHarness();
    const { manager } = harness;
    const queue = 'issue118-buffered-completed-cleanup';
    const storage = injectTransientBufferedWrites(manager);
    const job = await manager.push(queue, { data: { transient: true } });

    try {
      expect(() => storage.flushWriteBuffer()).toThrow('remains buffered');
      expect((await manager.pull(queue))?.id).toBe(job.id);
      await manager.ack(job.id, { persisted: 'result' });
      expect(await manager.getJobState(job.id)).toBe('completed');
      expect(storage.pendingCount).toBe(1);
      expect(storage.countCompletedJobs()).toBe(0);
      expect(manager.getJobs(queue, { state: 'completed' }).map(({ id }) => id)).toContain(job.id);
      expect(manager.getJobs(queue, { state: 'waiting' }).map(({ id }) => id)).not.toContain(
        job.id
      );

      await cleanup(backgroundContext(manager));
      expect(manager.listQueues()).toContain(queue);
    } finally {
      storage.restore();
    }
    storage.flushWriteBuffer();
    expect(storage.getJobStateRaw(job.id)).toBe('completed');
    expect(storage.countCompletedJobs()).toBe(1);
    expect(storage.getResult(job.id)).toEqual({ persisted: 'result' });

    const recovered = restart(harness);
    expect(await recovered.getJobState(job.id)).toBe('completed');
    expect(recovered.getResult(job.id)).toEqual({ persisted: 'result' });
  });

  test('evicted buffered completions remain queryable and keep their queues owned', async () => {
    const harness = createHarness({ maxCompletedJobs: 1 });
    const { manager } = harness;
    const queueA = 'issue118-buffered-evicted-a';
    const queueB = 'issue118-buffered-evicted-b';
    const storage = injectTransientBufferedWrites(manager);
    const first = await manager.push(queueA, { data: { order: 1 } });
    const second = await manager.push(queueB, { data: { order: 2 } });

    try {
      expect(() => storage.flushWriteBuffer()).toThrow('remains buffered');
      expect((await manager.pull(queueA))?.id).toBe(first.id);
      await manager.ack(first.id);
      expect((await manager.pull(queueB))?.id).toBe(second.id);
      await manager.ack(second.id);
      expect(storage.pendingCount).toBe(2);
      expect(storage.countCompletedJobs()).toBe(0);

      expect(manager.getJobs(queueA, { state: 'completed' }).map(({ id }) => id)).toContain(
        first.id
      );
      expect(await manager.getJobState(first.id)).toBe('completed');
      expect((await manager.getJob(first.id))?.id).toBe(first.id);
      expect(manager.getJobs(queueB, { state: 'completed' }).map(({ id }) => id)).toContain(
        second.id
      );
      await cleanup(backgroundContext(manager));
      expect(manager.listQueues()).toEqual(expect.arrayContaining([queueA, queueB]));
    } finally {
      storage.restore();
    }
    storage.flushWriteBuffer();
    expect(storage.countCompletedJobs()).toBe(2);

    const recovered = restart(harness, { maxCompletedJobs: 1 });
    expect(await recovered.getJobState(first.id)).toBe('completed');
    expect(await recovered.getJobState(second.id)).toBe('completed');
  });

  test('a waiting-children callback cannot resurrect a single buffered push', async () => {
    const harness = createHarness();
    const { manager } = harness;
    const queue = 'issue118-reentrant-single';
    const buffer = suspendWriteBuffer(manager);
    let fired = false;
    manager.setDashboardEmit((event) => {
      if (event !== 'job:waiting-children' || fired) return;
      fired = true;
      manager.obliterate(queue);
    });

    const pushed = await manager.push(queue, {
      data: { reentrant: true },
      dependsOn: [jobId('issue118-reentrant-single-missing')],
    });

    expect(fired).toBe(true);
    expect(await manager.getJobState(pushed.id)).toBe('unknown');
    expect(buffer.pendingCount).toBe(0);
    expect(manager.listQueues()).not.toContain(queue);
    const recovered = restart(harness);
    expect(await recovered.getJobState(pushed.id)).toBe('unknown');
  });

  test('a waiting-children callback cannot publish a batch tail after obliterate', async () => {
    const harness = createHarness();
    const { manager } = harness;
    const queue = 'issue118-reentrant-batch';
    const buffer = suspendWriteBuffer(manager);
    let fired = false;
    manager.setDashboardEmit((event) => {
      if (event !== 'job:waiting-children' || fired) return;
      fired = true;
      manager.obliterate(queue);
    });

    const ids = await manager.pushBatch(
      queue,
      Array.from({ length: 3 }, (_, index) => ({
        data: { index },
        dependsOn: [jobId(`issue118-reentrant-batch-missing-${index}`)],
      }))
    );

    expect(fired).toBe(true);
    for (const id of ids) expect(await manager.getJobState(id)).toBe('unknown');
    expect(buffer.pendingCount).toBe(0);
    expect(manager.listQueues()).not.toContain(queue);
    const recovered = restart(harness);
    for (const id of ids) expect(await recovered.getJobState(id)).toBe('unknown');
  });

  test('a waiting-children callback cannot publish a flow tail after obliterate', async () => {
    const harness = createHarness();
    const { manager } = harness;
    const queue = 'issue118-reentrant-flow';
    const parentId = jobId('issue118-reentrant-flow-parent');
    const childId = jobId('issue118-reentrant-flow-child');
    let fired = false;
    manager.setDashboardEmit((event) => {
      if (event !== 'job:waiting-children' || fired) return;
      fired = true;
      manager.obliterate(queue);
    });

    const result = await manager.pushFlow({
      jobs: [
        {
          id: parentId,
          queue,
          input: {
            data: { __childrenIds: [String(childId)] },
            dependsOn: [childId],
            childrenIds: [childId],
          },
        },
        {
          id: childId,
          queue,
          input: {
            data: { __parentId: String(parentId), __parentQueue: queue },
            parentId,
          },
        },
      ],
    });

    expect(fired).toBe(true);
    for (const job of result.jobs) expect(await manager.getJobState(job.id)).toBe('unknown');
    expect(manager.listQueues()).not.toContain(queue);
    const recovered = restart(harness);
    for (const job of result.jobs) expect(await recovered.getJobState(job.id)).toBe('unknown');
  });

  test('completed recovery uses a binary ID tie-break for equal timestamps', async () => {
    const harness = createHarness({ maxCompletedJobs: 1 });
    const queue = 'issue118-completed-recovery-tie';
    const lowerBinaryId = 'issue118-completed-recovery-A';
    const higherBinaryId = 'issue118-completed-recovery-a';
    for (const customId of [lowerBinaryId, higherBinaryId]) {
      const pushed = await harness.manager.push(queue, {
        customId,
        data: { customId },
        durable: true,
      });
      expect((await harness.manager.pull(queue))?.id).toBe(pushed.id);
      await harness.manager.ack(pushed.id);
    }
    harness.manager.shutdown();
    const database = new Database(harness.path);
    try {
      database
        .prepare("UPDATE jobs SET completed_at = 123 WHERE queue = ? AND state = 'completed'")
        .run(queue);
    } finally {
      database.close();
    }

    harness.manager = new QueueManager({
      dataPath: harness.path,
      cleanupIntervalMs: 60_000,
      maxCompletedJobs: 1,
    });
    const completedJobs = (
      harness.manager as unknown as { completedJobs: { has(id: JobId): boolean } }
    ).completedJobs;
    expect(completedJobs.has(jobId(higherBinaryId))).toBe(true);
    expect(completedJobs.has(jobId(lowerBinaryId))).toBe(false);
  });

  test('reusing a cold completed custom ID retires its stale result', async () => {
    const harness = createHarness({ maxCompletedJobs: 1, maxJobResults: 10 });
    const { manager } = harness;
    const queue = 'issue118-cold-custom-reuse';
    const first = await manager.push(queue, {
      customId: 'issue118-cold-custom-id',
      data: { generation: 1 },
      durable: true,
    });
    expect((await manager.pull(queue))?.id).toBe(first.id);
    await manager.ack(first.id, { generation: 1 });
    const filler = await manager.push(queue, { data: { filler: true }, durable: true });
    expect((await manager.pull(queue))?.id).toBe(filler.id);
    await manager.ack(filler.id, { filler: true });
    const database = new Database(harness.path);
    try {
      database
        .prepare(
          'UPDATE jobs SET completed_at = CASE id WHEN ? THEN 1 ELSE 2 END WHERE id IN (?, ?)'
        )
        .run(first.id, first.id, filler.id);
    } finally {
      database.close();
    }
    expect(
      (manager as unknown as { completedJobs: { has(id: JobId): boolean } }).completedJobs.has(
        first.id
      )
    ).toBe(false);
    expect(manager.getResult(first.id)).toEqual({ generation: 1 });
    const coldManager = restart(harness, { maxCompletedJobs: 1, maxJobResults: 10 });
    expect(
      (coldManager as unknown as { completedJobs: { has(id: JobId): boolean } }).completedJobs.has(
        first.id
      )
    ).toBe(false);
    expect(coldManager.getResult(first.id)).toEqual({ generation: 1 });

    const current = await coldManager.push(queue, {
      customId: String(first.id),
      data: { generation: 2 },
      durable: true,
    });

    expect(current.id).toBe(first.id);
    expect(await coldManager.getJobState(current.id)).toBe('waiting');
    expect(coldManager.getResult(current.id)).toBeUndefined();
    const recovered = restart(harness, { maxCompletedJobs: 1, maxJobResults: 10 });
    expect(await recovered.getJobState(current.id)).toBe('waiting');
    expect(recovered.getResult(current.id)).toBeUndefined();
  });
});

describe('issue #118 memory-only completion ownership', () => {
  test('empty-queue cleanup preserves a memory-only completed queue', async () => {
    const manager = new QueueManager({ cleanupIntervalMs: 60_000, maxCompletedJobs: 10 });
    try {
      const completed = await manager.push('issue118-memory-completed', { data: { live: true } });
      expect((await manager.pull('issue118-memory-completed'))?.id).toBe(completed.id);
      await manager.ack(completed.id);

      await cleanup(backgroundContext(manager));

      expect(manager.listQueues()).toContain('issue118-memory-completed');
      expect(await manager.getJobState(completed.id)).toBe('completed');
      expect(manager.getQueueJobCounts('issue118-memory-completed').completed).toBe(1);
    } finally {
      manager.shutdown();
    }
  });

  test('obliterate removes completion evidence only for its source queue', async () => {
    const manager = new QueueManager({ cleanupIntervalMs: 60_000, maxCompletedJobs: 10 });
    try {
      const removedA = await completeRemoved(manager, 'issue118-memory-source-a', 'memory-a');
      const removedB = await completeRemoved(manager, 'issue118-memory-source-b', 'memory-b');

      manager.obliterate('issue118-memory-source-a');

      expect(completionTracker(manager).has(removedA)).toBe(false);
      expect(completionTracker(manager).has(removedB)).toBe(true);
      const blocked = await manager.push('issue118-memory-consumer', {
        data: { source: 'a' },
        dependsOn: [removedA],
        priority: 1,
      });
      const ready = await manager.push('issue118-memory-consumer', {
        data: { source: 'b' },
        dependsOn: [removedB],
        priority: 1,
      });
      expect(await manager.getJobState(blocked.id)).toBe('waiting-children');
      expect(await manager.getJobState(ready.id)).toBe('prioritized');
    } finally {
      manager.shutdown();
    }
  });

  test('same-ID reuse transfers completion ownership to the new queue', async () => {
    const manager = new QueueManager({ cleanupIntervalMs: 60_000, maxCompletedJobs: 10 });
    try {
      const sharedId = await completeRemoved(manager, 'issue118-memory-old-owner', 'shared-id');
      expect(
        await completeRemoved(manager, 'issue118-memory-current-owner', String(sharedId))
      ).toBe(sharedId);

      manager.obliterate('issue118-memory-old-owner');
      expect(completionTracker(manager).has(sharedId)).toBe(true);
      manager.obliterate('issue118-memory-current-owner');
      expect(completionTracker(manager).has(sharedId)).toBe(false);
    } finally {
      manager.shutdown();
    }
  });

  test('eviction clears stale ownership before an ID is reused', () => {
    const tracker = new DependencyCompletionTracker(1);
    const sharedId = jobId('issue118-memory-evicted-shared');
    const otherId = jobId('issue118-memory-evicted-other');
    tracker.add(sharedId, 'issue118-memory-evicted-old');
    tracker.add(otherId, 'issue118-memory-evicted-other');
    tracker.add(sharedId, 'issue118-memory-evicted-current');

    expect(tracker.deleteForQueue('issue118-memory-evicted-old')).toEqual([]);
    expect(tracker.has(sharedId)).toBe(true);
    expect(tracker.deleteForQueue('issue118-memory-evicted-current')).toEqual([sharedId]);
    expect(tracker.has(sharedId)).toBe(false);
  });
});
