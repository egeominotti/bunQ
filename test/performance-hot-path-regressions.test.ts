import { describe, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { cleanup } from '../src/application/cleanupTasks';
import { DependencyCompletionTracker } from '../src/application/dependencyCompletions';
import { QueueManager } from '../src/application/queueManager';
import type { BackgroundContext } from '../src/application/types';
import { jobId, type JobId } from '../src/domain/types/job';
import { EventType } from '../src/domain/types/queue';
import { SqliteStorage } from '../src/infrastructure/persistence/sqlite';

function backgroundContext(manager: QueueManager): BackgroundContext {
  return (
    manager as unknown as {
      contextFactory: { getBackgroundContext(): BackgroundContext };
    }
  ).contextFactory.getBackgroundContext();
}

describe('hot-path performance regressions', () => {
  test('evicts dependency completions without restarting a Set iterator', () => {
    const evicted: JobId[] = [];
    const tracker = new DependencyCompletionTracker(8, (id) => evicted.push(id));
    const recent = (tracker as unknown as { recent: Set<JobId> }).recent;
    const originalValues = recent.values.bind(recent);
    let iteratorRestarts = 0;
    Object.defineProperty(recent, 'values', {
      configurable: true,
      value: () => {
        iteratorRestarts++;
        return originalValues();
      },
    });

    const ids = Array.from({ length: 64 }, (_, index) => jobId(`completion-${index}`));
    for (const id of ids) tracker.add(id, 'performance-regression');

    expect(iteratorRestarts).toBe(0);
    expect(tracker.size).toBe(8);
    expect(evicted).toEqual(ids.slice(0, 56));
    for (const id of ids.slice(0, 56)) expect(tracker.has(id)).toBe(false);
    for (const id of ids.slice(56)) expect(tracker.has(id)).toBe(true);
  });

  test('keeps FIFO order exact across pin, delete, re-add, hydrate, and clear', () => {
    const evicted: JobId[] = [];
    const tracker = new DependencyCompletionTracker(3, (id) => evicted.push(id));
    const [a, b, c, d, e, f] = ['a', 'b', 'c', 'd', 'e', 'f'].map((id) => jobId(id));

    tracker.add(a, 'first');
    tracker.add(b, 'first');
    tracker.add(c, 'first');
    tracker.pin(a, 'first');
    tracker.add(d, 'second');
    expect(tracker.delete(b)).toBe(true);
    tracker.add(e, 'second');
    tracker.unpin(a, true);
    tracker.add(f, 'third');

    expect(evicted).toEqual([c, d]);
    expect(tracker.has(a)).toBe(true);
    expect(tracker.has(e)).toBe(true);
    expect(tracker.has(f)).toBe(true);
    expect(tracker.has(b)).toBe(false);
    expect(tracker.deleteForQueue('second')).toEqual([e]);

    tracker.hydrate([
      { jobId: a, queue: 'hydrated', pinned: false },
      { jobId: b, queue: 'hydrated', pinned: false },
      { jobId: c, queue: 'hydrated', pinned: false },
      { jobId: d, queue: 'hydrated', pinned: false },
      { jobId: e, queue: 'pinned', pinned: true },
    ]);
    expect(tracker.size).toBe(4);
    expect(tracker.has(a)).toBe(false);
    for (const id of [b, c, d, e]) expect(tracker.has(id)).toBe(true);

    tracker.clear();
    expect(tracker.size).toBe(0);
    tracker.add(a, 'after-clear');
    expect(tracker.has(a)).toBe(true);
  });

  test('empty-queue cleanup releases retained event history but preserves metrics', async () => {
    const manager = new QueueManager({ maxQueueEvents: 100 });
    const queue = 'event-history-cleanup';
    try {
      const pushed = await manager.push(queue, {
        data: { retained: true },
        removeOnComplete: true,
      });
      expect((await manager.pull(queue))?.id).toBe(pushed.id);
      await manager.ack(pushed.id);
      expect(manager.getQueueMetrics(queue, 'completed').meta.count).toBe(1);

      await cleanup(backgroundContext(manager));

      expect(manager.listQueues()).not.toContain(queue);
      expect(manager.trimQueueEvents(queue, 0)).toBe(0);
      expect(manager.getQueueMetrics(queue, 'completed').meta.count).toBe(1);
    } finally {
      manager.shutdown();
    }
  });

  test('prepares SQLite telemetry statements once per storage lifetime', () => {
    const directory = mkdtempSync(join(tmpdir(), 'bunqueue-telemetry-prepare-regression-'));
    const storage = new SqliteStorage({ path: join(directory, 'queue.db') });
    const db = (storage as unknown as { db: Database }).db;
    const originalPrepare = db.prepare.bind(db) as Database['prepare'];
    let prepareCalls = 0;
    db.prepare = ((sql: string) => {
      prepareCalls++;
      return originalPrepare(sql);
    }) as Database['prepare'];

    try {
      storage.recordQueueEvent(
        {
          eventType: EventType.Completed,
          queue: 'cached-statements',
          jobId: 'completion-1',
          timestamp: Date.now(),
        },
        100,
        100
      );
      storage.recordQueueEvent(
        {
          eventType: EventType.Pushed,
          queue: 'cached-statements',
          jobId: 'push-1',
          timestamp: Date.now(),
        },
        100,
        100
      );

      expect(storage.countQueueEvents('cached-statements')).toBe(2);
      expect(storage.getQueueMetrics('cached-statements', 'completed', 100).meta.count).toBe(1);
      expect(prepareCalls).toBe(0);
    } finally {
      storage.close();
      rmSync(directory, { recursive: true, force: true });
    }
  });

  test('keeps cached SQLite event counts exact across trim, clear, and queue deletion', () => {
    const directory = mkdtempSync(join(tmpdir(), 'bunqueue-telemetry-count-regression-'));
    const storage = new SqliteStorage({ path: join(directory, 'queue.db') });
    const queue = 'cached-counts';
    const record = (index: number): void => {
      storage.recordQueueEvent(
        {
          eventType: EventType.Pushed,
          queue,
          jobId: `event-${index}`,
          timestamp: Date.now() + index,
        },
        3,
        10
      );
    };

    try {
      for (let index = 0; index < 5; index++) record(index);
      expect(storage.countQueueEvents(queue)).toBe(3);
      expect(storage.trimQueueEvents(queue, 1)).toBe(2);
      record(5);
      record(6);
      expect(storage.countQueueEvents(queue)).toBe(3);

      storage.clearQueueTelemetry(queue);
      expect(storage.countQueueEvents(queue)).toBe(0);
      record(7);
      expect(storage.countQueueEvents(queue)).toBe(1);

      storage.deleteJobsForQueue(queue, new Set(), 10);
      expect(storage.countQueueEvents(queue)).toBe(0);
      record(8);
      expect(storage.countQueueEvents(queue)).toBe(1);
    } finally {
      storage.close();
      rmSync(directory, { recursive: true, force: true });
    }
  });

  test('restores SQLite event counts before enforcing retention after reopen', () => {
    const directory = mkdtempSync(join(tmpdir(), 'bunqueue-telemetry-reopen-regression-'));
    const path = join(directory, 'queue.db');
    const queue = 'restored-counts';
    const record = (storage: SqliteStorage, index: number): void => {
      storage.recordQueueEvent(
        {
          eventType: EventType.Pushed,
          queue,
          jobId: `reopen-event-${index}`,
          timestamp: Date.now() + index,
        },
        3,
        10
      );
    };

    try {
      const initial = new SqliteStorage({ path });
      try {
        for (let index = 0; index < 3; index++) record(initial, index);
        expect(initial.countQueueEvents(queue)).toBe(3);
      } finally {
        initial.close();
      }

      const reopened = new SqliteStorage({ path });
      try {
        record(reopened, 3);
        expect(reopened.countQueueEvents(queue)).toBe(3);
        expect(reopened.trimQueueEvents(queue, 0)).toBe(3);
      } finally {
        reopened.close();
      }
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });
});
