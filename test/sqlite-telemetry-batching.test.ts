import { afterEach, describe, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { QueueTelemetryJournal } from '../src/application/queueTelemetryJournal';
import { EventType, type JobEvent } from '../src/domain/types/queue';
import { SqliteStorage } from '../src/infrastructure/persistence/sqlite';

const MINUTE = 60_000;
const directories: string[] = [];

afterEach(() => {
  for (const directory of directories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

function createStorage(): { storage: SqliteStorage; db: Database } {
  const directory = mkdtempSync(join(tmpdir(), 'bunqueue-telemetry-batch-test-'));
  directories.push(directory);
  const storage = new SqliteStorage({ path: join(directory, 'queue.db') });
  const db = (storage as unknown as { db: Database }).db;
  return { storage, db };
}

function event(
  eventType: EventType,
  queue: string,
  jobId: string,
  minute: number,
  options: Pick<JobEvent, 'terminal' | 'data'> = {}
): JobEvent {
  return {
    eventType,
    queue,
    jobId,
    timestamp: minute * MINUTE + (Number(jobId.replace(/\D/g, '') || 0) % 1_000),
    ...options,
  };
}

function snapshot(db: Database): unknown {
  const events = db
    .query<
      {
        queue: string;
        event_type: string;
        job_id: string;
        occurred_at: number;
        payload: Uint8Array;
      },
      []
    >('SELECT queue, event_type, job_id, occurred_at, payload FROM queue_events ORDER BY queue, id')
    .all()
    .map((row) => ({ ...row, payload: Array.from(row.payload) }));
  const buckets = db
    .query<{ queue: string; type: string; minute: number; count: number }, []>(
      'SELECT queue, type, minute, count FROM queue_metric_buckets ORDER BY queue, type, minute'
    )
    .all();
  const meta = db
    .query<
      {
        queue: string;
        type: string;
        total_count: number;
        prev_ts: number;
        prev_count: number;
      },
      []
    >(
      'SELECT queue, type, total_count, prev_ts, prev_count FROM queue_metrics_meta ORDER BY queue, type'
    )
    .all();
  return { events, buckets, meta };
}

describe('SQLite telemetry batching', () => {
  test('matches sequential retention and metrics for mixed, out-of-order events', () => {
    const sequential = createStorage();
    const batched = createStorage();
    const initial = [
      event(EventType.Completed, 'primary', 'initial-1', 10),
      event(EventType.Completed, 'primary', 'initial-2', 15),
      event(EventType.Failed, 'other', 'initial-3', 3),
    ];
    const events = [
      event(EventType.Completed, 'primary', 'batch-1', 9, { data: { order: 1 } }),
      event(EventType.Completed, 'primary', 'batch-2', 15),
      event(EventType.Failed, 'primary', 'batch-3', 20, { terminal: false }),
      event(EventType.Failed, 'other', 'batch-4', 4),
      event(EventType.Completed, 'primary', 'batch-5', 9),
      event(EventType.Completed, 'primary', 'batch-6', 16),
      event(EventType.Completed, 'primary', 'batch-7', 9),
    ];

    try {
      for (const entry of initial) {
        sequential.storage.recordQueueEvent(entry, 3, 2);
        batched.storage.recordQueueEvent(entry, 3, 2);
      }
      for (const entry of events) sequential.storage.recordQueueEvent(entry, 3, 2);

      const recordBatch = (
        batched.storage as SqliteStorage & {
          recordQueueEventsBatch?: (
            entries: readonly JobEvent[],
            maxEvents: number,
            maxMetricDataPoints: number
          ) => void;
        }
      ).recordQueueEventsBatch;
      expect(recordBatch).toBeFunction();
      recordBatch!.call(batched.storage, events, 3, 2);

      expect(snapshot(batched.db)).toEqual(snapshot(sequential.db));
    } finally {
      sequential.storage.close();
      batched.storage.close();
    }
  });

  test('falls back per event when one row rejects the batch transaction', () => {
    const { storage, db } = createStorage();
    const journal = new QueueTelemetryJournal(storage, 10, 10);
    db.run(`CREATE TRIGGER reject_bad_telemetry
      BEFORE INSERT ON queue_events
      WHEN NEW.job_id = 'bad'
      BEGIN
        SELECT RAISE(ABORT, 'rejected telemetry');
      END`);

    try {
      const recordBatch = (
        journal as QueueTelemetryJournal & { recordBatch?: (entries: readonly JobEvent[]) => void }
      ).recordBatch;
      expect(recordBatch).toBeFunction();
      recordBatch!.call(journal, [
        event(EventType.Completed, 'fallback', 'good-1', 1),
        event(EventType.Completed, 'fallback', 'bad', 1),
        event(EventType.Completed, 'fallback', 'good-2', 1),
      ]);

      expect(storage.countQueueEvents('fallback')).toBe(2);
      expect(storage.getQueueMetrics('fallback', 'completed', 10).meta.count).toBe(2);
    } finally {
      storage.close();
    }
  });

  test('matches scalar writes across deterministic mixed-event campaigns', () => {
    let randomState = 0x5eed1234;
    const random = (): number => {
      randomState = (Math.imul(randomState, 1_664_525) + 1_013_904_223) >>> 0;
      return randomState / 0x1_0000_0000;
    };

    for (let campaign = 0; campaign < 24; campaign++) {
      const sequential = createStorage();
      const batched = createStorage();
      const maxEvents = campaign % 8;
      const maxMetricDataPoints = campaign % 6;
      const makeEvents = (count: number, phase: string): JobEvent[] =>
        Array.from({ length: count }, (_, index) => {
          const eventType = [EventType.Pushed, EventType.Completed, EventType.Failed][
            Math.floor(random() * 3)
          ];
          return {
            eventType,
            queue: random() < 0.65 ? 'primary' : 'other',
            jobId: `${phase}-${campaign}-${index}`,
            timestamp: 1_800_000_000_000 + (Math.floor(random() * 13) - 3) * MINUTE + index,
            data: { campaign, index },
            terminal: eventType === EventType.Failed ? random() < 0.5 : undefined,
          };
        });
      const initial = makeEvents(9, 'initial');
      const events = makeEvents(31, 'batch');

      try {
        for (const entry of initial) {
          sequential.storage.recordQueueEvent(entry, maxEvents, maxMetricDataPoints);
          batched.storage.recordQueueEvent(entry, maxEvents, maxMetricDataPoints);
        }
        for (const entry of events) {
          sequential.storage.recordQueueEvent(entry, maxEvents, maxMetricDataPoints);
        }
        batched.storage.recordQueueEventsBatch(events, maxEvents, maxMetricDataPoints);

        expect(snapshot(batched.db)).toEqual(snapshot(sequential.db));
      } finally {
        sequential.storage.close();
        batched.storage.close();
      }
    }
  }, 15_000);

  test('routes public lifecycle batches through one telemetry write per operation', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'bunqueue-telemetry-routing-test-'));
    directories.push(directory);
    const manager = new QueueManager({ dataPath: join(directory, 'queue.db') });
    const storage = (manager as unknown as { storage: SqliteStorage }).storage;
    const candidate = storage as SqliteStorage & {
      recordQueueEventsBatch?: (
        entries: readonly JobEvent[],
        maxEvents: number,
        maxMetricDataPoints: number
      ) => void;
    };
    const originalBatch = candidate.recordQueueEventsBatch?.bind(storage);
    const originalSingle = storage.recordQueueEvent.bind(storage);
    let batchCalls = 0;
    let singleCalls = 0;
    const observed: JobEvent[] = [];
    manager.subscribe((entry) => observed.push(entry));

    try {
      expect(originalBatch).toBeFunction();
      candidate.recordQueueEventsBatch = (entries, maxEvents, maxMetricDataPoints) => {
        batchCalls++;
        originalBatch!(entries, maxEvents, maxMetricDataPoints);
      };
      storage.recordQueueEvent = (entry, maxEvents, maxMetricDataPoints) => {
        singleCalls++;
        originalSingle(entry, maxEvents, maxMetricDataPoints);
      };

      const ids = await manager.pushBatch(
        'routing',
        Array.from({ length: 8 }, (_, index) => ({ data: { index }, durable: true }))
      );
      const pulled = await manager.pullBatch('routing', ids.length);
      const firstCompletion = manager.waitForJobCompletion(pulled[0].id, 1_000);
      await manager.ackBatch(pulled.map((job) => job.id));

      expect(batchCalls).toBe(3);
      expect(singleCalls).toBe(0);
      expect(observed.map((entry) => entry.eventType)).toEqual([
        ...Array(8).fill(EventType.Pushed),
        ...Array(8).fill(EventType.Pulled),
        ...Array(8).fill(EventType.Completed),
      ]);
      expect(manager.getQueueMetrics('routing', 'completed').meta.count).toBe(8);
      expect(await firstCompletion).toBe(true);
    } finally {
      manager.shutdown();
    }
  });
});
