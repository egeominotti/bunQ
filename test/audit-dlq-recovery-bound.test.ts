/**
 * Audit / regression test for BUG M5:
 *
 * src/domain/queue/dlqShard.ts
 *   - add()          (~lines 80-88): enforces config.maxEntries with a
 *                    `while (entries.length >= config.maxEntries) entries.shift()`
 *                    eviction loop -> DLQ stays bounded.
 *   - restoreEntry() (~lines 92-99): used during recovery/load on restart
 *                    (called from src/domain/queue/shard.ts:302, which is driven
 *                    by backgroundTasks.ts). It just does `entries.push(entry)`
 *                    WITHOUT any maxEntries enforcement.
 *
 * Consequence: after a restart that restores many persisted DLQ rows, the
 * in-memory DLQ can grow past its configured maxEntries bound (unbounded growth),
 * defeating the memory bound that add() carefully enforces.
 *
 * CORRECT behavior (asserted here): restoreEntry() must respect maxEntries the
 * same way add() does -> size <= maxEntries, oldest entries evicted.
 *
 * With the current buggy restoreEntry(), this test is RED (size grows to 20).
 */

import { describe, test, expect, beforeEach } from 'bun:test';
import { DlqShard, type DlqStatsCallback } from '../src/domain/queue/dlqShard';
import {
  createDlqEntry,
  FailureReason,
  type DlqEntry,
} from '../src/domain/types/dlq';
import { createJob, jobId, type Job } from '../src/domain/types/job';

const QUEUE = 'recovery-queue';
const MAX_ENTRIES = 5;
const LOAD = 20; // restore/add far more than the bound

/** Minimal stats callback that tracks the net live DLQ count. */
function createMockStats(): DlqStatsCallback & { net: number } {
  const mock = {
    net: 0,
    incrementDlq() {
      mock.net++;
    },
    decrementDlq(count = 1) {
      mock.net -= count;
    },
  };
  return mock;
}

/** Build a job tagged with an index so we can verify eviction order. */
function makeJob(n: number): Job {
  return createJob(jobId(`job-${n}`), QUEUE, { data: { n } });
}

/** Build a DlqEntry as if it were loaded from persistence on restart. */
function makeRestoredEntry(shard: DlqShard, n: number): DlqEntry {
  // Mirror how a persisted entry is reconstructed: a full DlqEntry blob.
  return createDlqEntry(makeJob(n), FailureReason.MaxAttemptsExceeded, `err-${n}`, shard.getConfig(QUEUE));
}

describe('BUG M5: DLQ recovery (restoreEntry) must enforce maxEntries bound', () => {
  let shard: DlqShard;
  let stats: ReturnType<typeof createMockStats>;

  beforeEach(() => {
    stats = createMockStats();
    shard = new DlqShard(stats);
    shard.setConfig(QUEUE, { maxEntries: MAX_ENTRIES });
  });

  test('restoreEntry stays bounded by maxEntries after restoring more rows than the bound', () => {
    expect(shard.getConfig(QUEUE).maxEntries).toBe(MAX_ENTRIES);

    // Simulate restart recovery: restore LOAD (20) persisted DLQ rows.
    for (let n = 0; n < LOAD; n++) {
      shard.restoreEntry(QUEUE, makeRestoredEntry(shard, n));
    }

    const size = shard.getCount(QUEUE);

    // CORRECT behavior: recovery must respect the configured memory bound.
    // BUGGY restoreEntry() -> size === 20 (RED).
    expect(size).toBeLessThanOrEqual(MAX_ENTRIES);

    // The bound should hold exactly (oldest evicted, like add()).
    expect(size).toBe(MAX_ENTRIES);

    // Oldest entries (job-0 .. job-14) must have been evicted; only the most
    // recent MAX_ENTRIES survive.
    const ids = shard.getEntries(QUEUE).map((e) => e.job.id);
    expect(ids).toEqual([
      jobId('job-15'),
      jobId('job-16'),
      jobId('job-17'),
      jobId('job-18'),
      jobId('job-19'),
    ]);

    // Stats counter must reflect the live (bounded) count, not the raw 20.
    expect(stats.net).toBe(MAX_ENTRIES);
  });

  test('comparison: add() with the same load DOES stay bounded (control)', () => {
    for (let n = 0; n < LOAD; n++) {
      shard.add(makeJob(n), FailureReason.MaxAttemptsExceeded, `err-${n}`);
    }

    const size = shard.getCount(QUEUE);

    // add() already enforces the bound -> this passes regardless of the bug,
    // proving the discrepancy is specific to restoreEntry().
    expect(size).toBe(MAX_ENTRIES);

    const ids = shard.getEntries(QUEUE).map((e) => e.job.id);
    expect(ids).toEqual([
      jobId('job-15'),
      jobId('job-16'),
      jobId('job-17'),
      jobId('job-18'),
      jobId('job-19'),
    ]);

    expect(stats.net).toBe(MAX_ENTRIES);
  });
});
