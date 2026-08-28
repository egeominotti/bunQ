import { describe, expect, test } from 'bun:test';
import { PostgresEventCatchupCursors } from '../src/infrastructure/persistence/postgres/eventCatchupCursors';
import type { PostgresEventPruneWatermark } from '../src/infrastructure/persistence/postgres/eventPruneWatermarks';

function watermark(
  queue: string,
  commitSeq: number,
  prunedCommitSeq: number,
  selfPrunedCommitSeq = 0,
  prunedThrough = commitSeq
): PostgresEventPruneWatermark {
  return {
    queue,
    sourceEventId: prunedThrough,
    prunedThrough,
    commitSeq,
    prunedCommitSeq,
    selfPrunedCommitSeq,
  };
}

describe('PostgreSQL event catch-up cursors', () => {
  test('refreshes a reader that is behind the pruned frontier', () => {
    const cursors = new PostgresEventCatchupCursors();
    cursors.reset(0);
    cursors.applied('q', 5);
    expect(cursors.requiresRefresh(watermark('q', 9, 7))).toBe(true);
  });

  test('refreshes a reader sitting exactly on the pruned frontier', () => {
    const cursors = new PostgresEventCatchupCursors();
    cursors.reset(0);
    cursors.applied('q', 7);
    // A commit split across drain batches leaves the applied cursor equal to a
    // frontier whose remaining events were pruned before they were read.
    expect(cursors.requiresRefresh(watermark('q', 9, 7))).toBe(true);
  });

  test('does not refresh a reader strictly ahead of the pruned frontier', () => {
    const cursors = new PostgresEventCatchupCursors();
    cursors.reset(0);
    cursors.applied('q', 8);
    expect(cursors.requiresRefresh(watermark('q', 9, 7))).toBe(false);
  });

  test('refreshes an unhandled self-pruning commit even when strictly ahead', () => {
    const cursors = new PostgresEventCatchupCursors();
    cursors.reset(0);
    cursors.applied('q', 12);
    expect(cursors.requiresRefresh(watermark('q', 11, 7, 7))).toBe(true);
  });

  test('refreshes an unchanged frontier only once', () => {
    const cursors = new PostgresEventCatchupCursors();
    cursors.reset(0);
    cursors.applied('q', 5);
    const stable = watermark('q', 9, 7, 7);
    expect(cursors.requiresRefresh(stable)).toBe(true);
    expect(cursors.requiresRefresh(stable)).toBe(false);
    expect(cursors.requiresRefresh(stable)).toBe(false);
  });

  test('refreshes again when the self-prune frontier advances', () => {
    const cursors = new PostgresEventCatchupCursors();
    cursors.reset(0);
    cursors.applied('q', 20);
    expect(cursors.requiresRefresh(watermark('q', 11, 7, 7))).toBe(true);
    expect(cursors.requiresRefresh(watermark('q', 15, 12, 12))).toBe(true);
  });

  test('keeps queues independent and honours the seeded baseline', () => {
    const cursors = new PostgresEventCatchupCursors();
    cursors.reset(30);
    expect(cursors.appliedThrough('unseen')).toBe(30);
    // Seeding at startup marks recorded prunes as covered by the snapshot.
    expect(cursors.remember(watermark('q', 11, 7, 7))).toBe(true);
    expect(cursors.requiresRefresh(watermark('q', 11, 7, 7))).toBe(false);
    expect(cursors.requiresRefresh(watermark('other', 11, 7, 7))).toBe(true);
  });

  test('never moves an applied cursor backwards', () => {
    const cursors = new PostgresEventCatchupCursors();
    cursors.reset(0);
    cursors.applied('q', 40);
    cursors.applied('q', 12);
    expect(cursors.appliedThrough('q')).toBe(40);
  });
});
