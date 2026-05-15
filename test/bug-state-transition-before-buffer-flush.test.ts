/**
 * Regression: state-transition writes must coordinate with the WriteBuffer so
 * the row exists on disk before the UPDATE runs.
 *
 * Bug history:
 *   - insertJob (non-durable) routed through WriteBuffer, flushed every 10ms.
 *   - markActive / markCompleted / markFailed ran safeWrite immediately and
 *     UPDATE'd the jobs table. If the corresponding INSERT was still in the
 *     WriteBuffer, the UPDATE matched 0 rows silently, and when the
 *     WriteBuffer eventually flushed it wrote the job with the state baked at
 *     insert-time ('waiting'/'delayed') — clobbering the intended state.
 *
 * Fix: src/infrastructure/persistence/sqlite.ts adds a `flushIfBuffered(id)`
 * helper invoked at the top of each state-mutating method. The helper checks
 * WriteBuffer.hasPending(id) and forces a flush so the row is present when
 * the UPDATE runs.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { Database } from 'bun:sqlite';
import { SqliteStorage } from '../src/infrastructure/persistence/sqlite';
import type { Job, JobId } from '../src/domain/types/job';
import { tmpdir } from 'os';
import { join } from 'path';
import { randomUUID } from 'crypto';
import { unlinkSync } from 'fs';

function newJob(id: number): Job {
  return {
    id: String(id) as JobId,
    queue: 'q',
    data: { v: id },
    priority: 0,
    createdAt: Date.now(),
    runAt: Date.now(),
    attempts: 0,
    maxAttempts: 3,
    backoff: 1000,
    ttl: null,
    timeout: null,
    uniqueKey: null,
    customId: null,
    dependsOn: [],
    parentId: null,
    childrenIds: [],
    tags: [],
    lifo: false,
    groupId: null,
    removeOnComplete: false,
    removeOnFail: false,
    stallTimeout: null,
    startedAt: null,
    completedAt: null,
    failedReason: null,
    progress: null,
    lastHeartbeat: null,
    lockToken: null,
    lockExpiresAt: null,
    timeline: [],
  } as unknown as Job;
}

describe('State-transition writes coordinate with WriteBuffer', () => {
  let dbPath: string;
  let storage: SqliteStorage;

  beforeEach(() => {
    dbPath = join(tmpdir(), `bunq-fix-${randomUUID()}.db`);
    storage = new SqliteStorage({
      path: dbPath,
      // Long flush so the test deterministically exercises the buffered window
      writeBufferFlushMs: 60_000,
      writeBufferSize: 1000,
    });
  });

  afterEach(() => {
    try {
      (storage as unknown as { close?: () => void }).close?.();
    } catch {
      // ignore
    }
    try {
      unlinkSync(dbPath);
    } catch {
      // ignore
    }
  });

  test('markActive on a buffered job persists as active (flushes buffer first)', () => {
    const job = newJob(1);
    storage.insertJob(job);
    storage.markActive(job.id, Date.now());

    const db = new Database(dbPath, { readonly: true });
    const row = db
      .prepare<{ state: string }, [string]>('SELECT state FROM jobs WHERE id = ?')
      .get(String(job.id));
    db.close();

    expect(row).not.toBeNull();
    expect(row!.state).toBe('active');
  });

  test('markCompleted on a buffered job persists as completed', () => {
    const job = newJob(2);
    storage.insertJob(job);
    storage.markCompleted(job.id, Date.now());

    const db = new Database(dbPath, { readonly: true });
    const row = db
      .prepare<{ state: string; completed_at: number | null }, [string]>(
        'SELECT state, completed_at FROM jobs WHERE id = ?'
      )
      .get(String(job.id));
    db.close();

    expect(row).not.toBeNull();
    expect(row!.state).toBe('completed');
    expect(row!.completed_at).not.toBeNull();
  });

  test('flush-on-transition does not regress unrelated non-buffered jobs', () => {
    // Two jobs: one buffered, one durable-inserted. Mutate the buffered one.
    const buffered = newJob(10);
    const durable = newJob(11);
    storage.insertJob(buffered);
    storage.insertJob(durable, true); // durable=true bypasses buffer

    storage.markActive(buffered.id, Date.now());

    const db = new Database(dbPath, { readonly: true });
    const states = db
      .prepare<{ id: string; state: string }, []>('SELECT id, state FROM jobs ORDER BY id')
      .all();
    db.close();

    const byId = new Map(states.map((s) => [s.id, s.state]));
    expect(byId.get(String(buffered.id))).toBe('active');
    // Durable insert keeps its insert-time state (waiting)
    expect(byId.get(String(durable.id))).toBe('waiting');
  });

  test('crash before flush still loses the buffered job (out of scope for this fix)', () => {
    // The fix prevents the silent UPDATE/INSERT race within the running
    // process. Crash-time durability is governed by writeBufferFlushMs and
    // the `durable` flag — this test documents the unchanged behavior to
    // prevent over-promising.
    const job = newJob(3);
    storage.insertJob(job);
    // No state transition this time — pure crash before flush.
    (storage as unknown as { db: Database }).db.close(false);

    const db = new Database(dbPath, { readonly: true });
    const row = db
      .prepare<{ state: string } | null, [string]>('SELECT state FROM jobs WHERE id = ?')
      .get(String(job.id));
    db.close();

    expect(row).toBeNull(); // unchanged — use durable:true for zero-loss writes
  });
});
