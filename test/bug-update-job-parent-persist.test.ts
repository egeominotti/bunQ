/**
 * Bug: updateJobParent mutates childrenIds in memory but never persists to SQLite.
 * After restart, parent-child relationships are lost.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { existsSync, unlinkSync } from 'fs';

const DB_PATH = '/tmp/test-parent-persist.db';

describe('updateJobParent persistence', () => {
  afterEach(() => {
    // Clean up test DB
    for (const ext of ['', '-wal', '-shm']) {
      const path = DB_PATH + ext;
      if (existsSync(path)) unlinkSync(path);
    }
  });

  test('childrenIds should survive server restart', async () => {
    // Phase 1: Create parent and child, set parent relationship
    let qm = new QueueManager({ dataPath: DB_PATH });

    // Use durable: true to bypass write buffer and write to SQLite immediately.
    // This ensures the initial INSERT completes before updateJobParent mutates
    // the in-memory objects, exposing the bug where mutations are not persisted.
    const parent = await qm.push('test-queue', { data: { role: 'parent' }, durable: true });
    const child = await qm.push('test-queue', { data: { role: 'child' }, durable: true });

    await qm.updateJobParent(child.id, parent.id);

    // Verify in-memory state
    const parentJob = await qm.getJob(parent.id);
    expect(parentJob).not.toBeNull();
    expect(parentJob!.childrenIds).toContain(child.id);

    // Verify child has parent ref
    const childJob = await qm.getJob(child.id);
    expect(childJob).not.toBeNull();
    expect((childJob!.data as Record<string, unknown>).__parentId).toBe(parent.id);

    qm.shutdown();

    // Phase 2: Restart and check persistence
    qm = new QueueManager({ dataPath: DB_PATH });

    const parentAfter = await qm.getJob(parent.id);
    expect(parentAfter).not.toBeNull();
    // This is the bug: childrenIds are not persisted
    expect(parentAfter!.childrenIds).toBeDefined();
    expect(parentAfter!.childrenIds!.length).toBeGreaterThan(0);
    expect(parentAfter!.childrenIds).toContain(child.id);

    // Child's parent ref should also survive
    const childAfter = await qm.getJob(child.id);
    expect(childAfter).not.toBeNull();
    expect((childAfter!.data as Record<string, unknown>).__parentId).toBe(parent.id);

    qm.shutdown();
  });
});
