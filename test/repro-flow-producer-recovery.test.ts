import { afterEach, describe, expect, test } from 'bun:test';
import { existsSync, unlinkSync } from 'fs';
import { QueueManager } from '../src/application/queueManager';
import { jobId } from '../src/domain/types/job';

const DB_PATH = '/tmp/bunqueue-repro-flow-recovery.db';

function cleanupDb(): void {
  for (const suffix of ['', '-wal', '-shm']) {
    const path = `${DB_PATH}${suffix}`;
    if (existsSync(path)) unlinkSync(path);
  }
}

afterEach(cleanupDb);

describe('flow persistence and recovery', () => {
  test('parent linkage and failure policies survive restart', async () => {
    cleanupDb();
    let manager = new QueueManager({ dataPath: DB_PATH });
    const parent = await manager.push('flow-recovery-parent', {
      customId: 'flow-recovery-parent-id',
      childrenIds: [jobId('flow-recovery-child-id')],
      data: { name: 'parent' },
      dependsOn: [jobId('flow-recovery-child-id')],
      durable: true,
    });
    const child = await manager.push('flow-recovery-child', {
      customId: 'flow-recovery-child-id',
      data: {
        name: 'child',
        __parentId: String(parent.id),
        __parentQueue: parent.queue,
      },
      durable: true,
      failParentOnFailure: true,
      parentId: parent.id,
    });
    manager.shutdown();

    manager = new QueueManager({ dataPath: DB_PATH });
    const recoveredChild = await manager.getJob(child.id);

    expect(recoveredChild?.parentId).toBe(parent.id);
    expect(recoveredChild?.failParentOnFailure).toBe(true);
    manager.shutdown();
  });

  test.each([
    ['removeDependencyOnFailure', { removeDependencyOnFailure: true }],
    ['ignoreDependencyOnFailure', { ignoreDependencyOnFailure: true }],
    ['continueParentOnFailure', { continueParentOnFailure: true }],
  ] as const)('%s dependency resolution is not resurrected by restart', async (_name, policy) => {
    cleanupDb();
    let manager = new QueueManager({ dataPath: DB_PATH });
    const childId = jobId('flow-recovery-policy-child');
    const parent = await manager.push('flow-recovery-policy-parent', {
      customId: 'flow-recovery-policy-parent',
      data: { name: 'parent' },
      dependsOn: [childId],
      childrenIds: [childId],
      durable: true,
    });
    const child = await manager.push('flow-recovery-policy-child', {
      ...policy,
      customId: String(childId),
      data: {
        name: 'child',
        __parentId: String(parent.id),
        __parentQueue: parent.queue,
      },
      durable: true,
      maxAttempts: 1,
      parentId: parent.id,
    });

    const pulled = await manager.pull(child.queue);
    expect(pulled?.id).toBe(child.id);
    await manager.fail(child.id, 'policy failure', undefined, true);
    expect(await manager.getJobState(parent.id)).not.toBe('waiting-children');
    manager.shutdown();

    manager = new QueueManager({ dataPath: DB_PATH });
    expect(await manager.getJobState(parent.id)).not.toBe('waiting-children');
    const failures =
      'ignoreDependencyOnFailure' in policy
        ? await manager.getIgnoredChildrenFailures(parent.id)
        : 'continueParentOnFailure' in policy
          ? await manager.getFailedChildrenValues(parent.id)
          : {};
    if ('ignoreDependencyOnFailure' in policy || 'continueParentOnFailure' in policy) {
      expect(failures[`${child.queue}:${child.id}`]).toContain('policy failure');
    }
    manager.shutdown();
  });

  test('manual dependency removal remains resolved after restart', async () => {
    cleanupDb();
    let manager = new QueueManager({ dataPath: DB_PATH });
    const childId = jobId('flow-recovery-manual-child');
    const parent = await manager.push('flow-recovery-manual-parent', {
      customId: 'flow-recovery-manual-parent',
      data: { name: 'parent' },
      dependsOn: [childId],
      childrenIds: [childId],
      durable: true,
    });
    const child = await manager.push('flow-recovery-manual-child', {
      customId: String(childId),
      data: {
        name: 'child',
        __parentId: String(parent.id),
        __parentQueue: parent.queue,
      },
      durable: true,
      parentId: parent.id,
    });

    expect(await manager.removeChildDependency(child.id)).toBe(true);
    expect(await manager.getJobState(parent.id)).toBe('waiting');
    expect((await manager.getJob(parent.id))?.childrenIds).toEqual([]);
    expect((await manager.getJob(child.id))?.parentId).toBeNull();
    manager.shutdown();

    manager = new QueueManager({ dataPath: DB_PATH });
    expect(await manager.getJobState(parent.id)).toBe('waiting');
    expect((await manager.getJob(parent.id))?.dependsOn).toEqual([]);
    expect((await manager.getJob(parent.id))?.childrenIds).toEqual([]);
    expect((await manager.getJob(child.id))?.parentId).toBeNull();
    manager.shutdown();
  });

  test('a detached active child can no longer fail its former parent', async () => {
    cleanupDb();
    const manager = new QueueManager({ dataPath: DB_PATH });
    const childId = jobId('flow-recovery-detached-child');
    const parent = await manager.push('flow-recovery-detached-parent', {
      customId: 'flow-recovery-detached-parent',
      data: { name: 'parent' },
      dependsOn: [childId],
      childrenIds: [childId],
      durable: true,
    });
    const child = await manager.push('flow-recovery-detached-child', {
      customId: String(childId),
      data: {
        name: 'child',
        __parentId: String(parent.id),
        __parentQueue: parent.queue,
      },
      durable: true,
      failParentOnFailure: true,
      maxAttempts: 1,
      parentId: parent.id,
    });
    expect((await manager.pull(child.queue))?.id).toBe(child.id);

    expect(await manager.removeChildDependency(child.id)).toBe(true);
    await manager.fail(child.id, 'detached child failure', undefined, true);

    expect(await manager.getJobState(parent.id)).toBe('waiting');
    manager.shutdown();
  });

  test('flow failure metadata is released when the parent terminally fails', async () => {
    cleanupDb();
    const manager = new QueueManager({ dataPath: DB_PATH });
    const childId = jobId('flow-recovery-terminal-parent-child');
    const parent = await manager.push('flow-recovery-terminal-parent', {
      customId: 'flow-recovery-terminal-parent',
      data: { name: 'parent' },
      dependsOn: [childId],
      childrenIds: [childId],
      durable: true,
      maxAttempts: 1,
    });
    const child = await manager.push('flow-recovery-terminal-parent-child', {
      customId: String(childId),
      data: {
        name: 'child',
        __parentId: String(parent.id),
        __parentQueue: parent.queue,
      },
      durable: true,
      continueParentOnFailure: true,
      maxAttempts: 1,
      parentId: parent.id,
    });

    expect((await manager.pull(child.queue))?.id).toBe(child.id);
    await manager.fail(child.id, 'child failure', undefined, true);
    expect(Object.keys(await manager.getFailedChildrenValues(parent.id))).toHaveLength(1);
    expect((await manager.pull(parent.queue))?.id).toBe(parent.id);
    await manager.fail(parent.id, 'parent failure', undefined, true);

    expect(await manager.getFailedChildrenValues(parent.id)).toEqual({});
    manager.shutdown();
  });
});
