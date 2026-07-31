import { afterEach, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { jobId } from '../src/domain/types/job';

let manager: QueueManager | null = null;
let directory: string | null = null;

afterEach(() => {
  manager?.shutdown();
  manager = null;
  if (directory) rmSync(directory, { force: true, recursive: true });
  directory = null;
});

const scenarios = [
  {
    name: 'removeDependencyOnFailure',
    options: { removeDependencyOnFailure: true },
    values: async (qm: QueueManager, parentId: string, _childKey: string) => {
      expect(await qm.getFailedChildrenValues(jobId(parentId))).toEqual({});
      expect(await qm.getIgnoredChildrenFailures(jobId(parentId))).toEqual({});
    },
  },
  {
    name: 'ignoreDependencyOnFailure',
    options: { ignoreDependencyOnFailure: true },
    values: async (qm: QueueManager, parentId: string, childKey: string) => {
      expect(await qm.getIgnoredChildrenFailures(jobId(parentId))).toEqual({
        [childKey]: 'failed before linkage',
      });
    },
  },
  {
    name: 'continueParentOnFailure',
    options: { continueParentOnFailure: true },
    values: async (qm: QueueManager, parentId: string, childKey: string) => {
      expect(await qm.getFailedChildrenValues(jobId(parentId))).toEqual({
        [childKey]: 'failed before linkage',
      });
    },
  },
] as const;

for (const scenario of scenarios) {
  test(`legacy late linkage preserves ${scenario.name} across restart`, async () => {
    directory = mkdtempSync(join(tmpdir(), `bunqueue-sdk-${scenario.name}-`));
    const dataPath = join(directory, 'queue.db');
    manager = new QueueManager({ dataPath });
    const child = await manager.push(`sdk-${scenario.name}-child`, {
      data: { role: 'child' },
      durable: true,
      maxAttempts: 1,
      parentId: jobId('pending'),
      ...scenario.options,
    });
    expect((await manager.pull(child.queue))?.id).toBe(child.id);
    await manager.fail(child.id, 'failed before linkage', undefined, true);
    const parent = await manager.push(`sdk-${scenario.name}-parent`, {
      childrenIds: [child.id],
      data: { role: 'parent' },
      dependsOn: [child.id],
      durable: true,
    });

    await manager.updateJobParent(child.id, parent.id);

    const childKey = `${child.queue}:${String(child.id)}`;
    expect((await manager.getJob(child.id))?.parentId).toBe(parent.id);
    expect((await manager.getJob(parent.id))?.dependsOn).toEqual([]);
    expect(await manager.getJobState(parent.id)).toBe('waiting');
    await scenario.values(manager, String(parent.id), childKey);

    manager.shutdown();
    manager = new QueueManager({ dataPath });

    expect((await manager.getJob(child.id))?.parentId).toBe(parent.id);
    expect((await manager.getJob(parent.id))?.dependsOn).toEqual([]);
    expect(await manager.getJobState(parent.id)).toBe('waiting');
    await scenario.values(manager, String(parent.id), childKey);
    expect((await manager.pull(parent.queue))?.id).toBe(parent.id);
  });
}
