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

function persistentManager(prefix: string): { manager: QueueManager; dataPath: string } {
  directory = mkdtempSync(join(tmpdir(), prefix));
  const dataPath = join(directory, 'queue.db');
  manager = new QueueManager({ dataPath });
  return { manager, dataPath };
}

test('legacy SDK backpatch preserves a parent that completed before UpdateParent', async () => {
  const setup = persistentManager('bunqueue-sdk-late-parent-link-');
  const dataPath = setup.dataPath;
  const qm = setup.manager;
  const childQueue = 'sdk-late-link-child';
  const parentQueue = 'sdk-late-link-parent';

  const child = await qm.push(childQueue, {
    data: { role: 'child' },
    durable: true,
    parentId: jobId('pending'),
  });
  expect((await qm.pull(childQueue))?.id).toBe(child.id);
  await qm.ack(child.id, { value: 42 });

  const parent = await qm.push(parentQueue, {
    childrenIds: [child.id],
    data: { role: 'parent' },
    dependsOn: [child.id],
    durable: true,
  });
  expect((await qm.pull(parentQueue))?.id).toBe(parent.id);
  await qm.ack(parent.id, { value: 'done' });
  expect(await qm.getJobState(parent.id)).toBe('completed');

  await qm.updateJobParent(child.id, parent.id);

  expect(await qm.getJobState(child.id)).toBe('completed');
  expect(await qm.getJobState(parent.id)).toBe('completed');
  expect(await qm.getChildrenValues(parent.id)).toEqual({
    [`${childQueue}:${String(child.id)}`]: { value: 42 },
  });

  qm.shutdown();
  manager = new QueueManager({ dataPath });

  expect(await manager.getJobState(child.id)).toBe('completed');
  expect(await manager.getJobState(parent.id)).toBe('completed');
  expect((await manager.getJob(child.id))?.parentId).toBe(parent.id);
  expect(await manager.getChildrenValues(parent.id)).toEqual({
    [`${childQueue}:${String(child.id)}`]: { value: 42 },
  });
});

test('legacy SDK backpatch preserves an active predeclared parent', async () => {
  persistentManager('bunqueue-sdk-active-parent-link-');
  const childQueue = 'sdk-active-link-child';
  const parentQueue = 'sdk-active-link-parent';
  const child = await manager!.push(childQueue, {
    data: { role: 'child' },
    durable: true,
    parentId: jobId('pending'),
  });
  expect((await manager!.pull(childQueue))?.id).toBe(child.id);
  await manager!.ack(child.id, 'child-result');

  const parent = await manager!.push(parentQueue, {
    childrenIds: [child.id],
    data: { role: 'parent' },
    dependsOn: [child.id],
    durable: true,
  });
  expect((await manager!.pull(parentQueue))?.id).toBe(parent.id);
  expect(await manager!.getJobState(parent.id)).toBe('active');

  await manager!.updateJobParent(child.id, parent.id);
  await manager!.updateJobParent(child.id, parent.id);

  expect(await manager!.getJobState(parent.id)).toBe('active');
  expect((await manager!.getJob(child.id))?.parentId).toBe(parent.id);
  expect(await manager!.getChildrenValues(parent.id)).toEqual({
    [`${childQueue}:${String(child.id)}`]: 'child-result',
  });

  await manager!.ack(parent.id, 'parent-result');
  expect(await manager!.getJobState(parent.id)).toBe('completed');
});

test('late backpatch cannot add new topology to a completed parent', async () => {
  persistentManager('bunqueue-sdk-illegal-late-link-');
  const queue = 'sdk-illegal-late-link';
  const child = await manager!.push(queue, {
    data: { role: 'child' },
    durable: true,
    parentId: jobId('pending'),
  });
  expect((await manager!.pull(queue))?.id).toBe(child.id);
  await manager!.ack(child.id, 'child-result');
  const parent = await manager!.push(queue, { data: { role: 'parent' }, durable: true });
  expect((await manager!.pull(queue))?.id).toBe(parent.id);
  await manager!.ack(parent.id, 'parent-result');

  await expect(manager!.updateJobParent(child.id, parent.id)).rejects.toThrow('not linkable');

  expect(await manager!.getJobState(parent.id)).toBe('completed');
  expect((await manager!.getJob(parent.id))?.childrenIds).toEqual([]);
  expect((await manager!.getJob(child.id))?.parentId).toBe(jobId('pending'));
});

test('legacy backpatch persists a failed child and its failure outbox under the real parent', async () => {
  const setup = persistentManager('bunqueue-sdk-failed-child-link-');
  const childQueue = 'sdk-failed-link-child';
  const parentQueue = 'sdk-failed-link-parent';
  const child = await setup.manager.push(childQueue, {
    data: { role: 'child' },
    durable: true,
    failParentOnFailure: true,
    maxAttempts: 1,
    parentId: jobId('pending'),
  });
  expect((await setup.manager.pull(childQueue))?.id).toBe(child.id);
  await setup.manager.fail(child.id, 'child failed before linkage', undefined, true);

  const parent = await setup.manager.push(parentQueue, {
    childrenIds: [child.id],
    data: { role: 'parent' },
    dependsOn: [child.id],
    durable: true,
  });
  await setup.manager.updateJobParent(child.id, parent.id);

  expect(await setup.manager.getJobState(child.id)).toBe('failed');
  expect(await setup.manager.getJobState(parent.id)).toBe('failed');
  expect((await setup.manager.getJob(child.id))?.parentId).toBe(parent.id);

  setup.manager.shutdown();
  manager = new QueueManager({ dataPath: setup.dataPath });

  expect(await manager.getJobState(child.id)).toBe('failed');
  expect(await manager.getJobState(parent.id)).toBe('failed');
  expect((await manager.getJob(child.id))?.parentId).toBe(parent.id);
});

test('legacy backpatch accepts a predeclared child already removed on completion', async () => {
  persistentManager('bunqueue-sdk-removed-child-link-');
  const childQueue = 'sdk-removed-link-child';
  const parentQueue = 'sdk-removed-link-parent';
  const child = await manager!.push(childQueue, {
    data: { role: 'child' },
    durable: true,
    parentId: jobId('pending'),
    removeOnComplete: true,
  });
  expect((await manager!.pull(childQueue))?.id).toBe(child.id);
  await manager!.ack(child.id, 'discarded-result');
  expect(await manager!.getJob(child.id)).toBeNull();

  const parent = await manager!.push(parentQueue, {
    childrenIds: [child.id],
    data: { role: 'parent' },
    dependsOn: [child.id],
    durable: true,
  });
  expect((await manager!.pull(parentQueue))?.id).toBe(parent.id);

  await manager!.updateJobParent(child.id, parent.id);

  expect(await manager!.getJobState(parent.id)).toBe('active');
  await manager!.ack(parent.id, 'parent-result');
  expect(await manager!.getJobState(parent.id)).toBe('completed');
});

test('legacy backpatch tolerates removeOnComplete between lookup and lock acquisition', async () => {
  persistentManager('bunqueue-sdk-racing-removed-child-link-');
  const child = await manager!.push('sdk-racing-removed-child', {
    data: { role: 'child' },
    durable: true,
    parentId: jobId('pending'),
    removeOnComplete: true,
  });
  expect((await manager!.pull(child.queue))?.id).toBe(child.id);
  const parent = await manager!.push('sdk-racing-removed-parent', {
    childrenIds: [child.id],
    data: { role: 'parent' },
    dependsOn: [child.id],
    durable: true,
  });

  const originalGetJob = manager!.getJob.bind(manager);
  let completedDuringLookup = false;
  manager!.getJob = async (id) => {
    if (id === parent.id && !completedDuringLookup) {
      completedDuringLookup = true;
      await manager!.ack(child.id, 'discarded-result');
    }
    return originalGetJob(id);
  };

  await manager!.updateJobParent(child.id, parent.id);

  expect(completedDuringLookup).toBe(true);
  expect(await originalGetJob(child.id)).toBeNull();
  expect(await manager!.getJobState(parent.id)).toBe('waiting');
});

test('recovery replays a reparented failure outbox after a crash between link and propagation', async () => {
  const setup = persistentManager('bunqueue-sdk-reparented-outbox-');
  const child = await setup.manager.push('sdk-outbox-child', {
    data: { role: 'child' },
    durable: true,
    failParentOnFailure: true,
    maxAttempts: 1,
    parentId: jobId('pending'),
  });
  expect((await setup.manager.pull(child.queue))?.id).toBe(child.id);
  await setup.manager.fail(child.id, 'outbox failure', undefined, true);
  const parent = await setup.manager.push('sdk-outbox-parent', {
    childrenIds: [child.id],
    data: { role: 'parent' },
    dependsOn: [child.id],
    durable: true,
  });

  const internals = setup.manager as unknown as {
    moveParentToFailed: () => Promise<void>;
  };
  internals.moveParentToFailed = async () => {
    throw new Error('simulated crash after durable backpatch');
  };
  await expect(setup.manager.updateJobParent(child.id, parent.id)).rejects.toThrow(
    'simulated crash'
  );

  setup.manager.shutdown();
  manager = new QueueManager({ dataPath: setup.dataPath });

  expect(await manager.getJobState(parent.id)).toBe('failed');
  expect((await manager.getJob(child.id))?.parentId).toBe(parent.id);
});
