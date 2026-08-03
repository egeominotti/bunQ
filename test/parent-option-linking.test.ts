import { afterEach, describe, expect, test } from 'bun:test';
import {
  type CoreE2eHarness,
  MODES,
  closeHarness,
  startHarness,
  waitForState,
} from './docs-guide-support';
import { jobId } from '../src/domain/types/job';
import { TcpClient } from '../src/client/tcp/client';

let harness: CoreE2eHarness | null = null;

afterEach(async () => {
  await closeHarness(harness);
  harness = null;
});

for (const mode of MODES) {
  describe(`parent option linking [${mode}]`, () => {
    test('preserves legacy bare parentId forward references', async () => {
      harness = await startHarness('parent-forward-reference', mode);
      const queue = harness.unique('legacy-flow');
      const childId = `legacy-child-${crypto.randomUUID()}`;
      const parentId = `legacy-parent-${crypto.randomUUID()}`;

      if (mode === 'embedded') {
        const manager = harness.brokerManager();
        const child = await manager.push(queue, {
          customId: childId,
          data: { role: 'child' },
          durable: true,
          name: 'child',
          parentId: jobId(parentId),
        });
        await manager.push(queue, {
          childrenIds: [jobId(childId)],
          customId: parentId,
          data: { role: 'parent' },
          dependsOn: [jobId(childId)],
          durable: true,
          name: 'parent',
        });

        expect(child.id).toBe(jobId(childId));
        expect((await manager.pull(queue))?.id).toBe(jobId(childId));
        return;
      }

      const client = new TcpClient({
        ...harness.connection(),
        autoReconnect: false,
        pingInterval: 0,
      });
      harness.addCleanup(() => client.close());
      await client.connect();
      const child = await client.send({
        cmd: 'PUSH',
        data: { role: 'child' },
        durable: true,
        jobId: childId,
        name: 'child',
        parentId,
        queue,
      });
      expect(child.ok).toBe(true);
      expect(
        (
          await client.send({
            childrenIds: [childId],
            cmd: 'PUSH',
            data: { role: 'parent' },
            dependsOn: [childId],
            durable: true,
            jobId: parentId,
            name: 'parent',
            queue,
          })
        ).ok
      ).toBe(true);
      const pulled = await client.send({ cmd: 'PULL', queue, timeout: 0 });
      expect((pulled.job as { id: string }).id).toBe(childId);
    });

    test('legacy updateJobParent links the canonical child used by detach', async () => {
      harness = await startHarness('parent-update-canonical', mode);
      const queue = harness.queue<{ role: string }>('legacy-update');
      const manager = harness.brokerManager();
      const parent = await manager.push(queue.name, {
        data: { role: 'parent' },
        durable: true,
        name: 'parent',
      });
      const child = await manager.push(queue.name, {
        data: { role: 'child' },
        durable: true,
        name: 'child',
      });

      await manager.updateJobParent(child.id, parent.id);
      const linked = await queue.getJob(String(child.id));
      expect(linked?.parent).toEqual({
        id: String(parent.id),
        queueQualifiedName: queue.name,
      });
      expect((await queue.getJobDependencies(String(parent.id))).unprocessed).toEqual([
        `${queue.name}:${String(child.id)}`,
      ]);
      expect(await linked?.removeChildDependency()).toBe(true);
      expect((await queue.getJob(String(child.id)))?.parent).toBeUndefined();
    });

    test('addBulk links every cross-queue child before either queue can run', async () => {
      harness = await startHarness('parent-bulk-cross-queue', mode);
      const parents = harness.queue<{ role: string }>('parents');
      const children = harness.queue<{ role: string }>('children');
      await parents.pauseAsync();
      await children.pauseAsync();

      const parent = await parents.add('parent', { role: 'parent' }, { durable: true });
      const added = await children.addBulk([
        {
          name: 'child-a',
          data: { role: 'child-a' },
          opts: { parent: { id: parent.id, queue: parents.name }, durable: true },
        },
        {
          name: 'child-b',
          data: { role: 'child-b' },
          opts: { parent: { id: parent.id, queue: parents.name }, durable: true },
        },
      ]);

      expect(await parents.getJobState(parent.id)).toBe('waiting-children');
      expect((await parents.getJobDependencies(parent.id)).unprocessed.sort()).toEqual(
        added.map((job) => `${children.name}:${job.id}`).sort()
      );
      for (const child of added) {
        expect((await children.getJob(child.id))?.parent).toEqual({
          id: parent.id,
          queueQualifiedName: parents.name,
        });
      }
    });

    test('concurrent cross-queue children cannot overwrite a sibling edge', async () => {
      harness = await startHarness('parent-concurrent-cross-queue', mode);
      const parents = harness.queue('parents');
      const left = harness.queue('left');
      const right = harness.queue('right');
      await parents.pauseAsync();
      await left.pauseAsync();
      await right.pauseAsync();

      const parent = await parents.add('parent', {}, { durable: true });
      const [leftChild, rightChild] = await Promise.all([
        left.add('left', {}, { parent: { id: parent.id, queue: parents.name }, durable: true }),
        right.add('right', {}, { parent: { id: parent.id, queue: parents.name }, durable: true }),
      ]);

      expect(await parents.getJobState(parent.id)).toBe('waiting-children');
      expect((await parents.getJobDependencies(parent.id)).unprocessed.sort()).toEqual(
        [`${left.name}:${leftChild.id}`, `${right.name}:${rightChild.id}`].sort()
      );
    });

    test('addBulk rolls back its accepted prefix when a parent does not exist', async () => {
      harness = await startHarness('parent-bulk-missing', mode);
      const queue = harness.queue('children');
      await queue.pauseAsync();
      const ordinaryId = `ordinary-${crypto.randomUUID()}`;
      const orphanId = `orphan-${crypto.randomUUID()}`;

      await expect(
        queue.addBulk([
          { name: 'ordinary', data: {}, opts: { jobId: ordinaryId, durable: true } },
          {
            name: 'orphan',
            data: {},
            opts: {
              jobId: orphanId,
              parent: { id: `missing-${crypto.randomUUID()}`, queue: 'missing' },
              durable: true,
            },
          },
        ])
      ).rejects.toThrow(/Parent job .*not found|not linkable/);

      expect(await queue.getJob(ordinaryId)).toBeNull();
      expect(await queue.getJob(orphanId)).toBeNull();
      expect(await queue.countAsync()).toBe(0);
    });

    test('an active parent rejects the child without leaving an orphan', async () => {
      harness = await startHarness('parent-active-race', mode);
      const parents = harness.queue('parents');
      const children = harness.queue('children');
      let releaseParent!: () => void;
      const parentGate = new Promise<void>((resolve) => {
        releaseParent = resolve;
      });
      harness.worker(parents.name, async () => {
        await parentGate;
        return true;
      });
      const parent = await parents.add('parent', {}, { durable: true });
      await waitForState(parents, parent.id, 'active', 10_000);
      const childId = `active-parent-child-${crypto.randomUUID()}`;

      try {
        await expect(
          children.add(
            'child',
            {},
            {
              jobId: childId,
              parent: { id: parent.id, queue: parents.name },
              durable: true,
            }
          )
        ).rejects.toThrow(/not linkable|changed state/);
        expect(await children.getJob(childId)).toBeNull();
        expect(await children.countAsync()).toBe(0);
      } finally {
        releaseParent();
      }
    }, 20_000);
  });
}
