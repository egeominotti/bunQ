import { afterEach, expect, test } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';

let manager: QueueManager | null = null;

afterEach(() => {
  manager?.shutdown();
  manager = null;
});

async function complete(queue: string, result: unknown): Promise<void> {
  const pulled = await manager!.pull(queue, 'result-pressure-worker');
  if (!pulled) throw new Error(`No job available in ${queue}`);
  await manager!.ack(pulled.id, result, pulled.token);
}

test('dependency results survive LRU pressure until the consumer reads them', async () => {
  manager = new QueueManager({ maxJobResults: 2, cleanupIntervalMs: 60_000 });
  const childQueue = 'flow-result-children';
  const parentQueue = 'flow-result-parent';
  const fillerQueue = 'flow-result-fillers';

  const childA = await manager.push(childQueue, { data: { child: 'a' } });
  const childB = await manager.push(childQueue, { data: { child: 'b' } });
  const parent = await manager.push(parentQueue, {
    data: { parent: true },
    dependsOn: [childA.id, childB.id],
    childrenIds: [childA.id, childB.id],
  });

  await complete(childQueue, { value: 10 });
  await complete(childQueue, { value: 20 });

  for (let i = 0; i < 5; i++) {
    await manager.push(fillerQueue, { data: { filler: i } });
    await complete(fillerQueue, { filler: i });
  }

  expect(await manager.getJobState(parent.id)).toBe('waiting');
  const values = Object.values(await manager.getChildrenValues(parent.id));
  expect(values).toHaveLength(2);
  expect(values).toContainEqual({ value: 10 });
  expect(values).toContainEqual({ value: 20 });
});

test('batch ACK protects removeOnComplete results for a live fan-in', async () => {
  manager = new QueueManager({ maxJobResults: 2, cleanupIntervalMs: 60_000 });
  const childQueue = 'flow-result-batch-children';
  const parentQueue = 'flow-result-batch-parent';
  const children = await Promise.all(
    Array.from({ length: 6 }, (_, index) =>
      manager!.push(childQueue, { data: { index }, removeOnComplete: true })
    )
  );
  const parent = await manager.push(parentQueue, {
    data: { parent: true },
    dependsOn: children.map((child) => child.id),
    childrenIds: children.map((child) => child.id),
  });

  const pulled = await manager.pullBatch(childQueue, children.length);
  expect(pulled).toHaveLength(children.length);
  await manager.ackBatchWithResults(
    pulled.map((job, index) => ({ id: job.id, result: { index } }))
  );

  const values = Object.values(await manager.getChildrenValues(parent.id));
  expect(values).toHaveLength(children.length);
  expect(values).toContainEqual({ index: 0 });
  expect(values).toContainEqual({ index: 5 });
});
