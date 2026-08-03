import { CoreE2eHarness, type CoreE2eMode } from '../support/harness';
import { CoverageTracker, ensure, eventually } from '../support/tracker';

export async function runFlowProducerContract(mode: CoreE2eMode): Promise<CoverageTracker> {
  const harness = await CoreE2eHarness.start(mode, 'flow-producer');
  const tracker = new CoverageTracker(mode, 'flow-producer-contract');

  try {
    const queue = harness.queue<Record<string, unknown>>('flow');
    const producer = harness.flow();
    await tracker.invoke('FlowProducer', 'waitUntilReady', () => producer.waitUntilReady());

    const root = await tracker.invoke('FlowProducer', 'add', () =>
      producer.add({
        name: 'parent',
        queueName: queue.name,
        data: { kind: 'parent' },
        children: [
          { name: 'child-a', queueName: queue.name, data: { kind: 'child', value: 1 } },
          { name: 'child-b', queueName: queue.name, data: { kind: 'child', value: 2 } },
        ],
      })
    );
    ensure(root.children?.length === 2, 'FlowProducer.add did not return both children');

    const tree = await tracker.invoke('FlowProducer', 'getFlow', () =>
      producer.getFlow({ id: root.job.id, queueName: queue.name, depth: 3, maxChildren: 10 })
    );
    ensure(tree?.job.id === root.job.id && tree.children?.length === 2, 'getFlow mismatch');

    const bulk = await tracker.invoke('FlowProducer', 'addBulk', () =>
      producer.addBulk([
        { name: 'bulk-a', queueName: queue.name, data: { value: 'a' } },
        { name: 'bulk-b', queueName: queue.name, data: { value: 'b' } },
      ])
    );
    ensure(bulk.length === 2, 'addBulk did not commit both roots');

    const chain = await tracker.invoke('FlowProducer', 'addChain', () =>
      producer.addChain([
        { name: 'chain-a', queueName: queue.name, data: { order: 1 } },
        { name: 'chain-b', queueName: queue.name, data: { order: 2 } },
        { name: 'chain-c', queueName: queue.name, data: { order: 3 } },
      ])
    );
    ensure(chain.jobIds.length === 3, 'addChain did not create every step');

    const converge = await tracker.invoke('FlowProducer', 'addBulkThen', () =>
      producer.addBulkThen(
        [
          { name: 'parallel-a', queueName: queue.name, data: { value: 1 } },
          { name: 'parallel-b', queueName: queue.name, data: { value: 2 } },
        ],
        { name: 'final', queueName: queue.name, data: { value: 3 } }
      )
    );
    ensure(
      converge.parallelIds.length === 2 && converge.finalId.length > 0,
      'addBulkThen mismatch'
    );

    const legacyTree = await tracker.invoke('FlowProducer', 'addTree', () =>
      producer.addTree({
        name: 'tree-root',
        queueName: queue.name,
        data: { value: 0 },
        children: [{ name: 'tree-child', queueName: queue.name, data: { value: 1 } }],
      })
    );
    ensure(legacyTree.jobIds.length === 2, 'addTree did not create root and child');

    const resultQueue = harness.queue<{ result: unknown }>('results');
    const resultWorker = harness.worker(resultQueue.name, async (job) => job.data.result);
    const resultValues = [42, 0, false, '', null] as const;
    const resultJobs = await Promise.all(
      resultValues.map((result, index) =>
        resultQueue.add(`result-${index}`, { result }, { durable: true })
      )
    );
    for (const resultJob of resultJobs) {
      await eventually(
        () => resultQueue.getJobState(resultJob.id),
        (state) => state === 'completed',
        'result fixture did not complete'
      );
    }
    const resultJob = resultJobs[0];
    const one = await tracker.invoke(
      'FlowProducer',
      'getParentResult',
      async () => await producer.getParentResult<number>(resultJob.id)
    );
    ensure(one === 42, `getParentResult returned ${one}`);
    const many = await tracker.invoke(
      'FlowProducer',
      'getParentResults',
      async () =>
        await producer.getParentResults<unknown>([
          ...resultJobs.map((job) => job.id),
          'missing-result-id',
        ])
    );
    ensure(
      resultJobs.every((job, index) => many.get(job.id) === resultValues[index]) &&
        !many.has('missing-result-id'),
      `getParentResults lost a falsy result, changed order, or retained a missing id: ${JSON.stringify([...many.entries()])}`
    );
    await resultWorker.close();

    const closeProducer = harness.flow();
    await tracker.invoke('FlowProducer', 'close', () => closeProducer.close());
    const disconnectProducer = harness.flow();
    await tracker.invoke('FlowProducer', 'disconnect', () => disconnectProducer.disconnect());
  } finally {
    await harness.close();
  }

  return tracker;
}
