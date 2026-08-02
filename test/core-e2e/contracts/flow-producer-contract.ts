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

    const resultQueue = harness.queue<{ value: number }>('results');
    const resultWorker = harness.worker(resultQueue.name, async (job) => job.data.value * 2);
    const resultJob = await resultQueue.add('result', { value: 21 }, { durable: true });
    await eventually(
      () => resultQueue.getJobState(resultJob.id),
      (state) => state === 'completed',
      'result fixture did not complete'
    );
    if (mode === 'embedded') {
      const one = tracker.call('FlowProducer', 'getParentResult', () =>
        producer.getParentResult<number>(resultJob.id)
      );
      ensure(one === 42, `getParentResult returned ${one}`);
      const many = tracker.call('FlowProducer', 'getParentResults', () =>
        producer.getParentResults<number>([resultJob.id])
      );
      ensure(many.get(resultJob.id) === 42, 'getParentResults missed result');
    } else {
      await tracker.rejects(
        'FlowProducer',
        'getParentResult',
        () => producer.getParentResult(resultJob.id),
        /only available in embedded mode/
      );
      await tracker.rejects(
        'FlowProducer',
        'getParentResults',
        () => producer.getParentResults([resultJob.id]),
        /only available in embedded mode/
      );
    }
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
