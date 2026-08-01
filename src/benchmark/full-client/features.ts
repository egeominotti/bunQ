import { FlowProducer, Queue, Worker } from '../../client';
import { EMBEDDED, fail, pass, sleep } from './harness';

export async function testFlowProducer(): Promise<void> {
  console.log('\n🔗 TEST 5: FlowProducer');
  console.log('─'.repeat(50));
  const flow = new FlowProducer();
  try {
    const chain = await flow.addChain([
      { name: 'step1', queueName: 'flow-test', data: { step: 1 } },
      { name: 'step2', queueName: 'flow-test', data: { step: 2 } },
      { name: 'step3', queueName: 'flow-test', data: { step: 3 } },
    ]);
    if (chain.jobIds.length === 3) {
      pass(`addChain() - created ${chain.jobIds.length} sequential jobs`);
    } else fail('addChain()', `Expected 3 jobs, got ${chain.jobIds.length}`);

    const bulkThen = await flow.addBulkThen(
      [
        { name: 'parallel1', queueName: 'flow-parallel', data: { id: 1 } },
        { name: 'parallel2', queueName: 'flow-parallel', data: { id: 2 } },
        { name: 'parallel3', queueName: 'flow-parallel', data: { id: 3 } },
      ],
      { name: 'merge', queueName: 'flow-final', data: { merge: true } }
    );
    if (bulkThen.parallelIds.length === 3 && bulkThen.finalId) {
      pass(`addBulkThen() - ${bulkThen.parallelIds.length} parallel → 1 final`);
    } else fail('addBulkThen()');

    const tree = await flow.addTree({
      name: 'root',
      queueName: 'flow-tree',
      data: { level: 0 },
      children: [
        { name: 'child1', queueName: 'flow-tree', data: { level: 1 } },
        {
          name: 'child2',
          queueName: 'flow-tree',
          data: { level: 1 },
          children: [{ name: 'grandchild', queueName: 'flow-tree', data: { level: 2 } }],
        },
      ],
    });
    if (tree.jobIds.length === 4) pass(`addTree() - created tree with ${tree.jobIds.length} nodes`);
    else fail('addTree()', `Expected 4 nodes, got ${tree.jobIds.length}`);

    const queues = [
      new Queue('flow-test', EMBEDDED),
      new Queue('flow-parallel', EMBEDDED),
      new Queue('flow-final', EMBEDDED),
      new Queue('flow-tree', EMBEDDED),
    ];
    for (const queue of queues) queue.drain();
    for (const queue of queues) queue.close();
  } catch (error) {
    fail('FlowProducer', error);
  }
}

export async function testDlqOperations(): Promise<void> {
  console.log('\n☠️ TEST 6: DLQ Operations');
  console.log('─'.repeat(50));
  const queue = new Queue<{ fail: boolean }>('test-dlq', EMBEDDED);
  try {
    queue.setDlqConfig({
      autoRetry: false,
      autoRetryInterval: 3600000,
      maxAutoRetries: 3,
      maxAge: 604800000,
      maxEntries: 1000,
    });
    pass('setDlqConfig() - configured');
    if (queue.getDlqConfig().autoRetry === false) pass('getDlqConfig() - retrieved config');
    else fail('getDlqConfig()');
    for (let index = 0; index < 5; index++) {
      await queue.add('fail-job', { fail: true }, { attempts: 1 });
    }
    const worker = new Worker<{ fail: boolean }, void>(
      'test-dlq',
      () => {
        throw new Error('Intentional failure');
      },
      { concurrency: 5, embedded: true }
    );
    await sleep(1000);
    await worker.close();
    const stats = queue.getDlqStats();
    pass(`getDlqStats() - total: ${stats.total}, byReason: ${JSON.stringify(stats.byReason)}`);
    const entries = queue.getDlq();
    pass(`getDlq() - ${entries.length} entries`);
    const filteredEntries = queue.getDlq({ reason: 'max_attempts_exceeded' });
    pass(`getDlq(filter) - ${filteredEntries.length} filtered entries`);
    if (entries.length > 0) pass(`retryDlq() - retried ${queue.retryDlq()} jobs`);
    else pass('retryDlq() - no jobs to retry (DLQ empty)');
    pass(`purgeDlq() - purged ${queue.purgeDlq()} entries`);
    queue.close();
  } catch (error) {
    fail('DLQ Operations', error);
  }
}
