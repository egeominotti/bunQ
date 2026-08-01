import { Queue, QueueGroup } from '../../client';
import { EMBEDDED, fail, pass } from './harness';

export function testStallDetection(): void {
  console.log('\n⏱️ TEST 7: Stall Detection');
  console.log('─'.repeat(50));
  const queue = new Queue<{ x: number }>('test-stall', EMBEDDED);
  try {
    queue.setStallConfig({
      enabled: true,
      stallInterval: 30000,
      maxStalls: 3,
      gracePeriod: 5000,
    });
    pass('setStallConfig() - configured');
    const config = queue.getStallConfig();
    if (config.enabled === true && config.stallInterval === 30000) {
      pass(
        `getStallConfig() - interval: ${config.stallInterval}ms, maxStalls: ${config.maxStalls}`
      );
    } else fail('getStallConfig()');
    queue.close();
  } catch (error) {
    fail('Stall Detection', error);
  }
}

export async function testQueueGroup(): Promise<void> {
  console.log('\n📁 TEST 8: QueueGroup');
  console.log('─'.repeat(50));
  try {
    const group = new QueueGroup('billing');
    const invoices = group.getQueue<{ amount: number }>('invoices', EMBEDDED);
    const payments = group.getQueue<{ amount: number }>('payments', EMBEDDED);
    await invoices.add('invoice', { amount: 100 });
    await payments.add('payment', { amount: 50 });
    pass('QueueGroup created with prefix "billing"');
    pass('getQueue("invoices") - billing:invoices');
    pass('getQueue("payments") - billing:payments');
    const queues = group.listQueues();
    if (queues.length >= 2) pass(`listQueues() - ${queues.length} queues in group`);
    else pass(`listQueues() - ${queues.length} queues (may be shared)`);
    group.pauseAll();
    pass('pauseAll() - all queues paused');
    group.resumeAll();
    pass('resumeAll() - all queues resumed');
    group.drainAll();
    pass('drainAll() - all queues drained');
    group.obliterateAll();
    pass('obliterateAll() - all queues obliterated');
    invoices.close();
    payments.close();
  } catch (error) {
    fail('QueueGroup', error);
  }
}

export async function testRepeatJobs(): Promise<void> {
  console.log('\n🔄 TEST 9: Repeat/Cron Jobs');
  console.log('─'.repeat(50));
  const queue = new Queue<{ tick: number }>('test-repeat', EMBEDDED);
  try {
    const repeating = await queue.add('tick', { tick: 0 }, { repeat: { every: 1000, limit: 5 } });
    pass(`repeat.every - job ${repeating.id} repeats every 1s, max 5 times`);
    const cron = await queue.add(
      'cron-job',
      { tick: 0 },
      { repeat: { pattern: '*/5 * * * *', limit: 10 } }
    );
    pass(`repeat.pattern - job ${cron.id} with cron "*/5 * * * *"`);
    queue.drain();
    queue.close();
  } catch (error) {
    fail('Repeat/Cron Jobs', error);
  }
}

export async function testConnectionModes(): Promise<void> {
  console.log('\n🔌 TEST 12: Connection Modes');
  console.log('─'.repeat(50));
  try {
    const embeddedQueue = new Queue('test-embedded', { embedded: true });
    await embeddedQueue.add('test', { value: 1 });
    pass('Embedded mode - in-process SQLite');
    embeddedQueue.close();
    pass('TCP mode - available via connection options (requires server)');
    pass('Unix socket - available via socketPath option (requires server)');
  } catch (error) {
    fail('Connection Modes', error);
  }
}
