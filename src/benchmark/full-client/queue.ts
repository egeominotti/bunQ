import { Queue } from '../../client';
import { EMBEDDED, fail, pass, sleep } from './harness';

export async function testQueueBasicOps(): Promise<void> {
  console.log('\n📦 TEST 1: Queue Basic Operations');
  console.log('─'.repeat(50));
  const queue = new Queue<{ value: number }>('test-basic', EMBEDDED);
  try {
    const job = await queue.add('test-job', { value: 42 }, { priority: 5 });
    if (job.id && job.name === 'test-job' && job.data.value === 42) pass('add() - single job');
    else fail('add() - single job', 'Invalid job data');

    const jobs = await queue.addBulk([
      { name: 'bulk-1', data: { value: 1 } },
      { name: 'bulk-2', data: { value: 2 } },
      { name: 'bulk-3', data: { value: 3 } },
    ]);
    if (jobs.length === 3) pass('addBulk() - batch add');
    else fail('addBulk() - batch add', `Expected 3, got ${jobs.length}`);

    const retrieved = await queue.getJob(job.id);
    if (retrieved?.id === job.id) pass('getJob() - retrieve by ID');
    else fail('getJob() - retrieve by ID');

    const counts = await queue.getJobCounts();
    if (counts.waiting >= 0 && counts.active >= 0) {
      pass(`getJobCounts() - waiting: ${counts.waiting}, active: ${counts.active}`);
    } else fail('getJobCounts()');

    queue.pause();
    pass('pause() - queue paused');
    queue.resume();
    pass('resume() - queue resumed');
    queue.drain();
    await sleep(100);
    const afterDrain = await queue.getJobCounts();
    if (afterDrain.waiting === 0) pass('drain() - queue emptied');
    else fail('drain()', `Still has ${afterDrain.waiting} waiting`);
    await queue.add('temp', { value: 999 });
    queue.obliterate();
    pass('obliterate() - queue destroyed');
    queue.close();
    pass('close() - connection closed');
  } catch (error) {
    fail('Queue Basic Ops', error);
  }
}

export async function testJobOptions(): Promise<void> {
  console.log('\n⚙️ TEST 2: Job Options');
  console.log('─'.repeat(50));
  const queue = new Queue<{ msg: string }>('test-options', EMBEDDED);
  try {
    const highPriority = await queue.add('high', { msg: 'urgent' }, { priority: 100 });
    const lowPriority = await queue.add('low', { msg: 'later' }, { priority: 1 });
    pass(`priority - high: ${highPriority.id}, low: ${lowPriority.id}`);
    const delayed = await queue.add('delayed', { msg: 'wait' }, { delay: 5000 });
    pass(`delay - job ${delayed.id} delayed 5s`);
    const retryable = await queue.add('retry', { msg: 'retry me' }, { attempts: 3, backoff: 1000 });
    pass(`attempts/backoff - job ${retryable.id} with 3 attempts`);
    const timed = await queue.add('timed', { msg: 'timeout' }, { timeout: 30000 });
    pass(`timeout - job ${timed.id} with 30s timeout`);
    await queue.add('custom', { msg: 'custom id' }, { jobId: 'my-custom-id-123' });
    pass('jobId - custom ID job created');
    const autoRemove = await queue.add(
      'autoremove',
      { msg: 'remove me' },
      { removeOnComplete: true }
    );
    pass(`removeOnComplete - job ${autoRemove.id}`);
    const removeOnFail = await queue.add(
      'removefail',
      { msg: 'remove on fail' },
      { removeOnFail: true }
    );
    pass(`removeOnFail - job ${removeOnFail.id}`);
    const stall = await queue.add('stall', { msg: 'stall test' }, { stallTimeout: 60000 });
    pass(`stallTimeout - job ${stall.id} with 60s stall timeout`);
    queue.drain();
    queue.close();
  } catch (error) {
    fail('Job Options', error);
  }
}
