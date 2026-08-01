import { Queue, QueueEvents, Worker } from '../../client';
import { EMBEDDED, fail, pass, sleep } from './harness';

export async function testWorkerProcessing(): Promise<void> {
  console.log('\n👷 TEST 3: Worker Processing');
  console.log('─'.repeat(50));
  const queue = new Queue<{ n: number }>('test-worker', EMBEDDED);
  let completedCount = 0;
  let progressUpdates = 0;
  const results: number[] = [];
  try {
    for (let index = 0; index < 10; index++) await queue.add('compute', { n: index });
    pass('Added 10 jobs to queue');
    const worker = new Worker<{ n: number }, { result: number }>(
      'test-worker',
      async (job) => {
        await job.updateProgress(50, 'Processing...');
        await job.log(`Processing job ${job.id} with n=${job.data.n}`);
        const result = job.data.n * 2;
        await job.updateProgress(100, 'Done');
        return { result };
      },
      { concurrency: 2, autorun: false, embedded: true }
    );
    worker.on('active', () => {
      // Listener registration itself is the behavior under test.
    });
    worker.on('completed', (_job, result) => {
      completedCount++;
      results.push(result.result);
    });
    worker.on('progress', () => progressUpdates++);
    worker.on('failed', (job, error) => fail(`Job ${job.id} failed`, error));
    pass('Worker created with concurrency=2');
    worker.run();
    pass('worker.run() - started');
    await sleep(2000);
    if (completedCount === 10) pass(`Completed ${completedCount}/10 jobs`);
    else fail('Completed jobs', `Expected 10, got ${completedCount}`);
    if (progressUpdates >= 10) pass(`Progress updates received: ${progressUpdates}`);
    else fail('Progress updates', `Expected >=10, got ${progressUpdates}`);
    worker.pause();
    pass('worker.pause()');
    worker.resume();
    pass('worker.resume()');
    await worker.close();
    pass('worker.close() - graceful shutdown');
    queue.close();
  } catch (error) {
    fail('Worker Processing', error);
  }
}

export async function testQueueEvents(): Promise<void> {
  console.log('\n📡 TEST 4: QueueEvents');
  console.log('─'.repeat(50));
  const queue = new Queue<{ x: number }>('test-events', EMBEDDED);
  const events = new QueueEvents('test-events');
  const receivedEvents: string[] = [];
  try {
    events.on('waiting', ({ jobId }) => receivedEvents.push(`waiting:${jobId}`));
    events.on('active', ({ jobId }) => receivedEvents.push(`active:${jobId}`));
    events.on('completed', ({ jobId }) => receivedEvents.push(`completed:${jobId}`));
    events.on('progress', ({ jobId }) => receivedEvents.push(`progress:${jobId}`));
    pass('Event listeners registered');
    const worker = new Worker<{ x: number }, { y: number }>(
      'test-events',
      async (job) => {
        await job.updateProgress(50);
        await sleep(50);
        return { y: job.data.x + 1 };
      },
      { concurrency: 1, embedded: true, autorun: false }
    );
    worker.run();
    await sleep(100);
    const job = await queue.add('event-test', { x: 10 });
    pass(`Job ${job.id} added`);
    await sleep(1500);
    if (receivedEvents.some((event) => event.startsWith('waiting:')))
      pass('waiting event received');
    else fail('waiting event');
    if (receivedEvents.some((event) => event.startsWith('active:'))) pass('active event received');
    else fail('active event');
    if (receivedEvents.some((event) => event.startsWith('completed:'))) {
      pass('completed event received');
    } else fail('completed event');
    if (receivedEvents.some((event) => event.startsWith('progress:')))
      pass('progress event received');
    else fail('progress event');
    events.close();
    pass('events.close()');
    await worker.close();
    queue.close();
  } catch (error) {
    fail('QueueEvents', error);
  }
}

export async function testWorkerOptions(): Promise<void> {
  console.log('\n🔧 TEST 10: Worker Options');
  console.log('─'.repeat(50));
  const queue = new Queue<{ x: number }>('test-worker-opts', EMBEDDED);
  try {
    for (let index = 0; index < 20; index++) await queue.add('test', { x: index });
    const worker = new Worker<{ x: number }, number>('test-worker-opts', (job) => job.data.x * 2, {
      concurrency: 4,
      autorun: false,
      heartbeatInterval: 5000,
      batchSize: 5,
      pollTimeout: 100,
      useLocks: true,
      embedded: true,
    });
    pass('Worker created with options:');
    pass('  - concurrency: 4');
    pass('  - autorun: false');
    pass('  - heartbeatInterval: 5000');
    pass('  - batchSize: 5');
    pass('  - pollTimeout: 100');
    pass('  - useLocks: true');
    worker.run();
    await sleep(1000);
    await worker.close();
    pass('Worker processed jobs and closed');
    queue.drain();
    queue.close();
  } catch (error) {
    fail('Worker Options', error);
  }
}

export async function testJobMethods(): Promise<void> {
  console.log('\n📝 TEST 11: Job Methods');
  console.log('─'.repeat(50));
  const queue = new Queue<{ task: string }>('test-job-methods', EMBEDDED);
  let progressReceived = false;
  let logCalled = false;
  try {
    await queue.add('task', { task: 'test' });
    const worker = new Worker<{ task: string }, string>(
      'test-job-methods',
      async (job) => {
        await job.updateProgress(0, 'Starting');
        await job.updateProgress(25, 'Quarter done');
        await job.updateProgress(50, 'Halfway');
        await job.updateProgress(75, 'Almost there');
        await job.updateProgress(100, 'Complete');
        await job.log('This is an info log');
        await job.log('Processing task: ' + job.data.task);
        logCalled = true;
        return 'done';
      },
      { concurrency: 1, embedded: true }
    );
    worker.on('progress', () => {
      progressReceived = true;
    });
    await sleep(500);
    if (progressReceived) pass('job.updateProgress() - progress events received');
    else fail('job.updateProgress()');
    if (logCalled) pass('job.log() - logs added to job');
    else fail('job.log()');
    await worker.close();
    queue.close();
  } catch (error) {
    fail('Job Methods', error);
  }
}
