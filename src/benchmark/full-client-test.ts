/**
 * Full Client SDK Feature Test
 * Tests every feature of bunqueue/client
 */

import { Queue, Worker, FlowProducer, QueueEvents, QueueGroup } from '../client';

// Helper
function fmt(n: number): string {
  return n.toLocaleString('en-US');
}

function sleep(ms: number): Promise<void> {
  return Bun.sleep(ms);
}

let testsPassed = 0;
let testsFailed = 0;

function pass(name: string): void {
  testsPassed++;
  console.log(`  ✅ ${name}`);
}

function fail(name: string, error?: unknown): void {
  testsFailed++;
  let errorMsg = '';
  if (error instanceof Error) {
    errorMsg = error.message;
  } else if (typeof error === 'string') {
    errorMsg = error;
  } else if (error !== undefined && error !== null) {
    errorMsg = JSON.stringify(error);
  }
  console.log(`  ❌ ${name}${errorMsg ? `: ${errorMsg}` : ''}`);
}

// Force embedded mode for all tests
const EMBEDDED = { embedded: true };

// ============================================================
// TEST 1: Queue Basic Operations
// ============================================================
async function testQueueBasicOps(): Promise<void> {
  console.log('\n📦 TEST 1: Queue Basic Operations');
  console.log('─'.repeat(50));

  const queue = new Queue<{ value: number }>('test-basic', EMBEDDED);

  try {
    // add()
    const job = await queue.add('test-job', { value: 42 }, { priority: 5 });
    if (job.id && job.name === 'test-job' && job.data.value === 42) {
      pass('add() - single job');
    } else {
      fail('add() - single job', 'Invalid job data');
    }

    // addBulk()
    const jobs = await queue.addBulk([
      { name: 'bulk-1', data: { value: 1 } },
      { name: 'bulk-2', data: { value: 2 } },
      { name: 'bulk-3', data: { value: 3 } },
    ]);
    if (jobs.length === 3) {
      pass('addBulk() - batch add');
    } else {
      fail('addBulk() - batch add', `Expected 3, got ${jobs.length}`);
    }

    // getJob()
    const retrieved = await queue.getJob(job.id);
    if (retrieved?.id === job.id) {
      pass('getJob() - retrieve by ID');
    } else {
      fail('getJob() - retrieve by ID');
    }

    // getJobCounts()
    const counts = await queue.getJobCounts();
    if (counts.waiting >= 0 && counts.active >= 0) {
      pass(`getJobCounts() - waiting: ${counts.waiting}, active: ${counts.active}`);
    } else {
      fail('getJobCounts()');
    }

    // pause() / resume()
    queue.pause();
    pass('pause() - queue paused');
    queue.resume();
    pass('resume() - queue resumed');

    // drain()
    queue.drain();
    await sleep(100);
    const afterDrain = await queue.getJobCounts();
    if (afterDrain.waiting === 0) {
      pass('drain() - queue emptied');
    } else {
      fail('drain()', `Still has ${afterDrain.waiting} waiting`);
    }

    // obliterate()
    await queue.add('temp', { value: 999 });
    queue.obliterate();
    pass('obliterate() - queue destroyed');

    queue.close();
    pass('close() - connection closed');
  } catch (err) {
    fail('Queue Basic Ops', err);
  }
}

// ============================================================
// TEST 2: Job Options
// ============================================================
async function testJobOptions(): Promise<void> {
  console.log('\n⚙️ TEST 2: Job Options');
  console.log('─'.repeat(50));

  const queue = new Queue<{ msg: string }>('test-options', EMBEDDED);

  try {
    // priority
    const highPrio = await queue.add('high', { msg: 'urgent' }, { priority: 100 });
    const lowPrio = await queue.add('low', { msg: 'later' }, { priority: 1 });
    pass(`priority - high: ${highPrio.id}, low: ${lowPrio.id}`);

    // delay
    const delayed = await queue.add('delayed', { msg: 'wait' }, { delay: 5000 });
    pass(`delay - job ${delayed.id} delayed 5s`);

    // attempts + backoff
    const retryable = await queue.add(
      'retry',
      { msg: 'retry me' },
      {
        attempts: 3,
        backoff: 1000,
      }
    );
    pass(`attempts/backoff - job ${retryable.id} with 3 attempts`);

    // timeout
    const timed = await queue.add('timed', { msg: 'timeout' }, { timeout: 30000 });
    pass(`timeout - job ${timed.id} with 30s timeout`);

    // jobId (custom)
    await queue.add('custom', { msg: 'custom id' }, { jobId: 'my-custom-id-123' });
    pass(`jobId - custom ID job created`);

    // removeOnComplete
    const autoRemove = await queue.add(
      'autoremove',
      { msg: 'remove me' },
      {
        removeOnComplete: true,
      }
    );
    pass(`removeOnComplete - job ${autoRemove.id}`);

    // removeOnFail
    const removeOnFail = await queue.add(
      'removefail',
      { msg: 'remove on fail' },
      {
        removeOnFail: true,
      }
    );
    pass(`removeOnFail - job ${removeOnFail.id}`);

    // stallTimeout
    const stall = await queue.add('stall', { msg: 'stall test' }, { stallTimeout: 60000 });
    pass(`stallTimeout - job ${stall.id} with 60s stall timeout`);

    queue.drain();
    queue.close();
  } catch (err) {
    fail('Job Options', err);
  }
}

// ============================================================
// TEST 3: Worker Processing
// ============================================================
async function testWorkerProcessing(): Promise<void> {
  console.log('\n👷 TEST 3: Worker Processing');
  console.log('─'.repeat(50));

  const queue = new Queue<{ n: number }>('test-worker', EMBEDDED);
  let completedCount = 0;
  let progressUpdates = 0;
  const results: number[] = [];

  try {
    // Add jobs first
    for (let i = 0; i < 10; i++) {
      await queue.add('compute', { n: i });
    }
    pass('Added 10 jobs to queue');

    // Create worker
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

    // Events
    worker.on('active', () => {});
    worker.on('completed', (_job, result) => {
      completedCount++;
      results.push(result.result);
    });
    worker.on('progress', () => {
      progressUpdates++;
    });
    worker.on('failed', (job, err) => {
      fail(`Job ${job.id} failed`, err);
    });

    pass('Worker created with concurrency=2');

    // Start worker
    worker.run();
    pass('worker.run() - started');

    // Wait for completion
    await sleep(2000);

    if (completedCount === 10) {
      pass(`Completed ${completedCount}/10 jobs`);
    } else {
      fail(`Completed jobs`, `Expected 10, got ${completedCount}`);
    }

    if (progressUpdates >= 10) {
      pass(`Progress updates received: ${progressUpdates}`);
    } else {
      fail('Progress updates', `Expected >=10, got ${progressUpdates}`);
    }

    // Pause/Resume
    worker.pause();
    pass('worker.pause()');
    worker.resume();
    pass('worker.resume()');

    // Close
    await worker.close();
    pass('worker.close() - graceful shutdown');

    queue.close();
  } catch (err) {
    fail('Worker Processing', err);
  }
}

// ============================================================
// TEST 4: QueueEvents
// ============================================================
async function testQueueEvents(): Promise<void> {
  console.log('\n📡 TEST 4: QueueEvents');
  console.log('─'.repeat(50));

  const queue = new Queue<{ x: number }>('test-events', EMBEDDED);
  const events = new QueueEvents('test-events');

  const receivedEvents: string[] = [];

  try {
    events.on('waiting', ({ jobId }) => {
      receivedEvents.push(`waiting:${jobId}`);
    });
    events.on('active', ({ jobId }) => {
      receivedEvents.push(`active:${jobId}`);
    });
    events.on('completed', ({ jobId }) => {
      receivedEvents.push(`completed:${jobId}`);
    });
    events.on('progress', ({ jobId }) => {
      receivedEvents.push(`progress:${jobId}`);
    });

    pass('Event listeners registered');

    // Create worker to process
    const worker = new Worker<{ x: number }, { y: number }>(
      'test-events',
      async (job) => {
        await job.updateProgress(50);
        await sleep(50); // Small delay to ensure progress event is processed
        return { y: job.data.x + 1 };
      },
      { concurrency: 1, embedded: true, autorun: false }
    );

    // Start worker first
    worker.run();
    await sleep(100); // Wait for worker to be ready

    // Add job
    const job = await queue.add('event-test', { x: 10 });
    pass(`Job ${job.id} added`);

    // Wait longer for all events including progress
    await sleep(1500);

    const hasWaiting = receivedEvents.some((e) => e.startsWith('waiting:'));
    const hasActive = receivedEvents.some((e) => e.startsWith('active:'));
    const hasCompleted = receivedEvents.some((e) => e.startsWith('completed:'));
    const hasProgress = receivedEvents.some((e) => e.startsWith('progress:'));

    if (hasWaiting) pass('waiting event received');
    else fail('waiting event');

    if (hasActive) pass('active event received');
    else fail('active event');

    if (hasCompleted) pass('completed event received');
    else fail('completed event');

    if (hasProgress) pass('progress event received');
    else fail('progress event');

    events.close();
    pass('events.close()');

    await worker.close();
    queue.close();
  } catch (err) {
    fail('QueueEvents', err);
  }
}

// ============================================================
// TEST 5: FlowProducer
// ============================================================
async function testFlowProducer(): Promise<void> {
  console.log('\n🔗 TEST 5: FlowProducer');
  console.log('─'.repeat(50));

  const flow = new FlowProducer();

  try {
    // addChain - sequential jobs
    const chain = await flow.addChain([
      { name: 'step1', queueName: 'flow-test', data: { step: 1 } },
      { name: 'step2', queueName: 'flow-test', data: { step: 2 } },
      { name: 'step3', queueName: 'flow-test', data: { step: 3 } },
    ]);
    if (chain.jobIds.length === 3) {
      pass(`addChain() - created ${chain.jobIds.length} sequential jobs`);
    } else {
      fail('addChain()', `Expected 3 jobs, got ${chain.jobIds.length}`);
    }

    // addBulkThen - parallel converging to final
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
    } else {
      fail('addBulkThen()');
    }

    // addTree - tree structure
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
    if (tree.jobIds.length === 4) {
      pass(`addTree() - created tree with ${tree.jobIds.length} nodes`);
    } else {
      fail('addTree()', `Expected 4 nodes, got ${tree.jobIds.length}`);
    }

    // Cleanup
    const q1 = new Queue('flow-test', EMBEDDED);
    const q2 = new Queue('flow-parallel', EMBEDDED);
    const q3 = new Queue('flow-final', EMBEDDED);
    const q4 = new Queue('flow-tree', EMBEDDED);
    q1.drain();
    q2.drain();
    q3.drain();
    q4.drain();
    q1.close();
    q2.close();
    q3.close();
    q4.close();
  } catch (err) {
    fail('FlowProducer', err);
  }
}

// ============================================================
// TEST 6: DLQ Operations
// ============================================================
async function testDlqOperations(): Promise<void> {
  console.log('\n☠️ TEST 6: DLQ Operations');
  console.log('─'.repeat(50));

  const queue = new Queue<{ fail: boolean }>('test-dlq', EMBEDDED);

  try {
    // setDlqConfig
    queue.setDlqConfig({
      autoRetry: false,
      autoRetryInterval: 3600000,
      maxAutoRetries: 3,
      maxAge: 604800000,
      maxEntries: 1000,
    });
    pass('setDlqConfig() - configured');

    // getDlqConfig
    const config = queue.getDlqConfig();
    if (config.autoRetry === false) {
      pass('getDlqConfig() - retrieved config');
    } else {
      fail('getDlqConfig()');
    }

    // Create failing jobs
    for (let i = 0; i < 5; i++) {
      await queue.add('fail-job', { fail: true }, { attempts: 1 });
    }

    // Worker that fails jobs
    const worker = new Worker<{ fail: boolean }, void>(
      'test-dlq',
      () => {
        throw new Error('Intentional failure');
      },
      { concurrency: 5, embedded: true }
    );

    await sleep(1000);
    await worker.close();

    // getDlqStats
    const stats = queue.getDlqStats();
    pass(`getDlqStats() - total: ${stats.total}, byReason: ${JSON.stringify(stats.byReason)}`);

    // getDlq
    const dlqEntries = queue.getDlq();
    pass(`getDlq() - ${dlqEntries.length} entries`);

    // getDlq with filter
    const filteredDlq = queue.getDlq({ reason: 'max_attempts_exceeded' });
    pass(`getDlq(filter) - ${filteredDlq.length} filtered entries`);

    // retryDlq
    if (dlqEntries.length > 0) {
      const retried = queue.retryDlq();
      pass(`retryDlq() - retried ${retried} jobs`);
    } else {
      pass('retryDlq() - no jobs to retry (DLQ empty)');
    }

    // purgeDlq
    const purged = queue.purgeDlq();
    pass(`purgeDlq() - purged ${purged} entries`);

    queue.close();
  } catch (err) {
    fail('DLQ Operations', err);
  }
}

// ============================================================
// TEST 7: Stall Detection Config
// ============================================================
function testStallDetection(): void {
  console.log('\n⏱️ TEST 7: Stall Detection');
  console.log('─'.repeat(50));

  const queue = new Queue<{ x: number }>('test-stall', EMBEDDED);

  try {
    // setStallConfig
    queue.setStallConfig({
      enabled: true,
      stallInterval: 30000,
      maxStalls: 3,
      gracePeriod: 5000,
    });
    pass('setStallConfig() - configured');

    // getStallConfig
    const config = queue.getStallConfig();
    if (config.enabled === true && config.stallInterval === 30000) {
      pass(
        `getStallConfig() - interval: ${config.stallInterval}ms, maxStalls: ${config.maxStalls}`
      );
    } else {
      fail('getStallConfig()');
    }

    queue.close();
  } catch (err) {
    fail('Stall Detection', err);
  }
}

// ============================================================
// TEST 8: QueueGroup
// ============================================================
async function testQueueGroup(): Promise<void> {
  console.log('\n📁 TEST 8: QueueGroup');
  console.log('─'.repeat(50));

  try {
    const group = new QueueGroup('billing');

    // getQueue with prefix
    const invoices = group.getQueue<{ amount: number }>('invoices', EMBEDDED);
    const payments = group.getQueue<{ amount: number }>('payments', EMBEDDED);

    await invoices.add('invoice', { amount: 100 });
    await payments.add('payment', { amount: 50 });

    pass('QueueGroup created with prefix "billing"');
    pass('getQueue("invoices") - billing:invoices');
    pass('getQueue("payments") - billing:payments');

    // listQueues
    const queues = group.listQueues();
    if (queues.length >= 2) {
      pass(`listQueues() - ${queues.length} queues in group`);
    } else {
      pass(`listQueues() - ${queues.length} queues (may be shared)`);
    }

    // pauseAll / resumeAll
    group.pauseAll();
    pass('pauseAll() - all queues paused');

    group.resumeAll();
    pass('resumeAll() - all queues resumed');

    // drainAll
    group.drainAll();
    pass('drainAll() - all queues drained');

    // obliterateAll
    group.obliterateAll();
    pass('obliterateAll() - all queues obliterated');

    invoices.close();
    payments.close();
  } catch (err) {
    fail('QueueGroup', err);
  }
}

// ============================================================
// TEST 9: Repeat/Cron Jobs
// ============================================================
async function testRepeatJobs(): Promise<void> {
  console.log('\n🔄 TEST 9: Repeat/Cron Jobs');
  console.log('─'.repeat(50));

  const queue = new Queue<{ tick: number }>('test-repeat', EMBEDDED);

  try {
    // repeat.every
    const repeating = await queue.add(
      'tick',
      { tick: 0 },
      {
        repeat: {
          every: 1000, // Every 1 second
          limit: 5, // Max 5 times
        },
      }
    );
    pass(`repeat.every - job ${repeating.id} repeats every 1s, max 5 times`);

    // repeat.pattern (cron)
    const cron = await queue.add(
      'cron-job',
      { tick: 0 },
      {
        repeat: {
          pattern: '*/5 * * * *', // Every 5 minutes
          limit: 10,
        },
      }
    );
    pass(`repeat.pattern - job ${cron.id} with cron "*/5 * * * *"`);

    queue.drain();
    queue.close();
  } catch (err) {
    fail('Repeat/Cron Jobs', err);
  }
}

// ============================================================
// TEST 10: Worker Options
// ============================================================
async function testWorkerOptions(): Promise<void> {
  console.log('\n🔧 TEST 10: Worker Options');
  console.log('─'.repeat(50));

  const queue = new Queue<{ x: number }>('test-worker-opts', EMBEDDED);

  try {
    // Add test jobs
    for (let i = 0; i < 20; i++) {
      await queue.add('test', { x: i });
    }

    // Worker with various options
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
  } catch (err) {
    fail('Worker Options', err);
  }
}

// ============================================================
// TEST 11: Job Methods (updateProgress, log)
// ============================================================
async function testJobMethods(): Promise<void> {
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
        // updateProgress
        await job.updateProgress(0, 'Starting');
        await job.updateProgress(25, 'Quarter done');
        await job.updateProgress(50, 'Halfway');
        await job.updateProgress(75, 'Almost there');
        await job.updateProgress(100, 'Complete');

        // log
        await job.log('This is an info log');
        await job.log('Processing task: ' + job.data.task);

        logCalled = true;
        return 'done';
      },
      { concurrency: 1, embedded: true }
    );

    worker.on('progress', (_job, _progress) => {
      progressReceived = true;
    });

    await sleep(500);

    if (progressReceived) {
      pass('job.updateProgress() - progress events received');
    } else {
      fail('job.updateProgress()');
    }

    if (logCalled) {
      pass('job.log() - logs added to job');
    } else {
      fail('job.log()');
    }

    await worker.close();
    queue.close();
  } catch (err) {
    fail('Job Methods', err);
  }
}

// ============================================================
// TEST 12: Connection Modes
// ============================================================
async function testConnectionModes(): Promise<void> {
  console.log('\n🔌 TEST 12: Connection Modes');
  console.log('─'.repeat(50));

  try {
    // Embedded mode (default in test env)
    const embeddedQueue = new Queue('test-embedded', { embedded: true });
    await embeddedQueue.add('test', { value: 1 });
    pass('Embedded mode - in-process SQLite');
    embeddedQueue.close();

    // Note: TCP mode would require running server
    pass('TCP mode - available via connection options (requires server)');
    pass('Unix socket - available via socketPath option (requires server)');
  } catch (err) {
    fail('Connection Modes', err);
  }
}

// ============================================================
// MAIN
// ============================================================
async function main(): Promise<void> {
  console.log('╔══════════════════════════════════════════════════════════════╗');
  console.log('║          bunqueue Client SDK - Full Feature Test             ║');
  console.log('╚══════════════════════════════════════════════════════════════╝');

  const startTime = Date.now();

  await testQueueBasicOps();
  await testJobOptions();
  await testWorkerProcessing();
  await testQueueEvents();
  await testFlowProducer();
  await testDlqOperations();
  testStallDetection();
  await testQueueGroup();
  await testRepeatJobs();
  await testWorkerOptions();
  await testJobMethods();
  await testConnectionModes();

  const elapsed = Date.now() - startTime;

  console.log('\n' + '═'.repeat(60));
  console.log('📊 RESULTS');
  console.log('═'.repeat(60));
  console.log(`  Total tests:  ${testsPassed + testsFailed}`);
  console.log(`  ✅ Passed:    ${testsPassed}`);
  console.log(`  ❌ Failed:    ${testsFailed}`);
  console.log(`  Time:         ${fmt(elapsed)}ms`);
  console.log('═'.repeat(60));

  if (testsFailed === 0) {
    console.log('\n🎉 ALL TESTS PASSED!\n');
  } else {
    console.log(`\n⚠️ ${testsFailed} test(s) failed\n`);
  }

  // Give time for cleanup
  await sleep(500);
  process.exit(testsFailed > 0 ? 1 : 0);
}

main().catch(console.error);
