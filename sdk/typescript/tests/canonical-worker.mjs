import assert from 'node:assert/strict';
import './canonical-sandboxed.mjs';
import { randomUUID } from 'node:crypto';
import { Queue, Worker, QueueEvents, UnrecoverableError } from '../dist/index.js';
import { waitFor, withBroker } from './canonical-harness.mjs';

await withBroker(async (connection) => {
  const name = `canonical-worker-${randomUUID()}`;
  const queue = new Queue(name, { embedded: false, connection });
  const events = new QueueEvents(name, { embedded: false, connection });
  const errors = [];
  const eventErrors = [];
  const completed = [];
  let worker;
  events.on('error', (error) => eventErrors.push(error.message));
  events.on('completed', (event) => completed.push(event.jobId));
  try {
    await events.waitUntilReady();
    worker = new Worker(
      name,
      async (job) => {
        await job.updateProgress(50);
        await job.log('processing');
        if (job.name === 'fail') throw new UnrecoverableError('canonical failure');
        return { doubled: job.data.n * 2 };
      },
      { embedded: false, connection, concurrency: 2 }
    );
    worker.on('error', (error) => errors.push(error));
    await worker.waitUntilReady();
    const success = await queue.add('success', { n: 7 });
    const failure = await queue.add('fail', { n: 9 }, { attempts: 3 });
    assert.deepEqual(await queue.waitJobUntilFinished(success.id, events, 10_000), { doubled: 14 });
    await waitFor(async () => (await queue.getJobState(failure.id)) === 'failed');
    await waitFor(() => completed.includes(success.id));
    await worker.close();
    assert.equal((await queue.getJobCountsAsync()).completed, 1);
    assert.equal((await queue.getJobCountsAsync()).failed, 1);
    assert.equal((await queue.getDlqAsync())[0].job.attemptsMade, 1);
    assert.equal((await queue.getMetrics('completed')).meta.count, 1);
    assert.deepEqual(errors, []);
    assert.deepEqual(eventErrors, ['canonical failure']);
    console.log(
      'PASS canonical Worker: processing, progress, logs, events, results, unrecoverable failure, shutdown'
    );
  } finally {
    if (worker) await worker.close();
    events.close();
    await queue.disconnect();
  }
});
