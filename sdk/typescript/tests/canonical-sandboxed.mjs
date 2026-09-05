import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Queue, SandboxedWorker } from '../dist/index.js';
import { withBroker, waitFor } from './canonical-harness.mjs';

await withBroker(async (connection) => {
  const directory = mkdtempSync(join(tmpdir(), 'canonical-sandboxed-'));
  const processor = join(directory, 'processor.mjs');
  writeFileSync(
    processor,
    'export default async job => { job.progress(50); job.log("worked"); return job.data.value * 2; };'
  );
  const name = `sandboxed-${randomUUID()}`;
  const queue = new Queue(name, { embedded: false, connection });
  const worker = new SandboxedWorker(name, { processor, connection, concurrency: 2 });
  const errors = [];
  worker.on('error', (error) => errors.push(error));
  try {
    await worker.start();
    const jobs = await queue.addBulk([
      { name: 'first', data: { value: 21 } },
      { name: 'second', data: { value: 6 } },
    ]);
    await waitFor(async () => (await queue.getJobCountsAsync()).completed === 2);
    assert.equal((await queue.getJob(jobs[0].id)).returnvalue, 42);
    assert.equal((await queue.getJob(jobs[1].id)).returnvalue, 12);
    assert.equal((await queue.getJobCountsAsync()).active, 0);
    await worker.stop(true);
    await worker.stop(true);
    assert.deepEqual(errors, []);
    console.log(
      'PASS canonical SandboxedWorker: processor, results, counters, idempotent shutdown'
    );
  } finally {
    await worker.stop(true);
    await queue.disconnect();
    rmSync(directory, { recursive: true, force: true });
  }
});
