/**
 * Test: Issue #102 - cancel() does not remove flow chain jobs in waitingDeps state
 *
 * When a flow chain (A -> B -> C) is created, the dependent jobs B and C are
 * parked in `shard.waitingDeps` (state "waiting-children") with a jobIndex
 * location of `{ type: 'queue' }`. cancelJob() only handled the run queue and
 * `waitingChildren` maps, so a cancel on a waitingDeps job returned false,
 * never called storage.deleteJob(), and the job survived a server restart.
 *
 * https://github.com/egeominotti/bunqueue/issues/102
 */
import { describe, it, expect, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { FlowProducer, Queue, shutdownManager } from '../src/client';
import { Database } from 'bun:sqlite';
import { unlinkSync, existsSync } from 'fs';

const TEST_DB = '/tmp/test-issue102-cancel-waitingdeps.db';

function cleanup() {
  for (const suffix of ['', '-wal', '-shm']) {
    try {
      if (existsSync(TEST_DB + suffix)) unlinkSync(TEST_DB + suffix);
    } catch {}
  }
}

describe('Issue #102: cancel() removes waitingDeps jobs from memory + SQLite', () => {
  afterEach(() => cleanup());

  it('cancel on a waitingDeps job removes it from memory and it stays gone after restart', async () => {
    cleanup();

    // ── Phase 1: build a chain A -> B (B depends on A, so B parks in waitingDeps) ──
    const manager1 = new QueueManager({ dataPath: TEST_DB });

    const [idA] = await manager1.pushBatch('chain', [{ data: { step: 'A' } }]);
    const [idB] = await manager1.pushBatch('chain', [{ data: { step: 'B' }, dependsOn: [idA] }]);

    // B is parked waiting on A (no worker drains A, so the dependency never resolves)
    expect(await manager1.getJobState(idB)).toBe('waiting-children');
    expect(await manager1.getJob(idB)).not.toBeNull();

    // ── Phase 2: cancel the waiting job ──
    const cancelled = await manager1.cancel(idB);
    expect(cancelled).toBe(true); // pre-fix: returned false

    // In-memory: gone immediately
    expect(await manager1.getJob(idB)).toBeNull();

    // ── Phase 3: flush + restart, the job must NOT reappear ──
    manager1.shutdown(); // close() flushes the write buffer to disk

    // SQLite row is gone (deleteJob was invoked)
    const db = new Database(TEST_DB);
    const row = db.query('SELECT id FROM jobs WHERE id = ?').get(String(idB)) as {
      id: string;
    } | null;
    db.close();
    expect(row).toBeNull();

    const manager2 = new QueueManager({ dataPath: TEST_DB });
    expect(await manager2.getJob(idB)).toBeNull(); // pre-fix: B reappears here
    expect(await manager2.getJob(idA)).not.toBeNull(); // sibling untouched
    manager2.shutdown();
  });

  it('releases the uniqueKey held by a cancelled waitingDeps job', async () => {
    cleanup();
    const manager = new QueueManager({ dataPath: TEST_DB });

    const [idA] = await manager.pushBatch('dedup-chain', [{ data: { step: 'A' } }]);
    const [idB] = await manager.pushBatch('dedup-chain', [
      { data: { step: 'B' }, dependsOn: [idA], uniqueKey: 'job-b' },
    ]);
    expect(await manager.getJobState(idB)).toBe('waiting-children');

    await manager.cancel(idB);

    // Same uniqueKey must now be reusable (the cancel released the reservation)
    const [idB2] = await manager.pushBatch('dedup-chain', [
      { data: { step: 'B-again' }, dependsOn: [idA], uniqueKey: 'job-b' },
    ]);
    expect(idB2).not.toBe(idB);
    expect(await manager.getJob(idB2)).not.toBeNull();

    manager.shutdown();
  });
});

describe('Issue #102: FlowProducer.addChain + Queue.removeAsync (embedded)', () => {
  afterEach(() => {
    shutdownManager();
  });

  it('removeAsync deletes a waiting-children chain job (faithful to the report)', async () => {
    const flow = new FlowProducer({ embedded: true });
    const queue = new Queue('issue102-chain', { embedded: true });
    queue.obliterate();

    const { jobIds } = await flow.addChain([
      { name: 'step-1', queueName: 'issue102-chain', data: { id: '1' } },
      { name: 'step-2', queueName: 'issue102-chain', data: { id: '2' } },
      { name: 'step-3', queueName: 'issue102-chain', data: { id: '3' } },
    ]);
    expect(jobIds).toHaveLength(3);

    // step-2 and step-3 depend on predecessors -> waiting-children
    expect(await queue.getJobState(jobIds[1])).toBe('waiting-children');
    expect(await queue.getJobState(jobIds[2])).toBe('waiting-children');

    await queue.removeAsync(jobIds[1]);
    await queue.removeAsync(jobIds[2]);

    expect(await queue.getJob(jobIds[1])).toBeNull();
    expect(await queue.getJob(jobIds[2])).toBeNull();

    flow.close();
    queue.close();
  });
});
