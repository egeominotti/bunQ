import { expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pullJob } from '../src/application/operations/pull';
import { Shard } from '../src/domain/queue/shard';
import { createJob, jobId } from '../src/domain/types/job';
import { SqliteStorage } from '../src/infrastructure/persistence/sqlite';
import { processingShardIndex, shardIndex, SHARD_COUNT } from '../src/shared/hash';
import { RWLock } from '../src/shared/lock';
import { RealQueue } from './model-based/queue-model-harness';

test('expired jobs are deleted once from memory, indexes, counters, and SQLite', async () => {
  const real = await RealQueue.create(`ttl-expiry-regression-${Date.now()}`);
  const id = 'model-expired-job';
  try {
    await real.send({
      cmd: 'PUSH',
      data: { generation: 1 },
      durable: true,
      jobId: id,
      queue: real.queue,
      timestamp: Date.now() - 10000,
      ttl: 1,
    });

    const first = await real.send({
      cmd: 'PULL',
      owner: 'ttl-worker',
      queue: real.queue,
      timeout: 0,
    });
    expect(first.job).toBeNull();
    expect((await real.send({ cmd: 'GetState', id })).state).toBe('unknown');
    expect((await real.send({ cmd: 'Count', queue: real.queue })).count).toBe(0);

    const db = new Database(real.dbPath, { readonly: true });
    try {
      const row = db
        .query<{ count: number }, [string]>('SELECT COUNT(*) AS count FROM jobs WHERE id = ?')
        .get(id);
      expect(row?.count).toBe(0);
    } finally {
      db.close();
    }

    await real.crashRestart();
    expect((await real.send({ cmd: 'GetState', id })).state).toBe('unknown');
    expect((await real.send({ cmd: 'Count', queue: real.queue })).count).toBe(0);
  } finally {
    await real.dispose();
  }
}, 30000);

test('TTL expiration cancels an unflushed buffered insert', async () => {
  const queue = 'ttl-buffered-regression';
  const id = jobId('buffered-expired-job');
  const dbPath = join(tmpdir(), `bunqueue-ttl-buffered-${process.pid}-${Date.now()}.db`);
  const storage = new SqliteStorage({
    path: dbPath,
    writeBufferFlushMs: 60_000,
    writeBufferSize: 1_000,
  });
  const shards = Array.from({ length: SHARD_COUNT }, () => new Shard());
  const shardLocks = Array.from({ length: SHARD_COUNT }, () => new RWLock());
  const processingShards = Array.from(
    { length: SHARD_COUNT },
    () => new Map<ReturnType<typeof jobId>, ReturnType<typeof createJob>>()
  );
  const processingLocks = Array.from({ length: SHARD_COUNT }, () => new RWLock());
  const idx = shardIndex(queue);
  const job = createJob(id, queue, { data: { buffered: true }, ttl: 1 }, Date.now() - 10_000);
  const jobIndex = new Map([[id, { type: 'queue' as const, shardIdx: idx, queueName: queue }]]);

  try {
    shards[idx].getQueue(queue).push(job);
    shards[idx].incrementQueued(job.id, false, job.createdAt, queue, job.runAt);
    storage.insertJob(job);

    expect(
      await pullJob(queue, 0, {
        storage,
        shards,
        shardLocks,
        processingShards,
        processingLocks,
        jobIndex,
        totalPulled: { value: 0n },
        broadcast: () => {
          // Expired jobs are not broadcast as pulled.
        },
      })
    ).toBeNull();
    expect(storage.flushWriteBuffer(), 'expired INSERT must no longer be pending').toBe(0);
    expect(jobIndex.has(id)).toBe(false);
    expect(shards[idx].getStats().queuedJobs).toBe(0);

    const db = new Database(dbPath, { readonly: true });
    try {
      expect(
        db
          .query<{ count: number }, [string]>('SELECT COUNT(*) AS count FROM jobs WHERE id = ?')
          .get(id)?.count
      ).toBe(0);
    } finally {
      db.close();
    }
  } finally {
    storage.close();
    rmSync(dbPath, { force: true });
    rmSync(`${dbPath}-wal`, { force: true });
    rmSync(`${dbPath}-shm`, { force: true });
  }
});
