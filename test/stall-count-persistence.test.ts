import { afterEach, describe, expect, test } from 'bun:test';
import { Database } from 'bun:sqlite';
import { randomUUID } from 'crypto';
import { existsSync, unlinkSync } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { jobId } from '../src/domain/types/job';
import { MIGRATION_TABLE, SCHEMA } from '../src/infrastructure/persistence/schema';
import { SqliteStorage } from '../src/infrastructure/persistence/sqlite';
import { pack } from '../src/infrastructure/persistence/sqliteSerializer';

const paths: string[] = [];

function dbPath(): string {
  const path = join(tmpdir(), `bunqueue-stall-count-${randomUUID()}.db`);
  paths.push(path);
  return path;
}

afterEach(() => {
  for (const path of paths.splice(0)) {
    for (const suffix of ['', '-wal', '-shm']) {
      if (existsSync(path + suffix)) unlinkSync(path + suffix);
    }
  }
});

describe('stall count persistence', () => {
  test('updateForRetry round-trips the cumulative stall count', () => {
    const storage = new SqliteStorage({ path: dbPath() });
    const id = jobId('persisted-stall-count');
    storage.insertJobImmediate({
      id,
      queue: 'recovery',
      data: { value: 1 },
      priority: 0,
      createdAt: Date.now(),
      runAt: Date.now(),
      startedAt: null,
      completedAt: null,
      attempts: 0,
      maxAttempts: 5,
      backoff: 1,
      backoffConfig: null,
      ttl: null,
      timeout: null,
      uniqueKey: null,
      customId: null,
      dependsOn: [],
      parentId: null,
      childrenIds: [],
      childrenCompleted: 0,
      tags: [],
      lifo: false,
      groupId: null,
      progress: 0,
      progressMessage: null,
      stacktrace: null,
      removeOnComplete: false,
      removeOnFail: false,
      repeat: null,
      lastHeartbeat: Date.now(),
      stallTimeout: null,
      stallCount: 0,
      stackTraceLimit: 10,
      keepLogs: null,
      sizeLimit: null,
      failParentOnFailure: false,
      removeDependencyOnFailure: false,
      continueParentOnFailure: false,
      ignoreDependencyOnFailure: false,
      deduplicationTtl: null,
      deduplicationExtend: false,
      deduplicationReplace: false,
      debounceId: null,
      debounceTtl: null,
      timeline: [],
    });

    const job = storage.getJob(id)!;
    job.attempts = 2;
    job.stallCount = 2;
    storage.updateForRetry(job);

    expect(storage.getJob(id)?.attempts).toBe(2);
    expect(storage.getJob(id)?.stallCount).toBe(2);
    storage.close();
  });

  test('migration 17 adds stall_count with a safe zero default', () => {
    const path = dbPath();
    const raw = new Database(path, { create: true });
    raw.run(SCHEMA.replace('    stall_count INTEGER NOT NULL DEFAULT 0,\\n', ''));
    raw.run(MIGRATION_TABLE);
    raw.run('INSERT INTO migrations (version, applied_at) VALUES (16, ?)', Date.now());
    raw
      .prepare('INSERT INTO jobs (id, queue, data, created_at, run_at) VALUES (?, ?, ?, ?, ?)')
      .run('legacy-active', 'recovery', pack({ legacy: true }), Date.now(), Date.now());
    raw.close();

    const storage = new SqliteStorage({ path });
    expect(storage.getJob(jobId('legacy-active'))?.stallCount).toBe(0);
    storage.close();
  });
});
