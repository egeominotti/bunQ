/**
 * Issue #110 — lock-expiry DLQ move is not persisted to SQLite.
 *
 * The #97 fix (2.8.17) added `ctx.storage?.saveDlqEntry(entry)` and
 * `ctx.storage?.deleteJob(jobId)` to handleMaxStallsExceeded, but the ONLY
 * production caller of checkExpiredLocks — the periodic sweep in
 * backgroundTasks.ts — builds its LockContext with a file-local
 * getLockContext(ctx) that omits `storage`. Both persistence calls silently
 * no-op through optional chaining, so SQLite keeps an orphan `active` jobs
 * row and the DLQ entry exists only in memory (lost on restart).
 *
 * This test goes through the REAL path (background interval, not a
 * hand-built context) and asserts DB residency, which the in-memory
 * assertions of lockExpiration.test.ts never checked.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { Database } from 'bun:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { Queue, shutdownManager } from '../src/client';
import { FailureReason } from '../src/domain/types/dlq';
import { shardIndex } from '../src/shared/hash';

const dirs: string[] = [];

afterEach(() => {
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

async function waitUntil(cond: () => boolean | Promise<boolean>, timeoutMs = 10_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await cond()) return true;
    await new Promise((r) => setTimeout(r, 50));
  }
  return false;
}

describe('issue #110: lock-expiry DLQ move persists to SQLite', () => {
  test('background sweep moves the job to the dlq TABLE and deletes the jobs row', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'bunq-110-'));
    dirs.push(dir);
    const dataPath = join(dir, 'bunq.db');
    const queue = 'trace110';

    // Fast sweep so the test does not wait the default 5s interval.
    const qm = new QueueManager({ dataPath, stallCheckMs: 250 });
    try {
      // First lock expiry sends straight to the DLQ.
      qm.getShards()[shardIndex(queue)].setStallConfig(queue, { maxStalls: 1 });

      await qm.push(queue, { data: { msg: 'will-orphan' }, maxAttempts: 5 });
      const { job, token } = await qm.pullWithLock(queue, 'frozen-worker', 0, 50); // 50ms TTL
      expect(job).not.toBeNull();
      expect(token).not.toBeNull();

      // Wait for the lock to expire AND the background sweep to process it
      // (in-memory state flips to failed = the sweep ran handleMaxStallsExceeded).
      const failed = await waitUntil(async () => (await qm.getJobState(job!.id)) === 'failed');
      expect(failed).toBe(true);

      // In-memory DLQ sees it (this always passed — it is what masked #97).
      expect(qm.getDlq(queue).some((j) => j.id === job!.id)).toBe(true);

      // THE BUG: assert actual SQLite residency, not the in-memory view.
      // Poll briefly to absorb write-buffer latency on the fixed path.
      const db = new Database(dataPath, { readonly: true });
      try {
        const persisted = await waitUntil(() => {
          const dlqRow = db.query('SELECT job_id FROM dlq WHERE job_id = ?').get(String(job!.id));
          const jobsRow = db.query('SELECT id FROM jobs WHERE id = ?').get(String(job!.id));
          return dlqRow !== null && jobsRow === null;
        }, 3_000);

        const dlqRow = db.query('SELECT job_id FROM dlq WHERE job_id = ?').get(String(job!.id));
        const jobsRow = db
          .query('SELECT id, state FROM jobs WHERE id = ?')
          .get(String(job!.id)) as { id: string; state: string } | null;

        expect(dlqRow).not.toBeNull(); // DLQ entry must survive a restart
        expect(jobsRow).toBeNull(); // no orphan active row left behind
        expect(persisted).toBe(true);
      } finally {
        db.close();
      }
    } finally {
      qm.shutdown();
    }
  }, 20_000);

  // Same class of bug, found by making LockContext/DlqContext.storage required:
  // the embedded client's getDlqContext (src/client/queue/helpers.ts) omitted
  // storage too, so retryDlqByFilter's deleteDlqEntry/insertJob silently
  // no-op'd — a filtered DLQ retry was never persisted (the dlq row
  // resurrected the job into the DLQ on restart, the requeued job was lost).
  test('embedded retryDlqByFilter persists: dlq row deleted, jobs row re-inserted', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'bunq-110b-'));
    dirs.push(dir);
    const dataPath = join(dir, 'bunq.db');
    const queueName = 'trace110-filter';

    await shutdownManager(); // reset the process-singleton manager → fresh dataPath
    const queue = new Queue(queueName, { embedded: true, dataPath });
    try {
      // Land a job in the DLQ via the ack path (maxAttempts 1 → first fail moves it).
      await queue.add('t', { msg: 'to-dlq' }, { attempts: 1 });

      const { getSharedManager } = await import('../src/client/manager');
      const qm = getSharedManager();
      const pulled = await qm.pull(queueName, 0);
      expect(pulled).not.toBeNull();
      await qm.fail(pulled!.id, 'boom');

      const failed = await waitUntil(async () => (await qm.getJobState(pulled!.id)) === 'failed');
      expect(failed).toBe(true);

      // Filtered retry through the PUBLIC embedded client API (the buggy path).
      const retried = queue.retryDlqByFilter({ reason: FailureReason.MaxAttemptsExceeded });
      expect(retried).toBe(1);

      // Assert SQLite residency: dlq row gone, jobs row present again.
      const db = new Database(dataPath, { readonly: true });
      try {
        const persisted = await waitUntil(() => {
          const dlqRow = db
            .query('SELECT job_id FROM dlq WHERE job_id = ?')
            .get(String(pulled!.id));
          const jobsRow = db.query('SELECT id FROM jobs WHERE id = ?').get(String(pulled!.id));
          return dlqRow === null && jobsRow !== null;
        }, 3_000);
        expect(persisted).toBe(true);
      } finally {
        db.close();
      }
    } finally {
      await queue.close();
      await shutdownManager();
    }
  }, 20_000);
});
