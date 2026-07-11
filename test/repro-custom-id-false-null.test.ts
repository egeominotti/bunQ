/**
 * REPRO — getJobByCustomId() transiently returns null for an EXISTING job
 * during the pull transition (queue -> processing), embedded mode.
 *
 * Run: bun test test/repro-custom-id-false-null.test.ts
 *
 * Suspected gap (flagged by the skeptic on the 2.8.31 fix): getJobByCustomId
 * (src/application/operations/queryOperations.ts:105) did not receive the
 * stale-snapshot chase that 2.8.31 added to getJob/getJobState, and it reads
 * the shard queue lock-free. If the pull path ever exposes a state where the
 * job has been popped from the shard priority queue while jobIndex still says
 * { type: 'queue' } (the exact pre-2.8.31 window), this reader follows the
 * stale location, misses in the queue, and answers null for a job that exists.
 *
 * Invariant under test: a job pushed with a custom jobId that has NOT
 * completed and has NOT been removed must NEVER be reported as nonexistent
 * by getJobByCustomId.
 *
 * Hammer design (tighter than repro-getjob-false-null-during-pull.test.ts):
 * jobs are drained via manager.pull() and deliberately NEVER acked during the
 * watch, so every pushed custom-id job is either waiting or active for the
 * whole window — any null from getJobByCustomId is unambiguously false
 * (no completion/removal can explain it). Concurrent pullers maximize
 * queue -> processing transitions on a single shard while a poller sweeps
 * every custom id between microtask yields, exactly the interleaving that
 * lands inside an await-boundary window if one exists.
 *
 * RED on code where the pop and the jobIndex flip are separated by an await
 * (pre-2.8.31 pull.ts) -> GREEN once the transition is atomic for observers.
 *
 * Embedded only; DOES NOT touch src/.
 */
import { describe, test, expect } from 'bun:test';
import { Queue } from '../src/client/queue/queue';
import { getSharedManager } from '../src/client/manager';

const ATTEMPTS = 20;
const JOBS_PER_ATTEMPT = 40;
const PULLER_COUNT = 4;
const DRAIN_DEADLINE_MS = 10000;

interface Violation {
  attempt: number;
  cid: string;
  jobId: string;
  stateAfter: string;
  indexAfter: string;
}

describe('repro: getJobByCustomId false null during pull transition', () => {
  test('getJobByCustomId never returns null for a live (un-acked, un-removed) custom-id job', async () => {
    const manager = getSharedManager();
    const violations: Violation[] = [];

    for (let attempt = 0; attempt < ATTEMPTS; attempt++) {
      const queueName = `repro-cid-false-null-${attempt}`;
      const queue = new Queue<{ v: number }>(queueName, { embedded: true });
      queue.obliterate();

      // Push jobs with custom ids. After add() resolves, both customIdMap and
      // jobIndex are populated, so a null is never a "push in flight" artifact.
      const cids: string[] = [];
      const cidToJobId = new Map<string, string>();
      for (let i = 0; i < JOBS_PER_ATTEMPT; i++) {
        const cid = `cid-${attempt}-${i}`;
        const job = await queue.add('probe', { v: i }, { jobId: cid });
        cids.push(cid);
        cidToJobId.set(cid, String(job.id));
      }

      // Sanity: all resolvable before any pull.
      for (const cid of cids) {
        expect(manager.getJobByCustomId(cid)).not.toBeNull();
      }

      // Drain the queue concurrently WITHOUT acking: every job stays live
      // (waiting -> active), so the invariant holds for the entire watch.
      const pulledIds: string[] = [];
      let draining = true;
      const deadline = Date.now() + DRAIN_DEADLINE_MS;

      const puller = async (): Promise<void> => {
        while (pulledIds.length < JOBS_PER_ATTEMPT && Date.now() < deadline) {
          const job = await manager.pull(queueName, 25);
          if (job) pulledIds.push(String(job.id));
        }
      };

      const poller = async (): Promise<void> => {
        let sweeps = 0;
        while (draining) {
          for (const cid of cids) {
            // Synchronous read, same as the TCP GetJobByCustomId handler and
            // the embedded deduplication path.
            const found = manager.getJobByCustomId(cid);
            if (found === null) {
              const jobId = cidToJobId.get(cid) ?? '?';
              // Capture what the system says about the job RIGHT NOW: if it
              // reports waiting/active/prioritized, the null was false.
              const stateAfter = String(
                await manager.getJobState(jobId as Parameters<typeof manager.getJobState>[0])
              );
              violations.push({
                attempt,
                cid,
                jobId,
                stateAfter,
                indexAfter: JSON.stringify(
                  manager.getJobByCustomId(cid) === null ? 'still-null' : 'reappeared'
                ),
              });
            }
          }
          sweeps++;
          // Yield so pull awaits (lock acquisitions, post-pop bookkeeping)
          // interleave with the sweeps; occasionally yield the macrotask
          // queue too so timer-based waits make progress.
          if (sweeps % 64 === 0) {
            await Bun.sleep(0);
          } else {
            await Promise.resolve();
          }
        }
      };

      const pullers = Array.from({ length: PULLER_COUNT }, () => puller());
      const pollerPromise = poller();
      await Promise.all(pullers);
      draining = false;
      await pollerPromise;

      // Everything was pulled and nothing acked: all custom ids must STILL
      // resolve (jobs are active in processingShards).
      expect(pulledIds.length).toBe(JOBS_PER_ATTEMPT);
      for (const cid of cids) {
        const found = manager.getJobByCustomId(cid);
        if (found === null) {
          violations.push({
            attempt,
            cid,
            jobId: cidToJobId.get(cid) ?? '?',
            stateAfter: 'post-drain steady-state null',
            indexAfter: 'steady-state',
          });
        }
      }

      // Cleanup: ack every pulled job so the shared manager does not
      // accumulate active jobs across attempts.
      for (const id of pulledIds) {
        await manager.ack(id as Parameters<typeof manager.ack>[0]);
      }
      queue.obliterate();

      if (violations.length > 0) break; // fail fast with captured evidence
    }

    if (violations.length > 0) {
      const detail = violations
        .slice(0, 10)
        .map(
          (v) =>
            `attempt=${v.attempt} cid=${v.cid} jobId=${v.jobId} ` +
            `stateAfterNull=${v.stateAfter} recheck=${v.indexAfter}`
        )
        .join('\n');
      expect(
        `getJobByCustomId returned null for ${violations.length} live custom-id job(s) ` +
          `(never acked, never removed) — false null during the pull transition:\n${detail}`
      ).toBeNull();
    }
  }, 90000);
});
