/**
 * TCP client↔server contract audit (issue #95 class on the TCP side).
 *
 * Each bug below was a silent param/field mismatch between the client SDK and
 * the server command handler — the call "succeeded" but did the wrong thing.
 *
 *  - RetryDlq      client sent `id`, handler reads `jobId`      → retried ALL
 *  - Clean         client sent `type`, handler reads `state`    → filter ignored
 *  - PromoteJobs   handler returns `count`, client read `promoted` → always 0
 *  - ExtendLocks   client sent `duration`, handler reads `durations[]` → old TTL
 *  - lockDuration  never sent as `lockTtl` on PULL              → server default
 *  - FAIL          `unrecoverable` dropped server-side          → retried over TCP
 */

import { describe, it, expect, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { retryDlq } from '../src/client/queue/dlq';
import { cleanAsync, promoteJobs } from '../src/client/queue/operations/management';
import { pullTcp } from '../src/client/worker/workerPull';
import { Worker } from '../src/client/worker';

// A fake TcpConnectionPool that records every command sent and returns a canned response.
function spyTcp(response: Record<string, unknown> = { ok: true }) {
  const sent: Array<Record<string, unknown>> = [];
  const tcp = {
    send: async (cmd: Record<string, unknown>) => {
      sent.push(cmd);
      return response;
    },
  };
  return { tcp: tcp as never, sent };
}

let qm: QueueManager | null = null;
afterEach(async () => {
  qm?.shutdown();
  qm = null;
  await Bun.sleep(20);
});

describe('TCP contract audit (#95 class)', () => {
  it('retryDlq sends jobId (not id)', () => {
    const { tcp, sent } = spyTcp();
    retryDlq({ name: 'q', embedded: false, tcp }, 'JOB-1');
    expect(sent).toHaveLength(1);
    expect(sent[0].cmd).toBe('RetryDlq');
    expect(sent[0].jobId).toBe('JOB-1');
    expect(sent[0].id).toBeUndefined();
  });

  it('cleanAsync sends state (not type)', async () => {
    const { tcp, sent } = spyTcp({ ok: true, ids: [] });
    await cleanAsync({ name: 'q', embedded: false, tcp }, 0, 100, 'failed');
    expect(sent[0].cmd).toBe('Clean');
    expect(sent[0].state).toBe('failed');
    expect(sent[0].type).toBeUndefined();
  });

  it('promoteJobs reads count from the response', async () => {
    const { tcp } = spyTcp({ ok: true, count: 7 });
    const n = await promoteJobs({ name: 'q', embedded: false, tcp }, { count: 10 });
    expect(n).toBe(7);
  });

  it('pullTcp sends lockTtl from lockDuration on a lock-based pull', async () => {
    const { tcp, sent } = spyTcp({ ok: true, job: null });
    await pullTcp(
      { name: 'q', workerId: 'wk', useLocks: true, pollTimeout: 0, lockDuration: 60000 },
      tcp,
      1,
      false
    );
    expect(sent[0].owner).toBe('wk');
    expect(sent[0].lockTtl).toBe(60000);
  });

  it('Worker.extendJobLocks sends a durations[] array and reads count', async () => {
    const { tcp, sent } = spyTcp({ ok: true, count: 2 });
    const worker = new Worker('q', async () => undefined, { embedded: false, autorun: false });
    // Inject the spy connection (the real pool is never connected).
    (worker as unknown as { tcp: unknown }).tcp = tcp;
    try {
      const n = await worker.extendJobLocks(['j1', 'j2'], ['t1', 't2'], 60000);
      expect(sent[0].cmd).toBe('ExtendLocks');
      expect(sent[0].durations).toEqual([60000, 60000]); // per-id array, not singular `duration`
      expect(sent[0].duration).toBeUndefined();
      expect(n).toBe(2); // read from `count`, not `extended`
    } finally {
      await worker.close(true);
    }
  });

  it('getJobCounts waiting-children matches getJobState/getJobs (counts waitingDeps too)', async () => {
    qm = new QueueManager();
    // A dependency job (incomplete) + a job that depends on it → the dependent
    // sits in waitingDeps and getJobState reports it as 'waiting-children'.
    const dep = await qm.push('q', { data: { role: 'dep' } });
    const blocked = await qm.push('q', { data: { role: 'blocked' }, dependsOn: [dep.id] });
    await Bun.sleep(50);

    const state = await qm.getJobState(blocked.id);
    expect(state).toBe('waiting-children');

    const counts = qm.getQueueJobCounts('q') as Record<string, number>;
    const listed = qm.getJobs('q', { state: 'waiting-children' });
    // count must agree with state + list (the #95-class consistency requirement)
    expect(counts['waiting-children']).toBeGreaterThanOrEqual(1);
    expect(counts['waiting-children']).toBe(listed.length);
  });

  it('FAIL with unrecoverable=true skips retries (terminal), unlike a normal fail', async () => {
    qm = new QueueManager();

    // Normal fail on a retryable job → goes back for retry (delayed/waiting).
    await qm.push('q', { data: { i: 1 }, maxAttempts: 3 });
    const j1 = await qm.pull('q');
    await qm.fail(j1!.id, 'boom');
    const s1 = await qm.getJobState(j1!.id);
    expect(['delayed', 'waiting']).toContain(s1);

    // Unrecoverable fail on an equally-retryable job → terminal (NOT retried).
    await qm.push('q', { data: { i: 2 }, maxAttempts: 3 });
    const j2 = await qm.pull('q');
    await qm.fail(j2!.id, 'fatal', undefined, true);
    const s2 = await qm.getJobState(j2!.id);
    expect(['delayed', 'waiting']).not.toContain(s2);
    expect(s2).toBe('failed');
    // And it landed in the DLQ (terminal), not silently lost.
    const dlq = qm.getDlqEntries('q') as Array<{ job?: { id?: string }; id?: string }>;
    const inDlq = dlq.some((e) => String(e.job?.id ?? e.id) === String(j2!.id));
    expect(inDlq).toBe(true);
  });
});
