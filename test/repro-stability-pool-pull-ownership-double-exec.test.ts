/**
 * REPRO (skeptic) — slug: pool-pull-ownership-double-exec
 *
 * CLAIM (HIGH): "Pool splits PULL-ownership from processing lifetime → DOUBLE
 * EXECUTION".
 *
 * The chain under test (all server-side):
 *   1. PULL binds the job to the *pulling connection's* server clientId
 *      (handlers/core.ts:137/145 registerClientJob(ctx.clientId, job.id)). Each
 *      TCP socket is assigned a distinct server clientId on `open`
 *      (server/tcp.ts:174 uuid()).
 *   2. JobHeartbeat renews freshness by TOKEN (or jobId), NOT by clientId
 *      (queueManager.jobHeartbeat → renewJobLock). So a heartbeat issued over a
 *      DIFFERENT socket keeps the job lock-fresh and lastHeartbeat current.
 *   3. When the *pulling* socket closes, the server runs
 *      releaseClientJobs(clientId_A) which re-queues EVERY processing job of
 *      that clientId with NO heartbeat-freshness / grace-period check
 *      (clientTracking.ts:47-132 → releaseJobToQueue: unconditional re-push).
 *   4. A second consumer can re-PULL the same job → it runs again concurrently
 *      with the original (still-alive) owner → double execution.
 *
 * THE CRUX (what the user doubts): is this re-dispatch of a job whose worker is
 * STILL ALIVE AND HEARTBEATING — or just the normal/correct "release a dead
 * client's jobs"? To distinguish, this test:
 *   - proves the heartbeat is FRESH at the moment socket A closes (a JobHeartbeat
 *     issued <1ms before close returns ok:true), i.e. a fully-alive worker, and
 *   - proves the stall detector would NOT reclaim it (far inside stallTimeout),
 *   - yet shows the SAME job id becomes pullable again from a second connection.
 *
 * This is a REAL server (QueueManager + createTcpServer), not a mock. We drive
 * raw TCP via two explicit TcpClient connections so "PULL on A, heartbeat on B,
 * close A" is fully deterministic (no pool round-robin nondeterminism). The pool
 * is exactly this pattern at scale (poolSize>1 → pull/heartbeat land on
 * different sockets), so a single-job two-connection repro is the minimal,
 * deterministic core of the claim.
 */

import { describe, it, expect, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';
import { TcpClient } from '../src/client/tcp/client';

/** Poll `fn` until truthy or deadline; returns last value (false on timeout). */
async function waitFor<T>(
  fn: () => T | Promise<T>,
  timeoutMs: number,
  intervalMs = 25
): Promise<T | false> {
  const start = Date.now();
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() - start >= timeoutMs) return false;
    await Bun.sleep(intervalMs);
  }
}

interface Harness {
  qm: QueueManager;
  server: TcpServer;
  port: number;
  clients: TcpClient[];
}

let harness: Harness | null = null;

async function startServer(): Promise<Harness> {
  const qm = new QueueManager(); // in-memory, no dataPath
  // Let the kernel allocate and bind the port atomically. Choosing a random
  // number first leaves a probe-to-bind race with parallel sandbox tests.
  const server = createTcpServer(qm, { hostname: '127.0.0.1', port: 0 });
  const port = server.server.port;
  const h: Harness = { qm, server, port, clients: [] };
  harness = h;
  return h;
}

async function openClient(h: Harness): Promise<TcpClient> {
  const client = new TcpClient({
    host: '127.0.0.1',
    port: h.port,
    autoReconnect: false, // we want explicit, single-socket control
    pingInterval: 0,
    commandTimeout: 2000,
    connectTimeout: 2000,
  });
  await client.connect();
  h.clients.push(client);
  return client;
}

afterEach(async () => {
  if (harness) {
    for (const c of harness.clients) {
      try {
        c.close();
      } catch {
        /* ignore */
      }
    }
    try {
      harness.server.stop();
    } catch {
      /* ignore */
    }
    try {
      harness.qm.shutdown();
    } catch {
      /* ignore */
    }
    harness = null;
  }
  // Let force-closed sockets fire their server-side `close` handlers
  // (unregisterWorkersByClientId + releaseClientJobs) before the next test.
  await Bun.sleep(100);
});

describe('REPRO pool-pull-ownership-double-exec', () => {
  it('must NOT re-dispatch a job whose owner is alive & freshly heartbeating when its pulling socket closes (RED today: it does → double-execution)', async () => {
    const h = await startServer();
    const QUEUE = 'q-double-exec';

    // Control connection: pushes the job and is used to re-pull from a SECOND
    // server clientId after socket A closes.
    const ctrl = await openClient(h);

    // One job.
    const pushRes = await ctrl.send({ cmd: 'PUSH', queue: QUEUE, data: { n: 1 } });
    expect(pushRes.ok).toBe(true);
    const targetId = String(pushRes.id);
    expect(targetId.length).toBeGreaterThan(0);

    // Connection A: the "worker" socket that PULLs (lock-based, with owner so a
    // token is issued — this is how Worker pulls with useLocks:true).
    const connA = await openClient(h);
    const pullRes = await connA.send({
      cmd: 'PULL',
      queue: QUEUE,
      owner: 'worker-A',
      lockTtl: 30000,
    });
    expect(pullRes.ok).toBe(true);
    const pulled = pullRes.job as { id?: string } | null;
    expect(pulled).not.toBeNull();
    expect(String(pulled!.id)).toBe(targetId);
    const token = String(pullRes.token);
    expect(token.length).toBeGreaterThan(0);

    // Sanity: job is now active (processing), not pullable from anyone else.
    const stateAfterPull = await ctrl.send({ cmd: 'GetState', id: targetId });
    expect(stateAfterPull.state).toBe('active');

    // The "second consumer" pulling NOW must get nothing — the job is owned/active.
    const earlySteal = await ctrl.send({ cmd: 'PULL', queue: QUEUE });
    expect(earlySteal.job).toBeNull();

    // --- The crux: the worker is FULLY ALIVE and heartbeating. ---
    // Issue a FRESH JobHeartbeat over a DIFFERENT socket (connB, a distinct
    // server clientId) using the token. It renews by token, not clientId, so it
    // succeeds — proving lastHeartbeat is current at this instant.
    const connB = await openClient(h);
    const hb = await connB.send({ cmd: 'JobHeartbeat', id: targetId, token });
    expect(hb.ok).toBe(true); // heartbeat accepted → job is fresh & alive

    // The job was pulled milliseconds ago with lockTtl 30s and just got a fresh
    // heartbeat. The stall detector (stallTimeout default ~30s, two-tick
    // confirmation) cannot legitimately reclaim it now. So the ONLY way it
    // becomes pullable again in the next few hundred ms is the disconnect path.

    // Now close ONLY socket A (the pulling socket). The worker process behind
    // connB is still alive — in the pool model this is one pooled socket dying
    // while the worker keeps running on the others.
    connA.close();

    // Assert: the SAME job id becomes pullable again from the second connection,
    // DESPITE the fresh heartbeat. If this returns the job, a second consumer
    // would run it concurrently with worker-A → double execution.
    const rePulled = await waitFor(
      async () => {
        const r = await connB.send({
          cmd: 'PULL',
          queue: QUEUE,
          owner: 'worker-B',
          lockTtl: 30000,
        });
        const j = r.job as { id?: string } | null;
        return j && String(j.id) === targetId ? r : null;
      },
      4000,
      30
    );

    // Snapshot final state for the diff regardless of outcome.
    const finalState = await ctrl.send({ cmd: 'GetState', id: targetId });

    // THE LOAD-BEARING ASSERTION — asserts CORRECT (post-fix) behavior, so it is
    // RED today. A job whose owner is alive + freshly heartbeating must NOT be
    // re-dispatched to a second consumer just because its pulling socket closed:
    // releaseClientJobs should skip/defer jobs whose lastHeartbeat is within
    // stallTimeout. Today it re-queues unconditionally → connB re-pulls the SAME
    // id (rePulled is the PULL response, not false) → this FAILS, proving the
    // double-execution window. After a fix, connB gets nothing → rePulled === false
    // and the job stays owned/active under worker-A.
    expect({
      sameJobReDispatchedToSecondConsumer: rePulled !== false,
      finalState: finalState.state,
    }).toEqual({
      sameJobReDispatchedToSecondConsumer: false,
      finalState: 'active',
    });
  }, 15000);

  it('CONTROL: a healthy ACK by the original owner still succeeds — proving the original execution was real, so re-dispatch = a SECOND run', async () => {
    // This guards against the "it was already gone / never really owned" reading.
    // We PULL on A, heartbeat fresh, and show A can STILL ack the job by token
    // (the work was genuinely in-flight). Combined with test #1's re-dispatch,
    // that means two independent executions of the same job are possible.
    const h = await startServer();
    const QUEUE = 'q-double-exec-ctrl';

    const ctrl = await openClient(h);
    const pushRes = await ctrl.send({ cmd: 'PUSH', queue: QUEUE, data: { n: 2 } });
    const targetId = String(pushRes.id);

    const connA = await openClient(h);
    const pullRes = await connA.send({
      cmd: 'PULL',
      queue: QUEUE,
      owner: 'worker-A',
      lockTtl: 30000,
    });
    expect(String((pullRes.job as { id: string }).id)).toBe(targetId);
    const token = String(pullRes.token);

    // Fresh heartbeat over a different socket — still alive.
    const connB = await openClient(h);
    const hb = await connB.send({ cmd: 'JobHeartbeat', id: targetId, token });
    expect(hb.ok).toBe(true);

    // Original owner completes the work normally (no disconnect). This is the
    // "real" single execution; it must succeed.
    const ack = await connA.send({ cmd: 'ACK', id: targetId, token });
    expect(ack.ok).toBe(true);

    const state = await ctrl.send({ cmd: 'GetState', id: targetId });
    expect(state.state).toBe('completed');
  }, 15000);
});
