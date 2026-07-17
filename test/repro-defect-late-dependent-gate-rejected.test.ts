/**
 * REPRO — slug: late-dependent-gate-rejected
 *
 * DEFECT: PUSH of a late dependent on an EVICTED removeOnComplete parent is
 * rejected at the server gate.
 *
 * The TCP PUSH handler validates dependsOn refs by consulting ONLY jobIndex and
 * completedJobs (handlers/core.ts ~44-52):
 *
 *   const exists =
 *     ctx.queueManager.getJobIndex().has(depJobId) ||
 *     ctx.queueManager.getCompletedJobs().has(depJobId);
 *   if (!exists) return resp.error(`Dependency job not found: ${depId}`);
 *
 * A parent completed with removeOnComplete is recorded ONLY in depCompletions
 * (NOT in completedJobs — its row is deleted, and it leaves jobIndex). So once
 * the parent is gone from jobIndex, a LATE dependent PUSH is rejected with
 * "Dependency job not found", even though the readiness path (push.ts
 * insertJobToShard) and the dependencyProcessor DO consult depCompletions. The
 * gate is the only place that still ignores depCompletions — this is the
 * gate-level half of the same dependency fix.
 *
 * This is a REAL in-process server (QueueManager + createTcpServer), driven over
 * raw TCP (TcpClient.send) so the gate path is exercised exactly as production.
 *
 * Reproduction (deterministic):
 *   1. PUSH parent P with removeOnComplete:true AND durable:true — durable means
 *      the removeOnComplete eviction is synchronous on ack (no write-buffer
 *      flush race), so P is gone from jobIndex + completedJobs and recorded only
 *      in depCompletions by the time we push the child.
 *   2. PULL + ACK P (it completes; removeOnComplete => not added to completedJobs,
 *      row deleted, but its bare id IS retained in depCompletions).
 *   3. PUSH child C with dependsOn:[P.id] over TCP.
 *   4. ASSERT the push SUCCEEDS (ok:true, returns an id). RED today: it returns
 *      ok:false, error "Dependency job not found".
 *
 * CONTROL: a child depending on a still-completed (removeOnComplete:false)
 * parent already succeeds — proving the rejection is specific to the
 * depCompletions-only code path, not dependency validation in general. A genuinely
 * missing dep is still rejected (the gate must keep rejecting real typos).
 */

import { describe, it, expect, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';
import { TcpClient } from '../src/client/tcp/client';

interface Harness {
  qm: QueueManager;
  server: TcpServer;
  port: number;
  clients: TcpClient[];
}

let harness: Harness | null = null;

function startServer(): Harness {
  const qm = new QueueManager(); // in-memory, no dataPath
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
    autoReconnect: false,
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
  await Bun.sleep(50);
});

describe('REPRO late-dependent-gate-rejected', () => {
  it('PUSH of a late dependent on an evicted removeOnComplete parent must SUCCEED at the gate (RED today: rejected with "Dependency job not found")', async () => {
    const h = startServer();
    const QUEUE = 'gate-late-dep';
    const client = await openClient(h);

    // 1. Parent: removeOnComplete + durable => evicted synchronously on ack.
    const pushParent = await client.send({
      cmd: 'PUSH',
      queue: QUEUE,
      data: { role: 'parent' },
      removeOnComplete: true,
      durable: true,
    });
    expect(pushParent.ok).toBe(true);
    const parentId = String(pushParent.id);
    expect(parentId.length).toBeGreaterThan(0);

    // 2. Pull + ack the parent -> completes & is evicted (gone from jobIndex +
    //    completedJobs, retained only in depCompletions).
    const pulled = await client.send({ cmd: 'PULL', queue: QUEUE });
    expect((pulled.job as { id?: string } | null)?.id).toBe(parentId);
    const ack = await client.send({ cmd: 'ACK', id: parentId, result: { ok: true } });
    expect(ack.ok).toBe(true);

    // Sanity: the parent is truly gone from the standard indexes (so the gate's
    // jobIndex/completedJobs checks both miss) — the ONLY record left is in
    // depCompletions.
    expect(h.qm.getJobIndex().has(parentId as never)).toBe(false);
    expect(h.qm.getCompletedJobs().has(parentId as never)).toBe(false);

    // 3. PUSH a late child depending on the evicted parent, over TCP (gate path).
    const pushChild = await client.send({
      cmd: 'PUSH',
      queue: QUEUE,
      data: { role: 'child' },
      dependsOn: [parentId],
    });

    // THE LOAD-BEARING ASSERTION — CORRECT post-fix behavior: the push must
    // succeed because the parent's completion is recorded in depCompletions, which
    // the readiness path already honors. RED today: ok:false, error
    // "Dependency job not found".
    expect({ ok: pushChild.ok, error: pushChild.error ?? null }).toEqual({
      ok: true,
      error: null,
    });
    expect(String(pushChild.id ?? '').length).toBeGreaterThan(0);
  }, 15000);

  it('CONTROL: late dependent on a removeOnComplete:false (still-completed) parent already succeeds', async () => {
    const h = startServer();
    const QUEUE = 'gate-late-dep-ctrl';
    const client = await openClient(h);

    const pushParent = await client.send({
      cmd: 'PUSH',
      queue: QUEUE,
      data: { role: 'parent' },
      removeOnComplete: false,
      durable: true,
    });
    expect(pushParent.ok).toBe(true);
    const parentId = String(pushParent.id);

    const pulled = await client.send({ cmd: 'PULL', queue: QUEUE });
    expect((pulled.job as { id?: string } | null)?.id).toBe(parentId);
    await client.send({ cmd: 'ACK', id: parentId, result: { ok: true } });

    // removeOnComplete:false => parent IS in completedJobs, so the gate already
    // admits the dependent.
    const pushChild = await client.send({
      cmd: 'PUSH',
      queue: QUEUE,
      data: { role: 'child' },
      dependsOn: [parentId],
    });
    expect(pushChild.ok).toBe(true);
    expect(String(pushChild.id ?? '').length).toBeGreaterThan(0);
  }, 15000);

  it('CONTROL: the gate STILL rejects a genuinely-missing dependency', async () => {
    const h = startServer();
    const QUEUE = 'gate-missing-dep';
    const client = await openClient(h);

    const pushChild = await client.send({
      cmd: 'PUSH',
      queue: QUEUE,
      data: { role: 'child' },
      dependsOn: ['definitely-not-a-real-job-id'],
    });

    // A real typo / non-existent ref must still be rejected after the fix.
    expect(pushChild.ok).toBe(false);
    expect(String(pushChild.error ?? '')).toContain('Dependency job not found');
  }, 15000);
});
