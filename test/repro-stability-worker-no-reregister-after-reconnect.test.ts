/**
 * REPRO — "Worker never re-registers after a TCP reconnect"
 * slug: worker-no-reregister-after-reconnect
 *
 * CLAIM (HIGH): A TCP Worker registers with the server exactly once, in run(),
 * guarded by `!this.registered` and then sets `registered = true` PERMANENTLY
 * (worker.ts:219-221 + registerWithServer worker.ts:822-843). The Worker
 * subscribes to NONE of the pool/client connected|disconnected|reconnecting
 * events. On any TCP drop the server close handler calls
 * unregisterWorkersByClientId (tcp.ts:291); the auto-reconnect opens a fresh
 * socket with a fresh uuid clientId. WorkerManager.heartbeat() returns false
 * for a missing worker and does NOT recreate it (workerManager.ts:99-100); PULL
 * handlers only registerClientJob, never workerManager.register (core.ts).
 *
 * NET CLAIM: after a transient TCP drop + reconnect the worker is GONE from the
 * server's WorkerManager PERMANENTLY (getForQueue()/list = 0) even though it
 * keeps pulling and processing jobs — so skipIfNoWorker crons wrongly skip.
 *
 * THE CRUX (what the user doubts): does the worker REALLY stay de-registered
 * forever while still working, or does some path (next heartbeat / next pull /
 * periodic re-register) restore it? This test checks AFTER reconnect AND after
 * several heartbeat cycles, and proves the worker is still alive by having it
 * consume a fresh job post-reconnect.
 *
 * SETUP: REAL in-process server (QueueManager + createTcpServer). The bug is
 * server-side bookkeeping, so a mock will not do. The worker uses embedded:false
 * explicitly to bypass the BUNQUEUE_EMBEDDED=1 test preload.
 */

import { describe, it, expect, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';
import { Worker } from '../src/client/worker';

function randomPort(): number {
  return 30000 + Math.floor(Math.random() * 25000);
}

/** Poll a predicate until true or the deadline elapses. Returns whether it became true. */
async function waitUntil(pred: () => boolean, timeoutMs: number, stepMs = 20): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (pred()) return true;
    await Bun.sleep(stepMs);
  }
  return pred();
}

describe('repro: worker-no-reregister-after-reconnect', () => {
  let qm: QueueManager | null = null;
  let server: TcpServer | null = null;
  let worker: Worker | null = null;

  afterEach(async () => {
    if (worker) {
      try {
        await worker.close(true);
      } catch {
        /* ignore */
      }
      worker = null;
    }
    if (server) {
      server.stop();
      server = null;
    }
    if (qm) {
      qm.shutdown();
      qm = null;
    }
    // Let any pending reconnect timers settle so they don't bleed into next test.
    await Bun.sleep(50);
  });

  it('stays registered (getForQueue >= 1) after a transient TCP drop + reconnect', async () => {
    const port = randomPort();
    const queue = 'reconnect-queue';
    const HEARTBEAT_MS = 200; // small so we can observe several cycles fast

    qm = new QueueManager();
    server = createTcpServer(qm, { hostname: '127.0.0.1', port, idleTimeoutMs: 0 });

    // Track processed jobs so we can prove the worker is alive after reconnect.
    const processed: string[] = [];

    worker = new Worker(
      queue,
      async (job) => {
        processed.push(String(job.id));
        return { ok: true };
      },
      {
        embedded: false, // bypass BUNQUEUE_EMBEDDED=1 test preload
        connection: { host: '127.0.0.1', port, pingInterval: 0 },
        concurrency: 1,
        heartbeatInterval: HEARTBEAT_MS,
      }
    );

    const wm = qm.workerManager;
    // The worker registers under queueKey === name (no prefixKey).
    const registeredCount = () => wm.getForQueue(queue).length;

    // 1. Wait for the initial RegisterWorker round-trip to land server-side.
    const becameRegistered = await waitUntil(() => registeredCount() === 1, 5000);
    expect(becameRegistered).toBe(true);
    expect(registeredCount()).toBe(1);

    // Sanity: at this point the server has exactly one TCP connection (poolSize=1
    // for concurrency=1). Capture the clientId so we can confirm the reconnect
    // produced a DIFFERENT one.
    const clientIdsBefore = [...(server.connections.keys() as IterableIterator<string>)];
    expect(clientIdsBefore.length).toBeGreaterThanOrEqual(1);

    // 2. Force the server-side socket(s) to drop. terminate() fires the server's
    //    close handler -> unregisterWorkersByClientId(clientId). The client's
    //    TcpClient.handleClose() then auto-reconnects with a fresh uuid clientId.
    for (const sock of server.connections.values()) {
      sock.terminate();
    }

    // 3. The worker should be unregistered server-side right after the drop
    //    (this is the trigger, not the bug). Confirm it actually happened so the
    //    test is exercising the real code path, not a no-op.
    const becameUnregistered = await waitUntil(() => registeredCount() === 0, 3000);
    expect(becameUnregistered).toBe(true);

    // 4. Wait for the client pool to re-establish a NEW connection (new clientId).
    const reconnected = await waitUntil(() => {
      const ids = [...(server!.connections.keys() as IterableIterator<string>)];
      // A new connection whose clientId is not among the originals.
      return ids.some((id) => !clientIdsBefore.includes(id));
    }, 5000);
    expect(reconnected).toBe(true);

    // 5. Prove the worker is ALIVE after the reconnect: push a fresh job and
    //    confirm it gets pulled + processed over the new connection.
    await qm.push(queue, { data: { n: 1 } });
    const consumedAfterReconnect = await waitUntil(() => processed.length >= 1, 5000);
    expect(consumedAfterReconnect).toBe(true);

    // 6. Now wait through SEVERAL heartbeat intervals. If any path (heartbeat,
    //    pull, periodic re-register) re-registered the worker, getForQueue would
    //    return to >= 1 within this window. The CRUX assertion: it must.
    await Bun.sleep(HEARTBEAT_MS * 4 + 200); // >= 4 heartbeat cycles

    const stillRegistered = await waitUntil(() => registeredCount() >= 1, 1500);

    // HEADLINE: after a transient drop+reconnect, while the worker is provably
    // still pulling and processing jobs, it should remain visible in the
    // server's WorkerManager. RED if it stays 0 forever.
    expect(registeredCount()).toBeGreaterThanOrEqual(1);
    expect(stillRegistered).toBe(true);
  }, 30000);
});
