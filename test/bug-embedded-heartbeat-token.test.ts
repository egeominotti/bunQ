/**
 * Bug reproduction: Embedded mode sendHeartbeat doesn't pass tokens
 * GitHub Issue #40: "Invalid or expired lock token"
 *
 * Root cause: createEmbeddedOps().sendHeartbeat ignores the tokens parameter,
 * so jobHeartbeat() never calls renewJobLock() and locks expire after 30s
 * even though heartbeats are firing.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { createEmbeddedOps } from '../src/client/sandboxed/queueOps';
import type { SharedManager } from '../src/client/manager';
import type { JobId } from '../src/domain/types/job';

describe('Bug #40: Embedded sendHeartbeat must pass tokens to renew locks', () => {
  let qm: QueueManager;

  beforeEach(() => {
    qm = new QueueManager();
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('sendHeartbeat with token renews lock TTL (prevents expiration)', async () => {
    // Push and pull a job with a short lock TTL (200ms)
    await qm.push('heartbeat-bug', { data: { msg: 'test' } });
    const { job, token } = await qm.pullWithLock('heartbeat-bug', 'worker-1', 0, 200);
    expect(job).not.toBeNull();
    expect(token).not.toBeNull();

    // Create embedded ops using the QueueManager as SharedManager
    const ops = createEmbeddedOps(qm as unknown as SharedManager);

    // Wait until halfway through the TTL so the renewal meaningfully extends it
    await new Promise((r) => setTimeout(r, 100));

    // Send heartbeat WITH token (this should renew the lock for another 200ms from now)
    await ops.sendHeartbeat([String(job!.id)], [token!]);

    // Wait past the original TTL (200ms from pull) but within renewed TTL (200ms from heartbeat)
    await new Promise((r) => setTimeout(r, 150));

    // The lock should still be valid because heartbeat renewed it
    const lockValid = qm.verifyLock(job!.id, token!);
    expect(lockValid).toBe(true);
  });

  test('ack succeeds after heartbeat-renewed lock (no "invalid or expired" error)', async () => {
    // Push and pull a job with short TTL
    await qm.push('heartbeat-ack-bug', { data: { msg: 'test' } });
    const { job, token } = await qm.pullWithLock('heartbeat-ack-bug', 'worker-1', 0, 200);
    expect(job).not.toBeNull();
    expect(token).not.toBeNull();

    const ops = createEmbeddedOps(qm as unknown as SharedManager);

    // Wait until halfway through the TTL
    await new Promise((r) => setTimeout(r, 100));

    // Heartbeat should renew lock for another 200ms from now
    await ops.sendHeartbeat([String(job!.id)], [token!]);

    // Wait past original TTL but within renewed TTL
    await new Promise((r) => setTimeout(r, 150));

    // Ack should succeed (not throw "Invalid or expired lock token")
    await expect(qm.ack(job!.id, { result: 'done' }, token!)).resolves.toBeUndefined();
  });

  test('batch heartbeat renews all locks', async () => {
    const ids: string[] = [];
    const tokens: string[] = [];

    for (let i = 0; i < 3; i++) {
      await qm.push(`heartbeat-batch-${i}`, { data: { i } });
      const { job, token } = await qm.pullWithLock(`heartbeat-batch-${i}`, 'worker-1', 0, 200);
      ids.push(String(job!.id));
      tokens.push(token!);
    }

    const ops = createEmbeddedOps(qm as unknown as SharedManager);

    // Wait until halfway through the TTL
    await new Promise((r) => setTimeout(r, 100));

    await ops.sendHeartbeat(ids, tokens);

    // Wait past original TTL but within renewed TTL
    await new Promise((r) => setTimeout(r, 150));

    // All locks should still be valid
    for (let i = 0; i < 3; i++) {
      const valid = qm.verifyLock(ids[i] as unknown as JobId, tokens[i]);
      expect(valid).toBe(true);
    }
  });
});
