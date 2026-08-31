/**
 * Bug reproduction: Embedded mode sendHeartbeat doesn't pass tokens
 * GitHub Issue #40: "Invalid or expired lock token"
 *
 * Root cause: createEmbeddedOps().sendHeartbeat ignores the tokens parameter,
 * so jobHeartbeat() never calls renewJobLock() and locks expire after 30s
 * even though heartbeats are firing.
 */

import { describe, test, expect, beforeEach, afterEach, setSystemTime } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { createEmbeddedOps } from '../src/client/sandboxed/queueOps';
import type { SharedManager } from '../src/client/manager';
import type { JobId } from '../src/domain/types/job';

const CLOCK_EPOCH_MS = Date.UTC(2026, 0, 1);
const LOCK_TTL_MS = 200;
const HEARTBEAT_OFFSET_MS = 100;
const AFTER_ORIGINAL_EXPIRY_MS = LOCK_TTL_MS + 1;

describe('Bug #40: Embedded sendHeartbeat must pass tokens to renew locks', () => {
  let qm: QueueManager;

  beforeEach(() => {
    setSystemTime(new Date(CLOCK_EPOCH_MS));
    qm = new QueueManager();
  });

  afterEach(() => {
    // Restore the process clock before teardown so a failure cannot poison later tests.
    setSystemTime();
    qm.shutdown();
  });

  test('sendHeartbeat with token renews lock TTL (prevents expiration)', async () => {
    // Push and pull a job with a short lock TTL (200ms)
    await qm.push('heartbeat-bug', { data: { msg: 'test' } });
    const { job, token } = await qm.pullWithLock('heartbeat-bug', 'worker-1', 0, LOCK_TTL_MS);
    expect(job).not.toBeNull();
    expect(token).not.toBeNull();

    // Create embedded ops using the QueueManager as SharedManager
    const ops = createEmbeddedOps(qm as unknown as SharedManager);
    const initialExpiry = qm.getLockInfo(job!.id)!.expiresAt;

    // Advance the wall clock halfway through the TTL so renewal extends it deterministically.
    setSystemTime(new Date(CLOCK_EPOCH_MS + HEARTBEAT_OFFSET_MS));

    // Send heartbeat WITH token (this should renew the lock for another 200ms from now)
    await ops.sendHeartbeat([String(job!.id)], [token!]);

    const renewedLock = qm.getLockInfo(job!.id);
    expect(renewedLock).toMatchObject({
      renewalCount: 1,
      lastRenewalAt: CLOCK_EPOCH_MS + HEARTBEAT_OFFSET_MS,
      expiresAt: CLOCK_EPOCH_MS + HEARTBEAT_OFFSET_MS + LOCK_TTL_MS,
    });
    expect(renewedLock!.expiresAt).toBe(initialExpiry + HEARTBEAT_OFFSET_MS);

    // Move past the original expiry while remaining inside the renewed lease.
    setSystemTime(new Date(CLOCK_EPOCH_MS + AFTER_ORIGINAL_EXPIRY_MS));
    const lockValid = qm.verifyLock(job!.id, token!);
    expect(lockValid).toBe(true);
  });

  test('ack succeeds after heartbeat-renewed lock (no "invalid or expired" error)', async () => {
    // Push and pull a job with short TTL
    await qm.push('heartbeat-ack-bug', { data: { msg: 'test' } });
    const { job, token } = await qm.pullWithLock('heartbeat-ack-bug', 'worker-1', 0, LOCK_TTL_MS);
    expect(job).not.toBeNull();
    expect(token).not.toBeNull();

    const ops = createEmbeddedOps(qm as unknown as SharedManager);

    setSystemTime(new Date(CLOCK_EPOCH_MS + HEARTBEAT_OFFSET_MS));

    // Heartbeat should renew lock for another 200ms from now
    await ops.sendHeartbeat([String(job!.id)], [token!]);
    expect(qm.getLockInfo(job!.id)?.renewalCount).toBe(1);

    setSystemTime(new Date(CLOCK_EPOCH_MS + AFTER_ORIGINAL_EXPIRY_MS));

    // Ack should succeed (not throw "Invalid or expired lock token")
    await expect(qm.ack(job!.id, { result: 'done' }, token!)).resolves.toBeUndefined();
  });

  test('batch heartbeat renews all locks', async () => {
    const ids: JobId[] = [];
    const tokens: string[] = [];

    for (let i = 0; i < 3; i++) {
      await qm.push(`heartbeat-batch-${i}`, { data: { i } });
      const { job, token } = await qm.pullWithLock(
        `heartbeat-batch-${i}`,
        'worker-1',
        0,
        LOCK_TTL_MS
      );
      ids.push(job!.id);
      tokens.push(token!);
    }

    const ops = createEmbeddedOps(qm as unknown as SharedManager);
    const initialExpiries = ids.map((id) => qm.getLockInfo(id)!.expiresAt);

    setSystemTime(new Date(CLOCK_EPOCH_MS + HEARTBEAT_OFFSET_MS));

    await ops.sendHeartbeat(ids.map(String), tokens);

    for (let i = 0; i < 3; i++) {
      const renewedLock = qm.getLockInfo(ids[i]);
      expect(renewedLock).toMatchObject({
        renewalCount: 1,
        lastRenewalAt: CLOCK_EPOCH_MS + HEARTBEAT_OFFSET_MS,
        expiresAt: initialExpiries[i] + HEARTBEAT_OFFSET_MS,
      });
    }

    setSystemTime(new Date(CLOCK_EPOCH_MS + AFTER_ORIGINAL_EXPIRY_MS));

    // All locks remain valid past their original expiry.
    for (let i = 0; i < 3; i++) {
      const valid = qm.verifyLock(ids[i], tokens[i]);
      expect(valid).toBe(true);
    }
  });
});
