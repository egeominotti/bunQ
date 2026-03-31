/**
 * Bug: JobHeartbeat handler ignores the duration field.
 * MCP adapter sends { cmd: 'JobHeartbeat', id, token, duration }
 * but the handler only passes (jid, token) to queueManager.jobHeartbeat(),
 * ignoring the duration. The lock is renewed with default TTL instead of
 * the requested duration.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import type { HandlerContext } from '../src/infrastructure/server/types';
import { handleCommand } from '../src/infrastructure/server/handler';
import type { Command } from '../src/domain/types/command';

function createContext(qm: QueueManager): HandlerContext {
  return {
    queueManager: qm,
    authTokens: new Set<string>(),
    authenticated: false,
    clientId: 'test-client-1',
  };
}

describe('JobHeartbeat duration support', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createContext(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('should renew lock with specified duration', async () => {
    // Push and pull with lock
    await qm.push('test-queue', { data: { x: 1 } });
    const { job, token } = await qm.pullWithLock('test-queue', 'worker-1', 0, 5000); // 5s TTL
    expect(job).not.toBeNull();
    expect(token).not.toBeNull();

    // Get initial lock info
    const lockBefore = qm.getLockInfo(job!.id);
    expect(lockBefore).not.toBeNull();
    const ttlBefore = lockBefore!.expiresAt - Date.now();

    // Send heartbeat with a much longer duration (60s)
    const res = await handleCommand(
      { cmd: 'JobHeartbeat', id: String(job!.id), token, duration: 60000 } as Command,
      ctx
    );
    expect(res.ok).toBe(true);

    // Get lock info after heartbeat
    const lockAfter = qm.getLockInfo(job!.id);
    expect(lockAfter).not.toBeNull();
    const ttlAfter = lockAfter!.expiresAt - Date.now();

    // The TTL should be approximately 60s now, not the original 5s
    // We use > 30000 as a safe threshold (original was ~5000)
    expect(ttlAfter).toBeGreaterThan(30000);
  });
});
