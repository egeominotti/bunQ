/**
 * Bug: MCP adapter sends { cmd: 'UnregisterWorker', id: workerId }
 * but the server expects { cmd: 'UnregisterWorker', workerId: '...' }.
 * The field name mismatch causes unregister to always fail.
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

describe('UnregisterWorker field handling', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createContext(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('should unregister worker with workerId field (standard)', async () => {
    // Register a worker first
    const regRes = await handleCommand(
      { cmd: 'RegisterWorker', name: 'test-worker', queues: ['q1'] } as Command,
      ctx
    );
    expect(regRes.ok).toBe(true);
    const workerId = (regRes as Record<string, unknown>).data as Record<string, unknown>;

    // Unregister with correct field name
    const unregRes = await handleCommand(
      { cmd: 'UnregisterWorker', workerId: (workerId as any).workerId } as Command,
      ctx
    );
    expect(unregRes.ok).toBe(true);
  });

  test('MCP adapter sends id instead of workerId - should still work', async () => {
    // Register a worker first
    const regRes = await handleCommand(
      { cmd: 'RegisterWorker', name: 'test-worker', queues: ['q1'] } as Command,
      ctx
    );
    expect(regRes.ok).toBe(true);
    const data = (regRes as Record<string, unknown>).data as Record<string, unknown>;
    const wid = data.workerId as string;

    // Simulate what MCP adapter sends: { cmd: 'UnregisterWorker', id: workerId }
    // This should also work (the handler should accept both 'id' and 'workerId')
    const unregRes = await handleCommand(
      { cmd: 'UnregisterWorker', id: wid, workerId: wid } as unknown as Command,
      ctx
    );
    expect(unregRes.ok).toBe(true);
  });
});
