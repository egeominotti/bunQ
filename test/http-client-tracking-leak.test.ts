/**
 * Memory Leak Test: HTTP client tracking
 *
 * BUG: Every HTTP request creates a new clientId (uuid). When PULL registers
 * a job under clientId-A, and ACK arrives on a separate HTTP request with
 * clientId-B, the unregisterClientJob call uses clientId-B which doesn't
 * match. The original clientId-A entry in clientJobs is never cleaned up.
 *
 * FIX: HTTP context should NOT set clientId. The clientId field is optional
 * and all handler guards check `if (ctx.clientId)` before registering.
 * Without clientId, no tracking entry is created, so nothing leaks.
 * The stall detector handles orphaned HTTP jobs via timeout.
 */

import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { handleCommand } from '../src/infrastructure/server/handler';
import type { HandlerContext } from '../src/infrastructure/server/types';

describe('HTTP clientJobs memory leak', () => {
  let qm: QueueManager;

  beforeEach(() => { qm = new QueueManager(); });
  afterEach(async () => { qm.shutdown(); await Bun.sleep(50); });

  test('HTTP without clientId does NOT leak', async () => {
    const queue = 'no-leak-http';

    for (let i = 0; i < 100; i++) {
      // Push
      await handleCommand(
        { cmd: 'PUSH', queue, data: { i } },
        { queueManager: qm, authTokens: new Set(), authenticated: true }
      );

      // Pull (HTTP: no clientId)
      const pull = await handleCommand(
        { cmd: 'PULL', queue },
        { queueManager: qm, authTokens: new Set(), authenticated: true }
      );
      const job = (pull as { job: { id: string } }).job;

      // ACK (HTTP: no clientId, separate "request")
      await handleCommand(
        { cmd: 'ACK', id: job.id },
        { queueManager: qm, authTokens: new Set(), authenticated: true }
      );
    }

    const memStats = qm.getMemoryStats();
    expect(memStats.clientJobs).toBe(0);
    expect(memStats.clientJobsTotal).toBe(0);
  });

  test('HTTP WITH clientId DOES leak (proves the bug)', async () => {
    const queue = 'leak-http';

    for (let i = 0; i < 100; i++) {
      await handleCommand(
        { cmd: 'PUSH', queue, data: { i } },
        { queueManager: qm, authTokens: new Set(), authenticated: true }
      );

      // Pull with unique clientId (old HTTP behavior)
      const pullCtx: HandlerContext = {
        queueManager: qm, authTokens: new Set(), authenticated: true,
        clientId: `pull-${i}`,
      };
      const pull = await handleCommand({ cmd: 'PULL', queue }, pullCtx);
      const job = (pull as { job: { id: string } }).job;

      // ACK with DIFFERENT clientId (old HTTP behavior - new request)
      const ackCtx: HandlerContext = {
        queueManager: qm, authTokens: new Set(), authenticated: true,
        clientId: `ack-${i}`,
      };
      await handleCommand({ cmd: 'ACK', id: job.id }, ackCtx);
    }

    const memStats = qm.getMemoryStats();
    // BUG: 100 orphaned entries
    expect(memStats.clientJobs).toBe(100);
    expect(memStats.clientJobsTotal).toBe(100);
  });

  test('TCP/WS with persistent clientId does NOT leak', async () => {
    const queue = 'no-leak-tcp';
    const tcpCtx: HandlerContext = {
      queueManager: qm, authTokens: new Set(), authenticated: true,
      clientId: 'tcp-connection-1', // same clientId for all operations
    };

    for (let i = 0; i < 100; i++) {
      await handleCommand({ cmd: 'PUSH', queue, data: { i } }, tcpCtx);
      const pull = await handleCommand({ cmd: 'PULL', queue }, tcpCtx);
      const job = (pull as { job: { id: string } }).job;
      await handleCommand({ cmd: 'ACK', id: job.id }, tcpCtx);
    }

    const memStats = qm.getMemoryStats();
    expect(memStats.clientJobs).toBe(0);
    expect(memStats.clientJobsTotal).toBe(0);
  });
});
