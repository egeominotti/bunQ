import { describe, expect, test } from 'bun:test';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer } from '../src/infrastructure/server/tcp';
import { runTcpProcessSweepCase } from '../bench/tcp-process-sweep';

describe('TCP process sweep integrity', () => {
  test('the runner gates throughput on authoritative broker completion', () => {
    const source = readFileSync(
      join(import.meta.dir, '..', 'bench', 'tcp-process-sweep.ts'),
      'utf8'
    );

    expect(source).toContain('if (import.meta.main)');
    expect(source).toContain('getJobCounts');
    expect(source).not.toContain('heartbeatInterval: 0');
    expect(source).not.toContain('while (processed < SCALE)');
  });

  test('a real high-concurrency sample reconciles every accepted job', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'bunqueue-tcp-sweep-test-'));
    const manager = new QueueManager({ dataPath: join(directory, 'broker.db') });
    const server = createTcpServer(manager, { hostname: '127.0.0.1', port: 0 });
    const port = server.server.port;

    try {
      expect(port).toBeGreaterThan(0);
      const result = await runTcpProcessSweepCase({
        scale: 500,
        concurrency: 200,
        batchSize: 200,
        host: '127.0.0.1',
        port,
        heartbeatInterval: 100,
        timeoutMs: 30_000,
      });

      expect(result.accepted).toBe(500);
      expect(result.invocations).toBe(500);
      expect(result.uniqueInvocations).toBe(500);
      expect(result.duplicateInvocations).toBe(0);
      expect(result.finalCounts).toEqual({
        waiting: 0,
        prioritized: 0,
        active: 0,
        completed: 500,
        failed: 0,
        delayed: 0,
        paused: 0,
        'waiting-children': 0,
      });
    } finally {
      server.stop(true);
      manager.shutdown();
      rmSync(directory, { recursive: true, force: true });
    }
  }, 45_000);

  test('TCP framing remains intact when a 200-way worker drains 1,000 jobs', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'bunqueue-tcp-backpressure-test-'));
    const manager = new QueueManager({ dataPath: join(directory, 'broker.db') });
    const server = createTcpServer(manager, { hostname: '127.0.0.1', port: 0 });
    const port = server.server.port;

    try {
      expect(port).toBeGreaterThan(0);
      const result = await runTcpProcessSweepCase({
        scale: 1_000,
        concurrency: 200,
        batchSize: 200,
        host: '127.0.0.1',
        port,
        heartbeatInterval: 5_000,
        timeoutMs: 30_000,
      });

      expect(result.accepted).toBe(1_000);
      expect(result.uniqueInvocations).toBe(1_000);
      expect(result.duplicateInvocations).toBe(0);
      expect(result.finalCounts.completed).toBe(1_000);
    } finally {
      server.stop(true);
      manager.shutdown();
      rmSync(directory, { recursive: true, force: true });
    }
  }, 45_000);

  test('the runner rejects a missing endpoint instead of falling back to port 6789', async () => {
    const result = runTcpProcessSweepCase({
      scale: 1,
      concurrency: 1,
      batchSize: 1,
      port: undefined as never,
    });

    await expect(result).rejects.toThrow(
      'TCP process sweep port must be an integer between 1 and 65535'
    );
  });
});
