import { expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { shutdownManager } from '../src/client/manager';
import { createTcpServer } from '../src/infrastructure/server/tcp';
import { EmbeddedBackend } from '../src/mcp/backend/embedded';
import { TcpBackend } from '../src/mcp/backend/tcp';

test('embedded MCP jobs expose an authoritative name without changing data.name', async () => {
  shutdownManager();
  const backend = new EmbeddedBackend();
  try {
    const added = await backend.addJob('mcp-name-embedded', 'mcp-job', {
      name: 'user-name',
      marker: 'embedded',
    });
    const job = await backend.getJob(added.jobId);
    expect(job?.name).toBe('mcp-job');
    expect(job?.data).toEqual({ name: 'user-name', marker: 'embedded' });
  } finally {
    backend.shutdown();
  }
});

test('TCP MCP jobs expose an authoritative name without changing data.name', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'bunqueue-mcp-name-'));
  const manager = new QueueManager({ dataPath: join(directory, 'queue.db') });
  const server = createTcpServer(manager, { hostname: '127.0.0.1', port: 0 });
  const backend = new TcpBackend({ host: '127.0.0.1', port: server.server.port });
  try {
    await backend.connect();
    const added = await backend.addJob('mcp-name-tcp', 'mcp-job', {
      name: 'user-name',
      marker: 'tcp',
    });
    const job = await backend.getJob(added.jobId);
    expect(job?.name).toBe('mcp-job');
    expect(job?.data).toEqual({ name: 'user-name', marker: 'tcp' });
  } finally {
    backend.shutdown();
    server.stop();
    manager.shutdown();
    rmSync(directory, { recursive: true, force: true });
  }
});
