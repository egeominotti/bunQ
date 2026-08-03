import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { closeAllSharedPools, shutdownManager } from '../src/client';
import { Engine, Workflow } from '../src/client/workflow';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';

type Mode = 'embedded' | 'tcp';

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve = () => undefined;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function within<T>(promise: Promise<T>, label: string): Promise<T> {
  return await Promise.race([
    promise,
    Bun.sleep(5_000).then(() => {
      throw new Error(`Timed out waiting for ${label}`);
    }),
  ]);
}

async function runCloseRecovery(mode: Mode): Promise<void> {
  shutdownManager();
  closeAllSharedPools();
  const dataDir = mkdtempSync(join(tmpdir(), `bunqueue-workflow-close-${mode}-`));
  const dataPath = join(dataDir, 'workflow.db');
  const queueName = `workflow-close-${mode}-${crypto.randomUUID()}`;
  const workflowName = `close-recovery-${mode}-${crypto.randomUUID()}`;
  const compensationStarted = deferred();
  const releaseCompensation = deferred();
  const compensationReturned = deferred();
  let manager: QueueManager | null = null;
  let server: TcpServer | null = null;
  let original: Engine | null = null;
  let recovered: Engine | null = null;
  let originalClosed = false;

  const definition = () =>
    new Workflow(workflowName)
      .step('completed-step', async () => ({ ok: true }), {
        compensate: async () => {
          compensationStarted.resolve();
          await releaseCompensation.promise;
          compensationReturned.resolve();
          throw new Error('deterministic compensation failure');
        },
      })
      .step('failing-step', async () => {
        throw new Error('deterministic forward failure');
      });

  try {
    if (mode === 'tcp') {
      manager = new QueueManager({ dataPath: join(dataDir, 'broker.db') });
      server = createTcpServer(manager, { hostname: '127.0.0.1', port: 0 });
    }
    const engineOptions = {
      embedded: mode === 'embedded',
      dataPath,
      queueName,
      concurrency: 1,
      ...(mode === 'tcp'
        ? { connection: { host: '127.0.0.1', port: server?.server.port ?? 0, poolSize: 1 } }
        : {}),
    };

    original = new Engine(engineOptions).register(definition());
    const run = await original.start(workflowName, { value: 1 });
    await within(compensationStarted.promise, 'the original compensation to start');

    await original.close(true);
    originalClosed = true;
    recovered = new Engine(engineOptions).register(definition());
    const recovery = recovered.recover();

    releaseCompensation.resolve();
    await within(compensationReturned.promise, 'the abandoned compensation handler to return');
    await within(recovery, 'the new engine to recover after the local claim is released');

    const execution = recovered.getExecution(run.id);
    expect(execution?.state).toBe('compensation-stuck');
    expect(execution?.rollbackStatus).toBe('stuck');
    expect(execution?.steps['completed-step']?.compensation?.status).toBe('compensation-failed');
  } finally {
    releaseCompensation.resolve();
    await Bun.sleep(50);
    await recovered?.close(true).catch(() => undefined);
    if (!originalClosed) await original?.close(true).catch(() => undefined);
    server?.stop();
    manager?.shutdown();
    closeAllSharedPools();
    shutdownManager();
    rmSync(dataDir, { recursive: true, force: true });
  }
}

afterEach(() => {
  closeAllSharedPools();
  shutdownManager();
});

describe('Workflow compensation recovery during forced close', () => {
  test('embedded waits for the local unwind claim and resumes recovery', async () => {
    await runCloseRecovery('embedded');
  }, 15_000);

  test('TCP waits for the local unwind claim and resumes recovery', async () => {
    await runCloseRecovery('tcp');
  }, 15_000);
});
