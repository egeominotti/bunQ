import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { QueueManager } from '../../src/application/queueManager';
import { Queue, Worker, closeAllSharedPools, shutdownManager } from '../../src/client';
import { createTcpServer, type TcpServer } from '../../src/infrastructure/server/tcp';

const mode = process.argv[2];
if (mode !== 'embedded' && mode !== 'tcp') {
  throw new Error(`Expected embedded or tcp mode, received ${String(mode)}`);
}

const dataDir = mkdtempSync(join(tmpdir(), `bunqueue-worker-lifecycle-${mode}-`));
const dataPath = join(dataDir, 'queue.db');
let manager: QueueManager | null = null;
let server: TcpServer | null = null;

try {
  if (mode === 'tcp') {
    manager = new QueueManager({ dataPath });
    server = createTcpServer(manager, { hostname: '127.0.0.1', port: 0 });
  }

  const runtimeOptions =
    mode === 'embedded'
      ? { embedded: true, dataPath }
      : {
          embedded: false,
          connection: { host: '127.0.0.1', port: server?.server.port ?? 0, poolSize: 1 },
        };
  const queueName = `worker-pause-resume-${mode}-${crypto.randomUUID()}`;
  const queue = new Queue(queueName, runtimeOptions);
  const worker = new Worker(queueName, async () => undefined, {
    ...runtimeOptions,
    autorun: false,
    heartbeatInterval: 20,
  });

  try {
    worker.run();
    await worker.waitUntilReady();
    await Bun.sleep(30);
    for (let cycle = 0; cycle < 10; cycle++) {
      worker.pause();
      worker.resume();
    }
    await Bun.sleep(30);
  } finally {
    await worker.close(true);
    queue.close();
  }
} finally {
  server?.stop();
  manager?.shutdown();
  closeAllSharedPools();
  shutdownManager();
  rmSync(dataDir, { recursive: true, force: true });
}
