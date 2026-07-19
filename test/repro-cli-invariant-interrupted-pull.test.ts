import { afterEach, describe, expect, test } from 'bun:test';
import { join } from 'node:path';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';
import { WaiterManager } from '../src/domain/queue/waiterManager';

const REPO = join(import.meta.dir, '..');
const QUEUE = `cli-interrupt-${process.pid}`;
const PORT = 20000 + (process.pid % 10000);

let manager: QueueManager | undefined;
let server: TcpServer | undefined;

afterEach(() => {
  server?.stop();
  manager?.shutdown();
  server = undefined;
  manager = undefined;
});

describe('CLI invariant: interrupted long-polls leave no hidden consumer', () => {
  test('aborting a waiter removes it without notification debt', async () => {
    const waiters = new WaiterManager();
    const controller = new AbortController();
    const waiting = waiters.waitForJob(QUEUE, 5000, controller.signal);
    expect(waiters.length).toBe(1);

    controller.abort();
    await waiting;
    expect(waiters.length).toBe(0);

    let secondResolved = false;
    const second = waiters.waitForJob(QUEUE, 20).then(() => {
      secondResolved = true;
    });
    await Bun.sleep(5);
    expect(secondResolved).toBe(false);
    await second;
  });

  test('killing an empty CLI pull cannot consume the next job', async () => {
    manager = new QueueManager();
    server = createTcpServer(manager, { port: PORT, hostname: '127.0.0.1' });

    const proc = Bun.spawn(
      ['bun', 'src/main.ts', 'pull', QUEUE, '--timeout', '5000', '--port', String(PORT)],
      {
        cwd: REPO,
        env: { ...process.env },
        stdout: 'pipe',
        stderr: 'pipe',
      }
    );

    await Bun.sleep(150);
    proc.kill();
    await proc.exited;
    const disconnectDeadline = Date.now() + 1000;
    while (server.getConnectionCount() !== 0 && Date.now() < disconnectDeadline) {
      await Bun.sleep(5);
    }
    expect(server.getConnectionCount()).toBe(0);

    const job = await manager.push(QUEUE, { data: { source: 'after-interrupt' } });
    await Bun.sleep(100);

    const pulled = await manager.pull(QUEUE);
    expect(pulled?.id).toBe(job.id);
  });
});
