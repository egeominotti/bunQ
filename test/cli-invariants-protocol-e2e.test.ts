import { afterEach, describe, expect, test } from 'bun:test';
import { rmSync } from 'node:fs';
import { QueueManager } from '../src/application/queueManager';
import { jobId } from '../src/domain/types/job';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';
import { parseSingleJson, runCli } from './cli-invariants/runtimeHarness';

const DB_PATH = `/tmp/bunqueue-cli-codec-${process.pid}-${Date.now()}.db`;
let manager: QueueManager | undefined;
let server: TcpServer | undefined;

function stop(): void {
  server?.stop();
  manager?.shutdown();
  server = undefined;
  manager = undefined;
}

afterEach(() => {
  stop();
  for (const suffix of ['', '-wal', '-shm']) {
    try {
      rmSync(DB_PATH + suffix);
    } catch {
      // already removed
    }
  }
});

describe('CLI protocol and persistence preserve arbitrary JSON keys', () => {
  test('push/get/restart is lossless and cannot pollute Object.prototype', async () => {
    const queue = `cli-codec-${process.pid}`;
    const data = JSON.parse(
      '{"__proto__":{"polluted":true},"__proto_":"distinct","constructor":"own","nested":[{"prototype":"own"}]}'
    );
    manager = new QueueManager({ dataPath: DB_PATH });
    server = createTcpServer(manager, { port: 0, hostname: '127.0.0.1' });

    const pushed = await runCli(
      ['push', queue, JSON.stringify(data), '--job-id', `codec-${process.pid}`, '--json'],
      server.server.port
    );
    expect(pushed.exitCode).toBe(0);
    const id = (parseSingleJson(pushed) as { id: string }).id;

    const fetched = parseSingleJson(
      await runCli(['job', 'get', id, '--json'], server.server.port)
    ) as { job: { data: unknown } };
    expect(fetched.job.data).toEqual(data);
    expect(({} as { polluted?: boolean }).polluted).toBeUndefined();

    stop();
    manager = new QueueManager({ dataPath: DB_PATH });
    server = createTcpServer(manager, { port: 0, hostname: '127.0.0.1' });
    const recovered = await manager.getJob(jobId(id));
    expect(recovered?.data).toEqual(data);
    expect(Object.hasOwn(recovered?.data as object, '__proto__')).toBe(true);
    expect(({} as { polluted?: boolean }).polluted).toBeUndefined();
  });
});
