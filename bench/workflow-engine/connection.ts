import { join } from 'node:path';
import type { EngineOptions } from '../../src/client/workflow';

export type BenchmarkBroker = ReturnType<typeof Bun.spawn>;

export function workflowConnection(
  tcp: boolean,
  port: number
): EngineOptions['connection'] | undefined {
  if (!tcp) return undefined;
  return {
    host: '127.0.0.1',
    port,
    poolSize: 8,
    commandTimeout: Number(Bun.env.BENCH_COMMAND_TIMEOUT ?? 30_000),
  };
}

async function waitForPort(port: number, timeoutMs = 15_000): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while (performance.now() < deadline) {
    try {
      const socket = await Bun.connect({
        hostname: '127.0.0.1',
        port,
        socket: { data: () => undefined },
      });
      socket.end();
      return;
    } catch {
      await Bun.sleep(50);
    }
  }
  throw new Error(`broker did not listen on port ${port}`);
}

export async function startBenchmarkBroker(port: number): Promise<BenchmarkBroker> {
  const broker = Bun.spawn([process.execPath, 'run', 'src/main.ts'], {
    cwd: join(import.meta.dir, '../..'),
    env: {
      ...process.env,
      TCP_PORT: String(port),
      HTTP_PORT: String(Number(Bun.env.BENCH_HTTP_PORT)),
      BUNQUEUE_DATA_PATH: Bun.env.BENCH_BROKER_PATH as string,
      LOG_LEVEL: 'error',
    },
    stdout: 'ignore',
    stderr: 'inherit',
  });
  try {
    await waitForPort(port);
    return broker;
  } catch (error) {
    broker.kill();
    await broker.exited;
    throw error;
  }
}

export async function stopBenchmarkBroker(broker?: BenchmarkBroker): Promise<void> {
  if (!broker) return;
  if (broker.exitCode === null) broker.kill();
  await broker.exited;
}
