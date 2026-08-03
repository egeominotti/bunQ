import { type ChildProcess, spawn } from 'node:child_process';
import { createInterface } from 'node:readline';
import { Wire } from './wire';

const REPO_ROOT = new URL('../../', import.meta.url).pathname;

async function freePort(): Promise<number> {
  const { createServer } = await import('node:net');
  return new Promise((resolve) => {
    const server = createServer();
    server.listen(0, '127.0.0.1', () => {
      const port = (server.address() as { port: number }).port;
      server.close(() => resolve(port));
    });
  });
}

export async function startServer(extraEnv: Record<string, string> = {}) {
  const port = await freePort();
  const httpPort = await freePort();
  const dataDir = `${REPO_ROOT}.conformance-tmp-${Math.random().toString(36).slice(2, 8)}`;
  const { mkdirSync } = await import('node:fs');
  mkdirSync(dataDir, { recursive: true });
  const proc = spawn('bun', ['src/main.ts'], {
    cwd: REPO_ROOT,
    env: {
      ...process.env,
      TCP_PORT: String(port),
      HTTP_PORT: String(httpPort),
      BUNQUEUE_DATA_PATH: `${dataDir}/bunq.db`,
      ...extraEnv,
    },
    stdio: 'ignore',
  });
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    try {
      const wire = new Wire();
      await wire.connect(port, extraEnv.AUTH_TOKENS?.split(',')[0]);
      return { port, proc, wire, dataDir };
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 150));
    }
  }
  proc.kill();
  throw new Error('server did not start within 15s');
}

export class Driver {
  private readonly proc: ChildProcess;
  private readonly pending = new Map<
    number,
    { resolve: (value: Record<string, unknown>) => void }
  >();
  private counter = 0;

  constructor(command: string) {
    this.proc = spawn('sh', ['-c', command], {
      cwd: new URL('.', import.meta.url).pathname,
      stdio: ['pipe', 'pipe', 'inherit'],
    });
    const output = this.proc.stdout;
    if (!output) throw new Error('driver stdout is unavailable');
    const lines = createInterface({ input: output });
    lines.on('line', (line) => {
      try {
        const message = JSON.parse(line) as { id: number };
        this.pending.get(message.id)?.resolve(message as unknown as Record<string, unknown>);
        this.pending.delete(message.id);
      } catch {
        /* non-JSON driver noise on stdout: ignore */
      }
    });
  }

  async op(
    op: string,
    fields: Record<string, unknown> = {},
    timeoutMs = 30_000
  ): Promise<Record<string, unknown>> {
    const id = ++this.counter;
    const answer = new Promise<Record<string, unknown>>((resolve, reject) => {
      this.pending.set(id, { resolve });
      setTimeout(() => {
        if (this.pending.delete(id))
          reject(new Error(`driver op '${op}' timed out after ${timeoutMs}ms`));
      }, timeoutMs).unref?.();
    });
    const input = this.proc.stdin;
    if (!input) throw new Error('driver stdin is unavailable');
    input.write(`${JSON.stringify({ id, op, ...fields })}\n`);
    return await answer;
  }

  /** Like op(), but throws when the driver answers ok: false. */
  async must(
    op: string,
    fields: Record<string, unknown> = {},
    timeoutMs = 30_000
  ): Promise<Record<string, unknown>> {
    const answer = await this.op(op, fields, timeoutMs);
    if (answer.ok !== true)
      throw new Error(`driver ${op} failed: ${String(answer.error ?? 'unknown')}`);
    return answer;
  }

  kill(): void {
    this.proc.kill();
  }
}
