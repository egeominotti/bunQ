import { join } from 'node:path';

const REPO = join(import.meta.dir, '../..');

export interface CliResult {
  exitCode: number | null;
  stdout: string;
  stderr: string;
  timedOut: boolean;
}

export async function runCli(args: string[], port: number, timeoutMs = 5000): Promise<CliResult> {
  return runCliRaw([...args, '--port', String(port)], timeoutMs);
}

export async function runCliRaw(
  args: string[],
  timeoutMs = 5000,
  extraEnv: Record<string, string> = {}
): Promise<CliResult> {
  const proc = Bun.spawn(['bun', 'src/main.ts', ...args], {
    cwd: REPO,
    env: {
      ...process.env,
      BQ_TOKEN: '',
      BUNQUEUE_TOKEN: '',
      TCP_PORT: '',
      BUNQUEUE_TCP_PORT: '',
      BQ_TCP_PORT: '',
      ...extraEnv,
    },
    stdout: 'pipe',
    stderr: 'pipe',
  });
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    proc.kill();
  }, timeoutMs);
  const exitCode = await proc.exited;
  clearTimeout(timer);
  return {
    exitCode,
    stdout: await new Response(proc.stdout).text(),
    stderr: await new Response(proc.stderr).text(),
    timedOut,
  };
}

export function parseSingleJson(result: CliResult): unknown {
  const stdout = result.stdout.trim();
  const stderr = result.stderr.trim();
  if (stdout && stderr) {
    throw new Error(`JSON CLI wrote to both streams: stdout=${stdout} stderr=${stderr}`);
  }
  const document = stdout || stderr;
  if (!document) throw new Error('JSON CLI produced no document');
  return JSON.parse(document);
}
