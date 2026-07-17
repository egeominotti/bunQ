#!/usr/bin/env bun
/** Run every TCP functional test against a fresh server and SQLite database. */

import { spawn, type Subprocess } from 'bun';
import { mkdtemp, readdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const SCRIPTS_DIR = import.meta.dir;
const PROJECT_ROOT = `${SCRIPTS_DIR}/../..`;
const FILE_TIMEOUT_MS = Number(Bun.env.BUNQUEUE_TEST_FILE_TIMEOUT_MS ?? 180_000);

interface ServerHandle {
  process: Subprocess;
  tempDir: string;
  tcpPort: number;
  httpPort: number;
}

interface TestResult {
  name: string;
  success: boolean;
  output: string;
  passed: number;
  failed: number;
  timedOut: boolean;
  durationMs: number;
}

let activeServer: ServerHandle | null = null;
let activeTest: Subprocess | null = null;
let stopping = false;

function safeBaseEnv(): Record<string, string> {
  const result: Record<string, string> = {};
  for (const key of [
    'PATH',
    'HOME',
    'TMPDIR',
    'TMP',
    'TEMP',
    'CI',
    'TERM',
    'TZ',
    'LANG',
    'LC_ALL',
  ]) {
    const value = Bun.env[key];
    if (value !== undefined) result[key] = value;
  }
  return result;
}

function getFreePort(): number {
  const listener = Bun.listen({
    hostname: '127.0.0.1',
    port: 0,
    socket: {
      data() {
        // Port reservation only; no payload is expected.
      },
    },
  });
  const port = listener.port;
  listener.stop(true);
  return port;
}

async function waitForServer(httpPort: number, maxWaitMs = 15_000): Promise<void> {
  const startedAt = Date.now();
  while (Date.now() - startedAt < maxWaitMs) {
    try {
      const response = await fetch(`http://127.0.0.1:${httpPort}/health`);
      if (response.ok) return;
    } catch {
      // Server has not bound the port yet.
    }
    await Bun.sleep(100);
  }
  throw new Error(`server failed to start on HTTP port ${httpPort}`);
}

async function stopProcess(process: Subprocess | null): Promise<void> {
  if (!process) return;
  try {
    process.kill();
  } catch {
    return;
  }
  const exited = await Promise.race([
    process.exited.then(() => true),
    Bun.sleep(5_000).then(() => false),
  ]);
  if (!exited) {
    try {
      process.kill(9);
      await process.exited;
    } catch {
      // It exited between the timeout and SIGKILL.
    }
  }
}

async function startServer(testFile: string): Promise<ServerHandle> {
  const tempDir = await mkdtemp(join(tmpdir(), 'bunqueue-tcp-'));
  const tcpPort = getFreePort();
  let httpPort = getFreePort();
  while (httpPort === tcpPort) httpPort = getFreePort();

  const env = {
    ...safeBaseEnv(),
    BUNQUEUE_DATA_PATH: join(tempDir, 'queue.db'),
    HOST: '127.0.0.1',
    HTTP_PORT: String(httpPort),
    LOG_LEVEL: 'error',
    NODE_ENV: 'test',
    TCP_PORT: String(tcpPort),
    ...(testFile === 'test-authentication.ts' && {
      AUTH_TOKENS: 'valid-token-1,valid-token-2',
      TEST_TOKEN: 'valid-token-1',
    }),
  };
  const proc = spawn([process.execPath, 'run', 'src/main.ts'], {
    cwd: PROJECT_ROOT,
    stdout: 'ignore',
    stderr: 'inherit',
    env,
  });
  const handle = { process: proc, tempDir, tcpPort, httpPort };
  activeServer = handle;

  try {
    await waitForServer(httpPort);
    return handle;
  } catch (error) {
    await stopProcess(proc);
    await rm(tempDir, { recursive: true, force: true });
    activeServer = null;
    throw error;
  }
}

async function stopServer(): Promise<void> {
  const server = activeServer;
  activeServer = null;
  if (!server) return;
  await stopProcess(server.process);
  await rm(server.tempDir, { recursive: true, force: true });
}

async function runTest(testFile: string, server: ServerHandle): Promise<TestResult> {
  const startedAt = performance.now();
  const name = testFile.replace('.ts', '').replace('test-', '');
  const env = {
    ...safeBaseEnv(),
    BUNQUEUE_EMBEDDED: '',
    HTTP_PORT: String(server.httpPort),
    NODE_ENV: 'test',
    TCP_PORT: String(server.tcpPort),
    ...(testFile === 'test-authentication.ts' && { TEST_TOKEN: 'valid-token-1' }),
  };
  const proc = spawn([process.execPath, 'run', testFile], {
    cwd: SCRIPTS_DIR,
    stdout: 'pipe',
    stderr: 'pipe',
    env,
  });
  activeTest = proc;
  let timedOut = false;
  const timeout = setTimeout(() => {
    timedOut = true;
    try {
      proc.kill(9);
    } catch {
      // The test finished at the timeout boundary.
    }
  }, FILE_TIMEOUT_MS);

  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  clearTimeout(timeout);
  activeTest = null;
  const output = stdout + (stderr ? `\nSTDERR:\n${stderr}` : '');
  const passed = Number(output.match(/Passed: (\d+)/)?.[1] ?? 0);
  const failed = Number(output.match(/Failed: (\d+)/)?.[1] ?? (exitCode === 0 ? 0 : 1));
  return {
    name,
    success: exitCode === 0 && !timedOut,
    output,
    passed,
    failed,
    timedOut,
    durationMs: performance.now() - startedAt,
  };
}

function printResult(testFile: string, result: TestResult): void {
  const lines = result.output.split('\n');
  const summaryStart = lines.findIndex((line) => line.includes('=== Summary ==='));
  if (result.success && summaryStart >= 0) console.log(lines.slice(summaryStart).join('\n'));
  if (!result.success) {
    console.log(result.output.trim() || '(no test output)');
    if (result.timedOut) console.log(`Timed out after ${FILE_TIMEOUT_MS}ms`);
  }
  console.log(
    `TEST_FILE_RESULT ${JSON.stringify({ file: testFile, passed: result.passed, failed: result.failed, durationMs: Math.round(result.durationMs) })}`
  );
  console.log(`\n${result.success ? 'PASSED' : 'FAILED'}: ${testFile}`);
  console.log('-'.repeat(60));
}

async function cleanupAndExit(code: number): Promise<never> {
  if (stopping) process.exit(code);
  stopping = true;
  await stopProcess(activeTest);
  activeTest = null;
  await stopServer();
  process.exit(code);
}

process.on('SIGINT', () => void cleanupAndExit(130));
process.on('SIGTERM', () => void cleanupAndExit(143));

async function main(): Promise<void> {
  console.log('bunqueue TCP functional tests (fresh server + SQLite per file)\n');
  const testFiles = (await readdir(SCRIPTS_DIR))
    .filter((file) => file.startsWith('test-') && file.endsWith('.ts'))
    .sort();
  console.log(`Found ${testFiles.length} test files.\n`);

  const results: TestResult[] = [];
  for (const testFile of testFiles) {
    console.log(`Running ${testFile}...`);
    try {
      const server = await startServer(testFile);
      const result = await runTest(testFile, server);
      results.push(result);
      printResult(testFile, result);
    } catch (error) {
      const result: TestResult = {
        name: testFile.replace('.ts', '').replace('test-', ''),
        success: false,
        output: String(error),
        passed: 0,
        failed: 1,
        timedOut: false,
        durationMs: 0,
      };
      results.push(result);
      printResult(testFile, result);
    } finally {
      await stopServer();
    }
  }

  const failed = results.filter((result) => !result.success);
  const passedAssertions = results.reduce((sum, result) => sum + result.passed, 0);
  const failedAssertions = results.reduce((sum, result) => sum + result.failed, 0);
  console.log('\nTCP FINAL SUMMARY');
  console.log(`Suites: ${results.length - failed.length}/${results.length} passed`);
  console.log(`Assertions: ${passedAssertions}/${passedAssertions + failedAssertions} passed`);
  if (failed.length > 0) {
    console.log(`Failed suites: ${failed.map((result) => result.name).join(', ')}`);
  }
  await cleanupAndExit(failed.length > 0 ? 1 : 0);
}

void main();
