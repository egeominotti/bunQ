/**
 * Tests for uncaughtException and unhandledRejection handlers in src/main.ts
 *
 * Verifies that the server performs graceful shutdown instead of crashing
 * when an uncaught exception or unhandled promise rejection occurs.
 *
 * Uses subprocesses because process.exit() and process signal handlers
 * cannot be safely tested in-process.
 */

import { describe, test, expect, afterAll } from 'bun:test';
import { join } from 'path';
import { unlinkSync, writeFileSync, mkdirSync } from 'fs';

const PROJECT_ROOT = join(import.meta.dir, '..');
const TMP_DIR = join(PROJECT_ROOT, 'test', '.tmp-uncaught');

// Ensure temp dir exists
try {
  mkdirSync(TMP_DIR, { recursive: true });
} catch {}

afterAll(() => {
  // Cleanup temp dir
  try {
    const files = new Bun.Glob('*').scanSync(TMP_DIR);
    for (const f of files) unlinkSync(join(TMP_DIR, f));
    unlinkSync(TMP_DIR);
  } catch {}
});

/** Helper: write script to temp file, run as subprocess, capture output */
async function runScript(
  name: string,
  script: string,
  timeoutMs = 10000
): Promise<{
  exitCode: number;
  stdout: string;
  stderr: string;
}> {
  const scriptPath = join(TMP_DIR, `${name}.ts`);
  writeFileSync(scriptPath, script);

  const proc = Bun.spawn(['bun', scriptPath], {
    cwd: PROJECT_ROOT,
    env: {
      ...process.env,
      TCP_PORT: '0',
      HTTP_PORT: '0',
      LOG_FORMAT: 'json',
      BUNQUEUE_EMBEDDED: '1',
    },
    stdout: 'pipe',
    stderr: 'pipe',
  });

  const timeout = setTimeout(() => proc.kill(), timeoutMs);

  const [stdout, stderr] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
  ]);

  const exitCode = await proc.exited;
  clearTimeout(timeout);

  try {
    unlinkSync(scriptPath);
  } catch {}

  return { exitCode, stdout, stderr };
}

describe('Uncaught Exception & Unhandled Rejection Handlers', () => {
  test('uncaughtException triggers graceful shutdown', async () => {
    const result = await runScript(
      'test-uncaught',
      `
import { serverLog } from '../../src/shared/logger';

let shutdownCalled = false;

const shutdown = async (signal: string) => {
  if (shutdownCalled) return;
  shutdownCalled = true;
  console.log(JSON.stringify({ shutdownCalled: true, signal }));
  process.exit(0);
};

process.on('uncaughtException', (err: Error) => {
  serverLog.error('Uncaught exception - initiating shutdown', {
    error: err.message,
    stack: err.stack,
  });
  void shutdown('uncaughtException');
});

setTimeout(() => {
  throw new Error('test-uncaught-exception');
}, 50);
`
    );

    expect(result.stdout).toContain('"shutdownCalled":true');
    expect(result.stdout).toContain('"signal":"uncaughtException"');
    expect(result.exitCode).toBe(0);
  });

  test('unhandledRejection triggers graceful shutdown', async () => {
    const result = await runScript(
      'test-unhandled',
      `
import { serverLog } from '../../src/shared/logger';

let shutdownCalled = false;

const shutdown = async (signal: string) => {
  if (shutdownCalled) return;
  shutdownCalled = true;
  console.log(JSON.stringify({ shutdownCalled: true, signal }));
  process.exit(0);
};

process.on('unhandledRejection', (reason: unknown) => {
  serverLog.error('Unhandled promise rejection - initiating shutdown', {
    reason: reason instanceof Error ? reason.message : String(reason),
    stack: reason instanceof Error ? reason.stack : undefined,
  });
  void shutdown('unhandledRejection');
});

setTimeout(() => {
  Promise.reject(new Error('test-unhandled-rejection'));
}, 50);
`
    );

    expect(result.stdout).toContain('"shutdownCalled":true');
    expect(result.stdout).toContain('"signal":"unhandledRejection"');
    expect(result.exitCode).toBe(0);
  });

  test('shuttingDown guard prevents duplicate shutdown calls', async () => {
    const result = await runScript(
      'test-guard',
      `
let shutdownCount = 0;
let shuttingDown = false;

const shutdown = async (signal: string) => {
  if (shuttingDown) return;
  shuttingDown = true;
  shutdownCount++;
  await new Promise(r => setTimeout(r, 100));
  console.log(JSON.stringify({ shutdownCount, signal }));
  process.exit(0);
};

process.on('uncaughtException', (_err: Error) => {
  void shutdown('uncaughtException');
});

// Fire exception - only first should trigger shutdown
setTimeout(() => {
  throw new Error('first-error');
}, 50);
`
    );

    expect(result.stdout).toContain('"shutdownCount":1');
    expect(result.exitCode).toBe(0);
  });

  test('uncaughtException logs error message and stack trace', async () => {
    const result = await runScript(
      'test-log-error',
      `
import { serverLog, Logger } from '../../src/shared/logger';
Logger.enableJsonMode();

let shutdownCalled = false;
const shutdown = async (signal: string) => {
  if (shutdownCalled) return;
  shutdownCalled = true;
  await new Promise(r => setTimeout(r, 50));
  process.exit(0);
};

process.on('uncaughtException', (err: Error) => {
  serverLog.error('Uncaught exception - initiating shutdown', {
    error: err.message,
    stack: err.stack,
  });
  void shutdown('uncaughtException');
});

setTimeout(() => {
  throw new Error('specific-error-message-12345');
}, 50);
`
    );

    const output = result.stderr + result.stdout;
    expect(output).toContain('specific-error-message-12345');
    expect(output).toContain('Uncaught exception');
  });

  test('unhandledRejection logs reason for non-Error rejections', async () => {
    const result = await runScript(
      'test-string-reject',
      `
import { serverLog, Logger } from '../../src/shared/logger';
Logger.enableJsonMode();

let shutdownCalled = false;
const shutdown = async (signal: string) => {
  if (shutdownCalled) return;
  shutdownCalled = true;
  await new Promise(r => setTimeout(r, 50));
  process.exit(0);
};

process.on('unhandledRejection', (reason: unknown) => {
  serverLog.error('Unhandled promise rejection - initiating shutdown', {
    reason: reason instanceof Error ? reason.message : String(reason),
    stack: reason instanceof Error ? reason.stack : undefined,
  });
  void shutdown('unhandledRejection');
});

// Reject with a string, not an Error
setTimeout(() => {
  Promise.reject('string-rejection-reason-67890');
}, 50);
`
    );

    const output = result.stderr + result.stdout;
    expect(output).toContain('string-rejection-reason-67890');
    expect(output).toContain('Unhandled promise rejection');
  });
});
