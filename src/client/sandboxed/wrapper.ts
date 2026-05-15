/**
 * Worker Wrapper Script Generator
 * Creates the wrapper script that loads processor in worker process
 */

import { mkdir, unlink, realpath } from 'node:fs/promises';
import { closeSync, fsyncSync, openSync, writeSync } from 'node:fs';
import { join, normalize } from 'node:path';

/**
 * Escape a string for safe embedding in a template literal
 * Prevents code injection via backticks or backslashes in paths
 */
function escapeForTemplateLiteral(str: string): string {
  return str.replace(/\\/g, '\\\\').replace(/`/g, '\\`').replace(/\$/g, '\\$');
}

/**
 * Create wrapper script file that loads the processor
 */
export async function createWrapperScript(
  queueName: string,
  processorPath: string
): Promise<string> {
  // Normalize the path so double slashes from callers like
  // `${TMPDIR}/file` (TMPDIR often ends with `/`) don't trip the Worker's
  // import resolver. Then resolve symlinks (e.g. macOS `/var` → `/private/var`)
  // so the import target matches what Bun's module loader will see.
  const normalized = normalize(
    processorPath.startsWith('/') ? processorPath : `${process.cwd()}/${processorPath}`
  );

  // Verify the processor file is visible on disk before generating a wrapper
  // that will import it from a Worker thread. On macOS, files written by
  // Bun.write (no fsync) can briefly be invisible to fresh Worker processes,
  // surfacing as ModuleNotFound when the wrapper's top-level await import()
  // runs. Poll briefly with a fast cap to catch the race without slowing
  // the happy path.
  let processorVisible = false;
  for (let i = 0; i < 20; i++) {
    if (await Bun.file(normalized).exists()) {
      processorVisible = true;
      break;
    }
    await Bun.sleep(5);
  }
  if (!processorVisible) {
    throw new Error(`Processor file not visible after polling: ${normalized}`);
  }

  // Resolve symlinks AFTER the existence check so a non-existent file fails
  // with a clear error rather than realpath's ENOENT.
  let fullPath: string;
  try {
    fullPath = await realpath(normalized);
  } catch {
    // realpath can fail in restricted sandboxes; fall back to normalized.
    fullPath = normalized;
  }

  // Escape the path to prevent code injection via backticks or template expressions
  const escapedPath = escapeForTemplateLiteral(fullPath);

  const wrapperCode = `
// Sandboxed Worker Wrapper
const processor = (await import('${escapedPath}')).default;

// Signal ready to parent
self.postMessage({ type: 'ready' });

self.onmessage = async (event) => {
  const { type, job } = event.data;
  if (type !== 'job') return;

  try {
    const result = await processor({
      id: job.id,
      data: job.data,
      queue: job.queue,
      attempts: job.attempts,
      parentId: job.parentId,
      progress: (value) => {
        self.postMessage({ type: 'progress', jobId: job.id, progress: value });
      },
      log: (message) => {
        self.postMessage({ type: 'log', jobId: job.id, message });
      },
      fail: (error) => {
        const msg = typeof error === 'string' ? error : error instanceof Error ? error.message : String(error);
        self.postMessage({ type: 'fail', jobId: job.id, error: msg });
      },
    });

    self.postMessage({ type: 'result', jobId: job.id, result });
  } catch (err) {
    self.postMessage({
      type: 'error',
      jobId: job.id,
      error: err instanceof Error ? err.message : String(err),
    });
  }
};
`;

  // path.join normalizes double slashes ($TMPDIR often ends in '/').
  const tempDir = join(Bun.env.TMPDIR ?? '/tmp', 'bunqueue-workers');
  await mkdir(tempDir, { recursive: true });

  const wrapperPath = join(
    tempDir,
    `worker-${queueName}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}.ts`
  );

  // Write with explicit fsync so Bun's Worker spawn sees the file on macOS,
  // where fs/promises.writeFile (no fdatasync) has produced ModuleNotFound.
  const fd = openSync(wrapperPath, 'w');
  try {
    writeSync(fd, wrapperCode);
    fsyncSync(fd);
  } finally {
    closeSync(fd);
  }

  // Belt-and-braces: some macOS environments still need a moment for the
  // dirent to be visible to a fresh Worker process. Poll, then throw if
  // still missing — swallowing here just moves the failure into Worker spawn.
  let visible = false;
  for (let i = 0; i < 10; i++) {
    if (await Bun.file(wrapperPath).exists()) {
      visible = true;
      break;
    }
    await Bun.sleep(5);
  }
  if (!visible) {
    throw new Error(`Wrapper script not visible after fsync: ${wrapperPath}`);
  }

  return wrapperPath;
}

/**
 * Cleanup wrapper script file
 */
export async function cleanupWrapperScript(wrapperPath: string | null): Promise<void> {
  if (!wrapperPath) return;

  try {
    const file = Bun.file(wrapperPath);
    if (await file.exists()) {
      await unlink(wrapperPath);
    }
  } catch {
    // Ignore cleanup errors
  }
}
