import { serverLog } from '../../shared/logger';
import { stopRateLimiter } from './rateLimiter';

export interface ServerShutdownResources {
  readonly shutdownTimeoutMs: number;
  readonly stopStats: () => void;
  readonly stopTcp: () => void;
  readonly stopHttp: () => void;
  readonly getActiveJobs: () => number;
  readonly stopBackup?: () => void;
  readonly emitShutdown: (signal: string) => void;
  readonly stopCloud?: () => Promise<void>;
  readonly shutdownStorage: () => Promise<void>;
}

export interface ServerShutdownRuntime {
  readonly now: () => number;
  readonly sleep: (ms: number) => Promise<void>;
  readonly stopRateLimiter: () => void;
  readonly exit: (code: number) => void;
  readonly info: (message: string, data?: Record<string, unknown>) => void;
  readonly warn: (message: string, data?: Record<string, unknown>) => void;
  readonly error: (message: string, data?: Record<string, unknown>) => void;
  readonly cloudTimeoutMs: number;
  readonly storageAttemptTimeoutMs: number;
}

const DEFAULT_RUNTIME: ServerShutdownRuntime = {
  now: Date.now,
  sleep: Bun.sleep,
  stopRateLimiter,
  exit: (code) => process.exit(code),
  info: (message, data) => serverLog.info(message, data),
  warn: (message, data) => serverLog.warn(message, data),
  error: (message, data) => serverLog.error(message, data),
  cloudTimeoutMs: 2_500,
  storageAttemptTimeoutMs: 5_000,
};

function errorData(error: unknown): Record<string, unknown> {
  return { error: error instanceof Error ? error.message : String(error) };
}

async function withTimeout(
  operation: () => void | Promise<void>,
  timeoutMs: number,
  label: string
): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(`${label} timed out after ${timeoutMs}ms`)),
      timeoutMs
    );
  });
  try {
    await Promise.race([Promise.resolve().then(operation), timeout]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function bestEffort(
  label: string,
  operation: () => void | Promise<void>,
  runtime: ServerShutdownRuntime
): Promise<void> {
  try {
    await operation();
  } catch (error) {
    runtime.warn(`${label} failed during shutdown`, errorData(error));
  }
}

async function drainActiveJobs(
  resources: ServerShutdownResources,
  runtime: ServerShutdownRuntime
): Promise<void> {
  const startedAt = runtime.now();
  while (runtime.now() - startedAt < resources.shutdownTimeoutMs) {
    let active: number;
    try {
      active = resources.getActiveJobs();
    } catch (error) {
      runtime.warn('Active-job inspection failed during shutdown', errorData(error));
      return;
    }
    if (active === 0) return;
    runtime.info(`Waiting for ${active} active jobs...`);
    await runtime.sleep(1_000);
  }
}

async function shutdownStorage(
  resources: ServerShutdownResources,
  runtime: ServerShutdownRuntime
): Promise<boolean> {
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      await withTimeout(
        resources.shutdownStorage,
        runtime.storageAttemptTimeoutMs,
        `Storage shutdown attempt ${attempt}`
      );
      return true;
    } catch (error) {
      if (attempt === 1) {
        runtime.warn('Storage shutdown failed; retrying once', errorData(error));
      } else {
        runtime.error('Storage shutdown failed after retry', errorData(error));
      }
    }
  }
  return false;
}

async function performShutdown(
  signal: string,
  resources: ServerShutdownResources,
  runtime: ServerShutdownRuntime
): Promise<void> {
  let exitCode = 0;
  runtime.info(`Received ${signal}, shutting down...`);
  try {
    await bestEffort('Statistics timer cleanup', resources.stopStats, runtime);
    await bestEffort('Rate limiter cleanup', runtime.stopRateLimiter, runtime);
    await bestEffort('TCP server stop', resources.stopTcp, runtime);
    await bestEffort('HTTP server stop', resources.stopHttp, runtime);
    if (resources.stopBackup) {
      await bestEffort('Backup manager stop', resources.stopBackup, runtime);
    }
    await bestEffort('Active-job drain', () => drainActiveJobs(resources, runtime), runtime);
    await bestEffort('Shutdown event emission', () => resources.emitShutdown(signal), runtime);
    const stopCloud = resources.stopCloud;
    if (stopCloud) {
      await bestEffort(
        'Cloud agent stop',
        () => withTimeout(stopCloud, runtime.cloudTimeoutMs, 'Cloud agent stop'),
        runtime
      );
    }
    if (!(await shutdownStorage(resources, runtime))) exitCode = 1;
  } catch (error) {
    exitCode = 1;
    runtime.error('Unexpected shutdown coordinator failure', errorData(error));
  } finally {
    runtime.info(exitCode === 0 ? 'Shutdown complete' : 'Shutdown completed with errors');
    runtime.exit(exitCode);
  }
}

/** Memoize the first shutdown so duplicate signals share one total cleanup task. */
export function createServerShutdown(
  resources: ServerShutdownResources,
  overrides: Partial<ServerShutdownRuntime> = {}
): (signal: string) => Promise<void> {
  const runtime = { ...DEFAULT_RUNTIME, ...overrides };
  let task: Promise<void> | null = null;
  return (signal) => (task ??= performShutdown(signal, resources, runtime));
}
