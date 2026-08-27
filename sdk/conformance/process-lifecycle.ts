import type { ChildProcess } from 'node:child_process';

export interface TerminationOptions {
  readonly killProcessGroup?: boolean;
  readonly killTimeoutMs?: number;
  readonly termTimeoutMs?: number;
}

function hasExited(child: ChildProcess): boolean {
  return child.exitCode !== null || child.signalCode !== null;
}

function isMissingProcess(error: unknown): boolean {
  return error instanceof Error && 'code' in error && error.code === 'ESRCH';
}

export async function waitForChildExit(child: ChildProcess, timeoutMs: number): Promise<boolean> {
  if (hasExited(child)) return true;

  return await new Promise<boolean>((resolve) => {
    let settled = false;
    const finish = (exited: boolean): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      child.removeListener('exit', onExit);
      resolve(exited);
    };
    const onExit = (): void => finish(true);
    const timer = setTimeout(() => finish(hasExited(child)), timeoutMs);
    timer.unref?.();
    child.once('exit', onExit);
    if (hasExited(child)) finish(true);
  });
}

function signalChild(child: ChildProcess, signal: NodeJS.Signals, processGroup: boolean): void {
  try {
    if (processGroup && process.platform !== 'win32' && child.pid) {
      process.kill(-child.pid, signal);
      return;
    }
    if (!child.kill(signal) && !hasExited(child)) {
      throw new Error(`failed to send ${signal} to child process`);
    }
  } catch (error) {
    if (hasExited(child) || isMissingProcess(error)) return;
    throw error;
  }
}

export async function terminateChild(
  child: ChildProcess,
  options: TerminationOptions = {}
): Promise<{ forced: boolean }> {
  if (hasExited(child)) return { forced: false };

  const processGroup = options.killProcessGroup === true;
  signalChild(child, 'SIGTERM', processGroup);
  if (await waitForChildExit(child, options.termTimeoutMs ?? 1000)) {
    return { forced: false };
  }

  signalChild(child, 'SIGKILL', processGroup);
  if (!(await waitForChildExit(child, options.killTimeoutMs ?? 2000))) {
    throw new Error('child process did not exit after SIGKILL');
  }
  return { forced: true };
}
