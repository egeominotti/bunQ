/** Load the same embedded engine under Bun without loading bun:sqlite under Node. */
import type * as Backend from '../../../../scripts/client-portable/embedded-entry.js';

const backendPath = './embedded.js';
const backend: typeof Backend | null = process.versions.bun
  ? ((await import(backendPath)) as typeof Backend)
  : null;

export function getSharedManager(dataPath?: string): ReturnType<typeof Backend.getSharedManager> {
  if (!backend)
    throw new Error('Embedded mode requires Bun; use a TCP connection in this runtime.');
  return backend.getSharedManager(dataPath);
}

export function shutdownManager(): void {
  backend?.shutdownManager();
}

export function peekSharedManager(): ReturnType<typeof Backend.peekSharedManager> {
  return backend?.peekSharedManager() ?? null;
}

export function embeddedDlq(): typeof Backend.dlq {
  if (!backend)
    throw new Error('Embedded mode requires Bun; use a TCP connection in this runtime.');
  return backend.dlq;
}
