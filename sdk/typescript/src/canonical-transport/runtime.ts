/** Runtime primitives only: job and scheduling logic stays in the canonical client. */
import { access, unlink } from 'node:fs/promises';
import { createHash, randomBytes } from 'node:crypto';
import { availableParallelism } from 'node:os';
export { ThreadWorker } from './thread-worker.js';

export const hardwareConcurrency = (() => {
  try {
    // The embedded engine sizes shards from this exact runtime value. Host
    // CPU inventory can exceed the quota visible to Bun inside a container.
    return globalThis.navigator
      ? globalThis.navigator.hardwareConcurrency || 4
      : availableParallelism() || 4;
  } catch {
    return 4;
  }
})();
export const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

/** Pool identities need a stable token fingerprint, never the token itself. */
export function hash(value: string): number {
  return createHash('sha256').update(value).digest().readUInt32BE(0);
}

/** RFC 9562 UUIDv7, retaining the canonical public ID format. */
export function uuid(): string {
  const bytes = randomBytes(16);
  bytes.writeUIntBE(Date.now(), 0, 6);
  bytes[6] = 0x70 | (bytes[6] & 0x0f);
  bytes[8] = 0x80 | (bytes[8] & 0x3f);
  const hex = bytes.toString('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

export function file(path: string): { exists(): Promise<boolean>; delete(): Promise<void> } {
  return {
    async exists() {
      try {
        await access(path);
        return true;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === 'ENOENT') return false;
        throw error;
      }
    },
    delete: () => unlink(path),
  };
}
