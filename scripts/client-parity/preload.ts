/**
 * Run unchanged native documentation contracts against the published package.
 * Use only in an isolated `bun test --preload ...` process: the only substitutions
 * are module entry points, and both the TCP broker and embedded engine stay real.
 */
import { mock } from 'bun:test';
import { resolve } from 'node:path';

const root = resolve(import.meta.dir, '../..');
const portable = await import(resolve(root, 'sdk/typescript/dist/index.js'));
const embedded = await import(resolve(root, 'sdk/typescript/dist/embedded.js'));
if (typeof portable.Queue !== 'function' || typeof embedded.getSharedManager !== 'function') {
  throw new Error('Build the portable client before running its shared contracts');
}

await mock.module(resolve(root, 'src/client/index.ts'), () => portable);
await mock.module(resolve(root, 'src/client/manager.ts'), () => ({
  getSharedManager: embedded.getSharedManager,
  peekSharedManager: embedded.peekSharedManager,
  shutdownManager: embedded.shutdownManager,
}));

const redirected = await import(resolve(root, 'src/client'));
if (redirected.Queue !== portable.Queue || redirected.Worker !== portable.Worker) {
  throw new Error('Shared contracts did not resolve to the published portable client');
}
