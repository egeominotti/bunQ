/**
 * Bun-only entry stub.
 *
 * bunqueue is built for the Bun runtime: its TCP transport, persistence and
 * client rely on Bun globals (Bun.connect, Bun.file, Bun.write, Bun.hash, ...)
 * with no Node fallback, and the published ESM uses directory/extensionless
 * relative specifiers that Node's ESM resolver rejects (ERR_UNSUPPORTED_DIR_IMPORT).
 *
 * The package.json `exports` map points the `"node"` condition at this single,
 * self-contained file so a Node import fails fast with a clear, actionable error
 * instead of a cryptic resolver crash or a deep `Bun is not defined`. Bun resolves
 * the real entry via the higher-priority `"bun"` condition and never loads this.
 *
 * Keep this file free of relative imports so it stays resolvable on its own.
 */

throw new Error(
  'bunqueue is Bun-only and requires the Bun runtime (https://bun.sh). ' +
    'Node.js is not supported: install Bun and run your program with `bun`. ' +
    'See https://github.com/egeominotti/bunqueue#requirements'
);
