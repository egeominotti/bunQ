/**
 * Bun-only runtime guard (defense-in-depth for the bundled path).
 *
 * The `"node"` export condition (-> bun-only.ts) already fails fast for a plain
 * `node` import and for node-target bundlers. This guard covers the remaining
 * case: a browser/neutral-target bundler that inlines the real client, whose
 * output is then run on Node. Without it, the failure surfaces deep inside as a
 * cryptic `Bun is not defined` / `Bun.connect is not a function`.
 *
 * Imported FIRST by the client entrypoints so it evaluates before any module
 * that touches `Bun.*` at top level. No-op under Bun. Keep relative-import-free.
 */

if (typeof (globalThis as { Bun?: unknown }).Bun === 'undefined') {
  throw new Error(
    'bunqueue is Bun-only and requires the Bun runtime (https://bun.sh). ' +
      'Node.js is not supported: install Bun and run your program with `bun`.'
  );
}

export {};
