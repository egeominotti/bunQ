/**
 * Repro: #93 — `bunqueue/client` is unusable from Node despite engines.node >=18.
 *
 * Two independent facts on 2.8.3:
 *  (1) The published ESM uses directory/extensionless relative specifiers
 *      (e.g. src/client/index.ts:22 `export { defineConfig } from '../config'`).
 *      tsc with moduleResolution:"bundler" emits those verbatim, and Node's ESM
 *      resolver rejects them with ERR_UNSUPPORTED_DIR_IMPORT at resolution time
 *      (before any module body runs, so a runtime guard cannot help).
 *  (2) The TCP transport uses Bun-only globals (Bun.connect at
 *      src/client/tcp/connection.ts) with no node:net fallback.
 *
 * Decision: bunqueue is Bun-only. So instead of shimming Node, we make the
 * contract honest and the failure actionable:
 *  - drop `node` from package.json `engines`;
 *  - add a `"bun"` export condition (the real client) and a `"node"` export
 *    condition pointing at a single self-contained stub (src/bun-only.ts ->
 *    dist/bun-only.js) that throws a clear "Bun is required" error. Node's ESM
 *    resolver short-circuits to the stub and throws the clear message instead of
 *    the cryptic ERR_UNSUPPORTED_DIR_IMPORT; Bun keeps resolving the real client.
 *
 * These assertions encode that fixed contract. They are RED before the fix
 * (engines still has `node`, no `node`/`bun` conditions, no stub) and GREEN after.
 */

import { describe, test, expect } from 'bun:test';
import {
  existsSync,
  readFileSync,
  mkdtempSync,
  mkdirSync,
  symlinkSync,
  writeFileSync,
  rmSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve, dirname } from 'node:path';

const ROOT = resolve(import.meta.dir, '..');
const pkg = JSON.parse(readFileSync(join(ROOT, 'package.json'), 'utf8')) as {
  engines?: Record<string, string>;
  exports: Record<string, Record<string, string> | string>;
};

// Every subpath that re-exports tsc-compiled (dir-import) code must short-circuit
// Node to the throwing stub. './package.json' is a plain string and is exempt.
const STUB = './dist/bun-only.js';
const SUBPATHS = ['.', './client', './queue', './mcp', './workflow'] as const;
const REAL: Record<(typeof SUBPATHS)[number], string> = {
  '.': './dist/main.js',
  './client': './dist/client/index.js',
  './queue': './dist/application/queueManager.js',
  './mcp': './dist/mcp/index.js',
  './workflow': './dist/client/workflow/index.js',
};

describe('#93 Bun-only contract', () => {
  test('engines no longer claims Node support', () => {
    expect(pkg.engines?.node).toBeUndefined();
    expect(pkg.engines?.bun).toBeDefined();
  });

  test('every code export has bun(real) + node(stub), with bun resolved before node', () => {
    for (const sub of SUBPATHS) {
      const cond = pkg.exports[sub];
      expect(typeof cond, `${sub} must be a conditions object`).toBe('object');
      const c = cond as Record<string, string>;
      expect(c.bun, `${sub} bun condition`).toBe(REAL[sub]);
      expect(c.node, `${sub} node condition`).toBe(STUB);
      // Bun honors the first matching condition: `bun` must precede `node`,
      // otherwise Bun would resolve the throwing stub too.
      const keys = Object.keys(c);
      expect(keys.indexOf('bun')).toBeLessThan(keys.indexOf('node'));
      // A non-Bun bundler (browser target, no `node` condition) still gets the
      // real client via import/default — it tolerates extensionless specifiers.
      expect(c.import ?? c.default).toBe(REAL[sub]);
    }
  });

  test('stub source exists, is self-contained (no relative imports), and throws a clear Bun-required error', () => {
    const stubSrc = join(ROOT, 'src', 'bun-only.ts');
    expect(existsSync(stubSrc), 'src/bun-only.ts must exist').toBe(true);
    const src = readFileSync(stubSrc, 'utf8');
    // Must be Node-resolvable on its own: no relative dir/extensionless imports.
    expect(src).not.toMatch(/from\s+['"]\.\.?\//);
    expect(src).not.toMatch(/import\s+['"]\.\.?\//);
    // Must throw and name Bun + bun.sh so the error is actionable.
    expect(src).toMatch(/throw\s+new\s+Error/);
    expect(src).toMatch(/[Bb]un/);
    expect(src).toMatch(/bun\.sh/);
  });

  // End-to-end proof: a real `node` process importing `bunqueue/client` resolves
  // the `node` condition -> stub -> clear throw, NOT ERR_UNSUPPORTED_DIR_IMPORT.
  // The stub is a build artifact, so build it when it is missing rather than
  // skipping. Skipping made this test's coverage depend on whether some other
  // file (e.g. the packed-tarball consumer test, which runs `build:lib` as a
  // side effect) happened to run first — and Bun discovers test files in
  // readdir order, so that differed between macOS and Linux.
  if (!existsSync(join(ROOT, 'dist', 'bun-only.js'))) {
    const build = Bun.spawnSync([process.execPath, 'run', 'build:lib'], {
      cwd: ROOT,
      stdout: 'pipe',
      stderr: 'pipe',
    });
    if (build.exitCode !== 0) {
      throw new Error(
        `build:lib failed with exit ${build.exitCode}:\n${build.stdout.toString()}${build.stderr.toString()}`
      );
    }
  }

  // The remaining gate is the toolchain, which is deterministic per environment
  // rather than dependent on test-file order. It must be a REAL Node: the Bun
  // image ships `/usr/local/bun-node-fallback-bin/node`, a shim that runs Bun.
  // Under that shim the import resolves the `bun` condition, loads the real
  // client and exits 0 — so an unguarded spawn asserts the opposite of the
  // contract while looking green.
  // `Bun.spawnSync` THROWS on a missing binary instead of returning a non-zero
  // exit code, and an uncaught throw in a describe body aborts the whole FILE
  // (0 tests run, 1 error) — it would take the three Node-independent contract
  // assertions with it. A node-less environment is not hypothetical: the
  // canonical release Machines are stock Ubuntu 24.04 / Debian 13 with only Bun
  // installed, and Bun's installer ships no `node` shim.
  let realNode = false;
  try {
    const nodeProbe = Bun.spawnSync(
      ['node', '-e', 'process.stdout.write(process.versions.bun ? "bun" : "node")'],
      { stdout: 'pipe', stderr: 'pipe' }
    );
    realNode = nodeProbe.exitCode === 0 && nodeProbe.stdout.toString() === 'node';
  } catch {
    realNode = false;
  }
  if (!realNode) {
    console.warn('[#93] e2e node-spawn skipped: no real Node on PATH (Bun shim or absent)');
  }

  test.skipIf(!realNode)(
    'node import of bunqueue/client throws the clear Bun-required message',
    async () => {
      expect(existsSync(join(ROOT, 'dist', 'bun-only.js'))).toBe(true);
      const consumer = mkdtempSync(join(tmpdir(), 'bq93-'));
      try {
        mkdirSync(join(consumer, 'node_modules'), { recursive: true });
        symlinkSync(ROOT, join(consumer, 'node_modules', 'bunqueue'), 'dir');
        writeFileSync(
          join(consumer, 'package.json'),
          JSON.stringify({ name: 'c', type: 'module' })
        );
        const repro = join(consumer, 'repro.mjs');
        writeFileSync(repro, `await import('bunqueue/client');\n`);

        const proc = Bun.spawnSync(['node', repro], {
          cwd: consumer,
          stderr: 'pipe',
          stdout: 'pipe',
        });
        const stderr = proc.stderr.toString();
        expect(proc.exitCode).not.toBe(0);
        expect(stderr).not.toContain('ERR_UNSUPPORTED_DIR_IMPORT');
        expect(stderr).toMatch(/bun\.sh/);
      } finally {
        rmSync(consumer, { recursive: true, force: true });
      }
    }
  );
});
